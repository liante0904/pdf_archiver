import hashlib
import re
import unicodedata
import html
import asyncio
import ssl
import aiohttp
from difflib import SequenceMatcher
from urllib.parse import quote, urlparse, urlunparse, unquote, urljoin

_FIRMS_NEEDING_COOKIE_SESSION = {"대신증권", "IBK투자증권", "삼성증권", "다올투자증권", "교보증권"}

def _truncate(value, limit=160):
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."

def _normalize_pdf_url_value(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None

def _normalize_pdf_url_sql(column_name):
    return f"NULLIF(BTRIM({column_name}), '')"

def _pdf_hash_bytes(data: bytes):
    if not data:
        return None
    return hashlib.sha256(data).digest()

def _pdf_signature_offset(data: bytes, max_scan=2048):
    if not data:
        return None
    head = data[:max_scan]
    for marker in (b"%PDF", b"\xef\xbb\xbf%PDF"):
        idx = head.find(marker)
        if idx != -1:
            return idx
    stripped = head.lstrip(b"\x00\t\r\n\x0c\x20")
    if stripped.startswith(b"%PDF"):
        return len(head) - len(stripped)
    return None

def _is_pdf_payload(data: bytes):
    """%PDF 시그니처 확인. Fasoo DRM 등 암호화 PDF 대응을 위해
    200KB 이상 파일은 시그니처가 없어도 유효한 PDF로 간주."""
    if _pdf_signature_offset(data) is not None:
        return True
    # DRM-encrypted PDF fallback (Fasoo DRM 등은 %PDF 시그니처가 없음)
    if len(data) > 200 * 1024:
        return True
    return False

def _report_prefix(firm, title, report_id, reg_dt=None):
    return f"[{firm} | {title} | report_id={report_id}" + (f" | reg_dt={reg_dt}]" if reg_dt else "]")

def _download_sources_for_firm(key_url, pdf_url, tel_url, dw_url):
    sources = [u for u in (pdf_url, tel_url, dw_url, key_url) if u and str(u).startswith("http")]
    return sources

def _browser_like_headers(referer=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.8,*/*;q=0.7",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer
    return headers

def _cookie_header_from_response(response):
    raw_values = []
    headers = getattr(response, "headers", None)
    if headers is None:
        return ""
    if hasattr(headers, "getall"):
        raw_values.extend(headers.getall("Set-Cookie", []))
    else:
        fallback = headers.get("set-cookie")
        if fallback:
            raw_values.append(fallback)

    cookies = []
    seen = set()
    for value in raw_values:
        for cookie in str(value).split(","):
            part = cookie.split(";", 1)[0].strip()
            if part and part not in seen:
                seen.add(part)
                cookies.append(part)
    return "; ".join(cookies)

def safe_encode_url(url):
    try:
        current = url
        prev = None
        while prev != current:
            prev = current
            current = unquote(current)
        parts = urlparse(current)
        return urlunparse((
            parts.scheme, parts.netloc,
            quote(parts.path, safe='/:@'),
            parts.params,
            quote(parts.query, safe='&='),
            parts.fragment
        ))
    except Exception:
        return url

def _encode_url_euc_kr(url):
    """safe_encode_url 의 EUC-KR 버전."""
    try:
        current = url
        prev = None
        while prev != current:
            prev = current
            current = unquote(current)
        parts = urlparse(current)
        return urlunparse((
            parts.scheme, parts.netloc,
            quote(parts.path, safe='/:@', encoding='euc-kr'),
            parts.params,
            quote(parts.query, safe='&=', encoding='euc-kr'),
            parts.fragment,
        ))
    except Exception:
        return url

def _has_korean(text):
    """문자열에 한글(가-힣)이 포함되어 있는지"""
    if not text:
        return False
    return bool(re.search(r'[가-힣]', text))

def _origin_referer(url):
    try:
        parts = urlparse(url)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}/"
    except Exception:
        pass
    return url

def _firm_base_domain(url: str) -> str | None:
    """URL의 scheme + netloc (도메인 루트) 반환"""
    try:
        parts = urlparse(url)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}/"
    except Exception:
        pass
    return None

def _get_first_url(pdf_url, key_url, tel_url, dw_url, candidates):
    """우선순위: pdf_url > candidates[0] > key_url > tel_url > dw_url"""
    for u in (pdf_url,):
        if u:
            return u
    if candidates:
        return candidates[0]
    for u in (key_url, tel_url, dw_url):
        if u:
            return u
    return None

async def _ensure_session_cookies_aiohttp(firm: str, target_url: str) -> str:
    """aiohttp 로 해당 증권사 도메인에 방문하여 세션 쿠키 문자열을 획득한다.

    wget --save-cookies 대신 aiohttp로 직접 방문하여 Set-Cookie 헤더를 추출.
    JSESSIONID 등 세션 쿠키가 설정되면 "key=value; key=value" 형태로 반환.

    대상: 대신증권, IBK투자증권, 삼성증권, 다올투자증권
    """
    if firm not in _FIRMS_NEEDING_COOKIE_SESSION:
        return ""

    domain = _firm_base_domain(target_url)
    if not domain:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
        "Referer": domain,
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    # 방문할 URL 목록: 도메인 루트, 경로만(쿼리 제외)
    visit_urls = [domain]
    path_only = target_url.split("?")[0] if "?" in target_url else target_url
    if path_only != domain:
        visit_urls.append(path_only)

    collected_cookies = {}
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector) as session:
        for visit_url in visit_urls:
            try:
                async with session.get(
                    visit_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                    allow_redirects=True,
                ) as resp:
                    await resp.read()
                    # Set-Cookie 헤더 추출
                    for raw_cookie in resp.headers.getall("Set-Cookie", []):
                        for part in str(raw_cookie).split(","):
                            cookie_entry = part.split(";", 1)[0].strip()
                            if "=" in cookie_entry:
                                key, val = cookie_entry.split("=", 1)
                                collected_cookies[key.strip()] = val.strip()
            except Exception:
                continue
            if collected_cookies:
                break

    if not collected_cookies:
        return ""

    return "; ".join(f"{k}={v}" for k, v in collected_cookies.items())

def _decode_mirae_html(body):
    if isinstance(body, str):
        return body
    for encoding in ("euc-kr", "cp949", "utf-8"):
        try:
            return body.decode(encoding)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="ignore")

def _normalize_match_text(value):
    normalized = unicodedata.normalize("NFC", html.unescape(value or "")).lower()
    return re.sub(r"[^0-9a-z가-힣]+", "", normalized)

def _find_mirae_board_download_url(board_body, title, reg_dt):
    board_html = _decode_mirae_html(board_body)
    target = _normalize_match_text(title)
    if not target:
        return None

    code_match = re.search(r"\((\d{6})", title or "")
    stock_code = code_match.group(1) if code_match else ""
    date_key = re.sub(r"[^0-9]", "", str(reg_dt or ""))[:8]
    best_url = None
    best_score = 0.0
    first_token = re.split(r"[\s(/\[]+", title or "", maxsplit=1)[0]
    normalized_first_token = _normalize_match_text(first_token)

    for match in re.finditer(
        r"downConfirm\('([^']+)'[^)]*\)[^>]*title=\"([^\"]*)\"",
        board_html,
        flags=re.S,
    ):
        url = html.unescape(match.group(1))
        attachment_title = html.unescape(match.group(2))
        row_start = board_html.rfind("<tr", 0, match.start())
        context_start = row_start if row_start >= 0 else max(0, match.start() - 900)
        context = re.sub(r"<[^>]+>", " ", board_html[context_start:match.start()])
        haystack = _normalize_match_text(context + " " + attachment_title)
        if stock_code and stock_code not in haystack:
            continue
        if not stock_code and normalized_first_token and normalized_first_token not in haystack:
            continue

        score = SequenceMatcher(None, target, haystack).ratio()
        if stock_code and stock_code in haystack:
            score += 0.35
        if date_key and date_key in haystack:
            score += 0.20
        if normalized_first_token and normalized_first_token in haystack:
            score += 0.20
        if score > best_score:
            best_score = score
            best_url = url

    return best_url if best_score >= 0.45 else None

def extract_dbfi_retry_candidates(body_text, base_url):
    candidates = []
    if not body_text:
        return candidates

    for pattern in (
        r'streamdocs/v4/documents/([A-Za-z0-9_\-]+)',
        r'href="([^"]+\.pdf[^"]*)"',
        r'data-url="([^"]+)"',
        r'url="([^"]+)"',
    ):
        for match in re.findall(pattern, body_text, flags=re.I):
            candidate = match if isinstance(match, str) else match[0]
            if candidate:
                candidates.append(urljoin(base_url, candidate))

    deduped = []
    seen = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)
    return deduped

def build_candidate_urls(firm, urls):
    candidates = list(urls)
    if firm == "IBK투자증권" and urls:
        base = urls[0]
        for path in ("invrespect", "invreport", "indreport", "comment"):
            alt = re.sub(r'(tradeinfo/)[^/]+(/)', rf'\1{path}\2', base)
            if alt not in candidates:
                candidates.append(alt)
    if firm == "유안타증권":
        for u in list(urls):
            if "ATTACH_FILE=" in u:
                seq = u.split("ATTACH_FILE=")[-1]
                alt = f"http://file.myasset.com/sitemanager/upload/{seq}"
                if alt not in candidates: candidates.append(alt)
    
    final = []
    seen = set()
    for u in candidates:
        encoded = safe_encode_url(u)
        variants = [u, encoded]
        # 한글이 포함된 URL은 EUC-KR percent-encoding variant 도 추가
        if _has_korean(u):
            euc_encoded = _encode_url_euc_kr(u)
            if euc_encoded and euc_encoded not in variants:
                variants.append(euc_encoded)
        for variant in variants:
            if variant not in seen:
                seen.add(variant)
                final.append(variant)
    return final

async def get_pdf_page_count(file_path):
    try:
        proc = await asyncio.create_subprocess_shell(
            f"grep -a /Count {file_path} | head -n 10 | grep -oE '/Count [0-9]+' | head -n 1 | grep -oE '[0-9]+'",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await proc.communicate()
        return int(stdout.decode().strip()) if stdout.strip() else 0
    except Exception:
        return 0
