# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiohttp",
#     "asyncpg",
#     "aiohttp-socks",
# ]
# ///

import asyncio
import aiohttp
import hashlib
import ssl
import os
import signal
import time
import logging
import fcntl
import sys
import re
import shutil
import unicodedata
import json
import html
import tempfile
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse, unquote, urljoin

try:
    import asyncpg
except ModuleNotFoundError:  # pragma: no cover - exercised only when postgres deps are absent locally
    asyncpg = None

try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None

from db_tables import PDF_ARCHIVE_TABLE, SOURCE_REPORTS_TABLE
from secret_env import load_workspace_secret_env_defaults

def _load_secret_env_defaults():
    """Load workspace-local secret defaults without overriding explicit env."""
    load_workspace_secret_env_defaults()


_load_secret_env_defaults()


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}

# --- 설정 (Config) ---
class Config:
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "ssh_reports_hub")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "ssh_reports_hub")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

    SOURCE_TABLE = SOURCE_REPORTS_TABLE
    META_TABLE = PDF_ARCHIVE_TABLE
    PDF_STATUS_COL = "pdf_sync_status"
    LEGACY_STATUS_COL = "sync_status"
    PDF_HASH_COL = "pdf_hash"

    LOCAL_BUFFER_DIR = os.getenv("LOCAL_BUFFER_DIR", os.path.expanduser("~/downloads/pdf_archive_temp"))
    RCLONE_BIN = (
        os.getenv("RCLONE_BIN")
        or (os.path.expanduser("~/.local/bin/rclone") if os.path.exists(os.path.expanduser("~/.local/bin/rclone")) else None)
        or shutil.which("rclone")
        or "/usr/bin/rclone"
    )
    RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf")
    RCLONE_CONFIG = os.getenv("RCLONE_CONFIG", os.path.expanduser("~/.config/rclone/rclone.conf"))
    LOCK_FILE = "/tmp/pdf_archiver_async.lock"
    ZOMBIE_TIMEOUT_SECONDS = int(os.getenv("PDF_ARCHIVER_ZOMBIE_TIMEOUT", "600"))

    BATCH_SIZE = 10
    DOWNLOAD_CONCURRENCY = 10
    FETCH_RETRY_LIMIT = int(os.getenv("FETCH_RETRY_LIMIT", "8"))
    FETCH_ONLY = _env_flag("PDF_ARCHIVER_FETCH_ONLY")
    RCLONE_TRANSFERS = int(os.getenv("RCLONE_TRANSFERS", "8"))
    RCLONE_CHECKERS = int(os.getenv("RCLONE_CHECKERS", "16"))
    RCLONE_RETRIES = int(os.getenv("RCLONE_RETRIES", "3"))
    RCLONE_LOW_LEVEL_RETRIES = int(os.getenv("RCLONE_LOW_LEVEL_RETRIES", "10"))
    ONEDRIVE_CHUNK_SIZE = os.getenv("ONEDRIVE_CHUNK_SIZE", "64000k")
    DBFI_DOWNLOAD_CONCURRENCY = int(os.getenv("DBFI_DOWNLOAD_CONCURRENCY", "1"))
    DBFI_REQUEST_DELAY_SECONDS = float(os.getenv("DBFI_REQUEST_DELAY_SECONDS", "2"))

    EXCLUDED_FIRMS = ('미래에셋증권', '유진투자증권', '상상인증권', 'BNK투자증권')
    DBFI_FIRM_ORDER = 19
    WARP_PROXY = os.getenv("WARP_PROXY", "127.0.0.1:9091")
    LOG_FILE = os.getenv("LOG_FILE", os.path.expanduser("~/logs/pdf_archiver_async.log"))

# 로깅 설정
os.makedirs(os.path.dirname(Config.LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(Config.LOG_FILE), logging.StreamHandler(sys.stdout)]
)

LOCAL_BUFFER_DIR = Config.LOCAL_BUFFER_DIR

# --- DB 싱글톤 관리 (DBManager) ---
class DBManager:
    _conn = None

    @classmethod
    async def get_conn(cls):
        if asyncpg is None:
            raise RuntimeError("asyncpg is required")
        if cls._conn is None or cls._conn.is_closed():
            print(f"DEBUG: Connecting to {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB} as {Config.POSTGRES_USER}")
            if Config.POSTGRES_URL:
                cls._conn = await asyncpg.connect(Config.POSTGRES_URL)
            else:
                cls._conn = await asyncpg.connect(
                    host=Config.POSTGRES_HOST,
                    port=Config.POSTGRES_PORT,
                    database=Config.POSTGRES_DB,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                )
        return cls._conn

    @classmethod
    async def close(cls):
        if cls._conn and not cls._conn.is_closed():
            await cls._conn.close()
            cls._conn = None

async def get_db_connection():
    return await DBManager.get_conn()


async def _table_has_column(conn, table_name, column_name):
    schema_table = table_name.strip('"')
    row = await conn.fetchrow(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = $1
          AND column_name = $2
        LIMIT 1
        """,
        schema_table,
        column_name,
    )
    return row is not None


async def ensure_pdf_sync_status_schema(conn):
    """Keep pdf_sync_status available and backfill from legacy sync_status once."""
    for table_name in (Config.SOURCE_TABLE, Config.META_TABLE):
        for legacy_attach_name, quoted in (("attach_url", False), ("attach_url", True)):
            if await _table_has_column(conn, table_name, legacy_attach_name):
                drop_name = legacy_attach_name
                await conn.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {drop_name}")

        pdf_status_existed = await _table_has_column(conn, table_name, Config.PDF_STATUS_COL)
        if not pdf_status_existed:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {Config.PDF_STATUS_COL} INTEGER DEFAULT 0")

        if not await _table_has_column(conn, table_name, Config.PDF_HASH_COL):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {Config.PDF_HASH_COL} BYTEA")

        if table_name == Config.META_TABLE:
            for column_name, column_sql in (
                ("title", "TEXT"),
                ("author", "TEXT"),
                ("has_text", "BOOLEAN"),
                ("is_encrypted", "BOOLEAN"),
                ("storage_backend", "TEXT DEFAULT 'onedrive'"),
                ("storage_key", "TEXT"),
                ("last_accessed_at", "TIMESTAMPTZ"),
            ):
                if not await _table_has_column(conn, table_name, column_name):
                    await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
            if not await _table_has_column(conn, table_name, "created_at"):
                await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW()")
            if not await _table_has_column(conn, table_name, "updated_at"):
                await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()")

        legacy_exists = await _table_has_column(conn, table_name, Config.LEGACY_STATUS_COL)
        if table_name == Config.META_TABLE and not legacy_exists:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {Config.LEGACY_STATUS_COL} INTEGER DEFAULT 0")
            legacy_exists = True

        if table_name == Config.META_TABLE and not await _table_has_column(conn, table_name, "retry_count"):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN retry_count INTEGER DEFAULT 0")

        if not pdf_status_existed and legacy_exists:
            await conn.execute(
                f"""
                UPDATE {table_name}
                SET {Config.PDF_STATUS_COL} = COALESCE({Config.LEGACY_STATUS_COL}, 0)
                WHERE {Config.PDF_STATUS_COL} IS NULL
                   OR {Config.PDF_STATUS_COL} != COALESCE({Config.LEGACY_STATUS_COL}, 0)
                """
            )
        else:
            await conn.execute(
                f"""
                UPDATE {table_name}
                SET {Config.PDF_STATUS_COL} = COALESCE({Config.PDF_STATUS_COL}, 0)
                WHERE {Config.PDF_STATUS_COL} IS NULL
                """
            )


def _row_payload(row):
    return {
        "row_id": row[0],
        "report_id": row[1],
        "sec_firm_order": row[2],
        "key": row[3],
        "pdf_url": row[4],
        "telegram_url": row[5],
        "download_url": row[6],
        "firm_nm": row[7],
        "title": row[8],
        "reg_dt": row[9],
    }


class WorkflowRecord(dict):
    ORDER = (
        "row_id",
        "report_id",
        "sec_firm_order",
        "key",
        "pdf_url",
        "telegram_url",
        "download_url",
        "firm_nm",
        "title",
        "reg_dt",
        "pdf_hash",
    )

    def __getitem__(self, key):
        if isinstance(key, int):
            return tuple(dict.__getitem__(self, field) for field in self.ORDER)[key]
        return super().__getitem__(key)


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


async def download_ds_pdf(source_url, target_path, title, report_id, firm, reg_dt, referer_hint=None):
    if not source_url:
        return False

    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            # 1. board_url 정제 (download.php -> board.php, 불필요한 인자 제거)
            board_url = source_url.replace("download.php", "board.php")
            if "&no=" in board_url:
                board_url = board_url.split("&no=")[0]
            
            base_headers = _browser_like_headers()
            
            # 2. 게시판 뷰 페이지 방문하여 세션 쿠키 획득
            async with session.get(board_url, headers=base_headers, allow_redirects=True) as board_response:
                await board_response.read()
                cookies = _cookie_header_from_response(board_response)

            # 3. PDF 다운로드 요청 (레퍼러를 다운로드 URL 자체로 설정)
            download_headers = _browser_like_headers(referer=source_url)
            if cookies:
                download_headers["Cookie"] = cookies

            tmp_path = target_path.with_suffix(".tmp")
            async with session.get(source_url, headers=download_headers, allow_redirects=True) as response:
                body = await response.read()
                content_type = response.headers.get("content-type", "")
                
                # DS는 파일이 없으면 HTML 에러 페이지를 반환함
                if response.status != 200 or "text/html" in content_type.lower() or len(body) < 5000:
                    return False

                if not _is_pdf_payload(body):
                    return False

                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.write_bytes(body)

            if target_path.exists():
                target_path.unlink()
            tmp_path.rename(target_path)
            pages = await get_pdf_page_count(target_path)
            return {
                "report_id": report_id, "firm": firm, "title": title, "path": target_path,
                "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
                "pdf_hash": _pdf_hash_bytes(body),
            }
        except Exception:
            return False
        finally:
            tmp_path = target_path.with_suffix(".tmp")
            if tmp_path.exists(): tmp_path.unlink(missing_ok=True)

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
    """safe_encode_url 의 EUC-KR 버전.
    iprovest.com(교보증권), myasset.com(유안타증권), imfnsec.com(IM증권) 등
    EUC-KR percent-encoding 을 기대하는 서버용."""
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


# --- 4개 증권사 wget 쿠키/Referer 처리 (대신증권, IBK투자증권, 삼성증권, 다올투자증권) ---
_FIRMS_NEEDING_COOKIE_SESSION = {"대신증권", "IBK투자증권", "삼성증권", "다올투자증권", "교보증권"}

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


async def download_mirae_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    if not candidates:
        return False

    tmp_path = target_path.with_suffix(".tmp")
    list_url = "https://securities.miraeasset.com/bbs/board/message/list.do?categoryId=1521"
    
    # 1. 후보 URL 목록 구성
    candidate_urls = list(candidates)
    
    # 2. wget을 이용한 순차 다운로드 시도
    body = None
    attempted_urls = []
    
    for candidate_url in candidate_urls:
        try:
            # 사용자가 wget이 바로 된다고 했으므로, wget을 사용하여 다운로드 시도
            cmd = [
                "wget", "-q", "-O", str(tmp_path),
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "--timeout=20", "--tries=2",
                "--no-check-certificate",
                candidate_url
            ]
            
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await proc.communicate()
            
            if proc.returncode == 0 and tmp_path.exists() and tmp_path.stat().st_size > 1000:
                candidate_body = tmp_path.read_bytes()
                if _is_pdf_payload(candidate_body):
                    body = candidate_body
                    break
            
            size = tmp_path.stat().st_size if tmp_path.exists() else 0
            attempted_urls.append((_truncate(candidate_url), proc.returncode, size))
            
        except Exception as e:
            attempted_urls.append((_truncate(candidate_url), "EXC", str(e)))
        finally:
            if tmp_path.exists() and body is None:
                tmp_path.unlink(missing_ok=True)

    # 3. Fallback: 실패 시 게시판 목록에서 URL을 다시 찾아 시도
    if body is None:
        try:
            board_tmp = tmp_path.with_suffix(".html")
            cmd = [
                "wget", "-q", "-O", str(board_tmp),
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "--timeout=15", "--tries=1",
                list_url
            ]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            
            if proc.returncode == 0 and board_tmp.exists():
                board_html = board_tmp.read_bytes()
                fallback_url = _find_mirae_board_download_url(board_html, title, reg_dt)
                if fallback_url and fallback_url not in candidate_urls:
                    cmd = [
                        "wget", "-q", "-O", str(tmp_path),
                        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                        "--timeout=30", "--tries=2",
                        fallback_url
                    ]
                    proc = await asyncio.create_subprocess_exec(*cmd)
                    await proc.wait()
                    if proc.returncode == 0 and tmp_path.exists() and tmp_path.stat().st_size > 1000:
                        candidate_body = tmp_path.read_bytes()
                        if _is_pdf_payload(candidate_body):
                            body = candidate_body
            if board_tmp.exists(): board_tmp.unlink()
        except Exception as e:
            logging.warning(f"Mirae: board fallback failed: {e}")

    if body is None:
        logging.warning(
            "%s Mirae: download failed attempts=%s",
            _report_prefix(firm, title, report_id, reg_dt),
            attempted_urls,
        )
        return False

    if target_path.exists():
        target_path.unlink()
    tmp_path.rename(target_path)
    pages = await get_pdf_page_count(target_path)
    
    return {
        "report_id": report_id, "firm": firm, "title": title, "path": target_path,
        "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
        "pdf_hash": _pdf_hash_bytes(body),
    }

async def download_kyobo_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    """교보증권 (iprovest.com) 전용 다운로드.
    게시판 뷰 페이지(board.php)에 접속하여 실제 PDF 다운로드 URL을 추출한 후 다운로드한다.
    """
    tmp_path = target_path.with_suffix(".tmp")
    # 1. 후보 URL 중 board.php 형태 찾기
    board_url = None
    for u in candidates:
        if "board.php" in u:
            board_url = u
            break
    if not board_url:
        # board.php 가 없으면 candidates 중 첫 번째 URL을 board.php 로 변환 시도
        for u in candidates:
            if "download.php" in u:
                board_url = u.replace("download.php", "board.php")
                if "&no=" in board_url:
                    board_url = board_url.split("&no=")[0]
                break
    if not board_url:
        return None

    # 2. 게시판 뷰 페이지 방문하여 세션 쿠키 획득 및 실제 PDF URL 추출
    timeout = aiohttp.ClientTimeout(total=30)
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            headers = _browser_like_headers()
            async with session.get(board_url, headers=headers, allow_redirects=True) as resp:
                board_html = await resp.text()
                cookies = _cookie_header_from_response(resp)

            # 3. board_html 에서 실제 PDF URL 추출
            # 교보증권 게시판 패턴: <a href="download.php?filename=...&..."> 또는 onclick="down('...')"
            pdf_url = None
            # 패턴 1: download.php?filename=...
            m = re.search(r'href=["\']([^"\']*download\.php[^"\']*)["\']', board_html, re.I)
            if m:
                pdf_url = urljoin(board_url, m.group(1))
            # 패턴 2: onclick="down('...')"
            if not pdf_url:
                m = re.search(r"onclick\s*=\s*['\"]down\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", board_html, re.I)
                if m:
                    pdf_url = urljoin(board_url, m.group(1))
            # 패턴 3: data-url 속성
            if not pdf_url:
                m = re.search(r'data-url\s*=\s*["\']([^"\']+)["\']', board_html, re.I)
                if m:
                    pdf_url = urljoin(board_url, m.group(1))

            if not pdf_url:
                logging.warning(
                    "%s 교보증권: board page에서 PDF URL을 찾을 수 없음 board_url=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(board_url, 160),
                )
                return None

            # 4. PDF 다운로드
            download_headers = _browser_like_headers(referer=board_url)
            if cookies:
                download_headers["Cookie"] = cookies

            async with session.get(pdf_url, headers=download_headers, allow_redirects=True) as pdf_resp:
                body = await pdf_resp.read()
                content_type = pdf_resp.headers.get("content-type", "")
                if pdf_resp.status != 200 or "text/html" in content_type.lower() or len(body) < 5000:
                    return None
                if not _is_pdf_payload(body):
                    return None

                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.write_bytes(body)

            if target_path.exists():
                target_path.unlink()
            tmp_path.rename(target_path)
            pages = await get_pdf_page_count(target_path)
            return {
                "report_id": report_id, "firm": firm, "title": title, "path": target_path,
                "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
                "pdf_hash": _pdf_hash_bytes(body),
            }
        except Exception as e:
            logging.warning(
                "%s 교보증권 다운로드 예외: %s: %r",
                _report_prefix(firm, title, report_id, reg_dt),
                type(e).__name__, e,
            )
            return None
        finally:
            tmp_path = target_path.with_suffix(".tmp")
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)


async def download_hana_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    """하나증권 전용 다운로드.
    게시판 목록 페이지에서 현재 유효한 다운로드 URL을 찾아 다운로드한다.
    하나증권은 파일 구조 변경으로 과거 URL이 유효하지 않을 수 있으므로,
    게시판에서 실제 다운로드 링크를 다시 추출한다.
    """
    tmp_path = target_path.with_suffix(".tmp")
    # 1. 후보 URL 중 게시판 URL 찾기
    board_url = None
    for u in candidates:
        # 하나증권 게시판 패턴: /board/view/... 또는 /board/content/...
        if "/board/" in u:
            board_url = u
            break
    if not board_url:
        # candidates 중 첫 번째 URL을 게시판 URL로 가정
        board_url = candidates[0] if candidates else None
    if not board_url:
        return None

    timeout = aiohttp.ClientTimeout(total=30)
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            headers = _browser_like_headers()
            async with session.get(board_url, headers=headers, allow_redirects=True) as resp:
                raw_body = await resp.read()
                content_type = resp.headers.get("content-type", "").lower()
                cookies = _cookie_header_from_response(resp)

            # 만약 board_url 자체가 바로 PDF를 반환했다면 (UnicodeDecodeError 방지)
            if "application/pdf" in content_type or _is_pdf_payload(raw_body):
                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.write_bytes(raw_body)
                if target_path.exists():
                    target_path.unlink()
                tmp_path.rename(target_path)
                pages = await get_pdf_page_count(target_path)
                return {
                    "report_id": report_id, "firm": firm, "title": title, "path": target_path,
                    "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
                    "pdf_hash": _pdf_hash_bytes(raw_body),
                }

            # HTML인 경우 디코딩하여 게시판 파싱 진행
            try:
                board_html = raw_body.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    board_html = raw_body.decode("euc-kr")
                except UnicodeDecodeError:
                    board_html = raw_body.decode("utf-8", errors="ignore")

            # 2. board_html 에서 실제 PDF 다운로드 URL 추출
            pdf_url = None
            # 패턴 1: <a href="...download..."> 또는 <a href="...file...">
            for pattern in [
                r'href=["\']([^"\']*download[^"\']*)["\']',
                r'href=["\']([^"\']*\.pdf[^"\']*)["\']',
                r'href=["\']([^"\']*file[^"\']*)["\']',
                r'data-url=["\']([^"\']+)["\']',
            ]:
                m = re.search(pattern, board_html, re.I)
                if m:
                    pdf_url = urljoin(board_url, m.group(1))
                    break
            # 패턴 2: onclick="fnDownload('...')"
            if not pdf_url:
                m = re.search(r"onclick\s*=\s*['\"]fnDownload\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", board_html, re.I)
                if m:
                    pdf_url = urljoin(board_url, m.group(1))

            if not pdf_url:
                logging.warning(
                    "%s 하나증권: board page에서 PDF URL을 찾을 수 없음 board_url=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(board_url, 160),
                )
                return None

            # 3. PDF 다운로드
            download_headers = _browser_like_headers(referer=board_url)
            if cookies:
                download_headers["Cookie"] = cookies

            async with session.get(pdf_url, headers=download_headers, allow_redirects=True) as pdf_resp:
                body = await pdf_resp.read()
                content_type = pdf_resp.headers.get("content-type", "")
                if pdf_resp.status != 200 or "text/html" in content_type.lower() or len(body) < 5000:
                    return None
                if not _is_pdf_payload(body):
                    return None

                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.write_bytes(body)

            if target_path.exists():
                target_path.unlink()
            tmp_path.rename(target_path)
            pages = await get_pdf_page_count(target_path)
            return {
                "report_id": report_id, "firm": firm, "title": title, "path": target_path,
                "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
                "pdf_hash": _pdf_hash_bytes(body),
            }
        except Exception as e:
            logging.warning(
                "%s 하나증권 다운로드 예외: %s: %r",
                _report_prefix(firm, title, report_id, reg_dt),
                type(e).__name__, e,
            )
            return None
        finally:
            tmp_path = target_path.with_suffix(".tmp")
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)


async def download_ls_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    """LS증권: msg.ls-sec.co.kr 직접 HTTPS or View.jsp → download.jsp 2-step via WARP"""
    tmp_path = target_path.with_suffix(".tmp")

    # 1. msg.ls-sec.co.kr 직접 HTTPS (프록시 불필요, static PDF)
    msg_url = None
    for u in candidates:
        if "msg.ls-sec.co.kr" in u:
            msg_url = u
            break
    if msg_url:
        try:
            conn = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=conn) as s:
                async with s.get(msg_url,
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                    body = await r.read()
                    if _is_pdf_payload(body):
                        tmp_path.parent.mkdir(parents=True, exist_ok=True)
                        tmp_path.write_bytes(body)
                        if target_path.exists(): target_path.unlink()
                        tmp_path.rename(target_path)
                        pages = await get_pdf_page_count(target_path)
                        return {
                            "report_id": report_id, "firm": firm, "title": title, "path": target_path,
                            "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
                            "pdf_hash": _pdf_hash_bytes(body),
                        }
        except Exception as e:
            logging.debug("LS msg direct error: %s: %r", type(e).__name__, e)

    # 2. View.jsp → download.jsp 2-step (WARP proxy 필요)
    view_url = None
    for u in candidates:
        if "View.jsp" in u:
            view_url = u
            break
    if view_url:
        try:
            http_url = view_url.replace("https://", "http://", 1)
            warp_proxy = os.getenv("WARP_PROXY", "127.0.0.1:9091")
            conn = ProxyConnector.from_url(f"socks5://{warp_proxy}")
            async with aiohttp.ClientSession(connector=conn) as s:
                # 2a. View.jsp fetch
                async with s.get(http_url,
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                    html = await r.read()
                dec = html.decode("euc-kr", errors="replace")
                m = re.search(r'download\("([^"]+)"\)', dec)
                if not m:
                    logging.debug("LS View.jsp: no download key found")
                    return None
                ek = m.group(1)
                # 2b. download.jsp?dataType=KEY 로 PDF 다운로드
                dw_url = f"http://www.ls-sec.co.kr/_bt_lib/util/download.jsp?dataType={ek}"
                async with s.get(dw_url,
                    headers={"User-Agent": "Mozilla/5.0", "Referer": http_url},
                    timeout=aiohttp.ClientTimeout(total=60)) as r2:
                    body = await r2.read()
                    if _is_pdf_payload(body):
                        tmp_path.parent.mkdir(parents=True, exist_ok=True)
                        tmp_path.write_bytes(body)
                        if target_path.exists(): target_path.unlink()
                        tmp_path.rename(target_path)
                        pages = await get_pdf_page_count(target_path)
                        return {
                            "report_id": report_id, "firm": firm, "title": title, "path": target_path,
                            "size": target_path.stat().st_size, "pages": pages, "reg_dt": reg_dt,
                            "pdf_hash": _pdf_hash_bytes(body),
                        }
        except Exception as e:
            logging.debug("LS View.jsp parse error: %s: %r", type(e).__name__, e)

    return None





async def extract_dbfi_pdf_meta(session, encoded_url):
    if not encoded_url:
        return None

    token = unquote(encoded_url)
    gate_q = quote(token, safe="")
    gate_url = f"https://whub.dbsec.co.kr/pv/gate?q={gate_q}"

    pv_headers = {
        "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
        "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": gate_url,
    }

    try:
        async with session.post("https://whub.dbsec.co.kr/pv/auth", headers=pv_headers) as auth_response:
            await auth_response.text()

        viewer_payload = {
            "q": token,
            "c": "",
            "target": "",
            "docId": "",
        }
        async with session.post(
            "https://whub.dbsec.co.kr/pv/viewer",
            headers=pv_headers,
            data=viewer_payload,
        ) as viewer_response:
            if viewer_response.status != 200:
                logging.warning(f"DBfi: viewer request failed ({viewer_response.status})")
                return None

            viewer_html = await viewer_response.text()
            if not viewer_html or len(viewer_html) < 200:
                logging.warning(f"DBfi: viewer HTML too short or empty: {viewer_html[:200]}")
            
            # 더 광범위한 docId 추출 패턴
            patterns = [
                r'id="([^"]+)"[^>]*class="item"',
                r'class="item"[^>]*id="([^"]+)"',
                r'id\s*:\s*["\']([^"\']+)["\']',
                r'docId\s*:\s*["\']([^"\']+)["\']'
            ]
            doc_id = None
            for p in patterns:
                m = re.search(p, viewer_html)
                if m:
                    doc_id = m.group(1)
                    break

            if not doc_id:
                logging.warning("DBfi: Could not find StreamDocs document id in viewer HTML")
                return None

            title_match = re.search(
                rf'<div[^>]*id="{re.escape(doc_id)}"[^>]*class="item"[^>]*>\s*<span>(.*?)</span>',
                viewer_html,
                flags=re.S,
            )
            file_name = title_match.group(1).strip() if title_match else "리서치"
            pdf_url = f"https://whub.dbsec.co.kr/streamdocs/v4/documents/{doc_id}"
            return {
                "gate_url": gate_url,
                "viewer_url": "https://whub.dbsec.co.kr/pv/viewer",
                "doc_id": doc_id,
                "file_name": file_name,
                "pdf_url": pdf_url,
            }
    except Exception as e:
        logging.error(f"DBfi: Failed to extract PDF URL: {type(e).__name__}: {e!r}")
        return None


async def download_dbfi_pdf(key_url, target_path, title, report_id, firm, reg_dt):
    if not key_url:
        logging.warning(f"DBfi: missing source URL for report_id={report_id}")
        return False

    dbfi_ssl_context = ssl.create_default_context()
    dbfi_ssl_context.check_hostname = False
    dbfi_ssl_context.verify_mode = ssl.CERT_NONE
    dbfi_ssl_context.set_ciphers("DEFAULT")

    timeout = aiohttp.ClientTimeout(total=45)
    connector = aiohttp.TCPConnector(ssl=dbfi_ssl_context)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            source_url = key_url
            pdf_url = source_url
            referer_url = source_url
            logging.info(
                "%s DBfi start source=%s",
                _report_prefix(firm, title, report_id, reg_dt),
                _truncate(source_url, 220),
            )
            if "/appData/descRsh/" in source_url:
                async with session.post(source_url, headers={
                    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
                    "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
                }) as response:
                    if response.status != 200:
                        logging.warning(
                            "%s DBfi: descRsh request failed status=%s source=%s",
                            _report_prefix(firm, title, report_id, reg_dt),
                            response.status,
                            _truncate(source_url, 180),
                        )
                        return False
                    detail_data = await response.json()

                encoded_url = (detail_data.get("data") or {}).get("url", "")
                if not encoded_url:
                    return False

                extracted = await extract_dbfi_pdf_meta(session, encoded_url)
                if not extracted:
                    return False

                pdf_url = extracted["pdf_url"]
                referer_url = extracted["viewer_url"]

            tmp_path = target_path.with_suffix(".tmp")
            
            # Try primary PDF URL
            pdf_responses = []
            primary_headers = {
                "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
                "Accept": "application/pdf,application/octet-stream,*/*",
                "Referer": referer_url,
            }
            
            # Possible PDF endpoints
            pdf_candidate_urls = [pdf_url]

            body = None
            failed_pdf_attempts = []
            idx = 0
            while idx < len(pdf_candidate_urls):
                p_url = pdf_candidate_urls[idx]
                idx += 1
                try:
                    async with session.get(p_url, headers=primary_headers) as pdf_response:
                        content_type = getattr(pdf_response, "headers", {}).get("Content-Type", "")
                        if pdf_response.status == 200:
                            candidate_body = await pdf_response.read()
                            if _is_pdf_payload(candidate_body):
                                body = candidate_body
                                break
                            text = candidate_body.decode("utf-8", errors="ignore")
                            linked_urls = []
                            for match in re.finditer(r'href=["\']([^"\']*/streamdocs/v4/documents/[^"\']+)["\']', text):
                                linked_url = urljoin(p_url, match.group(1))
                                if linked_url not in pdf_candidate_urls:
                                    linked_urls.append(linked_url)
                            if linked_urls:
                                pdf_candidate_urls[idx:idx] = linked_urls
                            elif "/streamdocs/v4/documents/" in p_url and "/download" not in p_url:
                                download_url = p_url + "/download"
                                if download_url not in pdf_candidate_urls:
                                    pdf_candidate_urls.append(download_url)
                            failed_pdf_attempts.append((p_url, pdf_response.status, content_type, len(candidate_body)))
                        else:
                            failed_pdf_attempts.append((p_url, pdf_response.status, content_type, 0))
                except Exception as e:
                    failed_pdf_attempts.append((p_url, type(e).__name__, "", 0))
            
            if not body:
                logging.warning(
                    "%s DBfi: no valid PDF payload attempts=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    [(_truncate(u, 160), status, ctype, size) for u, status, ctype, size in failed_pdf_attempts],
                )
                return False

            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp_path, "wb") as fp:
                fp.write(body)

            if not tmp_path.exists() or tmp_path.stat().st_size <= 1024:
                return False
            body = tmp_path.read_bytes()
            if not _is_pdf_payload(body):
                tmp_path.unlink(missing_ok=True)
                return False

            if target_path.exists():
                target_path.unlink()
            tmp_path.rename(target_path)
            pages = await get_pdf_page_count(target_path)
            pdf_hash = _pdf_hash_bytes(body)
            return {
                "report_id": report_id,
                "firm": firm,
                "title": title,
                "path": target_path,
                "size": target_path.stat().st_size,
                "pages": pages,
                "reg_dt": reg_dt,
                "pdf_hash": pdf_hash,
            }
        except Exception as e:
            logging.error(f"DBfi: Download failed for {report_id}: {type(e).__name__}: {e!r}")
            return False
        finally:
            tmp_path = target_path.with_suffix(".tmp")
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

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
        # (교보증권/iprovest, 유안타증권/myasset, IM증권/imfnsec 등 대응)
        if _has_korean(u):
            euc_encoded = _encode_url_euc_kr(u)
            if euc_encoded and euc_encoded not in variants:
                variants.append(euc_encoded)
        for variant in variants:
            if variant not in seen:
                seen.add(variant)
                final.append(variant)
    return final

async def check_and_restart_warp():
    # 도커 환경에서는 별도의 warp 컨테이너나 호스트의 warp를 사용함
    try:
        # 1단계: 프록시 포트 오픈 확인 (단순 소켓 연결)
        host, port = Config.WARP_PROXY.split(":")
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=3)
        writer.close()
        await writer.wait_closed()
        logging.info(f"WARP proxy ({Config.WARP_PROXY}) is reachable.")
    except Exception:
        logging.warning(f"WARP proxy ({Config.WARP_PROXY}) seems down or unreachable. (Docker 'warp' container check needed)")
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

class PDFArchiver:
    def __init__(self):
        self.local_dir = Path(Config.LOCAL_BUFFER_DIR)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(Config.DOWNLOAD_CONCURRENCY)
        self.dbfi_semaphore = asyncio.Semaphore(Config.DBFI_DOWNLOAD_CONCURRENCY)
        self.success_downloads = []
        self.total_targets = 0
        self.processed_count = 0
        self._counter_lock = asyncio.Lock()

    async def _increment_processed(self, ok, firm, title, report_id, pdf_url=None):
        async with self._counter_lock:
            self.processed_count += 1
            total = self.total_targets if self.total_targets > 0 else 1
            pct = (self.processed_count / total * 100)
            
            # 상태바 생성 [##########----------]
            bar_length = 20
            filled_length = int(bar_length * self.processed_count // total)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            emoji = "✅" if ok else "❌"
            short_url = _truncate(pdf_url, 60) if pdf_url else "N/A"
            
            # 로그 출력: 상태바 + 퍼센트 + 결과 이모티콘 + report_id + 증권사 | 제목
            logging.info(f"|{bar}| {pct:5.1f}% {emoji} [{self.processed_count:3}/{self.total_targets:3}] report_id={report_id} {firm} | {title[:25]}... | {short_url}")

    def _add_success_record(self, row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt, target_path, size, pages, pdf_hash):
        self.success_downloads.append(WorkflowRecord({
            "row_id": row_id, "report_id": report_id, "sec_firm_order": sec_firm_order,
            "key": key_url, "pdf_url": pdf_url, "telegram_url": tel_url, "download_url": dw_url,
            "firm_nm": firm, "title": title, "reg_dt": reg_dt, "path": target_path,
            "size": size, "pages": pages, "pdf_hash": pdf_hash,
        }))

    async def _try_await_record_download(self, row_meta, target_path, coro):
        """비동기 다운로드 래퍼: await coro → 성공 시 success_downloads 에 기록하고 True."""
        result = await coro
        if result:
            row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt = row_meta
            self._add_success_record(
                row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url,
                firm, title, reg_dt, target_path,
                result["size"], result["pages"], result.get("pdf_hash"),
            )
            return True
        return False

    def _make_file_path(self, firm, title, reg_dt, report_id):
        clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
        y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
        yy_mm_dd = clean_dt[2:8]
        normalized = unicodedata.normalize('NFC', title or '')
        safe_title = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]\(\)]', ' ', normalized)
        safe_title = '_'.join(safe_title.split())[:60].strip('_') or 'untitled'
        filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
        return self.local_dir / y_m / firm / filename

    async def download_task(self, row):
        # row: (id, report_id, sec_firm_order, key, pdf_url, telegram_url, download_url, firm, title, reg_dt)
        row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt = row
        row_meta = (row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt)
        raw_urls = _download_sources_for_firm(key_url, pdf_url, tel_url, dw_url)
        candidates = build_candidate_urls(firm, raw_urls)
        
        proxy_url = os.getenv("WARP_PROXY")
        use_proxy = proxy_url and any(k in firm for k in ("LS",))

        target_path = self._make_file_path(firm, title, reg_dt, report_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix('.tmp')

        ok = False
        async with self.semaphore:
            # 1. DBfi 특수 처리
            if not ok and sec_firm_order == Config.DBFI_FIRM_ORDER:
                dbfi_source_url = pdf_url or key_url
                if not dbfi_source_url and tel_url and "/appData/descRsh/" in str(tel_url):
                    dbfi_source_url = tel_url
                async with self.dbfi_semaphore:
                    ok = await self._try_await_record_download(
                        row_meta, target_path,
                        download_dbfi_pdf(dbfi_source_url, target_path, title, report_id, firm, reg_dt),
                    )
                    if Config.DBFI_REQUEST_DELAY_SECONDS > 0:
                        await asyncio.sleep(Config.DBFI_REQUEST_DELAY_SECONDS)

            # 2. 미래에셋증권 특수 처리
            if not ok and firm == "미래에셋증권":
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_mirae_pdf(candidates, target_path, title, report_id, firm, reg_dt),
                )

            # 3. DS투자증권 특수 처리
            if not ok and firm == "DS투자증권":
                for url in candidates:
                    ok = await self._try_await_record_download(
                        row_meta, target_path,
                        download_ds_pdf(url, target_path, title, report_id, firm, reg_dt),
                    )
                    if ok:
                        break


            # LS증권 특수 처리 (View.jsp 2-step 파싱)
            if not ok and "LS" in firm:
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_ls_pdf(candidates, target_path, title, report_id, firm, reg_dt),
                )
            # 3b. 교보증권 특수 처리 (게시판 뷰 페이지에서 실제 PDF URL 추출)
            if not ok and firm == "교보증권":
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_kyobo_pdf(candidates, target_path, title, report_id, firm, reg_dt),
                )

            # 3c. 하나증권 특수 처리 (게시판에서 유효한 다운로드 URL 재추출)
            if not ok and firm == "하나증권":
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_hana_pdf(candidates, target_path, title, report_id, firm, reg_dt),
                )

            # 4. 일반 다운로드 (wget) — 4개 증권사(대신/IBK/삼성/다올/교보)는 쿠키+Referer 처리
            if not ok and firm not in ("미래에셋증권", "DS투자증권", "교보증권", "하나증권") and sec_firm_order != Config.DBFI_FIRM_ORDER:
                # 4a. aiohttp로 세션 쿠키 사전 획득
                cookie_string = ""
                if firm in _FIRMS_NEEDING_COOKIE_SESSION:
                    first_url = _get_first_url(pdf_url, key_url, tel_url, dw_url, candidates)
                    if first_url:
                        cookie_string = await _ensure_session_cookies_aiohttp(firm, first_url)
                        if cookie_string:
                            logging.info(
                                "%s session cookies acquired: %s",
                                _report_prefix(firm, title, report_id, reg_dt),
                                cookie_string,
                            )

                for url in candidates:
                    # 4b. wget 명령어 구성
                    # 4개 대상 증권사는 Referer를 다운로드 URL 자체로 설정 (서블릿/Referer 검증 대응)
                    if firm in _FIRMS_NEEDING_COOKIE_SESSION:
                        referer = pdf_url or key_url or url
                    else:
                        referer = _origin_referer(pdf_url or key_url or url)
                    cmd = [
                        "wget", "-q", "-O", str(tmp_path),
                        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                        "--referer=" + referer,
                        "--timeout=30", "--tries=2",
                        "--no-check-certificate",
                    ]
                    if cookie_string:
                        cmd.insert(1, "--header=Cookie: " + cookie_string)
                    cmd.append(url)

                    if use_proxy:
                        os.environ["all_proxy"] = f"socks5h://{proxy_url}"

                    try:
                        proc = await asyncio.create_subprocess_exec(*cmd, env=os.environ)
                        await proc.wait()

                        if proc.returncode == 0 and tmp_path.exists() and tmp_path.stat().st_size > 1024:
                            body = tmp_path.read_bytes()
                            if _is_pdf_payload(body):
                                if target_path.exists(): target_path.unlink()
                                tmp_path.rename(target_path)
                                pages = await get_pdf_page_count(target_path)
                                self._add_success_record(row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt, target_path, target_path.stat().st_size, pages, _pdf_hash_bytes(body))
                                ok = True
                                break
                        else:
                            logging.warning(
                                "%s download failed returncode=%s size=%s url=%s",
                                _report_prefix(firm, title, report_id, reg_dt),
                                proc.returncode,
                                tmp_path.stat().st_size if tmp_path.exists() else 0,
                                _truncate(url, 220),
                            )
                    except Exception as e:
                        logging.warning(
                            "%s download exception %s: %r url=%s",
                            _report_prefix(firm, title, report_id, reg_dt),
                            type(e).__name__,
                            e,
                            _truncate(url, 220),
                        )
                    finally:
                        if use_proxy: os.environ.pop("all_proxy", None)
                        if tmp_path.exists(): tmp_path.unlink(missing_ok=True)
        
        await self._increment_processed(ok, firm, title, report_id, pdf_url=pdf_url or key_url)
        return ok

    async def _update_source_workflow(self, conn, payload, pdf_status, retry_delta=0):
        pdf_url_norm = _normalize_pdf_url_value(payload.get("pdf_url"))
        pdf_hash = payload.get("pdf_hash")
        await conn.execute(
            f'''
            UPDATE {Config.SOURCE_TABLE}
            SET {Config.PDF_STATUS_COL} = $2,
                retry_count = COALESCE(retry_count, 0) + $3,
                {Config.PDF_HASH_COL} = COALESCE($4, {Config.PDF_HASH_COL})
            WHERE report_id = $1
               OR (NULLIF(BTRIM(pdf_url), '') = $5 AND $5 IS NOT NULL)
            ''',
            int(payload["report_id"]), pdf_status, retry_delta, pdf_hash, pdf_url_norm,
        )

    async def _upsert_archive_workflow(self, conn, payload, pdf_status, retry_delta=0, file_path=None, file_size=None, page_count=None, archive_status=None, download_status_yn=None, storage_key=None):
        file_name = Path(file_path).name if file_path else None
        await conn.execute(
            f'''
            INSERT INTO {Config.META_TABLE} (
                report_id, firm_nm, title, author, reg_dt, pdf_url, {Config.PDF_HASH_COL},
                has_text, is_encrypted, storage_backend, storage_key, download_url, telegram_url,
                key, archive_status, file_name, download_status_yn, file_path, file_size, page_count,
                last_accessed_at, {Config.PDF_STATUS_COL}, created_at, updated_at, retry_count
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, COALESCE($23, NOW()), NOW(), $24
            )
            ON CONFLICT (report_id) DO UPDATE SET
                firm_nm = EXCLUDED.firm_nm, title = EXCLUDED.title, author = COALESCE(EXCLUDED.author, {Config.META_TABLE}.author),
                reg_dt = EXCLUDED.reg_dt, pdf_url = EXCLUDED.pdf_url, {Config.PDF_HASH_COL} = COALESCE(EXCLUDED.{Config.PDF_HASH_COL}, {Config.META_TABLE}.{Config.PDF_HASH_COL}),
                storage_backend = COALESCE(EXCLUDED.storage_backend, {Config.META_TABLE}.storage_backend),
                storage_key = COALESCE(EXCLUDED.storage_key, {Config.META_TABLE}.storage_key),
                archive_status = COALESCE(EXCLUDED.archive_status, {Config.META_TABLE}.archive_status),
                file_name = COALESCE(EXCLUDED.file_name, {Config.META_TABLE}.file_name),
                download_status_yn = COALESCE(EXCLUDED.download_status_yn, {Config.META_TABLE}.download_status_yn),
                file_path = COALESCE(EXCLUDED.file_path, {Config.META_TABLE}.file_path),
                file_size = COALESCE(EXCLUDED.file_size, {Config.META_TABLE}.file_size),
                page_count = COALESCE(EXCLUDED.page_count, {Config.META_TABLE}.page_count),
                {Config.PDF_STATUS_COL} = EXCLUDED.{Config.PDF_STATUS_COL},
                updated_at = NOW(), retry_count = COALESCE({Config.META_TABLE}.retry_count, 0) + $24
            ''',
            int(payload["report_id"]), payload.get("firm_nm"), payload.get("title"), payload.get("author"),
            payload.get("reg_dt"), payload.get("pdf_url"), payload.get("pdf_hash"), payload.get("has_text"),
            payload.get("is_encrypted"), payload.get("storage_backend") or "onedrive",
            storage_key or payload.get("storage_key") or (str(file_path) if file_path else None),
            payload.get("download_url"), payload.get("telegram_url"), payload.get("key"),
            archive_status, file_name, download_status_yn, str(file_path) if file_path else None,
            file_size, page_count, payload.get("last_accessed_at"), pdf_status, None, retry_delta,
        )

    async def _apply_workflow_update(self, conn, payload, pdf_status, **kwargs):
        """Helper to update both source and archive tables."""
        await self._update_source_workflow(conn, payload, pdf_status, retry_delta=kwargs.get("retry_delta", 0))
        await self._upsert_archive_workflow(conn, payload, pdf_status, **kwargs)

    async def _rclone_cleanup(self):
        """OneDrive cleanup (느림. 필요할 때만 명시적 호출)"""
        logging.info("Running rclone cleanup on remote...")
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(
            Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG,
            "cleanup", Config.RCLONE_REMOTE, env=rclone_env,
        )
        await proc.wait()
        return proc.returncode == 0

    @staticmethod
    def _rclone_filter_escape(name: str) -> str:
        """Escape one path segment for rclone include filters."""
        return re.sub(r"([\\*?\[\]{}])", r"\\\1", name)

    @staticmethod
    def _rclone_is_missing_or_dir_error(text: str) -> bool:
        lowered = (text or "").lower()
        return (
            "doesn't exist" in lowered
            or "does not exist" in lowered
            or "is a directory" in lowered
            or "directory not found" in lowered
            or "object not found" in lowered
        )

    async def _rclone_delete_remote(
        self,
        remote_path: str,
        remote_dir: str | None = None,
        filename: str | None = None,
    ) -> tuple[bool, str]:
        """Delete a remote file, falling back to parent-dir filtered delete."""
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "deletefile", remote_path, env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, stderr = await proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode == 0 or not remote_dir or not filename:
            return proc.returncode == 0, stderr_text
        if self._rclone_is_auth_error(stderr_text) or not self._rclone_is_missing_or_dir_error(stderr_text):
            return False, stderr_text

        include_filter = f"/{self._rclone_filter_escape(filename)}"
        logging.warning("deletefile could not address %s directly; trying filtered delete in parent dir with include=%s", remote_path, include_filter)
        fallback = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "delete", remote_dir, "--max-depth", "1", "--include", include_filter, env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, fallback_stderr = await fallback.communicate()
        fallback_err = fallback_stderr.decode("utf-8", errors="replace").strip()
        if fallback.returncode != 0:
            return False, f"{stderr_text}\nfiltered delete: {fallback_err}".strip()

        remote_files, list_err = await self._rclone_lsjson_dir(remote_dir)
        if list_err:
            return False, f"{stderr_text}\nfiltered delete verify: {list_err}".strip()
        if filename in remote_files:
            return False, (f"{stderr_text}\nfiltered delete returned ok but file still exists (size={remote_files[filename]})")

        logging.info("filtered delete removed stale remote file: %s/%s", remote_dir, filename)
        return True, f"{stderr_text}\nfiltered delete ok".strip()

    @staticmethod
    def _rclone_is_auth_error(text: str) -> bool:
        """rclone/OneDrive 인증 오류 여부."""
        lowered = (text or "").lower()
        return any(
            marker in lowered
            for marker in (
                "unauthenticated",
                "invalidauthenticationtoken",
                "access token has expired",
                "token expired",
                "refresh token",
                "unauthorized",
            )
        )

    async def _rclone_stat_remote(self, remote_path: str) -> tuple[int | None, str]:
        """lsjson으로 원격 파일 크기 확인. (없으면 None, 오류는 stderr 반환)"""
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "lsjson", remote_path, env=rclone_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, stderr = await proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            return None, stderr_text
        if not out:
            return None, ""
        try:
            items = json.loads(out.decode())
            if items and len(items) > 0:
                return items[0].get("Size", 0), ""
            return None, ""
        except Exception as exc:
            return None, f"failed to parse lsjson output: {exc}"

    async def _rclone_check_remote(self, remote_path: str) -> int | None:
        """lsjson으로 원격 파일 존재여부/크기 확인. 없으면 None, 있으면 size 반환"""
        size, err = await self._rclone_stat_remote(remote_path)
        if err:
            logging.warning("Remote stat failed for %s: %s", remote_path, err[:300])
        return size

    @staticmethod
    def _normalize_filename_for_match(name: str) -> str:
        """파일명 비교용 정규화: NFC + 모든 따옴표/특수문자/공백을 _로 통일"""
        n = unicodedata.normalize("NFC", name)
        # 모든 종류의 따옴표 통일 (스마트따옴표, 홑따옴표, 겹따옴표 등)
        n = re.sub(r'[\u2018\u2019\u201c\u201d\u0022\u0027\u0060\u00b4\uff07]', '_', n)
        # 특수문자/공백을 _로
        n = re.sub(r'[\s\\/:*?<>|,.!@#$%^&ⓒ;()\[\]]+', '_', n)
        n = n.strip('_').lower()
        return n

    def _find_remote_filename(self, local_fname: str, remote_files: dict[str, int]) -> str | None:
        """로컬 파일명을 remote_files(lsl 결과)에서 찾음. 유니코드/따옴표 차이도 보정하여 lsl의 정확한 파일명 반환"""
        # 1) exact match
        if local_fname in remote_files:
            return local_fname
        # 2) normalize match (따옴표/특수문자 무시)
        norm_local = self._normalize_filename_for_match(local_fname)
        for rname in remote_files:
            if self._normalize_filename_for_match(rname) == norm_local:
                return rname
        # 3) partial match (report_id로 찾기)
        local_lower = local_fname.lower()
        for rname in remote_files:
            if local_lower in rname.lower() or rname.lower() in local_lower:
                return rname
        return None

    async def _rclone_lsjson_dir(self, remote_dir: str) -> tuple[dict[str, int], str]:
        """rclone lsjson remote_dir --files-only → {filename: size_bytes, ...}."""
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "lsjson", remote_dir, "--files-only", env=rclone_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            return {}, stderr_text
        if not stdout.strip():
            return {}, ""
        try:
            result: dict[str, int] = {}
            for item in json.loads(stdout.decode()):
                if item.get("IsDir"):
                    continue
                name = item.get("Name") or item.get("Path")
                if name:
                    result[name] = int(item.get("Size", 0))
            return result, ""
        except Exception as exc:
            return {}, f"failed to parse lsjson output: {exc}"

    async def _rclone_lsl_dir(self, remote_dir: str) -> dict[str, int]:
        """
        rclone lsjson remote_dir → {filename: size_bytes, ...}
        디렉토리 단위 한 번에 조회 (파일 개별 lsl보다 몇 배 빠름)
        """
        remote_files, err = await self._rclone_lsjson_dir(remote_dir)
        if err:
            logging.warning("Remote dir listing failed for %s: %s", remote_dir, err[:300])
        return remote_files

    async def _delete_stale_zero_byte_upload_targets(self) -> int:
        """
        이번 배치의 업로드 대상 중 원격에 0바이트로 남은 파일만 삭제한다.
        전체 remote cleanup은 OneDrive 전체를 훑어 매우 느리므로 정기 업로드 경로에서 쓰지 않는다.
        """
        local_map: dict[str, WorkflowRecord] = {
            str(p["path"].relative_to(self.local_dir)): p
            for p in self.success_downloads
        }
        dir_groups: dict[str, list[str]] = {}
        for rel_path in local_map:
            dir_groups.setdefault(os.path.dirname(rel_path), []).append(os.path.basename(rel_path))

        deleted = 0
        for sub_dir, filenames in dir_groups.items():
            remote_dir = f"{Config.RCLONE_REMOTE}/{sub_dir}" if sub_dir else Config.RCLONE_REMOTE
            remote_files = await self._rclone_lsl_dir(remote_dir)
            if not remote_files:
                continue

            for fname in filenames:
                exact_remote_name = self._find_remote_filename(fname, remote_files)
                if exact_remote_name is None: continue
                if remote_files[exact_remote_name] != 0: continue

                remote_full = f"{remote_dir}/{exact_remote_name}"
                ok, err = await self._rclone_delete_remote(remote_full, remote_dir=remote_dir, filename=exact_remote_name)
                if ok:
                    deleted += 1
                    logging.info("Deleted stale 0-byte remote before upload: %s", remote_full)
                elif self._rclone_is_auth_error(err):
                    logging.error("rclone auth failed while deleting stale 0-byte remote: %s", err[:300])
                    return deleted
                else:
                    logging.warning("Failed to delete stale 0-byte remote before upload: %s: %s", remote_full, err[:300])
        return deleted

    async def upload_to_onedrive(self) -> list[WorkflowRecord]:
        """
        0-byte target cleanup → rclone copy → nameAlreadyExists 체크 → 디렉토리 배치 lsl 검증 → 로컬 정리.

        nameAlreadyExists 처리:
          - 파일 하나하나 lsl 호출하지 않고, 부모 디렉토리별로 rclone lsl 한 번씩만 호출
          - 10개 파일이 3개 디렉토리에 있으면 lsl 3번 = API 3번 (기존 방식은 10번)
          - 전체 rclone cleanup은 너무 느려 기본 흐름에서는 사용하지 않음
        """
        total = len(self.success_downloads)
        logging.info("Upload %d files...", total)

        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        deleted_zero_byte = await self._delete_stale_zero_byte_upload_targets()
        if deleted_zero_byte:
            logging.info("Deleted %d stale 0-byte remote upload target(s).", deleted_zero_byte)

        # 로컬 매핑: relative_path → WorkflowRecord
        local_map: dict[str, WorkflowRecord] = {}
        for p in self.success_downloads: local_map[str(p["path"].relative_to(self.local_dir))] = p

        files_from_path = None
        files_from_fp = tempfile.NamedTemporaryFile("w", encoding="utf-8", prefix="pdf_archiver_rclone_files_", suffix=".txt", delete=False)
        try:
            files_from_path = files_from_fp.name
            for rel_path in local_map: files_from_fp.write(rel_path + "\n")
        finally:
            files_from_fp.close()

        # --- rclone copy (move 대신 copy 사용 후 수동 검증+삭제가 더 안전함) ---
        try:
            proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "copy", Config.LOCAL_BUFFER_DIR, Config.RCLONE_REMOTE, "--files-from", files_from_path, "--transfers", str(Config.RCLONE_TRANSFERS), "--checkers", str(Config.RCLONE_CHECKERS), "--no-traverse", "--onedrive-chunk-size", Config.ONEDRIVE_CHUNK_SIZE, "--retries", str(Config.RCLONE_RETRIES), "--low-level-retries", str(Config.RCLONE_LOW_LEVEL_RETRIES), "--onedrive-no-versions", env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
            _, stderr = await proc.communicate()
            stderr_text = stderr.decode("utf-8", errors="replace")
        finally:
            if files_from_path:
                try: os.unlink(files_from_path)
                except OSError: pass

        if proc.returncode != 0 and stderr_text.strip():
            stderr_lines = [line for line in stderr_text.splitlines() if line.strip()]
            logging.warning("rclone copy stderr tail: %s", " | ".join(stderr_lines[-8:])[:1200])
            if "Failed to create file system" in stderr_text:
                logging.error("rclone configuration error during batch copy. Local files will be kept for next run.")
                return []

        # 에러가 발생한 파일 경로 수집 (중복 또는 기타 에러)
        error_paths: list[str] = []
        for line in stderr_text.splitlines():
            if "ERROR" in line:
                m = re.search(r"ERROR\s*:\s*(.+?\.pdf):", line, re.I)
                if m: error_paths.append(m.group(1).strip())
                elif "Failed to copy" in line or "nameAlreadyExists" in line:
                    m = re.search(r"(?:Failed to copy|nameAlreadyExists).*?:\s+(.+?\.pdf)", line, re.I)
                    if m: error_paths.append(m.group(1).strip())
                if proc.returncode != 0 and not any(k in line for k in ("nameAlreadyExists", "Couldn't delete", "no such file or directory")):
                    logging.error("rclone: %s", line)

        unique_errors = list(dict.fromkeys(error_paths))

        if self._rclone_is_auth_error(stderr_text):
            logging.error("rclone auth failed during batch copy. Skipping verification/repair; local files will be kept for next run.")
            return []

        verified: set[str] = set()

        if proc.returncode == 0:
            for p in self.success_downloads: verified.add(str(p["report_id"]))
            logging.info("Upload OK (%d files).", total)
        else:
            logging.warning("rclone reported errors (code=%d). Starting verification...", proc.returncode)

        if (proc.returncode != 0) or unique_errors:
            candidates = list(set(unique_errors + [rel for rel in local_map.keys() if str(local_map[rel]["report_id"]) not in verified]))
            if candidates:
                dir_groups: dict[str, list[str]] = {}
                for rel_path in candidates:
                    d = os.path.dirname(rel_path)
                    dir_groups.setdefault(d, []).append(os.path.basename(rel_path))
                logging.info("Verifying %d files in %d dirs...", len(candidates), len(dir_groups))
                for sub_dir, filenames in dir_groups.items():
                    remote_dir = f"{Config.RCLONE_REMOTE}/{sub_dir}"
                    remote_files = await self._rclone_lsl_dir(remote_dir)
                    for fname in filenames:
                        rel_path = f"{sub_dir}/{fname}" if sub_dir else fname
                        payload = local_map.get(rel_path)
                        if not payload: continue
                        exact_remote_name = self._find_remote_filename(fname, remote_files)
                        if exact_remote_name is None: continue
                        rs = remote_files[exact_remote_name]
                        ls = payload.get("size", 0)
                        if rs and rs > 0 and rs == ls:
                            logging.info("Verification match (size=%d): %s", ls, rel_path)
                            verified.add(str(payload["report_id"]))
                        elif rs is not None and rs != ls:
                            remote_full = f"{remote_dir}/{exact_remote_name}"
                            lf = os.path.join(Config.LOCAL_BUFFER_DIR, rel_path)
                            if not os.path.exists(lf):
                                logging.warning("Local file missing for %s, skipping retry", rel_path)
                                continue
                            max_retries = 5
                            retry_delays = [3, 6, 12, 24, 48]
                            success = False
                            last_error = ""
                            auth_failed = False
                            for attempt in range(1, max_retries + 1):
                                logging.warning("Size mismatch (remote=%s local=%s): %s. Retry %d/%d: delete remote and re-upload...", rs, ls, rel_path, attempt, max_retries)
                                delete_ok = False
                                for del_attempt in range(3):
                                    ok, err = await self._rclone_delete_remote(remote_full, remote_dir=remote_dir, filename=exact_remote_name)
                                    if self._rclone_is_auth_error(err):
                                        auth_failed = True
                                        last_error = f"delete auth failed: {err[:200]}"
                                        logging.error("rclone auth failed while deleting %s. Stop retrying this file; local file will be kept.", rel_path)
                                        break
                                    if ok:
                                        await asyncio.sleep(2)
                                        after_size, stat_err = await self._rclone_stat_remote(remote_full)
                                        if self._rclone_is_auth_error(stat_err):
                                            auth_failed = True
                                            last_error = f"stat after delete auth failed: {stat_err[:200]}"
                                            logging.error("rclone auth failed while verifying delete for %s. Stop retrying this file; local file will be kept.", rel_path)
                                            break
                                        if after_size is None:
                                            delete_ok = True
                                            break
                                        logging.warning("Delete returned ok but file still exists (size=%s) for %s, stderr=%s", after_size, rel_path, err)
                                    else:
                                        after_size, stat_err = await self._rclone_stat_remote(remote_full)
                                        if self._rclone_is_auth_error(stat_err):
                                            auth_failed = True
                                            last_error = f"stat after failed delete auth failed: {stat_err[:200]}"
                                            logging.error("rclone auth failed while checking failed delete for %s. Stop retrying this file; local file will be kept.", rel_path)
                                            break
                                        if after_size is None and not stat_err:
                                            delete_ok = True
                                            logging.info("Remote already absent after failed delete command: %s", rel_path)
                                            break
                                        logging.warning("Delete attempt %d/3 failed for %s: %s", del_attempt + 1, rel_path, err or "(no stderr)")
                                    await asyncio.sleep(2 ** del_attempt)
                                if auth_failed: break
                                if not delete_ok:
                                    last_error = "delete failed after 3 attempts (file still on remote)"
                                    logging.warning("Delete ultimately failed for %s. Skip copyto until the remote file is actually gone.", rel_path)
                                    await asyncio.sleep(retry_delays[attempt - 1] if attempt <= len(retry_delays) else 60)
                                    continue
                                await asyncio.sleep(retry_delays[attempt - 1] if attempt <= len(retry_delays) else 60)
                                rp = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "copyto", lf, remote_full, "--onedrive-chunk-size", Config.ONEDRIVE_CHUNK_SIZE, "--retries", str(max(Config.RCLONE_RETRIES, 5)), "--low-level-retries", str(Config.RCLONE_LOW_LEVEL_RETRIES), "--onedrive-no-versions", env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
                                _, rp_stderr = await rp.communicate()
                                rp_stderr_text = rp_stderr.decode("utf-8", errors="replace")
                                upload_ok = rp.returncode == 0
                                if rp.returncode != 0 or "nameAlreadyExists" in rp_stderr_text or "ERROR" in rp_stderr_text:
                                    upload_ok = False
                                    last_error = f"copyto: {rp_stderr_text.strip()[:200]}"
                                    logging.warning("Retry %d copyto problem for %s (rc=%d): %s", attempt, rel_path, rp.returncode, last_error)
                                    if self._rclone_is_auth_error(rp_stderr_text):
                                        auth_failed = True
                                        logging.error("rclone auth failed while re-uploading %s. Stop retrying this file; local file will be kept.", rel_path)
                                        break
                                if not upload_ok: continue
                                await asyncio.sleep(1)
                                uploaded_size, stat_err = await self._rclone_stat_remote(remote_full)
                                if self._rclone_is_auth_error(stat_err):
                                    auth_failed = True
                                    last_error = f"stat after upload auth failed: {stat_err[:200]}"
                                    logging.error("rclone auth failed while verifying upload for %s. Stop retrying this file; local file will be kept.", rel_path)
                                    break
                                if uploaded_size == ls:
                                    logging.info("Retry %d successful (size=%d): %s", attempt, ls, rel_path)
                                    verified.add(str(payload["report_id"]))
                                    success = True
                                    break
                                elif uploaded_size is not None:
                                    rs = uploaded_size
                                    last_error = f"size mismatch (remote={uploaded_size} local={ls})"
                                    logging.warning("Retry %d %s for %s", attempt, last_error, rel_path)
                                else:
                                    last_error = "remote file not found after upload"
                                    logging.warning("Retry %d remote empty for %s", attempt, rel_path)
                            if not success:
                                logging.error("Giving up on %s after %d retries. Last error: %s", rel_path, attempt if auth_failed else max_retries, last_error)

        deleted = 0
        kept = 0
        for p in self.success_downloads:
            rid = str(p["report_id"])
            lp = p["path"]
            if rid in verified:
                try:
                    if lp.exists(): os.remove(str(lp))
                    deleted += 1
                except OSError as e: logging.warning("rm fail: %s: %s", lp, e)
            else: kept += 1

        for root, dirs, files in os.walk(Config.LOCAL_BUFFER_DIR, topdown=False):
            for d in dirs:
                try: os.rmdir(os.path.join(root, d))
                except OSError: pass

        result = [p for p in self.success_downloads if str(p["report_id"]) in verified]
        logging.info("Upload: %d ok, %d kept. (%d dir lsl calls)", len(result), kept, len(dir_groups) if unique_errors else 0)
        return result

    @staticmethod
    def _target_firm_counts(targets) -> dict[str, int]:
        firm_counts: dict[str, int] = {}
        for target in targets:
            firm = target.get("firm_nm") or "UNKNOWN"
            firm_counts[firm] = firm_counts.get(firm, 0) + 1
        return firm_counts

    def _build_target_query(self, excluded: str) -> str:
        """
        Fetch a small, firm-diversified batch.

        Ordering by firm_rank first gives a round-robin shape:
        each eligible firm contributes its first candidate before any firm contributes
        its second candidate.
        """
        return f"""
            WITH base AS (
                SELECT report_id, sec_firm_order, key, pdf_url, telegram_url, download_url, firm_nm, article_title, reg_dt,
                       {Config.PDF_STATUS_COL} as status,
                       retry_count,
                       CASE
                           WHEN NULLIF(BTRIM(pdf_url), '') IS NOT NULL
                             OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
                             OR NULLIF(BTRIM(download_url), '') IS NOT NULL
                             OR NULLIF(BTRIM(key), '') IS NOT NULL
                           THEN 1 ELSE 0
                       END AS has_source_url,
                       COALESCE(ENCODE({Config.PDF_HASH_COL}, 'hex'), NULLIF(BTRIM(pdf_url), ''), report_id::TEXT) AS pdf_key
                FROM {Config.SOURCE_TABLE}
                WHERE {Config.PDF_STATUS_COL} IN (0, 3)
                  AND (
                      COALESCE(retry_count, 0) < 5
                      OR (
                          {Config.PDF_STATUS_COL} = 3
                          AND COALESCE(retry_count, 0) < {Config.FETCH_RETRY_LIMIT}
                          AND (
                              NULLIF(BTRIM(pdf_url), '') IS NOT NULL
                              OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
                              OR NULLIF(BTRIM(download_url), '') IS NOT NULL
                              OR NULLIF(BTRIM(key), '') IS NOT NULL
                          )
                      )
                  )
                  AND firm_nm NOT IN ({excluded})
                  AND report_id IS NOT NULL
            ),
            distinct_targets AS (
                SELECT DISTINCT ON (pdf_key) * FROM base
                ORDER BY pdf_key, report_id ASC
            ),
            ranked_targets AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY firm_nm
                           ORDER BY
                               (CASE WHEN status = 0 THEN 0 ELSE 1 END),
                               COALESCE(retry_count, 0) ASC,
                               has_source_url DESC,
                               reg_dt DESC,
                               report_id DESC
                       ) AS firm_rank
                FROM distinct_targets
            )
            SELECT report_id as row_id, report_id, sec_firm_order, key, pdf_url, telegram_url, download_url, firm_nm, article_title, reg_dt
            FROM ranked_targets
            ORDER BY firm_rank, reg_dt DESC, firm_nm, report_id DESC
            LIMIT {Config.BATCH_SIZE}
        """

    async def _fetch_targets(self, conn):
        excluded = ', '.join(f"'{f}'" for f in Config.EXCLUDED_FIRMS)
        return await conn.fetch(self._build_target_query(excluded))

    def _print_target_summary(self, targets) -> None:
        self.total_targets = len(targets)
        print(f"DEBUG: 조회된 대상 수 = {self.total_targets}개")
        if targets:
            print(f"DEBUG: 대상 증권사 분포 = {self._target_firm_counts(targets)}")

    async def _mark_download_failures(self, conn, failed_targets) -> None:
        for target in failed_targets:
            payload = _row_payload(target)
            logging.warning(
                "DOWNLOAD FAILED report_id=%s firm=%s title=%s pdf_url=%s",
                payload["report_id"], payload["firm_nm"],
                _truncate(payload["title"], 60),
                _truncate(payload["pdf_url"], 120),
            )
            await self._update_source_workflow(conn, payload, 3, retry_delta=1)

    async def _apply_upload_results(self, conn, uploaded_payloads) -> None:
        if uploaded_payloads:
            print(f"DEBUG: 업로드 성공 ({len(uploaded_payloads)}건). DB 메타데이터 업데이트 중...")
            for payload in uploaded_payloads:
                local_path = payload["path"]
                relative_path = local_path.relative_to(self.local_dir)
                storage_key = str(relative_path)

                await self._apply_workflow_update(
                    conn, payload, 2, retry_delta=0, file_path=str(local_path),
                    file_size=payload["size"], page_count=payload["pages"],
                    archive_status="ARCHIVED", download_status_yn="Y",
                    storage_key=storage_key
                )

        uploaded_ids = {str(p["report_id"]) for p in (uploaded_payloads or [])}
        failed_payloads = [p for p in self.success_downloads if str(p["report_id"]) not in uploaded_ids]
        if failed_payloads:
            print(f"DEBUG: {len(failed_payloads)}건 업로드 실패. 상태 롤백 중...")
            for payload in failed_payloads:
                await self._update_source_workflow(conn, payload, 3, retry_delta=0)

    async def run(self):
        print("DEBUG: [1/4] DB 연결 준비 중...")
        # (WARP 체크는 나중에 LS증권 대상이 있을 때만 수행하도록 위치 변경됨)
        
        print("DEBUG: [2/4] DB 연결 중...")
        conn = await DBManager.get_conn()
        
        try:
            print("DEBUG: [3/4] 스키마 확인 및 대상 쿼리 실행 중...")
            if Config.FETCH_ONLY:
                print("DEBUG: fetch-only 모드: 스키마 변경, 다운로드, 업로드를 건너뜁니다.")
            else:
                await ensure_pdf_sync_status_schema(conn)

            targets = await self._fetch_targets(conn)
            self._print_target_summary(targets)

            if not targets:
                # 0개인 원인 분석을 위해 전체 대기 건수 확인
                wait_count = await conn.fetchval(f"SELECT COUNT(*) FROM {Config.SOURCE_TABLE} WHERE {Config.PDF_STATUS_COL} IN (0, 3)")
                print(f"DEBUG: DB내 총 대기 레코드(필터 전) = {wait_count}개")
                logging.info("No pending targets.")
                return

            if Config.FETCH_ONLY:
                for idx, target in enumerate(targets, 1):
                    print(
                        "DEBUG: fetch-only target "
                        f"{idx:02d}: {target.get('firm_nm')} report_id={target.get('report_id')} "
                        f"reg_dt={target.get('reg_dt')} title={_truncate(target.get('article_title'), 80)}"
                    )
                return

            # LS증권이 포함된 경우에만 WARP 프록시 체크
            if any("LS" in (target.get("firm_nm") or "") for target in targets):
                print(f"DEBUG: LS증권이 포함되어 WARP 체크 중...")
                await check_and_restart_warp()
            else:
                print("DEBUG: LS증권이 없어 WARP 체크를 건너뜁니다.")

            print(f"DEBUG: [4/4] 다운로드 시작 ({self.total_targets}건)...")
            results = await asyncio.gather(*[self.download_task(t) for t in targets])
            failed_targets = [target for target, ok in zip(targets, results) if not ok]
            
            success_count = len(self.success_downloads)
            print(f"DEBUG: 다운로드 완료 (성공: {success_count}건, 실패: {len(failed_targets)}건)")

            if failed_targets:
                await self._mark_download_failures(conn, failed_targets)

            if self.success_downloads:
                print(f"DEBUG: OneDrive 업로드 시작 ({success_count}건)...")
                uploaded_payloads = await self.upload_to_onedrive()
                await self._apply_upload_results(conn, uploaded_payloads)
            
            print("DEBUG: 모든 작업 완료.")

        except Exception as e:
            print(f"DEBUG: 실행 중 에러 발생: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await DBManager.close()

def _get_process_elapsed(pid: int) -> float | None:
    """Return elapsed seconds since process started, or None if undetermined."""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            stat = f.read()
        # find end of comm field (enclosed in parens, may contain spaces)
        comm_end = stat.rfind(")")
        fields = stat[comm_end + 2:].split()
        starttime_ticks = int(fields[19])               # field 19 = starttime
        with open("/proc/uptime", "r") as f:
            uptime_sec = float(f.read().split()[0])
        clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return uptime_sec - (starttime_ticks / clk_tck)
    except Exception:
        return None


def _acquire_lock(lock_path: str, timeout: int) -> tuple[bool, object | None]:
    """Try to acquire a fcntl advisory lock.

    Returns (True, file_handle) on success.
    If the lock is held by a zombie (elapsed > *timeout* seconds), kill it
    and re-acquire.  Otherwise returns (False, None).
    """
    # attempt without truncating so we can read the old PID on failure
    try:
        lock_f = open(lock_path, "r+")
    except FileNotFoundError:
        lock_f = open(lock_path, "w+")

    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # success — stamp our PID
        lock_f.seek(0)
        lock_f.truncate()
        lock_f.write(str(os.getpid()))
        lock_f.flush()
        return True, lock_f
    except (IOError, OSError):
        # lock held by another process
        lock_f.seek(0)
        old_pid_str = lock_f.read().strip()
        lock_f.close()

        if not old_pid_str:
            return False, None

        try:
            old_pid = int(old_pid_str)
        except ValueError:
            return False, None

        elapsed = _get_process_elapsed(old_pid)
        if elapsed is None or elapsed <= timeout:
            if elapsed is not None:
                print(f"DEBUG: 다른 인스턴스 실행 중 (PID={old_pid}, 경과={elapsed:.0f}초 < {timeout}초). 종료합니다.")
            return False, None

        # zombie detected — kill and retry
        print(f"DEBUG: 좀비 프로세스 발견 (PID={old_pid}, 실행시간={elapsed:.0f}초 > {timeout}초). SIGKILL 전송.")
        try:
            os.kill(old_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        time.sleep(1)

        # retry lock acquisition
        lock_f = open(lock_path, "w")
        try:
            fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_f.write(str(os.getpid()))
            lock_f.flush()
            print(f"DEBUG: 좀비 종료 후 락 획득 성공 (PID={os.getpid()}).")
            return True, lock_f
        except (IOError, OSError):
            lock_f.close()
            print(f"DEBUG: 좀비 종료했으나 락 획득 실패. 종료합니다.")
            return False, None


if __name__ == "__main__":
    if "--fetch-only" in sys.argv:
        Config.FETCH_ONLY = True

    ok, lock_f = _acquire_lock(Config.LOCK_FILE, Config.ZOMBIE_TIMEOUT_SECONDS)
    if not ok:
        sys.exit(0)
    try:
        asyncio.run(PDFArchiver().run())
    finally:
        try:
            lock_f.close()
        except Exception:
            pass
