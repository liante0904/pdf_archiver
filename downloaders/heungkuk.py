"""흥국증권 전용 downloader — board list에서 key 찾아 다운로드"""
import re
import logging
import aiohttp
from urllib.parse import urljoin

HEUNGKUK_BASE = "https://www.heungkuksec.co.kr"
LIST_PAGES = [
    "/research/market/list.do",      # Daily & Comment
    "/research/company/list.do",     # 산업/기업분석
    "/research/industry/list.do",    # 투자전략
]

log = logging.getLogger("pdf_archiver_v2.heungkuk")


async def download_heungkuk_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    """흥국증권: board list에서 title 매칭 → key 추출 → download.do"""
    tmp_path = target_path.with_suffix(".tmp")

    # 검색용 키워드: title 앞부분 20자 + 뒷부분 10자
    search_title = (title or "").strip()
    if not search_title or len(search_title) < 3:
        return None

    # 키워드 추출: 특수문자/괄호 제거한 첫 30자
    clean_title = re.sub(r'[\[\]()「」\s]+', ' ', search_title).strip()
    keywords = clean_title[:40]

    connector = aiohttp.TCPConnector(ssl=False, limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        found_key = None
        for list_path in LIST_PAGES:
            try:
                list_url = urljoin(HEUNGKUK_BASE, list_path)
                async with session.get(list_url, headers=headers, timeout=20) as resp:
                    if resp.status != 200:
                        continue
                    body = await resp.read()
                    try:
                        html = body.decode("euc-kr")
                    except Exception:
                        html = body.decode("utf-8", errors="ignore")

                # 패턴: onclick="nav.go('view', 'key=XXXXX');">TITLE</a>
                pattern = re.compile(
                    r"""nav\.go\(['\"]view['\"],\s*['\"]key=(\d+)['\"]\)[^>]*>([^<]+)</a>""",
                    re.IGNORECASE,
                )
                for m in pattern.finditer(html):
                    key = m.group(1)
                    link_title = m.group(2).strip()
                    # 타이틀 매칭: 앞 30자 비교
                    link_clean = re.sub(r'[\[\]()「」\s]+', ' ', link_title).strip()
                    if link_clean[:30] == clean_title[:30] or clean_title[:30] in link_clean:
                        found_key = key
                        break

                if found_key:
                    break
            except Exception as e:
                log.debug("heungkuk list error: %s: %s", list_path, e)
                continue

        if not found_key:
            log.warning(
                "흥국증권: report_id=%s title=«%s» — board list에서 key 못 찾음",
                report_id, clean_title[:60],
            )
            return None

        # 다운로드
        download_url = f"{HEUNGKUK_BASE}/download.do?type=Board&key={found_key}"
        dl_headers = {
            "User-Agent": headers["User-Agent"],
            "Referer": f"{HEUNGKUK_BASE}/research/company/view.do?key={found_key}",
            "Accept": "application/pdf,*/*",
        }
        try:
            async with session.get(download_url, headers=dl_headers, timeout=30) as resp:
                if resp.status != 200:
                    log.warning(
                        "흥국증권: report_id=%s download 실패 HTTP %s key=%s",
                        report_id, resp.status, found_key,
                    )
                    return None
                body = await resp.read()
                if len(body) < 5000 or body[:5] != b"%PDF-":
                    log.warning(
                        "흥국증권: report_id=%s not a PDF body len=%s key=%s",
                        report_id, len(body), found_key,
                    )
                    return None

            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(body)
            if target_path.exists():
                target_path.unlink()
            tmp_path.rename(target_path)

            from utils import _pdf_hash_bytes, get_pdf_page_count

            pages = await get_pdf_page_count(target_path)
            return {
                "report_id": report_id, "firm": firm, "title": title,
                "path": target_path,
                "size": target_path.stat().st_size, "pages": pages,
                "reg_dt": reg_dt, "pdf_hash": _pdf_hash_bytes(body),
            }
        except Exception as e:
            log.warning("흥국증권: report_id=%s download 예외: %s", report_id, e)
            return None
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
