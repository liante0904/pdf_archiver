"""메리츠증권 전용 downloader — board page에서 PDF URL 추출 후 Referer 다운로드"""
import re
import logging
import aiohttp

log = logging.getLogger("pdf_archiver_v2.meritz")

PDF_URL_RE = re.compile(
    r"""href=['\"]\s*(https?://home\.imeritz\.com/include/resource/research/WorkFlow/[^'\"]+\.pdf)\s*['\"]""",
    re.IGNORECASE,
)
DOWNLOAD_GATE_RE = re.compile(
    r"""getDownLoadFile\s*\(\s*['\"](/bbs/BbsDownLoad\.go)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]""",
    re.IGNORECASE,
)


async def download_meritz_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    """메리츠증권: BbsRead.go → PDF 링크 추출 → Referer 다운로드"""
    tmp_path = target_path.with_suffix(".tmp")

    # 1. BbsRead.go URL 찾기
    board_url = None
    for u in candidates:
        if u and "BbsRead.go" in str(u):
            board_url = str(u)
            break
    if not board_url:
        return None

    connector = aiohttp.TCPConnector(ssl=False, limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # 2. Board page 요청 → HTML 파싱
        try:
            async with session.get(board_url, headers=headers, timeout=20) as resp:
                if resp.status != 200:
                    log.warning("메리츠: report_id=%s board HTTP %s", report_id, resp.status)
                    return None
                html = await resp.text()
        except Exception as e:
            log.warning("메리츠: report_id=%s board fetch error: %s", report_id, e)
            return None

        # 3. PDF URL 추출
        pdf_url = None
        m = PDF_URL_RE.search(html)
        if m:
            pdf_url = m.group(1)
        else:
            # fallback: BbsDownLoad.go gate
            m2 = DOWNLOAD_GATE_RE.search(html)
            if m2:
                gate_path, grp, bid, turn, fidx = m2.group(1), m2.group(2), m2.group(3), m2.group(4), m2.group(5)
                pdf_url = f"https://home.imeritz.com{gate_path}?bbsGrpId={grp}&bbsId={bid}&bbsCnttTurnNo={turn}&fileIndex={fidx}"

        if not pdf_url:
            log.warning("메리츠: report_id=%s board에서 PDF URL 못 찾음", report_id)
            return None

        # 4. PDF 다운로드 (board page를 Referer로)
        dl_headers = {
            "User-Agent": headers["User-Agent"],
            "Referer": board_url,
            "Accept": "application/pdf,*/*",
        }
        try:
            async with session.get(pdf_url, headers=dl_headers, timeout=30, allow_redirects=True) as resp:
                if resp.status != 200:
                    log.warning("메리츠: report_id=%s PDF HTTP %s url=%s", report_id, resp.status, pdf_url[:120])
                    return None
                body = await resp.read()
                if len(body) < 5000 or body[:5] != b"%PDF-":
                    log.warning("메리츠: report_id=%s not PDF len=%s", report_id, len(body))
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
            log.warning("메리츠: report_id=%s download 예외: %s", report_id, e)
            return None
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
