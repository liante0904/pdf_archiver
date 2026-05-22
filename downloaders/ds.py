import aiohttp
from .base import BaseDownloader
from utils import _browser_like_headers, _is_pdf_payload, _pdf_hash_bytes, get_pdf_page_count

async def download_ds_pdf(source_url, target_path, title, report_id, firm, reg_dt, referer_hint=None):
    if not source_url:
        return False

    downloader = BaseDownloader()
    session = await downloader.get_session()
    try:
        # 1. board_url 정제 (download.php -> board.php, 불필요한 인자 제거)
        board_url = source_url.replace("download.php", "board.php")
        if "&no=" in board_url:
            board_url = board_url.split("&no=")[0]
        
        # 2. 게시판 뷰 페이지 방문하여 세션 쿠키 획득
        cookies = await downloader.fetch_cookies(board_url)

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
        await downloader.close()
        tmp_path = target_path.with_suffix(".tmp")
        if tmp_path.exists(): tmp_path.unlink(missing_ok=True)
