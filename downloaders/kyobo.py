import aiohttp
import ssl
import re
import logging
from urllib.parse import urljoin
from .base import BaseDownloader
from utils import _browser_like_headers, _cookie_header_from_response, _is_pdf_payload, _pdf_hash_bytes, get_pdf_page_count, _report_prefix, _truncate

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

    downloader = BaseDownloader()
    session = await downloader.get_session()
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
        await downloader.close()
        tmp_path = target_path.with_suffix(".tmp")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
