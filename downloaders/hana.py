import aiohttp
import ssl
import re
import logging
from urllib.parse import urljoin
from .base import BaseDownloader
from utils import _browser_like_headers, _cookie_header_from_response, _is_pdf_payload, _pdf_hash_bytes, get_pdf_page_count, _report_prefix, _truncate

async def download_hana_pdf(candidates, target_path, title, report_id, firm, reg_dt):
    """하나증권 전용 다운로드.
    게시판 목록 페이지에서 현재 유효한 다운로드 URL을 찾아 다운로드한다.
    """
    tmp_path = target_path.with_suffix(".tmp")
    # 1. 후보 URL 중 게시판 URL 찾기
    board_url = None
    for u in candidates:
        if "/board/" in u:
            board_url = u
            break
    if not board_url:
        board_url = candidates[0] if candidates else None
    if not board_url:
        return None

    downloader = BaseDownloader()
    session = await downloader.get_session()
    try:
        headers = _browser_like_headers()
        async with session.get(board_url, headers=headers, allow_redirects=True) as resp:
            raw_body = await resp.read()
            content_type = resp.headers.get("content-type", "").lower()
            cookies = _cookie_header_from_response(resp)

        # 만약 board_url 자체가 바로 PDF를 반환했다면
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
        await downloader.close()
        tmp_path = target_path.with_suffix(".tmp")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
