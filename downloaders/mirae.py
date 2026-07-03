import asyncio
import logging
from utils import _is_pdf_payload, _pdf_hash_bytes, get_pdf_page_count, _find_mirae_board_download_url, _report_prefix, _truncate

async def download_mirae_pdf(candidates, target_path, title, report_id, firm, report_date):
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
                fallback_url = _find_mirae_board_download_url(board_html, title, report_date)
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
            _report_prefix(firm, title, report_id, report_date),
            attempted_urls,
        )
        return False

    if target_path.exists():
        target_path.unlink()
    tmp_path.rename(target_path)
    pages = await get_pdf_page_count(target_path)
    
    return {
        "report_id": report_id, "firm": firm, "title": title, "path": target_path,
        "size": target_path.stat().st_size, "pages": pages, "report_date": report_date,
        "pdf_hash": _pdf_hash_bytes(body),
    }
