import asyncio
import aiohttp
import os
import re
import logging
try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None
from utils import _is_pdf_payload, _pdf_hash_bytes, get_pdf_page_count

async def download_ls_pdf(candidates, target_path, title, report_id, firm, report_date):
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
                            "size": target_path.stat().st_size, "pages": pages, "report_date": report_date,
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
            if ProxyConnector:
                conn = ProxyConnector.from_url(f"socks5://{warp_proxy}")
                async with aiohttp.ClientSession(connector=conn) as s:
                    # 2a. View.jsp fetch
                    async with s.get(http_url,
                        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
                        timeout=aiohttp.ClientTimeout(total=30)) as r:
                        html_bytes = await r.read()
                    dec = html_bytes.decode("euc-kr", errors="replace")
                    m = re.search(r'download\("([^"]+)"\)', dec)
                    if m:
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
                                    "size": target_path.stat().st_size, "pages": pages, "report_date": report_date,
                                    "pdf_hash": _pdf_hash_bytes(body),
                                }
        except Exception as e:
            logging.debug("LS View.jsp parse error: %s: %r", type(e).__name__, e)

    return None
