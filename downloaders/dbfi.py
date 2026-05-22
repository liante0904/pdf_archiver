import asyncio
import aiohttp
import ssl
import re
import logging
from urllib.parse import quote, unquote, urljoin
import utils
from utils import _is_pdf_payload, _pdf_hash_bytes, get_pdf_page_count, _report_prefix, _truncate

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

        viewer_payload = {"q": token, "c": "", "target": "", "docId": ""}
        async with session.post("https://whub.dbsec.co.kr/pv/viewer", headers=pv_headers, data=viewer_payload) as viewer_response:
            if viewer_response.status != 200:
                return None

            viewer_html = await viewer_response.text()
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
            if not doc_id: return None

            pdf_url = f"https://whub.dbsec.co.kr/streamdocs/v4/documents/{doc_id}"
            return {"viewer_url": "https://whub.dbsec.co.kr/pv/viewer", "doc_id": doc_id, "pdf_url": pdf_url}
    except Exception: return None

async def download_dbfi_pdf(key_url, target_path, title, report_id, firm, reg_dt):
    if not key_url: return False

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=45)) as session:
        try:
            pdf_url = key_url
            referer_url = key_url
            if "/appData/descRsh/" in key_url:
                async with session.post(key_url, headers={
                    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
                    "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
                }) as resp:
                    if resp.status != 200: return False
                    detail_data = await resp.json()
                encoded_url = (detail_data.get("data") or {}).get("url", "")
                if not encoded_url: return False
                extracted = await extract_dbfi_pdf_meta(session, encoded_url)
                if not extracted: return False
                pdf_url = extracted["pdf_url"]
                referer_url = extracted["viewer_url"]

            tmp_path = target_path.with_suffix(".tmp")
            pdf_candidate_urls = [pdf_url]
            body = None
            idx = 0
            while idx < len(pdf_candidate_urls):
                p_url = pdf_candidate_urls[idx]
                idx += 1
                try:
                    async with session.get(p_url, headers={"User-Agent": "Mozilla/5.0", "Referer": referer_url}) as pdf_resp:
                        if pdf_resp.status == 200:
                            candidate_body = await pdf_resp.read()
                            if _is_pdf_payload(candidate_body):
                                body = candidate_body
                                break
                            text = candidate_body.decode("utf-8", errors="ignore")
                            linked_urls = [urljoin(p_url, m.group(1)) for m in re.finditer(r'href=["\']([^"\']*/streamdocs/v4/documents/[^"\']+)["\']', text)]
                            for lu in linked_urls:
                                if lu not in pdf_candidate_urls: pdf_candidate_urls.append(lu)
                            if "/streamdocs/v4/documents/" in p_url and "/download" not in p_url:
                                if (p_url + "/download") not in pdf_candidate_urls: pdf_candidate_urls.append(p_url + "/download")
                except Exception: continue
            
            if not body: return False
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(body)
            if not tmp_path.exists() or tmp_path.stat().st_size <= 1024 or not _is_pdf_payload(body):
                if tmp_path.exists(): tmp_path.unlink()
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
        except Exception: return False
