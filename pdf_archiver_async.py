# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiohttp",
#     "asyncpg",
# ]
# ///

import asyncio
import aiohttp
import hashlib
import ssl
import os
import time
import logging
import fcntl
import sys
import re
import shutil
import unicodedata
import json
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse, unquote, urljoin

try:
    import asyncpg
except ModuleNotFoundError:  # pragma: no cover - exercised only when postgres deps are absent locally
    asyncpg = None

from db_tables import PDF_ARCHIVE_TABLE, SOURCE_REPORTS_TABLE
from secret_env import load_workspace_secret_env_defaults

def _load_secret_env_defaults():
    """Load workspace-local secret defaults without overriding explicit env."""
    load_workspace_secret_env_defaults()


_load_secret_env_defaults()

# --- DB 설정 ---
POSTGRES_URL = os.getenv("POSTGRES_URL") # postgresql://user:pass@host:port/dbname
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

BATCH_SIZE = 200
DOWNLOAD_CONCURRENCY = 10
RCLONE_TRANSFERS = 5

EXCLUDED_FIRMS = ('현대차증권', '유진투자증권', '상상인증권')
DBFI_FIRM_ORDER = 19

LOG_FILE = os.getenv("LOG_FILE", os.path.expanduser("~/log/pdf_archiver_async.log"))
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)

async def get_db_connection():
    if asyncpg is None:
        raise RuntimeError("asyncpg is required for pdf_archiver_async.py")
    if POSTGRES_URL:
        return await asyncpg.connect(POSTGRES_URL)
    return await asyncpg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


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
    for table_name in (SOURCE_TABLE, META_TABLE):
        for legacy_attach_name, quoted in (("attach_url", False), ("attach_url", True)):
            if await _table_has_column(conn, table_name, legacy_attach_name):
                drop_name = legacy_attach_name
                await conn.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {drop_name}")

        pdf_status_existed = await _table_has_column(conn, table_name, PDF_STATUS_COL)
        if not pdf_status_existed:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {PDF_STATUS_COL} INTEGER DEFAULT 0")

        if not await _table_has_column(conn, table_name, PDF_HASH_COL):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {PDF_HASH_COL} BYTEA")

        if table_name == META_TABLE:
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

        legacy_exists = await _table_has_column(conn, table_name, LEGACY_STATUS_COL)
        if table_name == META_TABLE and not legacy_exists:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {LEGACY_STATUS_COL} INTEGER DEFAULT 0")
            legacy_exists = True

        if table_name == META_TABLE and not await _table_has_column(conn, table_name, "retry_count"):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN retry_count INTEGER DEFAULT 0")

        if not pdf_status_existed and legacy_exists:
            await conn.execute(
                f"""
                UPDATE {table_name}
                SET {PDF_STATUS_COL} = COALESCE({LEGACY_STATUS_COL}, 0)
                WHERE {PDF_STATUS_COL} IS NULL
                   OR {PDF_STATUS_COL} != COALESCE({LEGACY_STATUS_COL}, 0)
                """
            )
        else:
            await conn.execute(
                f"""
                UPDATE {table_name}
                SET {PDF_STATUS_COL} = COALESCE({PDF_STATUS_COL}, 0)
                WHERE {PDF_STATUS_COL} IS NULL
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
    return _pdf_signature_offset(data) is not None


def _download_context(row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt):
    return {
        "row_id": row_id,
        "report_id": report_id,
        "sec_firm_order": sec_firm_order,
        "firm_nm": firm,
        "title": title,
        "reg_dt": reg_dt,
        "key": _truncate(key_url),
        "pdf_url": _truncate(pdf_url),
        "telegram_url": _truncate(tel_url),
        "download_url": _truncate(dw_url),
    }


def _report_prefix(firm, title, report_id, reg_dt=None):
    return f"[{firm} | {title} | report_id={report_id}" + (f" | reg_dt={reg_dt}]" if reg_dt else "]")


def _download_sources_for_firm(firm, key_url, pdf_url, tel_url, dw_url):
    sources = [u for u in (key_url, pdf_url, dw_url) if u and str(u).startswith("http")]
    if firm != "DS투자증권":
        sources.extend([u for u in (tel_url,) if u and str(u).startswith("http")])
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
        logging.warning("%s DS download missing source url", _report_prefix(firm, title, report_id, reg_dt))
        return False

    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            board_url = referer_hint or source_url.replace("download.php", "board.php")
            base_headers = _browser_like_headers()
            cookies = ""

            if board_url:
                logging.info(
                    "%s DS board preflight board_url=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(board_url, 220),
                )
                async with session.get(board_url, headers=base_headers, allow_redirects=True) as board_response:
                    await board_response.read()
                    cookies = _cookie_header_from_response(board_response)
                    logging.info(
                        "%s DS board preflight status=%s cookies=%s content_type=%s",
                        _report_prefix(firm, title, report_id, reg_dt),
                        board_response.status,
                        bool(cookies),
                        board_response.headers.get("content-type", ""),
                    )

            download_headers = _browser_like_headers(referer=board_url)
            if cookies:
                download_headers["Cookie"] = cookies

            tmp_path = target_path.with_suffix(".tmp")
            logging.info(
                "%s DS download begin url=%s referer=%s",
                _report_prefix(firm, title, report_id, reg_dt),
                _truncate(source_url, 220),
                _truncate(board_url, 220),
            )
            async with session.get(source_url, headers=download_headers, allow_redirects=True) as response:
                body = await response.read()
                content_type = response.headers.get("content-type", "")
                logging.info(
                    "%s DS download response status=%s content_type=%s bytes=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    response.status,
                    content_type,
                    len(body),
                )
                if response.status != 200:
                    preview = body[:300].decode("utf-8", errors="ignore")
                    logging.warning(
                        "%s DS download failed status=%s preview=%s",
                        _report_prefix(firm, title, report_id, reg_dt),
                        response.status,
                        _truncate(preview, 300),
                    )
                    return False

                if "text/html" in content_type.lower() or len(body) < 5000:
                    preview = body[:500].decode("utf-8", errors="ignore")
                    logging.warning(
                        "%s DS blocked or html response preview=%s",
                        _report_prefix(firm, title, report_id, reg_dt),
                        _truncate(preview, 300),
                    )
                    return False

                signature_offset = _pdf_signature_offset(body)
                if signature_offset is None:
                    preview = body[:500].decode("utf-8", errors="ignore")
                    logging.warning(
                        "%s DS response is not PDF preview=%s",
                        _report_prefix(firm, title, report_id, reg_dt),
                        _truncate(preview, 300),
                    )
                    return False

                if signature_offset > 0:
                    logging.info(
                        "%s DS PDF signature found after %s leading bytes",
                        _report_prefix(firm, title, report_id, reg_dt),
                        signature_offset,
                    )

                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                with open(tmp_path, "wb") as fp:
                    fp.write(body)

            if not tmp_path.exists() or tmp_path.stat().st_size <= 1024:
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
            logging.error(
                "%s DS download failed url=%s err=%s",
                _report_prefix(firm, title, report_id, reg_dt),
                _truncate(source_url, 220),
                e,
            )
            return False
        finally:
            tmp_path = target_path.with_suffix(".tmp")
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

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
            match = re.search(r'<div[^>]*id="([^"]+)"[^>]*class="item"', viewer_html)
            if not match:
                match = re.search(r'<div[^>]*class="item"[^>]*id="([^"]+)"', viewer_html)
            if not match:
                logging.warning("DBfi: Could not find StreamDocs document id in viewer HTML")
                return None

            doc_id = match.group(1)
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
        logging.error(f"DBfi: Failed to extract PDF URL: {e}")
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
                logging.info("%s DBfi source is detail json", _report_prefix(firm, title, report_id, reg_dt))
                async with session.post(source_url, headers={
                    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
                    "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
                }) as response:
                    if response.status != 200:
                        logging.warning("%s DBfi detail request failed status=%s source=%s", _report_prefix(firm, title, report_id, reg_dt), response.status, _truncate(source_url, 220))
                        return False
                    detail_data = await response.json()

                encoded_url = (detail_data.get("data") or {}).get("url", "")
                if not encoded_url:
                    logging.warning("%s DBfi empty encoded url source=%s", _report_prefix(firm, title, report_id, reg_dt), _truncate(source_url, 220))
                    return False

                extracted = await extract_dbfi_pdf_meta(session, encoded_url)
                if not extracted:
                    logging.warning("%s DBfi PDF meta extraction failed source=%s", _report_prefix(firm, title, report_id, reg_dt), _truncate(source_url, 220))
                    return False

                pdf_url = extracted["pdf_url"]
                referer_url = extracted["viewer_url"]
                logging.info(
                    "%s DBfi resolved pdf_url=%s referer=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(pdf_url, 220),
                    _truncate(referer_url, 220),
                )

            tmp_path = target_path.with_suffix(".tmp")
            logging.info("%s DBfi GET begin url=%s", _report_prefix(firm, title, report_id, reg_dt), _truncate(pdf_url, 220))
            async with session.get(
                pdf_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
                    "Accept": "application/pdf,*/*",
                    "Referer": referer_url,
                },
            ) as pdf_response:
                if pdf_response.status != 200:
                    logging.warning("%s DBfi PDF GET failed status=%s url=%s", _report_prefix(firm, title, report_id, reg_dt), pdf_response.status, _truncate(pdf_url, 220))
                    return False

                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                body = await pdf_response.read()
                logging.info("%s DBfi GET done bytes=%s", _report_prefix(firm, title, report_id, reg_dt), len(body))
                if not body.startswith(b"%PDF"):
                    text = body.decode("utf-8", errors="ignore")
                    candidates = extract_dbfi_retry_candidates(text, pdf_url)
                    for candidate in candidates:
                        async with session.get(
                            candidate,
                            headers={
                                "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
                                "Accept": "application/pdf,*/*",
                                "Referer": pdf_url,
                            },
                        ) as retry_response:
                            if retry_response.status != 200:
                                continue
                            retry_body = await retry_response.read()
                            if retry_body.startswith(b"%PDF"):
                                body = retry_body
                                logging.info(
                                    "%s DBfi retry candidate succeeded candidate=%s",
                                    _report_prefix(firm, title, report_id, reg_dt),
                                    _truncate(candidate, 220),
                                )
                                break
                    else:
                        logging.warning("%s DBfi downloaded content is not PDF", _report_prefix(firm, title, report_id, reg_dt))
                        return False

                with open(tmp_path, "wb") as fp:
                    fp.write(body)

            if not tmp_path.exists() or tmp_path.stat().st_size <= 1024:
                return False
            body = tmp_path.read_bytes()
            signature_offset = _pdf_signature_offset(body)
            if signature_offset is None:
                logging.warning(
                    "%s DBfi downloaded content is not PDF preview=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(body[:200].decode("utf-8", errors="ignore"), 200),
                )
                tmp_path.unlink(missing_ok=True)
                return False
            if signature_offset > 0:
                logging.info(
                    "%s DBfi PDF signature found after %s leading bytes",
                    _report_prefix(firm, title, report_id, reg_dt),
                    signature_offset,
                )

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
            logging.error("%s DBfi download failed source=%s err=%s", _report_prefix(firm, title, report_id, reg_dt), _truncate(key_url, 220), e)
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
        for variant in (u, encoded):
            if variant not in seen:
                seen.add(variant)
                final.append(variant)
    return final

async def check_and_restart_warp():
    # 도커 환경에서는 별도의 warp 컨테이너나 호스트의 warp를 사용하므로 
    # 여기서는 프록시 접근성만 체크
    proxy_url = os.getenv("WARP_PROXY", "localhost:9091")
    try:
        proc = await asyncio.create_subprocess_exec(
            "curl", "-o", "/dev/null", "-s", "-w", "%{http_code}",
            "--connect-timeout", "5", "--socks5-hostname", proxy_url,
            "https://www.google.com",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await proc.communicate()
        if stdout.decode().strip() != "200":
            logging.warning(f"WARP proxy ({proxy_url}) unreachable.")
    except Exception as e:
        logging.error(f"WARP check error: {e}")

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
        self.local_dir = Path(LOCAL_BUFFER_DIR)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
        self.success_downloads = []

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
        # row: (id, report_id, sec_firm_order, key, pdf_url, tel_url, dw_url, firm, title, reg_dt)
        row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt = row
        raw_urls = _download_sources_for_firm(firm, key_url, pdf_url, tel_url, dw_url)
        candidates = build_candidate_urls(firm, raw_urls)
        logging.info(
            "%s candidate prepared: %s",
            _report_prefix(firm, title, report_id, reg_dt),
            _download_context(row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, reg_dt),
        )
        logging.info(
            "%s download candidates: %s",
            _report_prefix(firm, title, report_id, reg_dt),
            [_truncate(candidate, 220) for candidate in candidates],
        )
        
        proxy_url = os.getenv("WARP_PROXY")
        use_proxy = proxy_url and any(k in firm for k in ("LS", "이베스트"))

        target_path = self._make_file_path(firm, title, reg_dt, report_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix('.tmp')

        async with self.semaphore:
            if sec_firm_order == DBFI_FIRM_ORDER:
                dbfi_source_url = key_url or pdf_url
                if not dbfi_source_url and tel_url and "/appData/descRsh/" in str(tel_url):
                    dbfi_source_url = tel_url
                logging.info(
                    "%s DBfi source selected: key=%s pdf_url=%s chosen=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(key_url),
                    _truncate(pdf_url),
                    _truncate(dbfi_source_url),
                )
                dbfi_result = await download_dbfi_pdf(dbfi_source_url, target_path, title, report_id, firm, reg_dt)
                if dbfi_result:
                    self.success_downloads.append(WorkflowRecord({
                        "row_id": row_id,
                        "report_id": report_id,
                        "sec_firm_order": sec_firm_order,
                        "key": key_url,
                        "pdf_url": pdf_url,
                        "telegram_url": tel_url,
                        "download_url": dw_url,
                        "firm_nm": firm,
                        "title": title,
                        "reg_dt": reg_dt,
                        "path": target_path,
                        "size": dbfi_result["size"],
                        "pages": dbfi_result["pages"],
                        "pdf_hash": dbfi_result.get("pdf_hash"),
                    }))
                    return True

            if firm == "DS투자증권":
                logging.info(
                    "%s telegram_url skipped as frontend-only source",
                    _report_prefix(firm, title, report_id, reg_dt),
                )
                ds_referer_hint = key_url or pdf_url
                for url in candidates:
                    logging.info(
                        "%s DS proxy flow trying url=%s referer_hint=%s",
                        _report_prefix(firm, title, report_id, reg_dt),
                        _truncate(url, 220),
                        _truncate(ds_referer_hint, 220),
                    )
                    ds_result = await download_ds_pdf(url, target_path, title, report_id, firm, reg_dt, referer_hint=ds_referer_hint)
                    if ds_result:
                        self.success_downloads.append(WorkflowRecord({
                            "row_id": row_id,
                            "report_id": report_id,
                            "sec_firm_order": sec_firm_order,
                            "key": key_url,
                            "pdf_url": pdf_url,
                            "telegram_url": tel_url,
                            "download_url": dw_url,
                            "firm_nm": firm,
                            "title": title,
                            "reg_dt": reg_dt,
                            "path": target_path,
                            "size": ds_result["size"],
                            "pages": ds_result["pages"],
                            "pdf_hash": ds_result.get("pdf_hash"),
                        }))
                        return True
                return False

            for url in candidates:
                logging.info(
                    "%s trying url=%s proxy=%s",
                    _report_prefix(firm, title, report_id, reg_dt),
                    _truncate(url, 220),
                    bool(use_proxy),
                )
                referer = key_url or pdf_url or url
                cmd = [
                    "curl",
                    "-L",
                    "-s",
                    "-w",
                    "%{http_code}",
                    "--connect-timeout",
                    "15",
                    "-A",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "-e",
                    referer,
                    "-H",
                    "Accept: application/pdf,*/*",
                    "-o",
                    str(tmp_path),
                    url,
                ]
                if use_proxy: cmd += ["--socks5-hostname", proxy_url]
                try:
                    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                    stdout, _ = await proc.communicate()
                    http_code = stdout.decode().strip()
                    body_preview = tmp_path.read_bytes()[:200] if tmp_path.exists() else b""
                    logging.info(
                        "%s download result url=%s returncode=%s http_code=%s size=%s preview=%s",
                        _report_prefix(firm, title, report_id, reg_dt),
                        _truncate(url, 220),
                        proc.returncode,
                        http_code,
                        tmp_path.stat().st_size if tmp_path.exists() else 0,
                        _truncate(body_preview.decode("utf-8", errors="ignore"), 200),
                    )
                    if proc.returncode == 0 and tmp_path.exists() and tmp_path.stat().st_size > 1024:
                        body = tmp_path.read_bytes()
                        signature_offset = _pdf_signature_offset(body)
                        if signature_offset is not None:
                            if signature_offset > 0:
                                logging.info(
                                    "%s PDF signature found after %s leading bytes url=%s",
                                    _report_prefix(firm, title, report_id, reg_dt),
                                    signature_offset,
                                    _truncate(url, 220),
                                )
                            if target_path.exists(): target_path.unlink()
                            tmp_path.rename(target_path)
                            pages = await get_pdf_page_count(target_path)
                            self.success_downloads.append(WorkflowRecord({
                                "row_id": row_id,
                                "report_id": report_id,
                                "sec_firm_order": sec_firm_order,
                                "key": key_url,
                                "pdf_url": pdf_url,
                                "telegram_url": tel_url,
                                "download_url": dw_url,
                                "firm_nm": firm,
                                "title": title,
                                "reg_dt": reg_dt,
                                "path": target_path,
                                "size": target_path.stat().st_size,
                                "pages": pages,
                                "pdf_hash": _pdf_hash_bytes(body),
                            }))
                            return True
                    else:
                        logging.warning(
                            "%s download attempt failed url=%s returncode=%s http_code=%s size=%s",
                            _report_prefix(firm, title, report_id, reg_dt),
                            _truncate(url, 220),
                            proc.returncode,
                            http_code,
                            tmp_path.stat().st_size if tmp_path.exists() else 0,
                        )
                except Exception: pass
                finally:
                    if tmp_path.exists(): tmp_path.unlink(missing_ok=True)
        return False

    async def _update_source_workflow(self, conn, payload, pdf_status, retry_delta=0):
        pdf_url_norm = _normalize_pdf_url_value(payload.get("pdf_url"))
        pdf_hash = payload.get("pdf_hash")
        await conn.execute(
            f'''
            UPDATE {SOURCE_TABLE}
            SET {PDF_STATUS_COL} = $2,
                retry_count = COALESCE(retry_count, 0) + $3,
                {PDF_HASH_COL} = COALESCE($4, {PDF_HASH_COL})
            WHERE report_id = $1
               OR NULLIF(BTRIM(pdf_url), '') = $5
            ''',
            int(payload["report_id"]),
            pdf_status,
            retry_delta,
            pdf_hash,
            pdf_url_norm,
        )

    async def _upsert_archive_workflow(self, conn, payload, pdf_status, retry_delta=0, file_path=None, file_size=None, page_count=None, archive_status=None, download_status_yn=None):
        file_name = Path(file_path).name if file_path else None
        await conn.execute(
            f'''
            INSERT INTO {META_TABLE} (
                report_id,
                firm_nm,
                title,
                author,
                reg_dt,
                pdf_url,
                {PDF_HASH_COL},
                has_text,
                is_encrypted,
                storage_backend,
                storage_key,
                download_url,
                telegram_url,
                key,
                archive_status,
                file_name,
                download_status_yn,
                file_path,
                file_size,
                page_count,
                last_accessed_at,
                {PDF_STATUS_COL},
                created_at,
                updated_at,
                retry_count
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, COALESCE($23, NOW()), NOW(), $24
            )
            ON CONFLICT (report_id) DO UPDATE SET
                firm_nm = EXCLUDED.firm_nm,
                title = EXCLUDED.title,
                author = COALESCE(EXCLUDED.author, {META_TABLE}.author),
                reg_dt = EXCLUDED.reg_dt,
                pdf_url = EXCLUDED.pdf_url,
                {PDF_HASH_COL} = COALESCE(EXCLUDED.{PDF_HASH_COL}, {META_TABLE}.{PDF_HASH_COL}),
                has_text = COALESCE(EXCLUDED.has_text, {META_TABLE}.has_text),
                is_encrypted = COALESCE(EXCLUDED.is_encrypted, {META_TABLE}.is_encrypted),
                storage_backend = COALESCE(EXCLUDED.storage_backend, {META_TABLE}.storage_backend),
                storage_key = COALESCE(EXCLUDED.storage_key, {META_TABLE}.storage_key),
                download_url = EXCLUDED.download_url,
                telegram_url = EXCLUDED.telegram_url,
                key = EXCLUDED.key,
                archive_status = COALESCE(EXCLUDED.archive_status, {META_TABLE}.archive_status),
                file_name = COALESCE(EXCLUDED.file_name, {META_TABLE}.file_name),
                download_status_yn = COALESCE(EXCLUDED.download_status_yn, {META_TABLE}.download_status_yn),
                file_path = COALESCE(EXCLUDED.file_path, {META_TABLE}.file_path),
                file_size = COALESCE(EXCLUDED.file_size, {META_TABLE}.file_size),
                page_count = COALESCE(EXCLUDED.page_count, {META_TABLE}.page_count),
                last_accessed_at = COALESCE(EXCLUDED.last_accessed_at, {META_TABLE}.last_accessed_at),
                {PDF_STATUS_COL} = EXCLUDED.{PDF_STATUS_COL},
                updated_at = NOW(),
                retry_count = COALESCE({META_TABLE}.retry_count, 0) + $24
            ''',
            int(payload["report_id"]),
            payload.get("firm_nm"),
            payload.get("title"),
            payload.get("author"),
            payload.get("reg_dt"),
            payload.get("pdf_url"),
            payload.get("pdf_hash"),
            payload.get("has_text"),
            payload.get("is_encrypted"),
            payload.get("storage_backend") or "onedrive",
            payload.get("storage_key") or (str(file_path) if file_path else None),
            payload.get("download_url"),
            payload.get("telegram_url"),
            payload.get("key"),
            archive_status,
            file_name,
            download_status_yn,
            str(file_path) if file_path else None,
            file_size,
            page_count,
            payload.get("last_accessed_at"),
            pdf_status,
            None,
            retry_delta,
        )

    async def _apply_workflow_update(self, conn, payload, pdf_status, retry_delta=0, file_path=None, file_size=None, page_count=None, archive_status=None, download_status_yn=None):
        await self._update_source_workflow(conn, payload, pdf_status, retry_delta=retry_delta)
        await self._upsert_archive_workflow(
            conn,
            payload,
            pdf_status,
            retry_delta=retry_delta,
            file_path=file_path,
            file_size=file_size,
            page_count=page_count,
            archive_status=archive_status,
            download_status_yn=download_status_yn,
        )

    async def run(self):
        await check_and_restart_warp()
        conn = await get_db_connection()
        
        try:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {META_TABLE} (
                    report_id BIGINT PRIMARY KEY,
                    sec_firm_order INTEGER,
                    article_board_order INTEGER,
                    firm_nm TEXT,
                    title TEXT,
                    reg_dt TEXT,
                    article_url TEXT,
                    save_time TEXT,
                    writer TEXT,
                    mkt_tp TEXT,
                    pdf_url TEXT,
                    pdf_hash BYTEA,
                    title TEXT,
                    author TEXT,
                    has_text BOOLEAN,
                    is_encrypted BOOLEAN,
                    storage_backend TEXT DEFAULT 'onedrive',
                    storage_key TEXT,
                    download_url TEXT,
                    telegram_url TEXT,
                    key TEXT,
                    archive_status TEXT,
                    file_name TEXT,
                    download_status_yn TEXT,
                    file_path TEXT,
                    file_size BIGINT,
                    page_count INT,
                    last_accessed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    pdf_sync_status INTEGER DEFAULT 0,
                    sync_status INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0
                )
            """)
            await ensure_pdf_sync_status_schema(conn)

            excluded = ', '.join(f"'{f}'" for f in EXCLUDED_FIRMS)
            query = f"""
                WITH source_rows AS (
                    SELECT
                        R.report_id AS id,
                        R.report_id,
                        CASE
                            WHEN R.firm_nm IN ('DB금융투자', 'DB증권') THEN {DBFI_FIRM_ORDER}
                            ELSE NULL
                        END AS sec_firm_order,
                        R.key,
                        R.pdf_url,
                        R.telegram_url,
                        R.download_url,
                        R.firm_nm,
                        R.article_title,
                        R.reg_dt,
                        R.{PDF_STATUS_COL} AS pdf_status,
                        R.{PDF_HASH_COL} AS pdf_hash,
                        COALESCE(ENCODE(R.{PDF_HASH_COL}, 'hex'), {_normalize_pdf_url_sql('R.pdf_url')}) AS pdf_record_key
                    FROM {SOURCE_TABLE} R
                    WHERE R.{PDF_STATUS_COL} IN (0, 3)
                      AND COALESCE(R.retry_count, 0) < 5
                      AND R.firm_nm NOT IN ({excluded})
                      AND R.report_id IS NOT NULL
                ),
                stored_keys AS (
                    SELECT DISTINCT COALESCE(ENCODE({PDF_HASH_COL}, 'hex'), NULLIF(BTRIM(pdf_url), '')) AS pdf_record_key
                    FROM {SOURCE_TABLE}
                    WHERE ({PDF_HASH_COL} IS NOT NULL OR NULLIF(BTRIM(pdf_url), '') IS NOT NULL)
                      AND {PDF_STATUS_COL} = 2
                    UNION
                    SELECT DISTINCT COALESCE(ENCODE({PDF_HASH_COL}, 'hex'), NULLIF(BTRIM(pdf_url), '')) AS pdf_record_key
                    FROM {META_TABLE}
                    WHERE ({PDF_HASH_COL} IS NOT NULL OR NULLIF(BTRIM(pdf_url), '') IS NOT NULL)
                      AND COALESCE({PDF_STATUS_COL}, sync_status, 0) = 2
                ),
                canonical_pdf_rows AS (
                    SELECT DISTINCT ON (pdf_record_key)
                        *
                    FROM source_rows
                    WHERE pdf_record_key IS NOT NULL
                      AND pdf_record_key NOT IN (SELECT pdf_record_key FROM stored_keys)
                    ORDER BY pdf_record_key, report_id ASC
                ),
                single_rows AS (
                    SELECT *
                    FROM source_rows
                    WHERE pdf_record_key IS NULL
                ),
                ordered_candidates AS (
                    SELECT *
                    FROM (
                        SELECT * FROM canonical_pdf_rows
                        UNION ALL
                        SELECT * FROM single_rows
                    ) candidates
                    ORDER BY (CASE WHEN pdf_record_key IS NULL THEN 1 ELSE 0 END), (CASE WHEN pdf_status = 3 THEN 0 ELSE 1 END), reg_dt DESC, report_id DESC
                    LIMIT {BATCH_SIZE}
                )
                SELECT id, report_id, sec_firm_order, key, pdf_url, telegram_url, download_url, firm_nm, article_title, reg_dt
                FROM ordered_candidates
            """

            targets = await conn.fetch(query)

            if not targets:
                logging.info("No pending targets.")
                return

            logging.info(f"Batch start: {len(targets)} targets")
            results = await asyncio.gather(*[self.download_task(t) for t in targets])
            failed_targets = [target for target, ok in zip(targets, results) if not ok]

            if failed_targets:
                logging.warning(f"Download failed for {len(failed_targets)} targets. Updating retry counters...")
                for target in failed_targets:
                    payload = _row_payload(target)
                    logging.warning(
                        "%s marking retry pdf_sync_status=3 retry_delta=1",
                        _report_prefix(payload["firm_nm"], payload["title"], payload["report_id"], payload["reg_dt"]),
                    )
                    await self._apply_workflow_update(conn, payload, 3, retry_delta=1)

            # rclone 업로드
            if self.success_downloads:
                logging.info(f"Downloaded {len(self.success_downloads)} files. Uploading via rclone move...")
            else:
                logging.info("Uploading via rclone move...")
            rclone_env = os.environ.copy()
            rclone_env.setdefault("HOME", os.path.expanduser("~"))
            rclone_env["RCLONE_CONFIG"] = RCLONE_CONFIG
            proc = await asyncio.create_subprocess_exec(
                RCLONE_BIN,
                "--config",
                RCLONE_CONFIG,
                "move",
                LOCAL_BUFFER_DIR,
                RCLONE_REMOTE,
                "--include",
                "*.pdf",
                "--transfers",
                str(RCLONE_TRANSFERS),
                "--retries",
                "3",
                "--delete-empty-src-dirs",
                env=rclone_env,
            )
            await proc.wait()
            if proc.returncode != 0:
                logging.error(f"rclone move failed with exit code {proc.returncode}. Marking pdf_sync_status=3 for retry.")
                for payload in self.success_downloads:
                    logging.warning(
                        "%s rclone failed marking retry pdf_sync_status=3",
                        _report_prefix(payload["firm_nm"], payload["title"], payload["report_id"], payload["reg_dt"]),
                    )
                    await self._apply_workflow_update(conn, payload, 3, retry_delta=1)
                return

            for payload in self.success_downloads:
                logging.info(
                    "%s archived successfully path=%s size=%s pages=%s",
                    _report_prefix(payload["firm_nm"], payload["title"], payload["report_id"], payload["reg_dt"]),
                    payload["path"],
                    payload["size"],
                    payload["pages"],
                )
                await self._apply_workflow_update(
                    conn,
                    payload,
                    2,
                    retry_delta=0,
                    file_path=str(payload["path"]),
                    file_size=payload["size"],
                    page_count=payload["pages"],
                    archive_status="ARCHIVED",
                    download_status_yn="Y",
                )

        finally:
            await conn.close()

if __name__ == "__main__":
    lock_f = open(LOCK_FILE, "w")
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        asyncio.run(PDFArchiver().run())
    except (IOError, OSError):
        sys.exit(0)
    finally:
        try:
            lock_f.close()
        except Exception:
            pass
