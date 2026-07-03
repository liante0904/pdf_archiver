#!/usr/bin/env python3
"""
LS증권 실패 건 보정처리 스크립트
- pdf_sync_status=3 인 LS증권 전체 재시도
- msg.ls-sec.co.kr (직접 PDF): HTTPS 직통
- upload 경로: WARP HTTP proxy -> View.jsp 파싱 -> download.jsp?dataType= 추출
"""

import asyncio
import aiohttp
import aiohttp_socks
import hashlib
import json
import os
import re
import sys
import logging
import unicodedata
import subprocess
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(os.path.expanduser("~/logs/retry_ls_failed.log"))],
)
log = logging.getLogger(__name__)

LOCAL_DIR = Path(os.path.expanduser("~/downloads/pdf_archive_temp"))
WARP_SOCKS5 = "socks5://127.0.0.1:9091"
CONCURRENCY = 5
BATCH_SIZE = 50
PSQL = "docker exec main-postgres psql -U ssh_reports_hub -d ssh_reports_hub"


# ---- DB 헬퍼 ----

def _fetch_json(sql: str) -> list[dict]:
    cmd = f"""{PSQL} -t -A -F'||' -c "SELECT row_to_json(t) FROM ({sql}) t;" """
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        rows = []
        for line in result.stdout.strip().split("\n"):
            line = line.strip()
            if line:
                try: rows.append(json.loads(line))
                except: pass
        return rows
    except Exception as e:
        log.error(f"DB fetch error: {e}")
        return []


def _sql_exec(query: str):
    try:
        subprocess.run(f"""{PSQL} -c {json.dumps(query)}""", shell=True, capture_output=True, timeout=60, check=True)
        return True
    except Exception as e:
        log.error(f"SQL exec error: {e}")
        return False


# ---- 파일/PDF 헬퍼 ----

def _safe_filename(title: str, max_len: int = 60) -> str:
    norm = unicodedata.normalize("NFC", title or "")
    safe = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]()]', ' ', norm)
    safe = "_".join(safe.split())[:max_len].strip("_") or "untitled"
    return safe


def _make_path(firm: str, title: str, report_date: str, report_id: int) -> Path:
    cd = re.sub(r'[^0-9]', '', str(report_date)) or "00000000"
    ym = f"{cd[:4]}-{cd[4:6]}"
    fn = f"{cd[2:8]}_{_safe_filename(title)}_{report_id}.pdf"
    return LOCAL_DIR / ym / firm / fn


def _is_pdf(data: bytes) -> bool:
    if not data or len(data) < 10: return False
    return data[:10].find(b"%PDF") != -1 or data[:10].find(b"\xef\xbb\xbf%PDF") != -1


def _pdf_hash_hex(data: bytes) -> str | None:
    return hashlib.sha256(data).hexdigest() if data else None


def _page_count(path: Path) -> int:
    try:
        r = subprocess.run(["grep", "-a", "/Count", str(path)], capture_output=True, text=True, timeout=5)
        m = re.search(r"/Count (\d+)", r.stdout)
        return int(m.group(1)) if m else 0
    except: return 0


# ---- LS증권 다운로드 ----

def _find_msg_url(rec: dict) -> str | None:
    for k in ("pdf_url", "report_unique_key", "download_url", "telegram_url"):
        v = str(rec.get(k) or "")
        if "msg.ls-sec.co.kr" in v:
            return v
    return None


def _find_view_url(rec: dict) -> str | None:
    for k in ("report_unique_key", "pdf_url"):
        v = str(rec.get(k) or "")
        if "View.jsp" in v:
            return v
    return None


async def _try_msg_direct(msg_url: str, timeout=30) -> tuple[bool, bytes | None]:
    """msg.ls-sec.co.kr 직접 HTTPS"""
    try:
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as s:
            async with s.get(msg_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                body = await r.read()
                if _is_pdf(body):
                    return True, body
        return False, None
    except Exception as e:
        log.debug(f"msg direct error: {e}")
        return False, None


async def _try_viewjsp_parse(view_url: str, timeout=30) -> tuple[bool, bytes | None]:
    """View.jsp -> download key -> PDF via WARP HTTP proxy"""
    try:
        http_url = view_url.replace("https://", "http://", 1)
        conn = aiohttp_socks.ProxyConnector.from_url(WARP_SOCKS5)
        async with aiohttp.ClientSession(connector=conn) as s:
            # 1. View.jsp fetch
            async with s.get(http_url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                html = await r.read()
            # 2. extract download key
            dec = html.decode("euc-kr", errors="replace")
            m = re.search(r'download\("([^"]+)"\)', dec)
            if not m:
                return False, None
            ek = m.group(1)
            # 3. download PDF
            dw_url = f"http://www.ls-sec.co.kr/_bt_lib/util/download.jsp?dataType={ek}"
            async with s.get(dw_url, headers={"User-Agent": "Mozilla/5.0", "Referer": http_url}, timeout=aiohttp.ClientTimeout(total=60)) as r2:
                body = await r2.read()
                if _is_pdf(body):
                    return True, body
        return False, None
    except Exception as e:
        log.debug(f"View.jsp parse error: {e}")
        return False, None


async def process_one(rec: dict) -> dict | None:
    rid = int(rec["report_id"])
    firm = rec["firm_nm"]
    title = str(rec.get("article_title") or "")
    report_date = str(rec.get("report_date") or "")
    target = _make_path(firm, title, report_date, rid)

    # 이미 있으면 skip
    if target.exists() and target.stat().st_size > 1024 and _is_pdf(target.read_bytes()):
        log.info(f"  SKIP (exists): {target.name}")
        return _mk_payload(rec, target, target.read_bytes())

    ok = False
    body = None

    # 1) msg direct
    msg_url = _find_msg_url(rec)
    if msg_url:
        ok, body = await _try_msg_direct(msg_url)

    # 2) View.jsp parsing
    if not ok:
        view_url = _find_view_url(rec)
        if view_url:
            ok, body = await _try_viewjsp_parse(view_url)

    if ok and body:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        log.info(f"  ✅ {target.name} ({len(body)} bytes)")
        return _mk_payload(rec, target, body)

    log.warning(f"  ❌ [{rid}] {title[:30]}...")
    return None


def _mk_payload(rec: dict, path: Path, body: bytes) -> dict:
    return {
        "report_id": int(rec["report_id"]),
        "firm_nm": rec["firm_nm"],
        "title": str(rec.get("article_title") or ""),
        "report_date": str(rec.get("report_date") or ""),
        "pdf_url": str(rec.get("pdf_url") or ""),
        "download_url": str(rec.get("download_url") or ""),
        "telegram_url": str(rec.get("telegram_url") or ""),
        "report_unique_key": str(rec.get("report_unique_key") or ""),
        "pdf_hash_hex": _pdf_hash_hex(body),
        "file_path": str(path),
        "file_size": len(body),
        "page_count": _page_count(path),
    }


# ---- rclone + DB 업데이트 ----

def rclone_upload() -> bool:
    log.info("Uploading via rclone copy...")
    r = subprocess.run([
        "rclone", "--config", os.path.expanduser("~/.config/rclone/rclone.conf"),
        "copy", str(LOCAL_DIR), "onedrive:/archive/pdf",
        "--include", "*.pdf", "--transfers", "8",
        "--no-traverse", "--onedrive-chunk-size", "64000k",
    ], capture_output=True, text=True, timeout=300)

    if r.returncode == 0:
        log.info("rclone upload OK")
        # 성공 시 모든 로컬 파일 삭제 (copy였으므로 수동 삭제)
        for p in Path(LOCAL_DIR).rglob("*.pdf"):
            try: p.unlink()
            except: pass
        return True

    # returncode != 0 인 경우 전체 검증 시작
    stderr = r.stderr or ""
    log.warning(f"rclone reported errors. Starting size-based verification... (stderr: {stderr[:200]}...)")
    
    # 로컬에 남은 pdf 파일들 확인하여 원격지와 크기 대조
    for p in Path(LOCAL_DIR).rglob("*.pdf"):
        rel_path = p.relative_to(LOCAL_DIR)
        remote_path = f"onedrive:/archive/pdf/{rel_path}"
        
        # rclone size 로 원격 크기 확인
        sr = subprocess.run(["rclone", "size", "--json", remote_path], capture_output=True, text=True)
        if sr.returncode == 0:
            try:
                import json
                remote_info = json.loads(sr.stdout)
                if remote_info.get("count", 0) > 0 and remote_info.get("total_bytes") == p.stat().st_size:
                    log.info(f"  Match found (size={p.stat().st_size}), deleting local: {rel_path}")
                    p.unlink()
                elif remote_info.get("total_bytes") == 0:
                    log.warning(f"  Remote file is 0 bytes, deleting and retrying now: {rel_path}")
                    # 0바이트 파일 원격에서 삭제
                    subprocess.run(["rclone", "deletefile", remote_path], capture_output=True)
                    
                    # 즉시 재업로드 시도
                    rp = subprocess.run([
                        "rclone", "--config", os.path.expanduser("~/.config/rclone/rclone.conf"),
                        "copyto", str(p), remote_path,
                        "--onedrive-chunk-size", "64000k",
                        "--retries", "3", "--onedrive-no-versions"
                    ], capture_output=True)
                    
                    if rp.returncode == 0:
                        # 재확인
                        sr2 = subprocess.run(["rclone", "size", "--json", remote_path], capture_output=True, text=True)
                        if sr2.returncode == 0:
                            try:
                                remote_info2 = json.loads(sr2.stdout)
                                if remote_info2.get("count", 0) > 0 and remote_info2.get("total_bytes") == p.stat().st_size:
                                    log.info(f"  Retry upload successful, deleting local: {rel_path}")
                                    p.unlink()
                            except Exception:
                                pass
            except Exception:
                pass
    
    # 다시 확인: LOCAL_DIR에 pdf가 남아있는지
    remaining = list(Path(LOCAL_DIR).rglob("*.pdf"))
    if not remaining:
        log.info("All files verified and local buffer cleared. Proceeding with DB update.")
        return True
    else:
        log.error(f"Some files could not be verified and remain in buffer: {[str(r) for r in remaining]}")

    return False


def _sq(v):
    if v is None: return "NULL"
    return "'" + str(v).replace("'", "''") + "'"

def _hex2bytea(h):
    if h is None: return "NULL"
    return f"'\\x{h}'::bytea"

def update_db(payloads: list[dict]):
    if not payloads: return
    log.info(f"DB update {len(payloads)}건...")

    for p in payloads:
        sk = str(Path(p["file_path"]).relative_to(LOCAL_DIR))
        fn = Path(p["file_path"]).name

        _sql_exec(f"""
            UPDATE tbl_sec_reports
            SET pdf_sync_status = 2,
                retry_count = COALESCE(retry_count, 0) + 1,
                pdf_hash = {_hex2bytea(p['pdf_hash_hex'])}
            WHERE report_id = {p['report_id']};
        """)

        _sql_exec(f"""
            INSERT INTO tbl_sec_reports_pdf_archive (
                report_id, firm_nm, title, report_date, pdf_url,
                pdf_hash, storage_backend, storage_key,
                download_url, telegram_url, key,
                archive_status, file_name, download_status_yn,
                file_path, file_size, page_count,
                pdf_sync_status, created_at, updated_at, retry_count
            ) VALUES (
                {p['report_id']}, {_sq(p['firm_nm'])}, {_sq(p['title'])}, {_sq(p['report_date'])}, {_sq(p['pdf_url'])},
                {_hex2bytea(p['pdf_hash_hex'])}, 'onedrive', {_sq(sk)},
                {_sq(p['download_url'])}, {_sq(p['telegram_url'])}, {_sq(p['report_unique_key'])},
                'ARCHIVED', {_sq(fn)}, 'Y',
                {_sq(p['file_path'])}, {p['file_size']}, {p['page_count']},
                2, NOW(), NOW(), 1
            )
            ON CONFLICT (report_id) DO UPDATE SET
                firm_nm = EXCLUDED.firm_nm, title = EXCLUDED.title,
                pdf_hash = COALESCE(EXCLUDED.pdf_hash, tbl_sec_reports_pdf_archive.pdf_hash),
                storage_backend = EXCLUDED.storage_backend,
                storage_key = EXCLUDED.storage_key,
                archive_status = EXCLUDED.archive_status,
                file_name = EXCLUDED.file_name,
                download_status_yn = EXCLUDED.download_status_yn,
                file_path = EXCLUDED.file_path,
                file_size = EXCLUDED.file_size,
                page_count = EXCLUDED.page_count,
                pdf_sync_status = EXCLUDED.pdf_sync_status,
                updated_at = NOW(),
                retry_count = COALESCE(tbl_sec_reports_pdf_archive.retry_count, 0) + 1;
        """)

    log.info(f"DB update OK: {len(payloads)}건")


# ---- 메인 ----

async def main():
    log.info("=" * 60)
    log.info("LS증권 실패 건 보정처리 시작")
    log.info("=" * 60)

    all_rows = _fetch_json("""
        SELECT report_id, firm_nm, article_title, pdf_url, report_unique_key, download_url, telegram_url, report_date
        FROM tbl_sec_reports
        WHERE firm_nm LIKE '%LS%' AND pdf_sync_status = 3
        ORDER BY report_date DESC
    """)
    total = len(all_rows)
    log.info(f"대상: {total}건")

    if total == 0:
        log.info("처리할 레코드 없음")
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    success = []
    done = 0

    async def limited(r):
        async with sem:
            return await process_one(r)

    for i in range(0, total, BATCH_SIZE):
        batch = all_rows[i:i + BATCH_SIZE]
        log.info(f"\n--- 배치 {i//BATCH_SIZE + 1}/{(total-1)//BATCH_SIZE + 1} ({i+1}~{i+len(batch)}/{total}) ---")

        results = await asyncio.gather(*[limited(r) for r in batch])
        batch_ok = [p for p in results if p is not None]
        done += len(batch)
        success.extend(batch_ok)

        log.info(f"  배치 결과: {len(batch_ok)}/{len(batch)} 성공 (누적: {len(success)}/{done})")

        # 매 배치마다 rclone + DB
        if batch_ok:
            if rclone_upload():
                update_db(batch_ok)
            else:
                log.warning("  rclone 실패, DB 업데이트는 다음에 재시도")

    log.info("\n" + "=" * 60)
    log.info(f"최종 결과: {len(success)}/{total} 성공")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
