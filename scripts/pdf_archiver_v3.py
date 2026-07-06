"""
pdf_archiver_v3.py — cloud_store 기반 차세대 PDF 아카이버

v2 대비 개선:
- cloud_store.CloudStore 로 rclone 추상화 (QuotaAwareUploader 내장)
- upload 로직 단순화 (quota 감지/backoff 라이브러리에서 처리)
- 나머지 로직(downloader, dedup, DB)은 v2와 동일

실행: uv run python scripts/pdf_archiver_v3.py
병행 테스트: v2 cron 돌리는 동안 수동 실행으로 비교 검증
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import sys
import logging
import fcntl
import unicodedata
from pathlib import Path
from typing import Optional

import asyncpg

# ── paths ─────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
LIB_ROOT = Path("/home/ubuntu/workspace/lib")
for p in [str(REPO_ROOT), str(LIB_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from _bootstrap import build_postgres_dsn
from cloud_store import CloudStore, QuotaExceededError

from downloaders import (
    download_ds_pdf, download_mirae_pdf, download_kyobo_pdf,
    download_hana_pdf, download_ls_pdf, download_dbfi_pdf,
    download_heungkuk_pdf, download_meritz_pdf,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pdf_archiver_v3")

# ── Config ───────────────────────────────────────────────────
BATCH_SIZE = int(os.getenv("V3_BATCH_SIZE", os.getenv("V2_BATCH_SIZE", "20")))
WORKERS = int(os.getenv("V3_WORKERS", os.getenv("V2_WORKERS", "6")))
HTTP_TIMEOUT = int(os.getenv("V3_HTTP_TIMEOUT", os.getenv("V2_HTTP_TIMEOUT", "45")))
RCLONE_REMOTE = os.getenv("V3_RCLONE_REMOTE", os.getenv("RCLONE_REMOTE", "gdrive:archive/pdf"))
RCLONE_CONFIG = os.getenv("RCLONE_CONFIG", os.path.expanduser("~/.config/rclone/rclone.conf"))
LOCAL_BUFFER = Path(os.getenv("V3_BUFFER_DIR", os.getenv("V2_BUFFER_DIR", "/tmp/pdf_archiver_v3")))
DB_RETRY_LIMIT = int(os.getenv("V3_RETRY_LIMIT", os.getenv("V2_RETRY_LIMIT", "8")))

# v3 uses a different lock file so it doesn't conflict with v2 cron
LOCK_FILE = "/tmp/pdf_archiver_v3.lock"

# Quota settings (can override via env)
MAX_QUOTA_FAILURES = int(os.getenv("V3_MAX_QUOTA_FAILURES", "12"))
MAX_RUNTIME_SECONDS = int(os.getenv("V3_MAX_RUNTIME", "1800"))
QUOTA_BACKOFF_BASE = float(os.getenv("V3_QUOTA_BACKOFF_BASE", "5.0"))

SOURCE_TABLE = '"tbl_sec_reports"'
ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'

# ── Downloader Registry ─────────────────────────────────────
DOWNLOADER_REGISTRY = {
    "DS": download_ds_pdf,
    "미래에셋": download_mirae_pdf,
    "교보": download_kyobo_pdf,
    "하나": download_hana_pdf,
    "LS": download_ls_pdf,
    "흥국": download_heungkuk_pdf,
    "메리츠": download_meritz_pdf,
}
DBFI_FIRM_ORDER = 19


def _select_downloader(firm_nm: str, sec_firm_order: int = 0):
    for keyword, fn in DOWNLOADER_REGISTRY.items():
        if keyword in (firm_nm or ""):
            return fn
    if sec_firm_order == DBFI_FIRM_ORDER:
        return download_dbfi_pdf
    return None


# ── DB helpers ──────────────────────────────────────────────

async def db_connect() -> asyncpg.Connection:
    return await asyncpg.connect(build_postgres_dsn(), ssl=False)


async def fetch_targets(conn: asyncpg.Connection, limit: int) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"""
        SELECT s.report_id, s.firm_id, s.report_unique_key, s.pdf_url, s.telegram_url, s.download_url,
               s.firm_nm, s.article_title, s.report_date,
               COALESCE(a.retry_count, 0) as retry_count
        FROM {SOURCE_TABLE} s
        LEFT JOIN {ARCHIVE_TABLE} a ON s.report_id = a.report_id
        WHERE COALESCE(a.pdf_sync_status, 0) IN (0, 3)
          AND COALESCE(a.retry_count, 0) < {DB_RETRY_LIMIT}
          AND (NULLIF(BTRIM(s.pdf_url), '') IS NOT NULL
               OR NULLIF(BTRIM(s.telegram_url), '') IS NOT NULL
               OR NULLIF(BTRIM(s.download_url), '') IS NOT NULL
               OR NULLIF(BTRIM(s.report_unique_key), '') IS NOT NULL)
        ORDER BY COALESCE(a.retry_count, 0) ASC, s.report_date DESC, s.report_id ASC
        LIMIT $1
        """,
        limit,
    )


async def find_by_hash(conn: asyncpg.Connection, pdf_hash: str) -> Optional[dict]:
    row = await conn.fetchrow(
        f"""
        SELECT report_id, storage_key, file_size, page_count, pdf_hash
        FROM {ARCHIVE_TABLE}
        WHERE encode(pdf_hash, 'hex') = $1
          AND archive_status = 'ARCHIVED'
        LIMIT 1
        """,
        pdf_hash,
    )
    return dict(row) if row else None


async def upsert_archive(conn: asyncpg.Connection, report_id: int, firm_nm: str,
                         title: str, report_date: str, pdf_url: str,
                         storage_key: str, file_size: int, page_count: int,
                         pdf_hash: str, pdf_hash_bytes: bytes, success: bool,
                         retry_delta: int = 0):
    status = 2 if success else 3
    archive_status = "ARCHIVED" if success else "INIT"
    dl_yn = "Y" if success else "N"
    file_name = Path(storage_key).name if storage_key else None

    await conn.execute(
        f"""
        INSERT INTO {ARCHIVE_TABLE} (
            report_id, firm_nm, title, report_date, pdf_url, pdf_hash,
            storage_backend, storage_key, file_name, file_size, page_count,
            archive_status, download_status_yn, pdf_sync_status, sync_status,
            created_at, updated_at, retry_count
        ) VALUES ($1,$2,$3,$4,$5,$6,'googledrive',$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW(),$15)
        ON CONFLICT (report_id) DO UPDATE SET
            firm_nm = EXCLUDED.firm_nm,
            title = EXCLUDED.title,
            report_date = EXCLUDED.report_date,
            pdf_url = EXCLUDED.pdf_url,
            pdf_hash = COALESCE(EXCLUDED.pdf_hash, {ARCHIVE_TABLE}.pdf_hash),
            storage_backend = 'googledrive',
            storage_key = COALESCE(EXCLUDED.storage_key, {ARCHIVE_TABLE}.storage_key),
            file_name = COALESCE(EXCLUDED.file_name, {ARCHIVE_TABLE}.file_name),
            file_size = COALESCE(EXCLUDED.file_size, {ARCHIVE_TABLE}.file_size),
            page_count = COALESCE(EXCLUDED.page_count, {ARCHIVE_TABLE}.page_count),
            archive_status = EXCLUDED.archive_status,
            download_status_yn = EXCLUDED.download_status_yn,
            pdf_sync_status = EXCLUDED.pdf_sync_status,
            sync_status = COALESCE({ARCHIVE_TABLE}.sync_status, EXCLUDED.sync_status),
            retry_count = COALESCE({ARCHIVE_TABLE}.retry_count, 0) + EXCLUDED.retry_count,
            updated_at = NOW()
        """,
        report_id, firm_nm, title, report_date, pdf_url, pdf_hash_bytes,
        storage_key, file_name, file_size, page_count,
        archive_status, dl_yn, status, status, retry_delta,
    )


# ── File path builder ───────────────────────────────────────

def build_storage_key(firm: str, title: str, report_date: str, report_id: int) -> str:
    clean_dt = re.sub(r'[^0-9]', '', str(report_date)) if report_date else "00000000"
    y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
    yy_mm_dd = clean_dt[2:8]
    normalized = unicodedata.normalize('NFC', title or '')
    safe_title = re.sub(r'[\\\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]()]', ' ', normalized)
    safe_title = '_'.join(safe_title.split())[:60].strip('_') or 'untitled'
    filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
    return f"{y_m}/{firm}/{filename}"


# ── PDF download (curl-based) ──────────────────────────────

def _clean_url(url: str) -> str:
    import re
    return re.sub(r"[')]+\s*$", "", url.strip())


def _encode_url(url: str) -> str:
    from urllib.parse import quote, unquote, urlparse, urlunparse
    parts = list(urlparse(url))
    parts = [
        parts[0], parts[1],
        quote(unquote(parts[2]), safe='/:@!$&*()+,;='),
        parts[3],
        quote(unquote(parts[4]), safe='/:@!$&*()+,;='),
        parts[5],
    ]
    return urlunparse(parts)


async def _download_wget(url: str, target_path: Path, timeout: int = 30) -> bool:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    url = _clean_url(url)
    encoded_url = _encode_url(url)
    from urllib.parse import urlparse as _urlparse
    try:
        parsed = _urlparse(url)
        referer = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        referer = url.rsplit('/', 1)[0] if '/' in url else url

    cmd = [
        "curl", "-sL", "-o", str(target_path),
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "-H", f"Referer: {referer}",
        "-H", "Accept: application/pdf,*/*",
        "--max-time", str(timeout),
        "--retry", "2", "--insecure",
        encoded_url,
    ]
    proc = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.PIPE)
    _, stderr = await proc.communicate()
    exists = target_path.exists()
    size = target_path.stat().st_size if exists else -1
    if proc.returncode != 0 or not exists or size <= 1024:
        err = stderr.decode(errors="replace")[:200] if stderr else ""
        log.warning(f"curl failed rc={proc.returncode} exists={exists} size={size} err={err}")
        return False
    return True


async def _is_pdf(path: Path) -> bool:
    try:
        return path.read_bytes()[:5].startswith(b"%PDF-")
    except Exception:
        return False


def compute_hash(path: Path) -> tuple[str, bytes]:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest(), digest.digest()


# ── Main processing ─────────────────────────────────────────

async def process_one(
    sem: asyncio.Semaphore,
    rclone_sem: asyncio.Semaphore,
    store: CloudStore,
    conn: asyncpg.Connection,
    db_lock: asyncio.Lock,
    row: asyncpg.Record,
):
    """레코드 1건 처리: 다운로드 → hash → 중복검사 → 업로드

    Returns:
        True: uploaded or duplicate found
        False: download or non-quota upload failure
        "quota": GDrive API quota exceeded (from CloudStore)
    """
    async with sem:
        report_id = row["report_id"]
        firm = row["firm_nm"] or "UNKNOWN"
        title = row["article_title"] or "untitled"
        report_date = row["report_date"] or ""
        report_date_str = str(report_date) if not isinstance(report_date, str) else report_date
        pdf_url = row["pdf_url"] or row["report_unique_key"] or row["telegram_url"] or row["download_url"]
        sec_order = row["firm_id"] or 0

        if not pdf_url:
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key="", file_size=0, page_count=0,
                                     pdf_hash="", pdf_hash_bytes=None, success=False, retry_delta=1)
            return False

        storage_key = build_storage_key(firm, title, report_date_str, report_id)
        local_target = LOCAL_BUFFER / storage_key
        local_target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_target.with_suffix(".tmp")

        # 1. download
        ok = False
        downloader = _select_downloader(firm, sec_order)
        if downloader:
            try:
                candidates = [
                    u for u in [row["pdf_url"], row["telegram_url"], row["download_url"], row["report_unique_key"]]
                    if u and str(u).strip()
                ]
                result = await downloader(candidates, local_target, title, report_id, firm, report_date)
                if result and isinstance(result, dict):
                    ok = True
                    if tmp_path.exists():
                        tmp_path.unlink(missing_ok=True)
            except Exception as e:
                log.warning(f"[{report_id}] custom downloader error: {e}")

        if not ok:
            ok = await _download_wget(pdf_url, tmp_path)
            if not ok:
                log.warning(f"[{report_id}] wget failed: {pdf_url[:100]}...")

        # 2. determine work path
        if ok:
            if local_target.exists() and local_target.stat().st_size > 1024:
                work_path = local_target
            elif tmp_path.exists() and tmp_path.stat().st_size > 1024:
                work_path = tmp_path
            else:
                ok = False
            if ok and not await _is_pdf(work_path):
                log.warning(f"[{report_id}] not a PDF, discarding size={work_path.stat().st_size}")
                ok = False

        if not ok:
            for p in [tmp_path, local_target]:
                if p.exists():
                    p.unlink(missing_ok=True)
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key="", file_size=0, page_count=0,
                                     pdf_hash="", pdf_hash_bytes=None, success=False, retry_delta=1)
            return False

        # 3. hash
        pdf_hash_hex, pdf_hash_bytes = compute_hash(work_path)
        file_size = work_path.stat().st_size

        # 4. dedup
        async with db_lock:
            existing = await find_by_hash(conn, pdf_hash_hex)
        if existing:
            log.info(f"[{report_id}] DUPLICATE → canonical={existing['report_id']} hash={pdf_hash_hex[:16]}...")
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     existing["storage_key"], existing["file_size"] or file_size,
                                     existing["page_count"] or 0, pdf_hash_hex, pdf_hash_bytes, True, retry_delta=0)
            work_path.unlink(missing_ok=True)
            return True

        # 5. upload via cloud_store
        if work_path != local_target:
            work_path.rename(local_target)
            work_path = local_target

        try:
            async with rclone_sem:
                await store.upload(str(local_target), storage_key)
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key, file_size, page_count=0,
                                     pdf_hash=pdf_hash_hex, pdf_hash_bytes=pdf_hash_bytes, success=True, retry_delta=0)
            local_target.unlink(missing_ok=True)
            log.info(f"[{report_id}] UPLOADED {firm} | {title[:30]}...")
            return True
        except QuotaExceededError:
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key, file_size, page_count=0,
                                     pdf_hash=pdf_hash_hex, pdf_hash_bytes=pdf_hash_bytes, success=False, retry_delta=0)
            log.warning(f"[{report_id}] QUOTA_EXCEEDED (retry not incremented) {firm} | {title[:30]}...")
            return "quota"
        except Exception as e:
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key, file_size, page_count=0,
                                     pdf_hash=pdf_hash_hex, pdf_hash_bytes=pdf_hash_bytes, success=False, retry_delta=1)
            log.warning(f"[{report_id}] UPLOAD FAILED {firm} | {title[:30]}... ({e})")
            return False


# ── Orchestrator ────────────────────────────────────────────

async def run():
    conn = await db_connect()
    import datetime
    start_time = datetime.datetime.now()
    consecutive_quota_failures = 0
    db_lock = asyncio.Lock()
    rclone_sem = asyncio.Semaphore(int(os.getenv("V3_RCLONE_WORKERS", "1")))

    async with CloudStore(RCLONE_REMOTE, config=RCLONE_CONFIG) as store:
        try:
            LOCAL_BUFFER.mkdir(parents=True, exist_ok=True)
            sem = asyncio.Semaphore(WORKERS)

            while True:
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                if elapsed > MAX_RUNTIME_SECONDS:
                    log.warning(f"Max runtime {MAX_RUNTIME_SECONDS}s reached. Exiting.")
                    break

                targets = await fetch_targets(conn, BATCH_SIZE)
                if not targets:
                    log.info("No pending targets.")
                    break

                log.info(f"Batch: {len(targets)} targets (quota_fails={consecutive_quota_failures}, elapsed={elapsed:.0f}s)")
                results = await asyncio.gather(
                    *(process_one(sem, rclone_sem, store, conn, db_lock, t) for t in targets),
                    return_exceptions=True,
                )

                ok = fail = quota = 0
                for r in results:
                    if r is True:
                        ok += 1
                    elif r == "quota":
                        quota += 1
                    elif isinstance(r, Exception):
                        log.error(f"Batch exception: {type(r).__name__}: {r}")
                        fail += 1
                    else:
                        fail += 1

                log.info(f"Batch done: {ok} ok, {fail} fail, {quota} quota_exceeded")

                if quota > 0:
                    consecutive_quota_failures += quota
                    if consecutive_quota_failures >= MAX_QUOTA_FAILURES:
                        log.error(f"Too many quota failures ({consecutive_quota_failures}). Giving up.")
                        break
                    delay = QUOTA_BACKOFF_BASE * (2 ** min(consecutive_quota_failures, 6))
                    log.warning(f"Quota backoff: sleeping {delay:.0f}s (failures={consecutive_quota_failures})")
                    await asyncio.sleep(delay)
                elif ok > 0:
                    consecutive_quota_failures = 0

                if len(targets) < BATCH_SIZE:
                    break
                await asyncio.sleep(1)

        finally:
            await conn.close()
            for root, dirs, files in os.walk(LOCAL_BUFFER, topdown=False):
                for d in dirs:
                    try:
                        os.rmdir(os.path.join(root, d))
                    except OSError:
                        pass


def acquire_lock() -> bool:
    """v3 전용 락 (v2와 충돌하지 않도록 별도 lock file 사용)"""
    try:
        with open(LOCK_FILE, "r") as f:
            old_pid = f.read().strip()
        if old_pid:
            try:
                os.kill(int(old_pid), 0)
            except (OSError, ValueError):
                log.warning(f"Stale lock (PID {old_pid} gone). Cleaning up.")
                try:
                    os.unlink(LOCK_FILE)
                except OSError:
                    pass
    except FileNotFoundError:
        pass

    try:
        lock_f = open(LOCK_FILE, "w")
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_f.write(str(os.getpid()))
        lock_f.flush()
        return True
    except (IOError, OSError):
        return False


if __name__ == "__main__":
    if not acquire_lock():
        log.info("v3 already running (lock held). Exiting.")
        sys.exit(0)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(130)
