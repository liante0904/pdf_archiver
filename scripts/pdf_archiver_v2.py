"""
pdf_archiver_v2.py — 차세대 PDF 아카이버

v1(pdf_archiver_async.py) 대비 개선점:
- hash 기반 중복 감지 → canonical PDF 재사용, 중복 업로드 방지
- Google Drive 저장 (RCLONE_REMOTE env로 설정)
- downloader registry 패턴 (증권사별 특수처리 모듈화)
- 단일 파일 ~500줄 목표

실행: uv run python scripts/pdf_archiver_v2.py
크론: */3 * * * * ... (v1과 동일한 주기로)
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import sys
import time
import logging
import fcntl
import tempfile
import unicodedata
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import aiohttp
import asyncpg

# ── bootstrap path ──────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from _bootstrap import build_postgres_dsn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pdf_archiver_v2")

# ── Config (env 기반) ───────────────────────────────────────
BATCH_SIZE = int(os.getenv("V2_BATCH_SIZE", "20"))
WORKERS = int(os.getenv("V2_WORKERS", "6"))
HTTP_TIMEOUT = int(os.getenv("V2_HTTP_TIMEOUT", "45"))
RCLONE_REMOTE = os.getenv("V2_RCLONE_REMOTE", os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf"))
RCLONE_BIN = os.getenv("RCLONE_BIN", "rclone")
RCLONE_CONFIG = os.getenv("RCLONE_CONFIG", os.path.expanduser("~/.config/rclone/rclone.conf"))
LOCAL_BUFFER = Path(os.getenv("V2_BUFFER_DIR", "/tmp/pdf_archiver_v2"))
DB_RETRY_LIMIT = int(os.getenv("V2_RETRY_LIMIT", "8"))
LOCK_FILE = "/tmp/pdf_archiver_v2.lock"

SOURCE_TABLE = '"tbl_sec_reports"'
ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'

# ── Downloader Registry ─────────────────────────────────────
# 각 증권사별로 download(url, target_path) -> bool|dict 를 구현한 함수를 등록
# 키워드 매칭: firm_nm 에 키워드가 포함되면 해당 downloader 사용

DOWNLOADER_REGISTRY = {}  # {keyword: downloader_function}

def register(keyword: str):
    """데코레이터: downloader registry 등록"""
    def decorator(fn):
        DOWNLOADER_REGISTRY[keyword] = fn
        return fn
    return decorator


def _select_downloader(firm_nm: str):
    """firm_nm 기준으로 등록된 downloader 찾기 (키워드 매칭)"""
    for keyword, fn in DOWNLOADER_REGISTRY.items():
        if keyword in (firm_nm or ""):
            return fn
    return None


# ── DB helpers ──────────────────────────────────────────────

async def db_connect() -> asyncpg.Connection:
    return await asyncpg.connect(build_postgres_dsn())


async def fetch_targets(conn: asyncpg.Connection, limit: int) -> list[asyncpg.Record]:
    """pdf_sync_status=0(대기) 또는 3(실패)인 레코드 fetch"""
    return await conn.fetch(
        f"""
        SELECT report_id, sec_firm_order, key, pdf_url, telegram_url, download_url,
               firm_nm, article_title, reg_dt, retry_count
        FROM {SOURCE_TABLE}
        WHERE pdf_sync_status IN (0, 3)
          AND COALESCE(retry_count, 0) < {DB_RETRY_LIMIT}
          AND (NULLIF(BTRIM(pdf_url), '') IS NOT NULL
               OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
               OR NULLIF(BTRIM(download_url), '') IS NOT NULL
               OR NULLIF(BTRIM(key), '') IS NOT NULL)
        ORDER BY retry_count ASC, reg_dt DESC, report_id ASC
        LIMIT $1
        """,
        limit,
    )


async def find_by_hash(conn: asyncpg.Connection, pdf_hash: str) -> Optional[dict]:
    """같은 hash를 가진 ARCHIVED 레코드 검색"""
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
                         title: str, reg_dt: str, pdf_url: str,
                         storage_key: str, file_size: int, page_count: int,
                         pdf_hash: str, pdf_hash_bytes: bytes, success: bool,
                         retry_delta: int = 0):
    """archive 테이블 UPSERT"""
    status = 2 if success else 3
    archive_status = "ARCHIVED" if success else "INIT"
    dl_yn = "Y" if success else "N"
    file_name = Path(storage_key).name if storage_key else None

    await conn.execute(
        f"""
        INSERT INTO {ARCHIVE_TABLE} (
            report_id, firm_nm, title, reg_dt, pdf_url, pdf_hash,
            storage_backend, storage_key, file_name, file_size, page_count,
            archive_status, download_status_yn, pdf_sync_status, sync_status,
            created_at, updated_at, retry_count
        ) VALUES ($1,$2,$3,$4,$5,$6,'googledrive',$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW(),NOW(),$16)
        ON CONFLICT (report_id) DO UPDATE SET
            firm_nm = EXCLUDED.firm_nm,
            title = EXCLUDED.title,
            reg_dt = EXCLUDED.reg_dt,
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
        report_id, firm_nm, title, reg_dt, pdf_url, pdf_hash_bytes,
        storage_key, file_name, file_size, page_count,
        archive_status, dl_yn, status, status, retry_delta,
    )


async def update_source_status(conn: asyncpg.Connection, report_id: int,
                               pdf_status: int, retry_delta: int = 0,
                               pdf_hash_bytes: bytes = None):
    """tbl_sec_reports 상태 업데이트"""
    await conn.execute(
        f"""
        UPDATE {SOURCE_TABLE}
        SET pdf_sync_status = $2,
            retry_count = COALESCE(retry_count, 0) + $3,
            pdf_hash = COALESCE($4, pdf_hash)
        WHERE report_id = $1
        """,
        report_id, pdf_status, retry_delta, pdf_hash_bytes,
    )


# ── rclone upload ───────────────────────────────────────────

async def rclone_upload(local_path: str, remote_path: str) -> bool:
    """rclone copyto → 성공 시 True"""
    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG

    remote_full = f"{RCLONE_REMOTE}/{remote_path}"
    cmd = [
        RCLONE_BIN, "--config", RCLONE_CONFIG,
        "copyto", str(local_path), remote_full,
        "--retries", "3",
        "--low-level-retries", "5",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        stderr_text = stderr.decode(errors="replace")[:300]
        log.warning(f"rclone upload failed: {stderr_text}")
        return False
    return True


# ── File path builder ───────────────────────────────────────

def build_storage_key(firm: str, title: str, reg_dt: str, report_id: int) -> str:
    """GDrive/OneDrive 경로 생성: YYYY-MM/firm/YYMMDD_title_report_id.pdf"""
    clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
    y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
    yy_mm_dd = clean_dt[2:8]
    normalized = unicodedata.normalize('NFC', title or '')
    safe_title = re.sub(r'[\\\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]()]', ' ', normalized)
    safe_title = '_'.join(safe_title.split())[:60].strip('_') or 'untitled'
    filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
    return f"{y_m}/{firm}/{filename}"


# ── PDF download (generic wget) ─────────────────────────────

async def _download_wget(url: str, target_path: Path, timeout: int = 30) -> bool:
    """wget으로 PDF 다운로드"""
    cmd = [
        "wget", "-q", "-O", str(target_path),
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        f"--timeout={timeout}", "--tries=2", "--no-check-certificate",
        url,
    ]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()
    return proc.returncode == 0 and target_path.exists() and target_path.stat().st_size > 1024


async def _is_pdf(path: Path) -> bool:
    """파일이 PDF인지 확인 (매직 바이트 검사)"""
    try:
        header = path.read_bytes()[:5]
        return header.startswith(b"%PDF-")
    except Exception:
        return False


# ── Hash computation ────────────────────────────────────────

def compute_hash(path: Path) -> tuple[str, bytes]:
    """파일의 SHA-256 해시 계산 → (hex, bytes)"""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest(), digest.digest()


# ── Main download + dedup logic ─────────────────────────────

async def process_one(sem: asyncio.Semaphore, conn: asyncpg.Connection,
                      row: asyncpg.Record) -> bool:
    """레코드 1건 처리: 다운로드 → hash → 중복검사 → 업로드/참조"""
    async with sem:
        report_id = row["report_id"]
        firm = row["firm_nm"] or "UNKNOWN"
        title = row["article_title"] or "untitled"
        reg_dt = row["reg_dt"] or ""
        pdf_url = row["pdf_url"] or row["key"] or row["telegram_url"] or row["download_url"]

        if not pdf_url:
            await update_source_status(conn, report_id, 3, 1)
            return False

        storage_key = build_storage_key(firm, title, reg_dt, report_id)
        local_target = LOCAL_BUFFER / storage_key
        local_target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_target.with_suffix(".tmp")

        # 1. 다운로드 시도 (downloader registry → fallback wget)
        ok = False
        downloader = _select_downloader(firm)
        if downloader:
            try:
                ok = await downloader(pdf_url, tmp_path)
            except Exception as e:
                log.warning(f"[{report_id}] custom downloader error: {e}")

        if not ok:
            ok = await _download_wget(pdf_url, tmp_path)

        if not ok or not _is_pdf(tmp_path):
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            await update_source_status(conn, report_id, 3, 1)
            return False

        # 2. hash 계산
        pdf_hash_hex, pdf_hash_bytes = compute_hash(tmp_path)
        file_size = tmp_path.stat().st_size

        # 3. 중복 검사
        existing = await find_by_hash(conn, pdf_hash_hex)
        if existing:
            # 중복: canonical 참조만 복사, 업로드 스킵
            log.info(f"[{report_id}] DUPLICATE → canonical={existing['report_id']} hash={pdf_hash_hex[:16]}...")
            await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
                                 existing["storage_key"], existing["file_size"] or file_size,
                                 existing["page_count"] or 0, pdf_hash_hex, pdf_hash_bytes, True)
            await update_source_status(conn, report_id, 2, 0, pdf_hash_bytes)
            tmp_path.unlink(missing_ok=True)
            return True

        # 4. 신규: tmp → final, rclone 업로드
        tmp_path.rename(local_target)
        uploaded = await rclone_upload(str(local_target), storage_key)

        if uploaded:
            await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
                                 storage_key, file_size, 0, pdf_hash_hex, pdf_hash_bytes, True)
            await update_source_status(conn, report_id, 2, 0, pdf_hash_bytes)
            local_target.unlink(missing_ok=True)
            log.info(f"[{report_id}] UPLOADED {firm} | {title[:30]}...")
            return True
        else:
            # 업로드 실패 → 파일 보존, 다음 run에서 재시도
            await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
                                 storage_key, file_size, 0, pdf_hash_hex, pdf_hash_bytes, False)
            await update_source_status(conn, report_id, 3, 1, pdf_hash_bytes)
            log.warning(f"[{report_id}] UPLOAD FAILED {firm} | {title[:30]}...")
            return False


# ── Orchestrator ────────────────────────────────────────────

async def run():
    conn = await db_connect()
    try:
        LOCAL_BUFFER.mkdir(parents=True, exist_ok=True)
        sem = asyncio.Semaphore(WORKERS)

        while True:
            targets = await fetch_targets(conn, BATCH_SIZE)
            if not targets:
                log.info("No pending targets.")
                break

            log.info(f"Batch: {len(targets)} targets")
            results = await asyncio.gather(
                *(process_one(sem, conn, t) for t in targets),
                return_exceptions=True,
            )
            ok = sum(1 for r in results if r is True)
            fail = len(targets) - ok
            log.info(f"Batch done: {ok} ok, {fail} fail")

            if len(targets) < BATCH_SIZE:
                break

            await asyncio.sleep(1)  # small breather between batches

    finally:
        await conn.close()
        # cleanup empty dirs
        for root, dirs, files in os.walk(LOCAL_BUFFER, topdown=False):
            for d in dirs:
                try:
                    os.rmdir(os.path.join(root, d))
                except OSError:
                    pass


def acquire_lock() -> bool:
    """파일 락 획득 → 이미 실행 중이면 False"""
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
        log.info("Already running (lock held). Exiting.")
        sys.exit(0)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(130)
