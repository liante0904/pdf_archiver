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
import datetime
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
from typing import Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import asyncpg

# ── bootstrap path ──────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from _bootstrap import build_postgres_dsn

# ── Downloader imports (populate registry) ──────────────────
from downloaders import (
    download_ds_pdf, download_mirae_pdf, download_kyobo_pdf,
    download_hana_pdf, download_ls_pdf, download_dbfi_pdf,
    download_heungkuk_pdf, download_meritz_pdf,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pdf_archiver_v2")

# ── Config (env 기반) ───────────────────────────────────────
BATCH_SIZE = int(os.getenv("V2_BATCH_SIZE", "20"))
WORKERS = int(os.getenv("V2_WORKERS", "6"))            # download concurrency
RCLONE_WORKERS = int(os.getenv("V2_RCLONE_WORKERS", "1"))  # rclone upload concurrency (limit API calls)
HTTP_TIMEOUT = int(os.getenv("V2_HTTP_TIMEOUT", "45"))
# ⚠️ GDrive: 서비스 계정(gdrive_sa) 사용 → OAuth 토큰 만료 없음
# - 서비스 계정: ag-cli-worker@gen-lang-client-0035351125.iam.gserviceaccount.com
# - JSON 키: /home/ubuntu/workspace/gcp-key.json
# - rclone remote: gdrive_sa (shared_with_me=true)
# - GDrive API로 파일 ID 조회 가능 → https://drive.google.com/file/d/{ID}/view
RCLONE_REMOTE = os.getenv("V2_RCLONE_REMOTE", os.getenv("RCLONE_REMOTE", "gdrive:archive/pdf"))
RCLONE_BIN = os.getenv("RCLONE_BIN", "rclone")
RCLONE_CONFIG = os.getenv("RCLONE_CONFIG", os.path.expanduser("~/.config/rclone/rclone.conf"))
LOCAL_BUFFER = Path(os.getenv("V2_BUFFER_DIR", "/tmp/pdf_archiver_v2"))
DB_RETRY_LIMIT = int(os.getenv("V2_RETRY_LIMIT", "8"))
LOCK_FILE = "/tmp/pdf_archiver_v2.lock"

# ── Quota / rate-limit guards ─────────────────────────────────
MAX_CONSECUTIVE_QUOTA_FAILURES = int(os.getenv("V2_MAX_QUOTA_FAILURES", "12"))
MAX_RUNTIME_SECONDS = int(os.getenv("V2_MAX_RUNTIME", "1800"))  # 30 min
QUOTA_BACKOFF_BASE = float(os.getenv("V2_QUOTA_BACKOFF_BASE", "5.0"))  # seconds
RCLONE_PACER_SLEEP = os.getenv("V2_RCLONE_PACER_SLEEP", "200ms")  # min sleep between API calls

SOURCE_TABLE = '"tbl_sec_reports"'
ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'

# ── Downloader Registry ─────────────────────────────────────
# 각 증권사별 downloader 함수 맵핑
# 키워드 매칭: firm_nm 에 키워드가 포함되면 해당 downloader 사용

DOWNLOADER_REGISTRY = {
    "DS": download_ds_pdf,
    "미래에셋": download_mirae_pdf,
    "교보": download_kyobo_pdf,
    "하나": download_hana_pdf,
    "LS": download_ls_pdf,
    "흥국": download_heungkuk_pdf,
    "메리츠": download_meritz_pdf,
}
DBFI_FIRM_ORDER = 19  # sec_firm_order 기준 매칭


def _select_downloader(firm_nm: str, sec_firm_order: int = 0):
    """firm_nm 기준으로 등록된 downloader 찾기 (키워드 매칭)"""
    # 1. keyword 매칭
    for keyword, fn in DOWNLOADER_REGISTRY.items():
        if keyword in (firm_nm or ""):
            return fn
    # 2. DBFi special case (order 19)
    if sec_firm_order == DBFI_FIRM_ORDER:
        return download_dbfi_pdf
    return None


# ── DB helpers ──────────────────────────────────────────────

# ⚠️ SSH 터널 통한 DB 연결은 ssl=False 필수 (asyncpg는 SSL 시도했다가 실패함)
# psql은 sslmode=prefer라 자동 fallback 되지만 asyncpg는 안 됨
# 터널: ssh -L 5433:10.0.0.111:5432 oci  (127.0.0.1 아님! DB가 10.0.0.111에 바인딩됨)
async def db_connect() -> asyncpg.Connection:
    return await asyncpg.connect(build_postgres_dsn(), ssl=False)


async def fetch_targets(conn: asyncpg.Connection, limit: int) -> list[asyncpg.Record]:
    """메인 테이블은 읽기 전용, 상태는 archive 테이블 LEFT JOIN 으로 판단"""
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
                         title: str, report_date: str, pdf_url: str,
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


# update_source_status 제거됨 — v2는 archive 테이블만 씀 (tbl_sec_reports 읽기 전용)


# ── rclone upload ───────────────────────────────────────────

async def rclone_upload(local_path: str, remote_path: str) -> Tuple[bool, bool]:
    """rclone copyto → (success, quota_exceeded)

    Returns (True, False) on success.
    Returns (False, True) if GDrive API quota exceeded (should backoff).
    Returns (False, False) for other failures.
    """
    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG

    remote_full = f"{RCLONE_REMOTE}/{remote_path}"
    cmd = [
        RCLONE_BIN, "--config", RCLONE_CONFIG,
        "copyto", str(local_path), remote_full,
        "--retries", "3",
        "--low-level-retries", "5",
        "--drive-pacer-min-sleep", RCLONE_PACER_SLEEP,
        "--drive-pacer-burst", "2",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        stderr_text = stderr.decode(errors="replace")
        # Detect quota / rate-limit errors
        is_quota = (
            "Quota exceeded" in stderr_text
            or "Error 403" in stderr_text
            or "rateLimitExceeded" in stderr_text
            or "userRateLimitExceeded" in stderr_text
        )
        if is_quota:
            log.warning(f"rclone QUOTA EXCEEDED: {stderr_text[:200]}")
            return False, True
        log.warning(f"rclone upload failed: {stderr_text[:300]}")
        return False, False
    return True, False


# ── File path builder ───────────────────────────────────────

def build_storage_key(firm: str, title: str, report_date: str, report_id: int) -> str:
    """GDrive/OneDrive 경로 생성: YYYY-MM/firm/YYMMDD_title_report_id.pdf"""
    clean_dt = re.sub(r'[^0-9]', '', str(report_date)) if report_date else "00000000"
    y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
    yy_mm_dd = clean_dt[2:8]
    normalized = unicodedata.normalize('NFC', title or '')
    safe_title = re.sub(r'[\\\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]()]', ' ', normalized)
    safe_title = '_'.join(safe_title.split())[:60].strip('_') or 'untitled'
    filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
    return f"{y_m}/{firm}/{filename}"


# ── PDF download (curl-based, handles Korean URLs + Referer) ─

def _clean_url(url: str) -> str:
    """URL 끝의 스크래핑 가비지 제거 (') , ' 등)"""
    import re
    # trailing garbage: ')  , ')  , '  등
    cleaned = re.sub(r"[')]+\s*$", "", url.strip())
    return cleaned


def _encode_url(url: str) -> str:
    """URL 내 한글 등 비ASCII 문자를 percent-encoding"""
    from urllib.parse import quote, unquote, urlparse, urlunparse
    parts = list(urlparse(url))
    # path 부분만 인코딩 (query는 그대로)
    parts = [
        parts[0], parts[1],
        quote(unquote(parts[2]), safe='/:@!$&*()+,;='),
        parts[3],
        quote(unquote(parts[4]), safe='/:@!$&*()+,;='),
        parts[5],
    ]
    return urlunparse(parts)


async def _download_wget(url: str, target_path: Path, timeout: int = 30) -> bool:
    """curl로 PDF 다운로드 (Referer + URL 인코딩)"""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    url = _clean_url(url)
    encoded_url = _encode_url(url)
    # Referer: scheme://host
    try:
        parsed = urlparse(url)
        referer = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        referer = url.rsplit('/', 1)[0] if '/' in url else url

    cmd = [
        "curl", "-sL", "-o", str(target_path),
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "-H", f"Referer: {referer}",
        "-H", "Accept: application/pdf,*/*",
        "--max-time", str(timeout),
        "--retry", "2",
        "--insecure",
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

async def process_one(sem: asyncio.Semaphore, rclone_sem: asyncio.Semaphore,
                      conn: asyncpg.Connection, db_lock: asyncio.Lock,
                      row: asyncpg.Record):
    """레코드 1건 처리: 다운로드 → hash → 중복검사 → 업로드/참조

    Returns:
        True: uploaded or duplicate found
        False: download or non-quota upload failure
        "quota": GDrive API quota exceeded
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

        # 1. 다운로드 시도 (downloader registry → fallback wget)
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

        # 2. determine which file to process (downloader → local_target, wget → tmp_path)
        if ok:
            if local_target.exists() and local_target.stat().st_size > 1024:
                work_path = local_target
            elif tmp_path.exists() and tmp_path.stat().st_size > 1024:
                work_path = tmp_path
            else:
                ok = False
            # verify it's actually a PDF
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

        # 3. hash 계산
        pdf_hash_hex, pdf_hash_bytes = compute_hash(work_path)
        file_size = work_path.stat().st_size

        # 4. 중복 검사
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

        # 5. 신규 업로드
        if work_path != local_target:
            work_path.rename(local_target)
            work_path = local_target

        async with rclone_sem:
            uploaded, quota_exceeded = await rclone_upload(str(local_target), storage_key)

        if uploaded:
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key, file_size, page_count=0,
                                     pdf_hash=pdf_hash_hex, pdf_hash_bytes=pdf_hash_bytes, success=True, retry_delta=0)
            local_target.unlink(missing_ok=True)
            log.info(f"[{report_id}] UPLOADED {firm} | {title[:30]}...")
            return True
        elif quota_exceeded:
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key, file_size, page_count=0,
                                     pdf_hash=pdf_hash_hex, pdf_hash_bytes=pdf_hash_bytes, success=False, retry_delta=0)
            log.warning(f"[{report_id}] QUOTA_EXCEEDED (retry not incremented) {firm} | {title[:30]}...")
            return "quota"
        else:
            # 업로드 실패 → 파일 보존, 다음 run에서 재시도
            async with db_lock:
                await upsert_archive(conn, report_id, firm, title, report_date_str, pdf_url,
                                     storage_key, file_size, page_count=0,
                                     pdf_hash=pdf_hash_hex, pdf_hash_bytes=pdf_hash_bytes, success=False, retry_delta=1)
            log.warning(f"[{report_id}] UPLOAD FAILED {firm} | {title[:30]}...")
            return False


# ── Orchestrator ────────────────────────────────────────────

async def run():
    conn = await db_connect()
    start_time = datetime.datetime.now()
    consecutive_quota_failures = 0
    db_lock = asyncio.Lock()  # serialize DB writes — asyncpg conn is single-operation
    rclone_sem = asyncio.Semaphore(RCLONE_WORKERS)  # limit concurrent GDrive API calls
    try:
        LOCAL_BUFFER.mkdir(parents=True, exist_ok=True)
        sem = asyncio.Semaphore(WORKERS)

        while True:
            # ── max runtime guard ──
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            if elapsed > MAX_RUNTIME_SECONDS:
                log.warning(f"Max runtime {MAX_RUNTIME_SECONDS}s reached (elapsed={elapsed:.0f}s). Exiting.")
                break

            targets = await fetch_targets(conn, BATCH_SIZE)
            if not targets:
                log.info("No pending targets.")
                break

            log.info(f"Batch: {len(targets)} targets (quota_fails={consecutive_quota_failures}, elapsed={elapsed:.0f}s)")
            results = await asyncio.gather(
                *(process_one(sem, rclone_sem, conn, db_lock, t) for t in targets),
                return_exceptions=True,
            )

            # ── tally results, detect quota failures ──
            ok = 0
            fail = 0
            quota = 0
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

            # ── quota backoff logic ──
            if quota > 0:
                consecutive_quota_failures += quota
                if consecutive_quota_failures >= MAX_CONSECUTIVE_QUOTA_FAILURES:
                    log.error(
                        f"Too many consecutive quota failures ({consecutive_quota_failures}). "
                        f"Giving up. Will retry on next cron run."
                    )
                    break
                # exponential backoff: base * 2^(min(failures, 6))
                delay = QUOTA_BACKOFF_BASE * (2 ** min(consecutive_quota_failures, 6))
                log.warning(
                    f"Quota backoff: sleeping {delay:.0f}s "
                    f"(consecutive_quota_failures={consecutive_quota_failures})"
                )
                await asyncio.sleep(delay)
            elif ok > 0:
                # successful uploads reset the backoff counter
                consecutive_quota_failures = 0

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
    """파일 락 획득 → 이미 실행 중이면 False

    Stale lock이면 (PID가 더 이상 존재하지 않으면) 정리 후 재시도.
    Python fcntl 락은 프로세스 종료 시 커널이 해제하지만,
    lock file 자체는 남아있을 수 있음.
    """
    # 1. stale lock check
    try:
        with open(LOCK_FILE, "r") as f:
            old_pid = f.read().strip()
        if old_pid:
            try:
                os.kill(int(old_pid), 0)  # signal 0 → 존재 확인만
            except (OSError, ValueError):
                # 프로세스 없음 → stale lock 정리
                log.warning(f"Stale lock detected (PID {old_pid} gone). Cleaning up.")
                try:
                    os.unlink(LOCK_FILE)
                except OSError:
                    pass
    except FileNotFoundError:
        pass  # lock file 없음 = 정상

    # 2. acquire fcntl lock
    try:
        lock_f = open(LOCK_FILE, "w")
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_f.write(str(os.getpid()))
        lock_f.flush()
        # keep lock_f open → 커널이 프로세스 종료 시 자동 해제
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
