"""
PDF 해시 백필(Backfill) 스크립트

이 스크립트는 기존 레코드들에 대해 PDF 파일의 해시(SHA-256)를 계산하여 DB에 채워 넣습니다:
1. 아카이브 테이블 및 메인 테이블의 pdf_hash 컬럼을 확인하고 없으면 생성합니다.
2. 로컬 파일 시스템 또는 HTTP URL을 통해 PDF 데이터를 읽어 해시를 계산합니다.
3. 비동기 HTTP 요청 및 병렬 처리를 통해 효율적으로 대량의 데이터를 처리합니다.
4. 아카이브 테이블의 제목, 저자 등 누락된 메타데이터도 함께 보강합니다.
5. 중복 실행 방지를 위해 파일 락(Lock)을 사용합니다.
"""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import sys
import time
from pathlib import Path
from typing import Iterable

import aiohttp
import asyncpg

from _bootstrap import build_postgres_dsn


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SOURCE_TABLE = '"tbl_sec_reports"'
ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'
LOCK_FILE = "/tmp/pdf_hash_backfill.lock"
BATCH_SIZE = int(os.getenv("PDF_HASH_BACKFILL_BATCH_SIZE", "500"))
WORKERS = int(os.getenv("PDF_HASH_BACKFILL_WORKERS", "12"))
HTTP_TIMEOUT = int(os.getenv("PDF_HASH_HTTP_TIMEOUT", "30"))
LOG_EVERY = int(os.getenv("PDF_HASH_BACKFILL_LOG_EVERY", "100"))


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def _table_ident(name: str) -> str:
    return name


async def _table_has_column(conn: asyncpg.Connection, table_name: str, column_name: str) -> bool:
    row = await conn.fetchrow(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = $1
          AND column_name = $2
        LIMIT 1
        """,
        table_name.strip('"'),
        column_name,
    )
    return row is not None


async def _ensure_backfill_schema(conn: asyncpg.Connection) -> None:
    for table_name in (SOURCE_TABLE, ARCHIVE_TABLE):
        if not await _table_has_column(conn, table_name, "pdf_hash"):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN pdf_hash BYTEA")

    for column_name, column_sql in (
        ("title", "TEXT"),
        ("author", "TEXT"),
        ("has_text", "BOOLEAN"),
        ("is_encrypted", "BOOLEAN"),
        ("storage_backend", "TEXT DEFAULT 'onedrive'"),
        ("storage_key", "TEXT"),
        ("last_accessed_at", "TIMESTAMPTZ"),
        ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
        ("updated_at", "TIMESTAMPTZ DEFAULT NOW()"),
    ):
        if not await _table_has_column(conn, ARCHIVE_TABLE, column_name):
            await conn.execute(f"ALTER TABLE {ARCHIVE_TABLE} ADD COLUMN {column_name} {column_sql}")


async def _get_source_path_expr(conn: asyncpg.Connection) -> str | None:
    candidates: list[str] = []
    for column_name in ("archive_path", "file_path"):
        if await _table_has_column(conn, SOURCE_TABLE, column_name):
            candidates.append(f"NULLIF(BTRIM({column_name}), '')")
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    return "COALESCE(" + ", ".join(candidates) + ")"


async def _get_source_text_column(conn: asyncpg.Connection, preferred_names: Iterable[str]) -> str | None:
    for column_name in preferred_names:
        if await _table_has_column(conn, SOURCE_TABLE, column_name):
            return column_name
    return None


async def _hash_http(session: aiohttp.ClientSession, url_text: str) -> bytes | None:
    try:
        async with session.get(url_text, allow_redirects=True) as response:
            if response.status != 200:
                return None
            digest = hashlib.sha256()
            async for chunk in response.content.iter_chunked(1024 * 1024):
                digest.update(chunk)
            return digest.digest()
    except Exception as exc:
        log(f"skip url={url_text} err={exc}")
        return None


async def _hash_local(path_text: str) -> bytes | None:
    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return None
    try:
        data = await asyncio.to_thread(path.read_bytes)
        return hashlib.sha256(data).digest()
    except Exception as exc:
        log(f"skip file={path_text} err={exc}")
        return None


async def _hash_value(session: aiohttp.ClientSession, value: str) -> bytes | None:
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return await _hash_http(session, text)
    return await _hash_local(text)


async def _ensure_temp_table(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS pdf_hash_backfill_tmp (
            report_id BIGINT PRIMARY KEY,
            pdf_hash BYTEA NOT NULL
        ) ON COMMIT PRESERVE ROWS
        """
    )


async def _update_from_temp(conn: asyncpg.Connection, table_name: str) -> int:
    result = await conn.execute(
        f"""
        UPDATE {table_name} t
        SET pdf_hash = tmp.pdf_hash
        FROM pdf_hash_backfill_tmp tmp
        WHERE t.report_id = tmp.report_id
          AND t.pdf_hash IS NULL
        """
    )
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


async def _fetch_pending_rows(conn: asyncpg.Connection, table_name: str, limit: int, path_expr: str) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"""
        SELECT report_id, {path_expr} AS file_path
        FROM {table_name}
        WHERE pdf_hash IS NULL
          AND {path_expr} IS NOT NULL
        ORDER BY report_id ASC
        LIMIT $1
        """,
        limit,
    )


async def _process_batch(
    conn: asyncpg.Connection,
    session: aiohttp.ClientSession,
    rows: list[asyncpg.Record],
    cache: dict[str, bytes],
    label: str,
) -> tuple[list[tuple[int, bytes]], int]:
    if not rows:
        return [], 0

    unique_values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        value = str(row["file_path"]).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        unique_values.append(value)

    log(f"{label}: fetched={len(rows)} unique={len(unique_values)} cache={len(cache)}")

    sem = asyncio.Semaphore(WORKERS)

    async def _one(value: str) -> tuple[str, bytes | None]:
        cached = cache.get(value)
        if cached is not None:
            return value, cached
        async with sem:
            pdf_hash = await _hash_value(session, value)
        if pdf_hash is not None:
            cache[value] = pdf_hash
        return value, pdf_hash

    hashed_pairs = await asyncio.gather(*(_one(value) for value in unique_values))
    hash_map = {value: pdf_hash for value, pdf_hash in hashed_pairs if pdf_hash is not None}

    records: list[tuple[int, bytes]] = []
    skipped = 0
    for row in rows:
        value = str(row["file_path"]).strip()
        pdf_hash = hash_map.get(value) or cache.get(value)
        if pdf_hash is None:
            skipped += 1
            continue
        records.append((int(row["report_id"]), pdf_hash))

    if not records:
        return [], skipped

    log(f"{label}: ready_records={len(records)} skipped={skipped}")
    return records, skipped


async def backfill_pdf_hash() -> None:
    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        await _ensure_backfill_schema(conn)
        source_path_expr = await _get_source_path_expr(conn)
        source_title_col = await _get_source_text_column(conn, ("article_title", "title"))
        source_author_col = await _get_source_text_column(conn, ("writer", "author"))

        cache: dict[str, bytes] = {}
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await _ensure_temp_table(conn)

            archive_total = 0
            source_total = 0
            archive_skipped = 0
            batch_no = 0
            while True:
                rows = await _fetch_pending_rows(
                    conn,
                    ARCHIVE_TABLE,
                    BATCH_SIZE,
                    "NULLIF(BTRIM(file_path), '')",
                )
                if not rows:
                    break
                batch_no += 1
                records, skipped = await _process_batch(conn, session, rows, cache, f"archive#{batch_no}")
                archive_skipped += skipped
                if not records:
                    log(f"archive#{batch_no}: no hashes ready")
                    continue
                await conn.execute("TRUNCATE pdf_hash_backfill_tmp")
                await conn.copy_records_to_table(
                    "pdf_hash_backfill_tmp",
                    records=records,
                    columns=("report_id", "pdf_hash"),
                )
                archive_updates = await _update_from_temp(conn, ARCHIVE_TABLE)
                source_updates = await _update_from_temp(conn, SOURCE_TABLE)
                archive_total += archive_updates
                source_total += source_updates
                log(
                    f"archive#{batch_no}: archive_updates={archive_updates} source_updates={source_updates}"
                )

            if source_title_col or source_author_col:
                title_expr = f"s.{source_title_col}" if source_title_col else "NULL"
                author_expr = f"s.{source_author_col}" if source_author_col else "NULL"
                await conn.execute(
                    f"""
                    UPDATE {ARCHIVE_TABLE} a
                    SET title = COALESCE(NULLIF(a.title, ''), NULLIF({title_expr}, '')),
                        author = COALESCE(NULLIF(a.author, ''), NULLIF({author_expr}, ''))
                    FROM {SOURCE_TABLE} s
                    WHERE s.report_id = a.report_id
                      AND (a.title IS NULL OR a.title = '' OR a.author IS NULL OR a.author = '')
                    """
                )

            source_skipped = 0
            if source_path_expr:
                batch_no = 0
                while True:
                    rows = await _fetch_pending_rows(
                        conn,
                        SOURCE_TABLE,
                        BATCH_SIZE,
                        source_path_expr,
                    )
                    if not rows:
                        break
                    batch_no += 1
                    records, skipped = await _process_batch(conn, session, rows, cache, f"source#{batch_no}")
                    source_skipped += skipped
                    if not records:
                        log(f"source#{batch_no}: no hashes ready")
                        continue
                    await conn.execute("TRUNCATE pdf_hash_backfill_tmp")
                    await conn.copy_records_to_table(
                        "pdf_hash_backfill_tmp",
                        records=records,
                        columns=("report_id", "pdf_hash"),
                    )
                    source_updates = await _update_from_temp(conn, SOURCE_TABLE)
                    source_total += source_updates
                    log(f"source#{batch_no}: source_updates={source_updates}")

            log(
                "Backfill complete. "
                f"archive_updates={archive_total} archive_skipped={archive_skipped} "
                f"source_updates={source_total} source_skipped={source_skipped} "
                f"batch_size={BATCH_SIZE} workers={WORKERS}"
            )
    finally:
        await conn.close()


def main() -> None:
    lock_f = open(LOCK_FILE, "w")
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("Another pdf_hash backfill is already running.")
        return

    log(
        "Starting pdf hash backfill "
        f"pid={os.getpid()} batch_size={BATCH_SIZE} workers={WORKERS} timeout={HTTP_TIMEOUT}s"
    )
    try:
        asyncio.run(backfill_pdf_hash())
    except KeyboardInterrupt:
        log("Interrupted.")
        raise
    finally:
        lock_f.close()


if __name__ == "__main__":
    main()
