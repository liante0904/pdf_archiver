import asyncio
import asyncpg
import fcntl
import hashlib
import os
import sys
import shutil
from pathlib import Path

from _bootstrap import build_postgres_dsn

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SOURCE_TABLE = '"tbl_sec_reports"'
ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'
LOCK_FILE = "/tmp/pdf_hash_backfill.lock"
BATCH_SIZE = int(os.getenv("PDF_HASH_BACKFILL_BATCH_SIZE", "500"))
RCLONE_BIN = os.getenv("RCLONE_BIN") or shutil.which("rclone") or "/usr/bin/rclone"
RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf")
LOCAL_BUFFER_DIR = Path(os.getenv("LOCAL_BUFFER_DIR", os.path.expanduser("~/downloads/pdf_archive_temp")))


def _table_ident(name: str) -> str:
    return name


async def _table_has_column(conn, table_name, column_name):
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


async def _hash_remote_path(remote_rel_path):
    if not remote_rel_path:
        return None
    remote_path = f"{RCLONE_REMOTE.rstrip('/')}/{remote_rel_path.lstrip('/')}"
    try:
        proc = await asyncio.create_subprocess_exec(
            RCLONE_BIN,
            "cat",
            remote_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        digest = hashlib.sha256()
        while True:
            chunk = await proc.stdout.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        returncode = await proc.wait()
        if returncode != 0:
            return None
        return digest.digest()
    except Exception:
        return None


async def _hash_file(path_text):
    if not path_text:
        return None
    path = Path(path_text)
    if path.exists() and path.is_file():
        try:
            return hashlib.sha256(path.read_bytes()).digest()
        except Exception:
            return None

    try:
        if path.is_absolute():
            rel_path = path.relative_to(LOCAL_BUFFER_DIR)
        else:
            rel_path = path
        return await _hash_remote_path(str(rel_path))
    except Exception:
        return None


async def _get_source_path_expr(conn):
    candidates = []
    for column_name in ("archive_path", "file_path"):
        if await _table_has_column(conn, SOURCE_TABLE, column_name):
            candidates.append(f"NULLIF(BTRIM({column_name}), '')")
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    return "COALESCE(" + ", ".join(candidates) + ")"


async def _get_source_text_column(conn, preferred_names):
    for column_name in preferred_names:
        if await _table_has_column(conn, SOURCE_TABLE, column_name):
            return column_name
    return None


async def backfill_pdf_hash():
    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        source_path_expr = await _get_source_path_expr(conn)
        source_title_col = await _get_source_text_column(conn, ("article_title", "title"))
        source_author_col = await _get_source_text_column(conn, ("writer", "author"))

        archive_rows = await conn.fetch(
            f"""
            SELECT report_id, NULLIF(BTRIM(file_path), '') AS file_path
            FROM {ARCHIVE_TABLE}
            WHERE pdf_hash IS NULL
              AND NULLIF(BTRIM(file_path), '') IS NOT NULL
            ORDER BY report_id ASC
            LIMIT $1
            """,
            BATCH_SIZE,
        )

        archive_updates = 0
        for row in archive_rows:
            pdf_hash = await _hash_file(row["file_path"])
            if not pdf_hash:
                continue
            result = await conn.execute(
                f"""
                UPDATE {ARCHIVE_TABLE}
                SET pdf_hash = $2
                WHERE report_id = $1
                  AND pdf_hash IS NULL
                """,
                int(row["report_id"]),
                pdf_hash,
            )
            if result.startswith("UPDATE 1"):
                archive_updates += 1

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

        await conn.execute(
            f"""
            UPDATE {SOURCE_TABLE} s
            SET pdf_hash = a.pdf_hash
            FROM {ARCHIVE_TABLE} a
            WHERE s.report_id = a.report_id
              AND s.pdf_hash IS NULL
              AND a.pdf_hash IS NOT NULL
            """
        )

        source_updates = 0
        if source_path_expr:
            source_rows = await conn.fetch(
                f"""
                SELECT report_id, {source_path_expr} AS file_path
                FROM {SOURCE_TABLE}
                WHERE pdf_hash IS NULL
                  AND {source_path_expr} IS NOT NULL
                ORDER BY report_id ASC
                LIMIT $1
                """,
                BATCH_SIZE,
            )
            for row in source_rows:
                pdf_hash = await _hash_file(row["file_path"])
                if not pdf_hash:
                    continue
                result = await conn.execute(
                    f"""
                    UPDATE {SOURCE_TABLE}
                    SET pdf_hash = $2
                    WHERE report_id = $1
                      AND pdf_hash IS NULL
                    """,
                    int(row["report_id"]),
                    pdf_hash,
                )
                if result.startswith("UPDATE 1"):
                    source_updates += 1

        print(
            f"Backfill complete. archive_updates={archive_updates}, source_updates={source_updates}, batch_size={BATCH_SIZE}"
        )
    finally:
        await conn.close()


def main():
    lock_f = open(LOCK_FILE, "w")
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Another pdf_hash backfill is already running.")
        return

    try:
        asyncio.run(backfill_pdf_hash())
    finally:
        lock_f.close()


if __name__ == "__main__":
    main()
