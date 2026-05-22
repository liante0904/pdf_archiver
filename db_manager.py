import asyncpg
import logging
from config import Config

class DBManager:
    _conn = None

    @classmethod
    async def get_conn(cls):
        if cls._conn is None or cls._conn.is_closed():
            logging.info(f"Connecting to {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB} as {Config.POSTGRES_USER}")
            if Config.POSTGRES_URL:
                cls._conn = await asyncpg.connect(Config.POSTGRES_URL)
            else:
                cls._conn = await asyncpg.connect(
                    host=Config.POSTGRES_HOST,
                    port=Config.POSTGRES_PORT,
                    database=Config.POSTGRES_DB,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                )
        return cls._conn

    @classmethod
    async def close(cls):
        if cls._conn and not cls._conn.is_closed():
            await cls._conn.close()
            cls._conn = None

async def get_db_connection():
    return await DBManager.get_conn()

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
    for table_name in (Config.SOURCE_TABLE, Config.META_TABLE):
        for legacy_attach_name, quoted in (("attach_url", False), ("attach_url", True)):
            if await _table_has_column(conn, table_name, legacy_attach_name):
                drop_name = legacy_attach_name
                await conn.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {drop_name}")

        pdf_status_existed = await _table_has_column(conn, table_name, Config.PDF_STATUS_COL)
        if not pdf_status_existed:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {Config.PDF_STATUS_COL} INTEGER DEFAULT 0")

        if not await _table_has_column(conn, table_name, Config.PDF_HASH_COL):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {Config.PDF_HASH_COL} BYTEA")

        if table_name == Config.META_TABLE:
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

        legacy_exists = await _table_has_column(conn, table_name, Config.LEGACY_STATUS_COL)
        if table_name == Config.META_TABLE and not legacy_exists:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {Config.LEGACY_STATUS_COL} INTEGER DEFAULT 0")
            legacy_exists = True

        if table_name == Config.META_TABLE and not await _table_has_column(conn, table_name, "retry_count"):
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN retry_count INTEGER DEFAULT 0")

        if not pdf_status_existed and legacy_exists:
            await conn.execute(
                f"""
                UPDATE {table_name}
                SET {Config.PDF_STATUS_COL} = COALESCE({Config.LEGACY_STATUS_COL}, 0)
                WHERE {Config.PDF_STATUS_COL} IS NULL
                   OR {Config.PDF_STATUS_COL} != COALESCE({Config.LEGACY_STATUS_COL}, 0)
                """
            )
        else:
            await conn.execute(
                f"""
                UPDATE {table_name}
                SET {Config.PDF_STATUS_COL} = COALESCE({Config.PDF_STATUS_COL}, 0)
                WHERE {Config.PDF_STATUS_COL} IS NULL
                """
            )
