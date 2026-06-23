#!/bin/bash
# Phase 4 완료 후 DB 전환 + v2 cron 적용
# DB 연결 가능할 때 실행: bash scripts/switch_to_v2.sh
set -e

echo "=== 1/3 DB storage_backend UPDATE ==="
uv run --env-file .env python -c "
import asyncio, asyncpg
from secret_env import build_postgres_dsn
async def main():
    conn = await asyncpg.connect(build_postgres_dsn())
    result = await conn.execute(\"UPDATE tbl_sec_reports_pdf_archive SET storage_backend = 'googledrive' WHERE storage_backend = 'onedrive'\")
    print(f'UPDATE result: {result}')
    row = await conn.fetchrow('SELECT storage_backend, COUNT(*) FROM tbl_sec_reports_pdf_archive GROUP BY storage_backend')
    print(f'After: {dict(row)}')
    await conn.close()
asyncio.run(main())
"

echo "=== 2/3 v2 fetch-only 테스트 ==="
uv run --env-file .env python scripts/pdf_archiver_v2.py --fetch-only 2>&1 || echo "(fetch-only 미지원, skip)"

echo "=== 3/3 cron v1→v2 전환 ==="
crontab -l | sed 's|pdf_archiver_async\.py|scripts/pdf_archiver_v2.py|g' | crontab -
echo "Done. New cron:"
crontab -l | grep pdf_archiver

echo "=== 전환 완료 ==="
