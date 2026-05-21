"""
리포트 아카이브 상태 검증 스크립트

지정된 report_id들에 대해 DB 내 아카이브 상태를 확인합니다:
1. 아카이브 메타데이터 테이블(tbl_sec_reports_pdf_archive)에 등록되어 있는지 확인하고 경로를 출력합니다.
2. 메타데이터에 없으면 메인 테이블(tbl_sec_reports)에 존재하는지 확인하여 '대기 중'인지 '누락'인지 판별합니다.
"""
import asyncio
import os
import asyncpg
from pathlib import Path

from _bootstrap import build_postgres_dsn

async def verify_reports(report_ids):
    # 시도할 설정들
    configs = [
        build_postgres_dsn(),
        os.getenv("POSTGRES_URL", ""),
    ]
    
    conn = None
    for url in configs:
        try:
            conn = await asyncpg.connect(url)
            print("Successfully connected using configured database credentials")
            break
        except Exception as e:
            continue

    if not conn:
        print("Error: Could not connect to database with any known config.")
        return

    print(f"\n{'Report ID':<10} | {'Status':<15} | {'Archive Path'}")
    print("-" * 100)

    for r_id in report_ids:
        row = await conn.fetchrow(
            'SELECT report_id, file_path FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1',
            r_id
        )
        
        if row:
            print(f"{r_id:<10} | {'[Archived]':<15} | {row['file_path']}")
        else:
            # Check if it exists in the main table
            # TBL_SEC_REPORTS 테이블명은 DB_BACKEND에 따라 다를 수 있으므로 두 가지 모두 시도
            main_row = None
            try:
                main_row = await conn.fetchrow('SELECT report_id FROM tbl_sec_reports WHERE report_id = $1', r_id)
            except:
                try:
                    main_row = await conn.fetchrow('SELECT report_id FROM data_main_daily_send WHERE report_id = $1', r_id)
                except:
                    pass
            
            if main_row:
                print(f"{r_id:<10} | {'[Pending]':<15} | Not yet archived in metadata")
            else:
                print(f"{r_id:<10} | {'[Missing]':<15} | Not found in database at all")

    await conn.close()

if __name__ == "__main__":
    ids_to_check = [1540, 116, 118, 119, 1495]
    asyncio.run(verify_reports(ids_to_check))
