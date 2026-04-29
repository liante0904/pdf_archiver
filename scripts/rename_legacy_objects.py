import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def rename_legacy_db_objects():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 시퀀스 확인
    sequences = await conn.fetch("""
        SELECT relname FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE c.relkind = 'S' AND relname LIKE '%data_main_daily_send%'
    """)
    
    print("--- 레거시 명칭 정리 작업 시작 ---")
    
    async with conn.transaction():
        # A. 제약 조건 및 인덱스 변경
        commands = [
            'ALTER TABLE tbl_sec_reports RENAME CONSTRAINT "data_main_daily_send_KEY_key" TO "TBL_SEC_REPORTS_KEY_key"',
            'ALTER TABLE tbl_sec_reports RENAME CONSTRAINT "data_main_daily_send_pkey" TO "TBL_SEC_REPORTS_pkey"',
            # 인덱스 이름은 제약 조건 이름을 바꾸면 자동으로 바뀌는 경우가 많으나 명시적으로 수행
            # (PostgreSQL은 내부적으로 인덱스와 제약조건을 연결해서 관리함)
        ]
        
        for cmd in commands:
            try:
                print(f"Executing: {cmd}")
                await conn.execute(cmd)
            except Exception as e:
                print(f"  (Note: {e})")

        # B. 시퀀스 변경
        for seq in sequences:
            old_seq = seq['relname']
            new_seq = old_seq.replace('data_main_daily_send', 'tbl_sec_reports')
            cmd = f'ALTER SEQUENCE "{old_seq}" RENAME TO "{new_seq}"'
            print(f"Executing: {cmd}")
            await conn.execute(cmd)

    print("\n정리가 완료되었습니다.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(rename_legacy_db_objects())
