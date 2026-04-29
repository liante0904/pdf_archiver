import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def rename_legacy_db_objects_safe():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 시퀀스 확인
    sequences = await conn.fetch("""
        SELECT relname FROM pg_class c 
        WHERE c.relkind = 'S' AND relname LIKE '%data_main_daily_send%'
    """)
    
    # 2. 제약 조건 및 인덱스 확인
    query = """
        SELECT conname AS name, 'constraint' AS type FROM pg_constraint WHERE conrelid = 'tbl_sec_reports'::regclass
        UNION ALL
        SELECT indexname AS name, 'index' AS type FROM pg_indexes WHERE tablename = 'tbl_sec_reports'
    """
    objects = await conn.fetch(query)
    
    print("--- 레거시 명칭 안전 정리 작업 시작 ---")
    
    # 각각을 개별적으로 처리하여 하나가 실패해도 다른 것은 영향 받지 않도록 함
    for obj in objects:
        old_name = obj['name']
        if 'data_main_daily_send' in old_name:
            new_name = old_name.replace('data_main_daily_send', 'tbl_sec_reports')
            try:
                if obj['type'] == 'index':
                    cmd = f'ALTER INDEX "{old_name}" RENAME TO "{new_name}"'
                else:
                    cmd = f'ALTER TABLE tbl_sec_reports RENAME CONSTRAINT "{old_name}" TO "{new_name}"'
                
                print(f"Executing: {cmd}")
                await conn.execute(cmd)
            except Exception as e:
                print(f"  Failed: {e}")

    for seq in sequences:
        old_name = seq['relname']
        new_name = old_name.replace('data_main_daily_send', 'tbl_sec_reports')
        try:
            cmd = f'ALTER SEQUENCE "{old_name}" RENAME TO "{new_name}"'
            print(f"Executing: {cmd}")
            await conn.execute(cmd)
        except Exception as e:
            print(f"  Failed (Sequence): {e}")

    print("\n작업이 완료되었습니다.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(rename_legacy_db_objects_safe())
