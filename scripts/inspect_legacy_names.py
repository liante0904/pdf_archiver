import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def inspect_constraints():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. tbl_sec_reports 테이블과 관련된 인덱스 및 제약 조건 조회
    query = """
        SELECT 
            conname AS name, 
            'constraint' AS type 
        FROM pg_constraint 
        WHERE conrelid = 'tbl_sec_reports'::regclass
        UNION ALL
        SELECT 
            indexname AS name, 
            'index' AS type 
        FROM pg_indexes 
        WHERE tablename = 'tbl_sec_reports'
    """
    
    rows = await conn.fetch(query)
    
    print("--- TBL_SEC_REPORTS 관련 레거시 명칭 조사 ---")
    legacy_names = [r for r in rows if 'data_main_daily_send' in r['name']]
    
    for r in legacy_names:
        print(f"[{r['type'].upper()}] {r['name']}")
        
    if not legacy_names:
        print("레거시 명칭이 발견되지 않았습니다.")
    else:
        # 변경 SQL 생성
        print("\n[변경 권장 SQL]")
        for r in legacy_names:
            new_name = r['name'].replace('data_main_daily_send', 'tbl_sec_reports')
            if r['type'] == 'index':
                print(f"ALTER INDEX \"{r['name']}\" RENAME TO \"{new_name}\";")
            else:
                print(f"ALTER TABLE \"TBL_SEC_REPORTS\" RENAME CONSTRAINT \"{r['name']}\" TO \"{new_name}\";")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(inspect_constraints())
