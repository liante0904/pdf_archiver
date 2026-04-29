import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def verify_sample_orphans():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    samples = [
        {'date': '20210127', 'title': '기아차(000270)'},
        {'date': '20210127', 'title': 'LG화학(051910)'},
        {'date': '20210119', 'title': '게임빌(063080)'}
    ]
    
    print("--- 고아 파일 샘플 DB 존재 여부 확인 ---")
    for s in samples:
        print(f"\n조사 중: {s['date']} | {s['title']}")
        # 제목에 해당 키워드가 포함되고 날짜가 같은 모든 레코드 조회
        rows = await conn.fetch("""
            SELECT report_id, firm_nm, article_title, reg_dt
            FROM tbl_sec_reports
            WHERE reg_dt = $1 AND article_title LIKE $2
        """, s['date'], f"%{s['title']}%")
        
        if rows:
            print(f"  => DB에 {len(rows)}개의 레코드가 존재합니다!")
            for r in rows:
                print(f"     [ID: {r['report_id']}] {r['article_title']}")
        else:
            print("  => DB에 해당 날짜/제목의 리포트가 전혀 없습니다.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(verify_sample_orphans())
