import asyncio
import asyncpg
import re

from _bootstrap import build_postgres_dsn

async def export_full_ls_duplicates():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id, report_date, key, firm_nm, article_title, "sync_status",
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title
            FROM tbl_sec_reports
            WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
        ),
        dup_groups AS (
            SELECT norm_title, report_date
            FROM normalized_reports
            GROUP BY norm_title, report_date
            HAVING COUNT(*) > 1
        )
        SELECT n.* FROM normalized_reports n
        JOIN dup_groups d ON n.norm_title = d.norm_title AND n.report_date = d.report_date
        ORDER BY n.report_date DESC, n.norm_title, n.report_id
    """
    
    rows = await conn.fetch(query)
    
    output = []
    output.append(f"--- LS/이베스트 중복 그룹 상세 리스트 (총 {len(rows)}건) ---")
    
    current_group_key = None
    for r in rows:
        group_id = (r['norm_title'], r['report_date'])
        if group_id != current_group_key:
            output.append("\n" + "="*140)
            output.append(f"[그룹] 날짜: {r['report_date']} | 제목: {r['article_title']}")
            output.append("-"*140)
            current_group_key = group_id
        
        output.append(f"ID: {r['report_id']:<10} | Status: {r['sync_status']} | Firm: {r['firm_nm']:<8} | KEY: {r['key']}")

    final_text = "\n".join(output)
    
    # 파일로 저장
    with open("tests/ls_duplicates_list.txt", "w", encoding="utf-8") as f:
        f.write(final_text)
    
    # 화면 출력
    print(final_text)

    await conn.close()

if __name__ == "__main__":
    asyncio.run(export_full_ls_duplicates())
