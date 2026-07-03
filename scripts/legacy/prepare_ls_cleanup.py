import asyncio
import asyncpg
import re

from _bootstrap import build_postgres_dsn

async def prepare_ls_cleanup_list():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 중복 그룹 및 각 그룹의 최소 ID(생존자) 찾기
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id, report_date, key, firm_nm, article_title, "sync_status",
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title
            FROM tbl_sec_reports
            WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
        ),
        dup_groups AS (
            SELECT norm_title, report_date, MIN(report_id) as survivor_id
            FROM normalized_reports
            GROUP BY norm_title, report_date
            HAVING COUNT(*) > 1
        )
        SELECT 
            n.report_id, n.report_date, n.article_title, n."sync_status", n.key,
            d.survivor_id,
            (SELECT m.file_path FROM "tbl_sec_reports_pdf_archive" m WHERE m.report_id = n.report_id) as file_path
        FROM normalized_reports n
        JOIN dup_groups d ON n.norm_title = d.norm_title AND n.report_date = d.report_date
        ORDER BY n.report_date DESC, n.norm_title, n.report_id
    """
    
    rows = await conn.fetch(query)
    
    to_delete = []
    to_migrate = [] # 생존자에게 메타데이터를 넘겨줘야 할 항목들
    
    current_group = None
    survivor_info = None
    
    for r in rows:
        group_key = (r['report_date'], r['survivor_id'])
        
        if r['report_id'] == r['survivor_id']:
            # 생존자 정보 저장
            survivor_info = r
            continue
        
        # 삭제 대상 (높은 ID)
        delete_item = {
            "id": r['report_id'],
            "survivor_id": r['survivor_id'],
            "title": r['article_title'],
            "date": r['report_date'],
            "status": r['sync_status'],
            "file_path": r['file_path']
        }
        to_delete.append(delete_item)
        
        # 만약 삭제될 놈은 파일이 있는데, 생존자는 파일이 없다면 이관 필요
        if r['file_path'] and survivor_info and not survivor_info['file_path']:
            to_migrate.append({
                "from_id": r['report_id'],
                "to_id": r['survivor_id'],
                "file_path": r['file_path']
            })

    print(f"--- LS/이베스트 DB 정규화(삭제) 계획 ---")
    print(f"1. 전체 중복 레코드 수: {len(rows)}건")
    print(f"2. 삭제 예정 레코드 수 (높은 ID): {len(to_delete)}건")
    print(f"3. 메타데이터 이관 필요 (파일 보존): {len(to_migrate)}건")
    
    if to_delete:
        print("\n[삭제 대상 상위 10건 예시]")
        for item in to_delete[:10]:
            migrate_str = " (★파일이관 필요)" if any(m['from_id'] == item['id'] for m in to_migrate) else ""
            print(f"Delete ID: {item['id']} | Survivor: {item['survivor_id']} | Date: {item['date']} | Status: {item['status']}{migrate_str}")
            print(f"   Title: {item['title'][:80]}")

    # 파일로 상세 리스트 저장
    with open("tests/ls_cleanup_plan.txt", "w", encoding="utf-8") as f:
        f.write("--- LS Cleanup Plan ---\n")
        for item in to_delete:
            f.write(f"DELETE: {item['id']} (KEEP: {item['survivor_id']}) | STATUS: {item['status']} | PATH: {item['file_path']}\n")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(prepare_ls_cleanup_list())
