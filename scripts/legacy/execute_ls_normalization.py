import asyncio
import asyncpg
import re
from urllib.parse import urlparse, parse_qs

from _bootstrap import build_postgres_dsn

def clean_ls_url(url):
    """URL에서 board_no와 board_seq만 남기고 나머지는 제거합니다."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    board_no = qs.get('board_no', [''])[0]
    board_seq = qs.get('board_seq', [''])[0]
    if not board_no or not board_seq:
        return url
    return f"https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no={board_no}&board_seq={board_seq}"

async def execute_ls_normalization():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 중복 그룹 추출
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id, reg_dt, key, firm_nm, article_title, "sync_status",
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title
            FROM tbl_sec_reports
            WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
        ),
        dup_groups AS (
            SELECT norm_title, reg_dt
            FROM normalized_reports
            GROUP BY norm_title, reg_dt
            HAVING COUNT(*) > 1
        )
        SELECT n.* FROM normalized_reports n
        JOIN dup_groups d ON n.norm_title = d.norm_title AND n.reg_dt = d.reg_dt
        ORDER BY n.reg_dt DESC, n.norm_title, n.report_id
    """
    
    rows = await conn.fetch(query)
    
    groups = {}
    for r in rows:
        key = (r['norm_title'], r['reg_dt'])
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(r))

    print(f"--- LS/이베스트 DB 정규화 실행 중... ---")
    
    async with conn.transaction():
        updated_count = 0
        deleted_count = 0
        migrated_meta_count = 0
        
        for key, members in groups.items():
            members.sort(key=lambda x: x['report_id'])
            survivor = members[0]
            duplicates = members[1:]
            
            # 1. 생존자 KEY 업데이트 (높은 ID의 정규화된 URL 사용)
            highest_id_member = members[-1]
            new_key = clean_ls_url(highest_id_member['key'])
            await conn.execute('UPDATE tbl_sec_reports SET key = $1 WHERE report_id = $2', new_key, survivor['report_id'])
            updated_count += 1
            
            for d in duplicates:
                d_id = d['report_id']
                
                # 2. 메타데이터 이관 (파일이 높은 ID에만 있는 경우)
                # survivor와 duplicate의 메타데이터 확인
                survivor_meta = await conn.fetchrow('SELECT report_id FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', survivor['report_id'])
                duplicate_meta = await conn.fetchrow('SELECT * FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', d_id)
                
                if duplicate_meta and not survivor_meta:
                    # 이관: 생존자용 메타데이터 생성 (중복된 놈의 정보 기반)
                    await conn.execute(
                        'INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, file_size, page_count, reg_dt) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                        survivor['report_id'], duplicate_meta['firm_nm'], duplicate_meta['title'], duplicate_meta['file_path'], 
                        duplicate_meta['file_size'], duplicate_meta['page_count'], duplicate_meta['reg_dt']
                    )
                    # 생존자 상태를 완료(2)로 변경
                    await conn.execute('UPDATE tbl_sec_reports SET sync_status = 2 WHERE report_id = $1', survivor['report_id'])
                    migrated_meta_count += 1
                
                # 3. 중복 데이터 삭제 (메타데이터 먼저, 그다음 메인 테이블)
                await conn.execute('DELETE FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', d_id)
                await conn.execute('DELETE FROM tbl_sec_reports WHERE report_id = $1', d_id)
                deleted_count += 1

        print(f"결과: {updated_count}개 업데이트, {deleted_count}개 삭제, {migrated_meta_count}개 메타데이터 이관 완료.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(execute_ls_normalization())
