import asyncio
import asyncpg
import re
from urllib.parse import urlparse, parse_qs

from _bootstrap import build_postgres_dsn

def get_url_params(url):
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    return qs.get('board_no', [''])[0], qs.get('board_seq', [''])[0]

def clean_ls_url(url):
    b_no, b_seq = get_url_params(url)
    if not b_no or not b_seq: return url
    return f"https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no={b_no}&board_seq={b_seq}"

async def execute_ls_strict_normalization_v2():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 모든 LS/이베스트 레코드 가져오기
    rows = await conn.fetch('SELECT report_id, key, article_title, reg_dt FROM tbl_sec_reports WHERE (firm_nm LIKE \'%LS%\' OR firm_nm LIKE \'%이베스트%\')')
    
    # 2. 그룹화 (정규화된 KEY 기준)
    # (제목_정규화, 날짜, board_no, board_seq)
    groups = {}
    for r in rows:
        norm_title = re.sub(r'\s+', '', r['article_title']).lower()
        b_no, b_seq = get_url_params(r['key'])
        if not b_no or not b_seq: continue
        
        group_key = (norm_title, r['reg_dt'], b_no, b_seq)
        if group_key not in groups: groups[group_key] = []
        groups[group_key].append(dict(r))

    print(f"--- LS/이베스트 엄격한 정규화 V2 실행 ---")
    
    updated_count = 0
    deleted_count = 0
    
    for key, members in groups.items():
        # ID 순 정렬
        members.sort(key=lambda x: x['report_id'])
        survivor_id = members[0]['report_id']
        duplicates = members[1:]
        
        # 정규화된 KEY
        final_key = clean_ls_url(members[-1]['key'])
        
        # A. 중복 삭제 먼저 수행 (Unique 제약 충돌 방지)
        for d in duplicates:
            d_id = d['report_id']
            # 메타데이터 이관
            duplicate_meta = await conn.fetchrow('SELECT * FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', d_id)
            if duplicate_meta:
                survivor_meta = await conn.fetchrow('SELECT report_id FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', survivor_id)
                if not survivor_meta:
                    await conn.execute('INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, file_size, page_count, reg_dt) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                        survivor_id, duplicate_meta['firm_nm'], duplicate_meta['title'], duplicate_meta['file_path'], 
                        duplicate_meta['file_size'], duplicate_meta['page_count'], duplicate_meta['reg_dt'])
                    await conn.execute('UPDATE tbl_sec_reports SET sync_status = 2 WHERE report_id = $1', survivor_id)
            
            await conn.execute('DELETE FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', d_id)
            await conn.execute('DELETE FROM tbl_sec_reports WHERE report_id = $1', d_id)
            deleted_count += 1
            
        # B. 생존자 KEY 업데이트
        try:
            await conn.execute('UPDATE tbl_sec_reports SET key = $1 WHERE report_id = $2', final_key, survivor_id)
            updated_count += 1
        except asyncpg.exceptions.UniqueViolationError:
            # 여전히 충돌이 난다면, 다른 (제목/날짜가 다른) 레코드가 이 KEY를 선점한 경우임.
            # 이 경우 강제로 업데이트하지 않고 넘어감 (데이터 무결성 유지)
            pass

    print(f"완료: {updated_count}개 업데이트, {deleted_count}개 삭제.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(execute_ls_strict_normalization_v2())
