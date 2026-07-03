import asyncio
import asyncpg
import re
from urllib.parse import urlparse, parse_qs

from _bootstrap import build_postgres_dsn

def get_url_params(url):
    """URL에서 board_no와 board_seq를 추출합니다."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    b_no = qs.get('board_no', [''])[0]
    b_seq = qs.get('board_seq', [''])[0]
    return b_no, b_seq

def clean_ls_url(url):
    """URL 정규화: board_no와 board_seq만 남깁니다."""
    b_no, b_seq = get_url_params(url)
    if not b_no or not b_seq:
        return url
    return f"https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no={b_no}&board_seq={b_seq}"

async def execute_ls_strict_normalization():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 모든 LS/이베스트 레코드 가져오기
    query = """
        SELECT report_id, report_date, key, firm_nm, article_title, "sync_status"
        FROM tbl_sec_reports
        WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
    """
    rows = await conn.fetch(query)
    
    # 2. 파이썬 레벨에서 엄격한 그룹화 (board_no, board_seq 포함)
    groups = {}
    for r in rows:
        norm_title = LOWER_TITLE = re.sub(r'\s+', '', r['article_title']).lower()
        b_no, b_seq = get_url_params(r['key'])
        
        # 게시판 정보가 없으면 개별 건으로 취급 (중복 제거 제외)
        if not b_no or not b_seq:
            continue
            
        # 그룹 키: (제목, 날짜, 게시판번호, 게시물번호)
        group_key = (norm_title, r['report_date'], b_no, b_seq)
        
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(dict(r))

    print(f"--- LS/이베스트 엄격한 정규화 실행 (게시판별 유지) ---")
    
    async with conn.transaction():
        updated_count = 0
        deleted_count = 0
        
        for key, members in groups.items():
            if len(members) <= 1:
                # 중복이 없는 경우 KEY만 정규화 시도
                target = members[0]
                new_key = clean_ls_url(target['key'])
                if target['key'] != new_key:
                    try:
                        await conn.execute('UPDATE tbl_sec_reports SET key = $1 WHERE report_id = $2', new_key, target['report_id'])
                        updated_count += 1
                    except asyncpg.exceptions.UniqueViolationError:
                        # 이미 다른 ID가 이 정규화된 KEY를 선점하고 있다면, 
                        # 사실상 그 ID와 중복이라는 뜻이므로 나중에 통합될 것임
                        pass
                continue

            # 중복 발생 시: 낮은 ID 유지
            members.sort(key=lambda x: x['report_id'])
            survivor = members[0]
            duplicates = members[1:]
            
            # 생존자 KEY 정규화
            new_key = clean_ls_url(survivor['key'])
            await conn.execute('UPDATE tbl_sec_reports SET key = $1 WHERE report_id = $2', new_key, survivor['report_id'])
            updated_count += 1
            
            for d in duplicates:
                d_id = d['report_id']
                # 메타데이터 이관 (파일 정보 보존)
                duplicate_meta = await conn.fetchrow('SELECT * FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', d_id)
                survivor_meta = await conn.fetchrow('SELECT report_id FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', survivor['report_id'])
                
                if duplicate_meta and not survivor_meta:
                    await conn.execute(
                        'INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, file_size, page_count, report_date) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                        survivor['report_id'], duplicate_meta['firm_nm'], duplicate_meta['title'], duplicate_meta['file_path'], 
                        duplicate_meta['file_size'], duplicate_meta['page_count'], duplicate_meta['report_date']
                    )
                    await conn.execute('UPDATE tbl_sec_reports SET sync_status = 2 WHERE report_id = $1', survivor['report_id'])

                # 중복 삭제
                await conn.execute('DELETE FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', d_id)
                await conn.execute('DELETE FROM tbl_sec_reports WHERE report_id = $1', d_id)
                deleted_count += 1

        print(f"결과: {updated_count}개 KEY 정규화, {deleted_count}개 중복 레코드 삭제 완료.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(execute_ls_strict_normalization())
