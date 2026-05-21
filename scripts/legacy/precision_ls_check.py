import asyncio
import asyncpg
import re

from _bootstrap import build_postgres_dsn

async def precision_ls_duplicate_check():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 제목 정규화 로직 (공백 제거, 특수문자 제거 등)을 쿼리에서 수행
    # 2. LS와 이베스트를 하나의 범주로 통합
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id,
                reg_dt,
                key,
                sync_status,
                -- 제목 정규화: 공백 제거, 소문자화
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title,
                firm_nm,
                article_title
            FROM tbl_sec_reports
            WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
        ),
        dup_groups AS (
            SELECT norm_title, reg_dt, COUNT(*) as cnt
            FROM normalized_reports
            GROUP BY norm_title, reg_dt
            HAVING COUNT(*) > 1
        )
            SELECT 
            n.report_id, n.reg_dt, n.firm_nm, n.article_title, n.key, n.sync_status
        FROM normalized_reports n
        JOIN dup_groups d ON n.norm_title = d.norm_title AND n.reg_dt = d.reg_dt
        ORDER BY n.reg_dt DESC, n.norm_title
    """
    
    rows = await conn.fetch(query)
    
    # 통계 계산
    group_set = set()
    for r in rows:
        # 제목 정규화하여 그룹 식별
        ntitle = re.sub(r'\s+', '', r['article_title']).lower()
        group_set.add((ntitle, r['reg_dt']))
    
    print(f"--- LS/이베스트 정밀 중복 분석 ---")
    print(f"1. 중복 의심 레코드 총수: {len(rows)}건")
    print(f"2. 중복 그룹 수 (동일 리포트로 추정되는 세트): {len(group_set)}개")
    print(f"3. 삭제 가능 후보 수 (전체 - 그룹수): {len(rows) - len(group_set)}건")

    if rows:
        print("\n[발견된 주요 중복 패턴]")
        count = 0
        last_group = None
        for r in rows:
            ntitle = re.sub(r'\s+', '', r['article_title']).lower()
            current_group = (ntitle, r['reg_dt'])
            if current_group != last_group:
                if count >= 3: break
                print(f"\n[날짜: {r['reg_dt']}] {r['article_title']}")
                last_group = current_group
                count += 1
            print(f"  - ID: {r['report_id']} | Status: {r['sync_status']} | Firm: {r['firm_nm']}")
            
    await conn.close()

if __name__ == "__main__":
    asyncio.run(precision_ls_duplicate_check())
