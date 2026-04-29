import asyncio
import asyncpg
import re
from collections import Counter
from urllib.parse import urlparse, parse_qs

from _bootstrap import build_postgres_dsn

async def analyze_key_patterns():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 중복 그룹 추출
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id, reg_dt, key, firm_nm, article_title,
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title
            FROM tbl_sec_reports
            WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
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
    
    patterns = []
    diff_types = Counter()
    
    current_group_key = None
    group_rows = []
    
    for r in rows:
        group_id = (r['norm_title'], r['reg_dt'])
        if group_id != current_group_key:
            if group_rows:
                # 그룹 내 KEY 차이 분석
                keys = [gr['key'] for gr in group_rows]
                p_keys = [urlparse(k) for k in keys]
                
                # 패턴 분류
                if all(k == keys[0] for k in keys):
                    diff_types["KEY가 완벽히 동일함"] += 1
                elif all(pk.netloc == p_keys[0].netloc for pk in p_keys):
                    # 도메인은 같으나 파라미터나 경로가 다름
                    diff_types["동일 도메인 내 파라미터/경로 차이"] += 1
                elif any('ls-sec.co.kr' in k for k in keys) and any('ebestsec.co.kr' in k for k in keys):
                    diff_types["도메인 혼용 (LS vs 이베스트)"] += 1
                else:
                    diff_types["기타 (완전히 다른 URL 구조)"] += 1
                
                patterns.append({
                    "title": group_rows[0]['article_title'],
                    "date": group_rows[0]['reg_dt'],
                    "keys": keys
                })
            
            group_rows = [r]
            current_group_key = group_id
        else:
            group_rows.append(r)

    print(f"--- LS/이베스트 KEY 분포 및 규칙 분석 ---")
    print(f"총 중복 그룹 수: {len(patterns)}개")
    print("\n[KEY 차이 유형 분포]")
    for t, c in diff_types.most_common():
        print(f"- {t}: {c}개 ({c/len(patterns)*100:.1f}%)")

    print("\n[주요 KEY 패턴 사례]")
    for p in patterns[:5]:
        print(f"\n그룹: {p['date']} | {p['title']}")
        for i, k in enumerate(p['keys']):
            # URL 핵심 부분만 출력
            parsed = urlparse(k)
            qs = parse_qs(parsed.query)
            # boardId, itemId 등 주요 파라미터 추출
            core_params = {k: v[0] for k, v in qs.items() if k in ('boardId', 'itemId', 'seq', 'no')}
            print(f"  {i+1}) Domain: {parsed.netloc} | Params: {core_params}")
            print(f"     Full: {k[:80]}...")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_key_patterns())
