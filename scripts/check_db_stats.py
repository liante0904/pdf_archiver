"""
DB 통계 확인 스크립트

이 스크립트는 다음 정보를 출력합니다:
1. LS증권/이베스트투자증권 관련 메인 테이블(tbl_sec_reports) 레코드 총수
2. LS증권/이베스트투자증권 관련 아카이브 테이블(tbl_sec_reports_pdf_archive) 레코드 총수
3. 메인 테이블에는 없지만 아카이브 테이블에만 존재하는 '고아' 메타데이터 수
"""
import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def check_db_stats():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. LS증권/이베스트 레코드 총수 (메인 테이블)
    ls_count = await conn.fetchval("""
        SELECT COUNT(*) FROM tbl_sec_reports 
        WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
    """)
    
    # 2. LS증권/이베스트 레코드 총수 (메타데이터 테이블)
    ls_meta_count = await conn.fetchval("""
        SELECT COUNT(*) FROM "tbl_sec_reports_pdf_archive" 
        WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
    """)

    # 3. 전체 고아 메타데이터 (메인 테이블에 없는 모든 것)
    total_orphans = await conn.fetchval("""
        SELECT COUNT(*) FROM "tbl_sec_reports_pdf_archive" m
        LEFT JOIN tbl_sec_reports r ON m.report_id = r.report_id
        WHERE r.report_id IS NULL
    """)
    
    print(f"LS/이베스트 메인 레코드 수: {ls_count}")
    print(f"LS/이베스트 메타데이터 수: {ls_meta_count}")
    print(f"전체 고아 메타데이터 수: {total_orphans}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_db_stats())
