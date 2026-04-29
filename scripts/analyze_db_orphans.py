import asyncio
import asyncpg
import json

from _bootstrap import build_postgres_dsn

async def analyze_orphan_records():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    # 1. tbl_sec_reports_pdf_archive 에는 있지만 TBL_SEC_REPORTS 에는 없는 레코드 찾기
    # 특히 LS증권/이베스트투자증권 필터링
    query = """
        SELECT m.report_id, m.firm_nm, m.title, m.file_path
        FROM "tbl_sec_reports_pdf_archive" m
        LEFT JOIN tbl_sec_reports r ON m.report_id = r.report_id
        WHERE r.report_id IS NULL
          AND (m.firm_nm LIKE '%LS%' OR m.firm_nm LIKE '%이베스트%')
    """
    
    orphans = await conn.fetch(query)
    
    print(f"--- DB 검증 결과 ---")
    print(f"메인 테이블에서 삭제된 LS/이베스트 리포트 수: {len(orphans)}개")
    
    if orphans:
        print("\n[상세 예시 (상위 10개)]")
        for o in orphans[:10]:
            print(f"ID: {o['report_id']} | 증권사: {o['firm_nm']} | 경로: {o['file_path']}")
            
    await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_orphan_records())
