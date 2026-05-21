"""
DB 심층 분석 스크립트

이 스크립트는 DB의 전반적인 상태를 심층적으로 분석합니다:
1. 전체 레코드 수 및 sync_status별 분포 확인
2. ARCHIVE_STATUS 및 DOWNLOAD_STATUS_YN 상태별 분포 확인
3. sync_status=1(완료) 임에도 ARCHIVE_PATH가 없는 불일치 사례 탐색
4. PDF URL은 있으나 아카이브 테이블에 등록되지 않은 레코드 수 확인
"""
import asyncio
import asyncpg
import os

from _bootstrap import build_postgres_dsn

async def analyze():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    print("--- Database Analysis ---")
    
    # 1. 전체 레코드 수
    total = await conn.fetchval('SELECT COUNT(*) FROM tbl_sec_reports')
    print(f"Total records in TBL_SEC_REPORTS: {total}")
    
    # 2. sync_status별 분포
    sync_stats = await conn.fetch('SELECT sync_status, COUNT(*) as cnt FROM tbl_sec_reports GROUP BY sync_status ORDER BY sync_status')
    print("\nsync_status distribution:")
    for r in sync_stats:
        print(f"  Status {r['sync_status']}: {r['cnt']} rows")
        
    # 3. ARCHIVE_STATUS별 분포
    archive_stats = await conn.fetch('SELECT archive_status, COUNT(*) as cnt FROM tbl_sec_reports GROUP BY archive_status')
    print("\nARCHIVE_STATUS distribution:")
    for r in archive_stats:
        print(f"  {r['archive_status']}: {r['cnt']} rows")

    # 4. DOWNLOAD_STATUS_YN별 분포
    download_stats = await conn.fetch('SELECT download_status_yn, COUNT(*) as cnt FROM tbl_sec_reports GROUP BY download_status_yn')
    print("\nDOWNLOAD_STATUS_YN distribution:")
    for r in download_stats:
        print(f"  {r['download_status_yn']}: {r['cnt']} rows")
        
    # 5. ARCHIVE_PATH는 없지만 sync_status가 1인 경우
    mismatch = await conn.fetchval('SELECT COUNT(*) FROM tbl_sec_reports WHERE sync_status = 1 AND archive_path IS NULL')
    print(f"\nsync_status=1 but ARCHIVE_PATH is NULL: {mismatch}")

    # 6. PDF 관련 URL이 존재하지만 아카이브 기록은 없는 경우
    has_url_no_archive = await conn.fetchval("""
        SELECT COUNT(*) FROM tbl_sec_reports 
        WHERE (pdf_url IS NOT NULL OR download_url IS NOT NULL)
          AND report_id NOT IN (SELECT report_id FROM tbl_sec_reports_pdf_archive)
    """)
    print(f"Has PDF URL but NOT in archive table: {has_url_no_archive}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze())
