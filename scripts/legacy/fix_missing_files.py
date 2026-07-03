import asyncio
import asyncpg
import subprocess

from _bootstrap import build_postgres_dsn

async def fix_missing_files():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    print("--- [긴급 복구] 유실된 파일 색출 및 재처리 설정 ---")
    
    # 1. 2021-01월 LS증권 실제 원드라이브 파일 목록 가져오기
    remote_path = "onedrive:/archive/pdf/2021-01/LS증권"
    proc = subprocess.run(["rclone", "lsf", "--files-only", remote_path], capture_output=True, text=True)
    actual_files = [f.strip() for f in proc.stdout.splitlines()]
    
    print(f"원드라이브(2021-01/LS증권)에 남아있는 파일 수: {len(actual_files)}개")

    # 2. DB 메타데이터에서 2021-01월 LS/이베스트 리포트 목록 가져오기
    rows = await conn.fetch("""
        SELECT r.report_id, m.file_path 
        FROM tbl_sec_reports r
        JOIN "tbl_sec_reports_pdf_archive" m ON r.report_id = m.report_id
        WHERE (r.firm_nm LIKE '%LS%' OR r.firm_nm LIKE '%이베스트%')
          AND r.report_date LIKE '202101%'
          AND r.sync_status = 2
    """)
    
    missing_ids = []
    
    # 3. DB에는 있다고 되어 있는데, 실제 파일 목록에 없으면 유실된 것임
    for r in rows:
        file_path = r['file_path']
        if not file_path: continue
        
        # file_path 예: 2021-01/LS증권/210119_제목_1234.pdf
        filename = file_path.split('/')[-1]
        
        if filename not in actual_files:
            missing_ids.append(r['report_id'])

    print(f"제 실수로 인해 휩쓸려 지워진 활성 리포트 수: {len(missing_ids)}개")

    # 4. 유실된 파일들에 대해 재처리(Status 3) 지시
    if missing_ids:
        await conn.execute("""
            UPDATE tbl_sec_reports 
            SET sync_status = 3, retry_count = 0
            WHERE report_id = ANY($1::bigint[])
        """, missing_ids)
        
        # 메타데이터에서도 삭제 (새로 다운받아 등록하게 하기 위함)
        await conn.execute('DELETE FROM "tbl_sec_reports_pdf_archive" WHERE report_id = ANY($1::bigint[])', missing_ids)
        
        print(f"-> {len(missing_ids)}개 레코드를 완벽하게 재처리(Status 3) 대상으로 복구했습니다.")
        print("   (아카이버가 다음 실행 시 다시 안전하게 다운로드할 것입니다.)")
    else:
        print("다행히 활성 리포트 중 지워진 파일은 없습니다.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(fix_missing_files())
