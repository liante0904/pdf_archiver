import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def emergency_rollback():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    print("--- [긴급 원상복구] 2021년 전체 데이터 Status 2로 복원 ---")
    
    # 1. 아까 3으로 바꿨던 2021년 전체 데이터를 다시 2(완료)로 롤백
    result_revert = await conn.execute("""
        UPDATE tbl_sec_reports 
        SET sync_status = 2
        WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
          AND report_date LIKE '2021%'
          AND sync_status = 3
    """)
    print(f"롤백 완료: {result_revert}건을 다시 정상(Status 2)으로 되돌렸습니다.")

    # 2. 실제 취소되기 전까지 rclone으로 파일이 지워졌던 딱 5건만 Status 3으로 설정
    target_ids = [231812913, 231812912, 231819281, 231819280, 231819276]
    print(f"\n--- [실제 처리건 복구] 삭제된 5건만 재처리(Status 3) 설정 ---")
    result_target = await conn.execute("""
        UPDATE tbl_sec_reports 
        SET sync_status = 3, retry_count = 0
        WHERE report_id = ANY($1::bigint[])
    """, target_ids)
    
    print(f"재처리 설정 완료: {result_target}건 (지워진 파일들만 타겟팅).")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(emergency_rollback())
