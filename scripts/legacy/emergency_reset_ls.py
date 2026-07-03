import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def emergency_reset_status():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 로그에 언급된 직접적인 ID들 및 2021-01월 근처의 LS/이베스트 리포트들 식별
    # 사용자가 언급한 21년도 데이터 전반에 대해 재처리 설정
    print("--- LS/이베스트 2021년 데이터 재처리(Status 3) 설정 시작 ---")
    
    # 특히 문제가 된 ID들 우선 확인
    target_ids = [231812913, 231812912, 231819281, 231819280, 231819276]
    
    # 해당 ID들이 DB에 있는지 확인하고 상태 업데이트
    # (만약 정규화 과정에서 삭제되었다면 다시 살릴 순 없으나, 
    #  남아있는 레코드들은 모두 재처리 대기로 변경)
    
    result = await conn.execute("""
        UPDATE tbl_sec_reports 
        SET sync_status = 3, retry_count = 0
        WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
          AND report_date LIKE '202101%'
    """)
    
    print(f"2021년 1월 LS/이베스트 리포트 {result}건을 재처리 대기(Status 3)로 변경했습니다.")

    # 추가로 2021년 전체 데이터에 대해서도 상태를 확인하고 필요한 경우 재처리 설정
    # (파일이 지워졌을 가능성이 있으므로 안전하게 다시 받도록 함)
    result_year = await conn.execute("""
        UPDATE tbl_sec_reports 
        SET sync_status = 3, retry_count = 0
        WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
          AND report_date LIKE '2021%'
          AND sync_status = 2
    """)
    
    print(f"2021년 전체 LS/이베스트 리포트 중 완료(2) 상태였던 {result_year}건을 재처리 대기(3)로 변경했습니다.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(emergency_reset_status())
