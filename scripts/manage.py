#!/usr/bin/env python3
"""
PDF 아카이브 관리 도구 통합 CLI (manage.py)

이 도구는 scripts/ 폴더 내의 다양한 스크립트들을 기능별로 분류하여
터미널에서 쉽게 선택하고 실행할 수 있도록 도와줍니다.
"""

import os
import subprocess
import sys

# 스크립트 카테고리 정의
CATEGORIES = {
    "1": {
        "name": "DB 분석 및 통계 (DB Analysis & Stats)",
        "scripts": [
            ("check_db_stats.py", "기본 DB 통계 확인"),
            ("deep_analyze_db.py", "DB 심층 분석 (불일치 및 누락 확인)"),
            ("check_columns.py", "테이블 컬럼 구조 확인"),
            ("inspect_source_table.py", "메인 테이블 상세 조사"),
            ("get_report_details.py", "특정 리포트 상세 정보 조회"),
        ]
    },
    "2": {
        "name": "파일 스캔 및 고아 찾기 (File Scanning & Orphan Detection)",
        "scripts": [
            ("find_orphans_v3.py", "고속 고아 파일 탐색 (실시간 스트리밍)"),
            ("deep_orphan_scan.py", "OneDrive 전수 조사 및 정밀 교차 검증"),
            ("find_orphan_pdfs.py", "일반 고아 PDF 탐색"),
            ("analyze_db_orphans.py", "DB상에만 존재하는 고아 레코드 분석"),
            ("analyze_orphan_actions.py", "고아 파일 처리 계획 수립 (삭제/리네임)"),
            ("surgical_monthly_scan.py", "월별 정밀 스캔 및 대조"),
            ("single_month_check.py", "특정 월 폴더 집중 스캔"),
        ]
    },
    "3": {
        "name": "중복 관리 및 데이터 정제 (Duplicate & Data Cleanup)",
        "scripts": [
            ("checksum_scan.py", "파일 내용(체크썸) 기반 중복 스캔"),
            ("pdf_duplicate_manager.py", "파일명 규칙 기반 중복 정리 도구"),
            ("analyze_pdf_hash_duplicates.py", "pdf_hash 중복 그룹 분석"),
            ("plan_content_dedup.py", "비파괴적 내용 중복 제거 계획 수립"),
            ("apply_pdf_url_aliases.py", "pdf_url 중복에 대한 DB 별칭 업데이트"),
            ("delete_pdf_url_duplicate_files.py", "검증된 중복 파일 삭제 실행"),
            ("copy_pdf_url_canonicals_to_gdrive.py", "표준 파일을 Google Drive로 복사"),
        ]
    },
    "4": {
        "name": "유지보수 및 아카이빙 (Maintenance & Archiving)",
        "scripts": [
            ("pdf_local_batch_organizer.py", "로컬 다운로드 파일 일괄 정리 및 업로드"),
            ("backfill_pdf_hash.py", "기존 레코드 PDF 해시(Hash) 채워넣기"),
            ("update_sync_status.py", "특정 리포트 재처리 대기(Status 3) 설정"),
            ("check_sync_status.py", "동기화 상태 확인"),
            ("verify_onedrive_files.py", "OneDrive 아카이브 경로 검증"),
            ("dbfi_smoke.py", "DB금융투자 다운로드 스모크 테스트"),
        ]
    }
}

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def run_script(script_path):
    print(f"\n[실행 중] python3 scripts/{script_path}")
    print("-" * 50)
    try:
        # scripts 폴더 내에서 실행되도록 경로 조정
        subprocess.run([sys.executable, f"scripts/{script_path}"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n[오류] 스크립트 실행 중 에러가 발생했습니다: {e}")
    except KeyboardInterrupt:
        print("\n[중단] 사용자에 의해 실행이 중단되었습니다.")
    print("-" * 50)
    input("\n계속하려면 엔터를 누르세요...")

def main_menu():
    while True:
        clear_screen()
        print("=" * 60)
        print("   PDF 아카이브 관리 도구 통합 메뉴")
        print("=" * 60)
        
        for key, cat in CATEGORIES.items():
            print(f" [{key}] {cat['name']}")
        
        print(" [Q] 종료")
        print("-" * 60)
        
        choice = input("선택 (1-4, Q): ").strip().upper()
        
        if choice == 'Q':
            print("\n프로그램을 종료합니다.")
            break
            
        if choice in CATEGORIES:
            submenu(choice)
        else:
            print("\n[경고] 잘못된 선택입니다.")
            import time
            time.sleep(1)

def submenu(cat_key):
    category = CATEGORIES[cat_key]
    while True:
        clear_screen()
        print("=" * 60)
        print(f"   {category['name']}")
        print("=" * 60)
        
        scripts = category['scripts']
        for i, (path, desc) in enumerate(scripts, 1):
            print(f" [{i}] {desc} ({path})")
            
        print(" [B] 이전 메뉴로")
        print("-" * 60)
        
        choice = input(f"실행할 스크립트 선택 (1-{len(scripts)}, B): ").strip().upper()
        
        if choice == 'B':
            break
            
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(scripts):
                script_path = scripts[idx][0]
                run_script(script_path)
            else:
                print("\n[경고] 범위를 벗어난 번호입니다.")
                import time
                time.sleep(1)
        else:
            print("\n[경고] 숫자를 입력하거나 B를 눌러주세요.")
            import time
            time.sleep(1)

if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\n프로그램을 종료합니다.")
        sys.exit(0)
