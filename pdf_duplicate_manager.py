import subprocess
import re
import os
import unicodedata
import sys
from collections import defaultdict

# --- 설정 ---
BASE_REMOTE_DIR = "onedrive:/archive/pdf"

def get_subfolders():
    """상위 폴더 목록(예: 2024-05, 2024-06)을 가져옵니다."""
    cmd = ["rclone", "lsf", "--dirs-only", BASE_REMOTE_DIR]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return [line.strip('/') for line in result.stdout.splitlines() if line.strip()]

def get_remote_files(target_subfolder):
    """지정된 하위 폴더에서 파일 목록을 가져와 NFC로 정규화합니다."""
    remote_path = f"{BASE_REMOTE_DIR}/{target_subfolder}"
    print(f"\n[INFO] {remote_path} 조사 중... 잠시만 기다려 주세요.")
    cmd = ["rclone", "lsf", "-R", "--files-only", remote_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"오류 발생: {result.stderr}")
        return []
    
    return [unicodedata.normalize('NFC', line) for line in result.stdout.splitlines() if line.strip()]

def score_filename(filename):
    """사용자님의 규칙에 얼마나 잘 맞는지 점수를 매깁니다. (높을수록 좋음)"""
    score = 0
    # 1. 공백이 없어야 함
    if ' ' not in filename: score += 20
    # 2. 특수문자(-, ;, [, ])가 없어야 함 (확장자 제외)
    title_part = filename.rsplit('.', 1)[0]
    clean_pattern = re.compile(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]\(\)\-\+_]')
    if not clean_pattern.search(title_part.replace('_', '')): # 언더바 제외 특수문자 체크
        score += 15
    # 3. 표준 패턴 YYMMDD_제목_ID.pdf 에 완벽히 부합하는지
    if re.match(r'^\d{6}_[a-zA-Z0-9가-힣_]+_\d+\.pdf$', filename):
        score += 30
    # 4. 파일명이 너무 길지 않은지 (축약 룰)
    if len(filename) < 100:
        score += 10
    return score

def run_cleanup(target_subfolder, execute=False):
    files = get_remote_files(target_subfolder)
    if not files:
        print(f"'{target_subfolder}' 폴더에 파일이 없습니다.")
        return

    # report_id별로 파일 그룹화
    id_map = defaultdict(list)
    id_pattern = re.compile(r'_(\d+)\.pdf$')

    for file_path in files:
        filename = os.path.basename(file_path)
        match = id_pattern.search(filename)
        if match:
            r_id = match.group(1)
            id_map[r_id].append(file_path)

    total_deleted = 0
    found_duplicates = 0
    remote_root = f"{BASE_REMOTE_DIR}/{target_subfolder}"

    for r_id, paths in id_map.items():
        if len(paths) > 1:
            found_duplicates += 1
            # 점수가 높은 순으로 정렬
            paths.sort(key=lambda x: score_filename(os.path.basename(x)), reverse=True)
            
            keep_path = paths[0]
            delete_paths = paths[1:]
            
            print(f"\n[ID: {r_id}] 중복 {len(paths)}개 발견")
            print(f"  √ 유지: {keep_path}")
            
            for dp in delete_paths:
                print(f"  × 삭제 대상: {dp}")
                if execute:
                    full_remote_path = f"{remote_root}/{dp}"
                    res = subprocess.run(["rclone", "deletefile", full_remote_path], capture_output=True, text=True)
                    if res.returncode == 0:
                        total_deleted += 1
                    else:
                        print(f"    - 삭제 실패: {res.stderr}")
                else:
                    total_deleted += 1

    if not execute:
        print("\n" + "="*50)
        print(f"[{target_subfolder}] 건조 실행(Dry Run) 완료.")
        print(f"중복 ID: {found_duplicates}개 / 삭제 예정: {total_deleted}개")
        print(f"실제로 삭제하려면 명령어 뒤에 --execute 를 붙여주세요.")
        print(f"예시: python3 pdf_duplicate_manager.py {target_subfolder} --execute")
        print("="*50)
    else:
        print(f"\n[{target_subfolder}] 정리 완료. 총 {total_deleted}개의 파일을 삭제했습니다.")

if __name__ == "__main__":
    # 플래그 확인
    is_execute = "--execute" in sys.argv
    is_all = "--all" in sys.argv
    # 폴더 인자가 있는지 확인
    folder_args = [a for a in sys.argv[1:] if not a.startswith("--")]
    
    if is_all:
        # 모든 폴더 순차 처리
        print("\n[INFO] 모든 폴더에 대해 중복 정리를 시작합니다.")
        try:
            folders = sorted(get_subfolders(), reverse=True)
            for folder in folders:
                run_cleanup(folder, execute=is_execute)
            print("\n[SUCCESS] 전체 폴더 정리가 완료되었습니다.")
        except KeyboardInterrupt:
            print("\n취소되었습니다.")
            sys.exit(0)
    elif folder_args:
        target = folder_args[0].strip('/')
        run_cleanup(target, execute=is_execute)
    else:
        # 인자가 없으면 대화형으로 폴더 목록 보여주기
        print("\n[중복 관리 도구] 조사할 폴더 번호를 선택하세요:")
        try:
            folders = sorted(get_subfolders(), reverse=True)
            if not folders:
                print("조사할 폴더가 없습니다.")
                sys.exit(0)
                
            for i, f in enumerate(folders, 1):
                print(f"  [{i}] {f}")
            
            choice = input(f"\n번호 입력 (1-{len(folders)}, 취소: q): ").strip()
            if choice.lower() == 'q':
                sys.exit(0)
            
            if choice.isdigit() and 1 <= int(choice) <= len(folders):
                target = folders[int(choice)-1]
            else:
                print("잘못된 선택입니다.")
                sys.exit(1)
        except KeyboardInterrupt:
            print("\n취소되었습니다.")
            sys.exit(0)

    if target:
        run_cleanup(target, execute=is_execute)
