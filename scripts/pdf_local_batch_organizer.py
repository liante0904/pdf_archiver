"""
로컬 PDF 파일 일괄 정리 및 OneDrive 업로드 도구

이 스크립트는 로컬에 다운로드된 PDF 파일들을 DB 정보를 바탕으로 이름을 변경하고 OneDrive로 업로드합니다:
1. ~/downloads 폴더 내의 '숫자.pdf' 형식의 파일들을 찾아 report_id로 인식합니다.
2. SQLite DB(telegram.db)에서 해당 report_id의 메타데이터(증권사, 제목, 날짜)를 가져옵니다.
3. 표준 파일명 규칙(YYMMDD_제목_ID.pdf)에 따라 이름을 변경하고 월별/증권사별 폴더 구조로 정리합니다.
4. rclone을 사용하여 OneDrive로 업로드하며, 업로드 성공 및 파일 크기 검증 후 로컬 파일을 삭제합니다.
5. 비동기(asyncio) 및 세마포어를 사용하여 병렬 업로드를 수행합니다.
"""
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiosqlite",
# ]
# ///

import asyncio
import aiosqlite
import os
import logging
import subprocess
import fcntl
import sys
import re
import shutil
import json
from pathlib import Path

# --- 설정 ---
DB_PATH = os.path.expanduser("~/sqlite3/telegram.db")
SEARCH_BASE = os.path.expanduser("~/downloads")
LOCAL_ARCHIVE_ROOT = os.path.expanduser("~/downloads/pdf_archive_temp")
RCLONE_BIN = shutil.which("rclone") or os.path.expanduser("~/.local/bin/rclone")
RCLONE_REMOTE = "onedrive:/archive/pdf"
LOCK_FILE = "/tmp/pdf_local_batch_organizer.lock"

# --- 제어 ---
MAX_CONCURRENCY = 5  # 동시에 실행할 rclone 작업 수 (추후 10으로 확장 가능)

# 로깅 설정
LOG_FILE = os.path.expanduser("~/logs/pdf_local_batch_organizer.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ASYNC_ORGANIZER] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)

class PDFLocalBatchOrganizer:
    def __init__(self):
        self.db_path = DB_PATH
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def get_file_metadata(self, db, report_id):
        """DB에서 report_id를 기반으로 정보 추출"""
        async with db.execute("""
            SELECT FIRM_NM, ARTICLE_TITLE, REG_DT, report_id
            FROM data_main_daily_send
            WHERE report_id = ?
            LIMIT 1
        """, (report_id,)) as cursor:
            return await cursor.fetchone()

    def _clean_title(self, title):
        if not title: return "no_title"
        text = re.sub(r'\[.*?\]|\(.*?\)|\【.*?\】', '', title)
        text = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,]', ' ', text)
        return "_".join(text.split())[:60].strip('_')

    async def check_remote_exists(self, remote_path):
        """rclone을 사용하여 원격지에 파일이 이미 존재하는지 비동기 확인"""
        cmd = [RCLONE_BIN, "lsf", f"{RCLONE_REMOTE}/{remote_path}"]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        return proc.returncode == 0 and stdout.decode().strip() != ""

    async def upload_and_remove(self, local_path, remote_rel_path):
        """rclone copy 후 원격지 파일 크기를 검증하여 성공 시에만 로컬 삭제"""
        remote_dest_dir = f"{RCLONE_REMOTE}/{os.path.dirname(remote_rel_path)}"
        filename = os.path.basename(remote_rel_path)
        
        # move 대신 copy 사용 (안전)
        cmd = [RCLONE_BIN, "copy", str(local_path), remote_dest_dir]
        
        try:
            # 1. 업로드 실행
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await proc.communicate()
            
            # 2. 원격지 검증 (lsjson)
            is_uploaded = False
            check_cmd = [RCLONE_BIN, "lsjson", f"{RCLONE_REMOTE}/{remote_rel_path}"]
            check_proc = await asyncio.create_subprocess_exec(
                *check_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            check_out, _ = await check_proc.communicate()

            stderr_text = stderr.decode()
            if check_out:
                try:
                    file_info = json.loads(check_out.decode())
                    if file_info and file_info[0].get('Size', 0) == local_path.stat().st_size:
                        is_uploaded = True
                except:
                    pass

            # 업로드 성공 또는 이미 존재(nameAlreadyExists)하는 경우 검증 통과 시 성공 처리
            if (proc.returncode == 0 or "nameAlreadyExists" in stderr_text) and is_uploaded:
                logging.info(f"성공 및 검증 완료: {remote_rel_path}")
                if local_path.exists():
                    local_path.unlink() # 검증 완료 후 삭제
                return True
            else:
                logging.error(f"실패 또는 검증 오류: {remote_rel_path} ({stderr_text.strip()})")
                return False        except Exception as e:
            logging.error(f"업로드 중 예외 발생: {e}")
            return False

    async def process_single_root_file(self, db, file_path):
        """루트에 있는 숫자.pdf 파일 하나를 처리 (세마포어 적용)"""
        async with self.semaphore:
            report_id = file_path.stem
            metadata = await self.get_file_metadata(db, report_id)
            
            if not metadata:
                return

            firm, title, report_date, r_id = metadata
            clean_dt = re.sub(r'[^0-9]', '', str(report_date)) if report_date else "00000000"
            y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
            yy_mm_dd = clean_dt[2:8]
            clean_title = self._clean_title(title)
            new_filename = f"{yy_mm_dd}_{clean_title}_{r_id}.pdf"
            remote_rel_path = f"{y_m}/{firm}/{new_filename}"

            # 1. 원격지 중복 체크
            if await self.check_remote_exists(remote_rel_path):
                logging.info(f"[중복 제거] 원격지 존재 -> 삭제: {file_path.name}")
                file_path.unlink()
                return

            # 2. 이동 및 업로드
            temp_organized_path = Path(LOCAL_ARCHIVE_ROOT) / y_m / firm / new_filename
            temp_organized_path.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                shutil.move(str(file_path), str(temp_organized_path))
                await self.upload_and_remove(temp_organized_path, remote_rel_path)
            except Exception as e:
                logging.error(f"이동 중 에러: {file_path.name} -> {e}")

    async def process_organized_file(self, file_path):
        """이미 정리된 하위 폴더 내의 파일 하나를 처리 (세마포어 적용)"""
        async with self.semaphore:
            rel_path = file_path.relative_to(LOCAL_ARCHIVE_ROOT)
            
            if await self.check_remote_exists(str(rel_path)):
                logging.info(f"[중복 제거] 원격지 존재 -> 삭제: {rel_path}")
                file_path.unlink()
            else:
                await self.upload_and_remove(file_path, str(rel_path))

    async def run(self):
        async with aiosqlite.connect(self.db_path) as db:
            # 1. 루트 파일들 수집
            root_files = []
            for folder in Path(SEARCH_BASE).glob("pdf_*"):
                if folder.is_dir():
                    for f in folder.glob("*.pdf"):
                        if f.parent == folder and f.stem.isdigit():
                            root_files.append(f)
            
            if root_files:
                logging.info(f"루트 파일 {len(root_files)}개 처리 시작 (병렬 5)...")
                tasks = [self.process_single_root_file(db, f) for f in root_files]
                await asyncio.gather(*tasks)

            # 2. 정리된 폴더 내 파일들 수집
            organized_files = list(Path(LOCAL_ARCHIVE_ROOT).glob("*/*/*.pdf"))
            if organized_files:
                logging.info(f"정리된 폴더 내 {len(organized_files)}개 중복 체크 시작...")
                tasks = [self.process_organized_file(f) for f in organized_files]
                await asyncio.gather(*tasks)

        # 3. 빈 폴더 삭제
        subprocess.run(["find", LOCAL_ARCHIVE_ROOT, "-type", "d", "-empty", "-delete"])
        logging.info("모든 작업 완료.")

if __name__ == "__main__":
    lock_f = open(LOCK_FILE, 'w')
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logging.error("이미 실행 중입니다.")
        sys.exit(1)

    try:
        asyncio.run(PDFLocalBatchOrganizer().run())
    finally:
        lock_f.close()
        if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)
