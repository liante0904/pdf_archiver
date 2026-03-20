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
LOG_FILE = os.path.expanduser("~/log/pdf_local_batch_organizer.log")
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
        """rclone move를 사용하여 업로드 후 로컬 삭제 (비동기)"""
        remote_dest_dir = f"{RCLONE_REMOTE}/{os.path.dirname(remote_rel_path)}"
        cmd = [RCLONE_BIN, "move", str(local_path), remote_dest_dir, "--quiet"]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            logging.info(f"성공: {remote_rel_path}")
            return True
        else:
            logging.error(f"실패: {remote_rel_path} ({stderr.decode().strip()})")
            return False

    async def process_single_root_file(self, db, file_path):
        """루트에 있는 숫자.pdf 파일 하나를 처리 (세마포어 적용)"""
        async with self.semaphore:
            report_id = file_path.stem
            metadata = await self.get_file_metadata(db, report_id)
            
            if not metadata:
                return

            firm, title, reg_dt, r_id = metadata
            clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
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
