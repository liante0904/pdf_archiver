# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiohttp",
# ]
# ///

import asyncio
import aiohttp
import sqlite3
import os
import logging
import subprocess
import fcntl
import sys
import re
from pathlib import Path
from datetime import datetime

import shutil

# --- 환경 및 설정 ---
DB_PATH = os.path.expanduser("~/sqlite3/telegram.db")
LOCAL_BUFFER_DIR = os.path.expanduser("~/downloads/pdf_archive_temp")
RCLONE_BIN = shutil.which("rclone") or os.path.expanduser("~/.local/bin/rclone")
RCLONE_REMOTE = "onedrive:/archive/pdf"
LOCK_FILE = "/tmp/pdf_archiver_async.lock"

# 요청하신 엄격한 제한 사항
MAX_PROCESS_COUNT = 3  # 무조건 3건만 처리
MAX_RETRY_PER_FILE = 3  # 파일당 최대 재시도 횟수
MAX_CONCURRENCY = 1    # 테스트 및 안정성을 위해 동시성 낮춤

# 로깅 설정
LOG_FILE = os.path.expanduser("~/log/pdf_archiver_async.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

class DownloadManager:
    def __init__(self):
        self.db_path = DB_PATH
        self.local_dir = Path(LOCAL_BUFFER_DIR)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.processed_count = 0
        self._setup_db()

    def _setup_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            try:
                # 필드 유무 확인 및 추가
                cursor = conn.execute("PRAGMA table_info(data_main_daily_send);")
                columns = [info[1] for info in cursor.fetchall()]
                if 'sync_status' not in columns:
                    conn.execute("ALTER TABLE data_main_daily_send ADD COLUMN sync_status INTEGER DEFAULT 0;")
                if 'retry_count' not in columns:
                    conn.execute("ALTER TABLE data_main_daily_send ADD COLUMN retry_count INTEGER DEFAULT 0;")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_status ON data_main_daily_send(sync_status);")
            except Exception as e:
                logging.error(f"DB Setup Error: {e}")
            conn.commit()

    def get_targets(self):
        """처리 대상 3건만 가져옴"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, report_id, ATTACH_URL, DOWNLOAD_URL, sync_status, retry_count, FIRM_NM, ARTICLE_TITLE, REG_DT
                FROM data_main_daily_send 
                WHERE sync_status IN (0, 1)
                AND report_id IS NOT NULL
                AND (ATTACH_URL IS NOT NULL AND ATTACH_URL != '' OR DOWNLOAD_URL IS NOT NULL AND DOWNLOAD_URL != '')
                AND retry_count < 5
                ORDER BY REG_DT DESC
                LIMIT ?
            """, (MAX_PROCESS_COUNT,))
            return cursor.fetchall()

    def update_db(self, row_id, status, retry):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE data_main_daily_send SET sync_status = ?, retry_count = ? WHERE id = ?",
                (status, retry, row_id)
            )
            conn.commit()

    async def upload_to_rclone(self, file_path):
        """개별 파일 업로드 (더 안전함)"""
        if not RCLONE_BIN or not os.path.exists(RCLONE_BIN):
            logging.warning(f"Rclone binary not found at {RCLONE_BIN}. Skipping upload for {file_path.name}")
            return False

        # 경로에서 상대 경로 추출 (y_m/firm/filename)
        rel_path = file_path.relative_to(self.local_dir)
        remote_dest = f"{RCLONE_REMOTE}/{rel_path.parent}"
        
        rclone_cmd = [
            RCLONE_BIN, "move", str(file_path), remote_dest,
            "--quiet", "--ignore-existing"
        ]
        
        try:
            proc = await asyncio.create_subprocess_exec(
                *rclone_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                logging.info(f"Rclone Success: {file_path.name}")
                return True
            else:
                logging.error(f"Rclone Error for {file_path.name}: {stderr.decode()}")
                return False
        except FileNotFoundError:
            logging.error(f"Rclone binary not found during execution: {RCLONE_BIN}")
            return False
        except Exception as e:
            logging.error(f"Unexpected Rclone Error: {e}")
            return False

    def _clean_title(self, title):
        if not title: return "no_title"
        text = re.sub(r'\[.*?\]|\(.*?\)|\【.*?\】', '', title)
        text = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,]', ' ', text)
        return "_".join(text.split())[:60].strip('_')

    async def download_file(self, session, url, file_path):
        """재시도 로직이 포함된 안전한 다운로드"""
        tmp_path = file_path.with_suffix('.tmp')
        for attempt in range(1, MAX_RETRY_PER_FILE + 1):
            try:
                # 타임아웃은 시도할 때마다 조금씩 늘림
                timeout = aiohttp.ClientTimeout(total=60 * attempt, connect=10, sock_read=45)
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 403 or resp.status == 404:
                        logging.warning(f"HTTP {resp.status} for {url}")
                        return False
                    
                    resp.raise_for_status()
                    
                    with open(tmp_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(128 * 1024): # 128KB chunks
                            f.write(chunk)
                    
                    # PDF 파일 유효성 검사 (최소 크기 및 헤더)
                    if tmp_path.stat().st_size < 100:
                        raise ValueError("File too small")
                        
                    with open(tmp_path, 'rb') as f:
                        header = f.read(4)
                        if header != b'%PDF':
                            raise ValueError(f"Not a PDF file (Header: {header})")
                    
                    if tmp_path.exists():
                        if file_path.exists(): os.remove(file_path)
                        os.rename(tmp_path, file_path)
                        return True
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed for {url}: {str(e)}")
                if tmp_path.exists(): os.remove(tmp_path)
                if attempt < MAX_RETRY_PER_FILE:
                    await asyncio.sleep(2 ** attempt) # Exponential backoff
        return False

    async def process_row(self, session, row):
        if self.processed_count >= MAX_PROCESS_COUNT:
            return
            
        row_id, report_id, attach_url, download_url, status, retry, firm, title, reg_dt = row
        url = download_url if download_url else attach_url
        
        # 파일명 및 경로 생성
        clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
        y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
        yy_mm_dd = clean_dt[2:8]
        clean_title = self._clean_title(title)
        filename = f"{yy_mm_dd}_{clean_title}_{report_id}.pdf"
        
        target_dir = self.local_dir / y_m / firm
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename

        logging.info(f"Processing ({self.processed_count+1}/{MAX_PROCESS_COUNT}): {filename}")
        
        async with self.semaphore:
            success = await self.download_file(session, url, file_path)
            
            if success:
                # 업로드 시도
                upload_success = await self.upload_to_rclone(file_path)
                if upload_success:
                    self.update_db(row_id, 2, 0) # 완료
                    self.processed_count += 1
                    logging.info(f"Successfully archived: {filename}")
                else:
                    self.update_db(row_id, 1, retry + 1) # 다운로드 성공했으나 업로드 실패
            else:
                self.update_db(row_id, 0, retry + 1) # 다운로드 실패
                logging.error(f"Failed to download after {MAX_RETRY_PER_FILE} attempts: {filename}")

    async def run(self):
        targets = self.get_targets()
        if not targets:
            logging.info("No targets found.")
            return
        
        logging.info(f"Starting PDF Archiver. Processing up to {MAX_PROCESS_COUNT} items.")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            for row in targets:
                if self.processed_count >= MAX_PROCESS_COUNT:
                    break
                await self.process_row(session, row)
                # 각 파일 처리 사이에 짧은 휴식 (서버 부하 방지)
                await asyncio.sleep(1)

        logging.info(f"Finished. Total processed: {self.processed_count}")

if __name__ == "__main__":
    # Lock Check
    lock_f = open(LOCK_FILE, 'w')
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logging.error("Another instance is already running.")
        sys.exit(1)
        
    try:
        asyncio.run(DownloadManager().run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        lock_f.close()
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
