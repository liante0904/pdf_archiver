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
import shutil
from pathlib import Path
from datetime import datetime

# --- 환경 및 설정 (서버/로컬 공용) ---
DB_PATH = os.path.expanduser("~/sqlite3/telegram.db")
LOCAL_BUFFER_DIR = os.path.expanduser("~/downloads/pdf_archive_temp")
RCLONE_BIN = shutil.which("rclone") or os.path.expanduser("~/.local/bin/rclone")
RCLONE_REMOTE = "onedrive:/archive/pdf"
LOCK_FILE = "/tmp/pdf_archiver_async.lock"

# --- 엄격한 실행 제어 ---
MAX_PROCESS_COUNT = 50  # 실운용을 위해 50건으로 확대
MAX_RETRY_PER_FILE = 3  # 파일당 재시도 횟수
MAX_CONCURRENCY = 1    # 안정성을 위해 동시 처리는 1건(Sequential)

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
        """최신 25건 + 과거 25건을 회사별로 교차하여 추출 (상태 3은 무조건 최우선)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # CTE를 사용하여 최신/과거 데이터를 각각 25건씩 독립적으로 추출 후 합침
            query = """
                WITH base AS (
                    SELECT * FROM data_main_daily_send
                    WHERE sync_status IN (0, 1, 3)
                    AND report_id IS NOT NULL
                    AND (TELEGRAM_URL != '' OR DOWNLOAD_URL != '' OR ATTACH_URL != '')
                    AND retry_count < 5
                    AND FIRM_NM NOT IN ('DB금융투자', '현대차증권')
                ),
                newest AS (
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY FIRM_NM ORDER BY REG_DT DESC) as firm_rank
                        FROM base
                    ) ORDER BY (CASE WHEN sync_status = 3 THEN 0 ELSE 1 END), firm_rank, REG_DT DESC LIMIT 25
                ),
                oldest AS (
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY FIRM_NM ORDER BY REG_DT ASC) as firm_rank
                        FROM base
                    ) ORDER BY (CASE WHEN sync_status = 3 THEN 0 ELSE 1 END), firm_rank, REG_DT ASC LIMIT 25
                )
                SELECT id, report_id, TELEGRAM_URL, DOWNLOAD_URL, ATTACH_URL, sync_status, retry_count, FIRM_NM, ARTICLE_TITLE, REG_DT
                FROM (
                    SELECT * FROM newest
                    UNION
                    SELECT * FROM oldest
                )
                ORDER BY (CASE WHEN sync_status = 3 THEN 0 ELSE 1 END), firm_rank, REG_DT DESC
                LIMIT 50
            """
            cursor.execute(query)
            return cursor.fetchall()

    def update_db(self, row_id, status, retry):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE data_main_daily_send SET sync_status = ?, retry_count = ? WHERE id = ?",
                (status, retry, row_id)
            )
            conn.commit()

    async def upload_to_rclone(self, file_path):
        """rclone move를 사용하여 원드라이브로 개별 이동"""
        if not RCLONE_BIN or not os.path.exists(RCLONE_BIN):
            logging.warning(f"Rclone binary not found at {RCLONE_BIN}. File remains at {file_path}")
            return False

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
        except Exception as e:
            logging.error(f"Rclone Exception: {e}")
            return False

    def _clean_title(self, title):
        if not title: return "no_title"
        text = re.sub(r'\[.*?\]|\(.*?\)|\【.*?\】', '', title)
        text = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,]', ' ', text)
        return "_".join(text.split())[:60].strip('_')

    async def download_file(self, session, urls, file_path, firm):
        """가용한 모든 URL(TELEGRAM -> DOWNLOAD -> ATTACH)을 순차적으로 시도"""
        tmp_path = file_path.with_suffix('.tmp')
        
        # 유효한 URL만 필터링
        valid_urls = [u for u in urls if u and u.startswith('http')]
        
        for url in valid_urls:
            for attempt in range(1, MAX_RETRY_PER_FILE + 1):
                try:
                    timeout = aiohttp.ClientTimeout(total=60 * attempt, connect=10, sock_read=45)
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 403 or resp.status == 404:
                            logging.warning(f"[{firm}] HTTP {resp.status} for {url}")
                            break # 다음 URL 시도
                        
                        resp.raise_for_status()
                        
                        with open(tmp_path, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(128 * 1024):
                                f.write(chunk)
                        
                        # PDF 최소 요건 및 헤더 검증
                        if tmp_path.stat().st_size < 100:
                            raise ValueError("File too small")
                        with open(tmp_path, 'rb') as f:
                            if f.read(4) != b'%PDF':
                                raise ValueError("Invalid PDF header")
                        
                        if file_path.exists(): os.remove(file_path)
                        os.rename(tmp_path, file_path)
                        return True
                except Exception as e:
                    logging.warning(f"[{firm}] Failed Attempt {attempt} | URL: {url} | Error: {e}")
                    if tmp_path.exists(): os.remove(tmp_path)
                    await asyncio.sleep(1)
            
            logging.info(f"[{firm}] Trying next available URL for {file_path.name}...")
        return False

    async def process_row(self, session, row):
        if self.processed_count >= MAX_PROCESS_COUNT:
            return
            
        row_id, report_id, tel_url, dw_url, att_url, status, retry, firm, title, reg_dt = row
        urls = [tel_url, dw_url, att_url] # 순서대로 시도
        
        # 파일명 및 경로 생성
        clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
        y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
        yy_mm_dd = clean_dt[2:8]
        clean_title = self._clean_title(title)
        filename = f"{yy_mm_dd}_{clean_title}_{report_id}.pdf"
        
        target_dir = self.local_dir / y_m / firm
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename
        rel_path = f"{y_m}/{firm}/{filename}"

        logging.info(f"Processing ({self.processed_count+1}/{MAX_PROCESS_COUNT}): [{firm}] {filename}")
        
        async with self.semaphore:
            success = await self.download_file(session, urls, file_path, firm)
            
            if success:
                # 다운로드 성공 시 상태 1로 우선 업데이트
                self.update_db(row_id, 1, 0)
                
                # 업로드 시도
                upload_success = await self.upload_to_rclone(file_path)
                
                if upload_success:
                    self.update_db(row_id, 2, 0) # 완전 완료
                    self.processed_count += 1
                    logging.info(f"Successfully archived: [OneDrive]/archive/pdf/{rel_path}")
                else:
                    logging.warning(f"[{firm}] Download OK but Upload Failed: {rel_path}. Status set to 1.")
            else:
                self.update_db(row_id, 0, retry + 1)
                valid_urls = [u for u in urls if u and u.startswith('http')]
                logging.error(f"[{firm}] ALL SOURCES FAILED: {filename} | Sources tried: {valid_urls}")

    async def run(self):
        targets = self.get_targets()
        if not targets:
            logging.info("No targets to process.")
            return
        
        logging.info(f"Starting PDF Archiver. Target: {len(targets)} items.")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            for row in targets:
                if self.processed_count >= MAX_PROCESS_COUNT:
                    break
                await self.process_row(session, row)
                await asyncio.sleep(0.5)

        logging.info(f"Archiver Finished. Processed: {self.processed_count} files.")

if __name__ == "__main__":
    lock_f = open(LOCK_FILE, 'w')
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logging.error("Another process is running.")
        sys.exit(1)
        
    try:
        asyncio.run(DownloadManager().run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Critical System Error: {e}", exc_info=True)
    finally:
        lock_f.close()
        if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)
