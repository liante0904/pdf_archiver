# /// script
# requires-python = ">=3.10"
# ///

import sqlite3
import os
import logging
import subprocess
import fcntl
import sys
import re
import shutil
from pathlib import Path

# --- 설정 (아카이버와 동일한 환경 설정 사용) ---
DB_PATH = os.path.expanduser("~/sqlite3/telegram.db")
RCLONE_BIN = shutil.which("rclone") or os.path.expanduser("~/.local/bin/rclone")
RCLONE_REMOTE = "onedrive:/archive/pdf"
LOCK_FILE = "/tmp/pdf_root_legacy_organizer.lock"

# 로깅 설정 (별도 로그 파일 사용)
LOG_FILE = os.path.expanduser("~/log/pdf_root_legacy_organizer.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ROOT_ORGANIZER] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)

class PDFRootLegacyOrganizer:
    def __init__(self):
        self.db_path = DB_PATH

    def get_file_metadata(self, report_id):
        """DB에서 report_id를 기반으로 정석 경로 및 파일명 정보 추출"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT FIRM_NM, ARTICLE_TITLE, REG_DT, report_id
                FROM data_main_daily_send
                WHERE report_id = ?
                LIMIT 1
            """, (report_id,))
            return cursor.fetchone()

    def _clean_title(self, title):
        if not title: return "no_title"
        text = re.sub(r'\[.*?\]|\(.*?\)|\【.*?\】', '', title)
        text = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,]', ' ', text)
        return "_".join(text.split())[:60].strip('_')

    def get_remote_files(self):
        """원드라이브 archive/pdf/ 루트에 있는 파일 목록 가져오기"""
        logging.info("원드라이브 루트 파일 목록 조회 중...")
        cmd = [RCLONE_BIN, "lsf", f"{RCLONE_REMOTE}", "--max-depth", "1"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"목록 조회 실패: {result.stderr}")
            return []
        # .pdf로 끝나고 숫자로만 된 파일(report_id.pdf) 필터링
        return [f for f in result.stdout.splitlines() if f.endswith('.pdf') and f.replace('.pdf', '').isdigit()]

    def migrate(self):
        files = self.get_remote_files()
        if not files:
            logging.info("정리할 대상 파일이 없습니다.")
            return

        logging.info(f"총 {len(files)}개의 파일을 정리합니다.")

        for filename in files:
            report_id = filename.replace('.pdf', '')
            metadata = self.get_file_metadata(report_id)
            
            if not metadata:
                logging.warning(f"DB 정보 없음 (건너뜀): {filename}")
                continue
            
            firm, title, reg_dt, r_id = metadata
            
            # 정석 경로 생성 (YYYY-MM/Firm/YYYYMMDD_Title_report_id.pdf)
            clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
            y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
            yy_mm_dd = clean_dt[2:8]
            clean_title = self._clean_title(title)
            new_filename = f"{yy_mm_dd}_{clean_title}_{r_id}.pdf"
            new_path = f"{y_m}/{firm}/{new_filename}"

            logging.info(f"이동: {filename} -> {new_path}")

            # rclone move 실행
            move_cmd = [
                RCLONE_BIN, "move", 
                f"{RCLONE_REMOTE}/{filename}", 
                f"{RCLONE_REMOTE}/{y_m}/{firm}/",
                "--dest-filename", new_filename,
                "--quiet"
            ]
            
            res = subprocess.run(move_cmd, capture_output=True, text=True)
            if res.returncode == 0:
                logging.info(f"성공: {filename}")
            else:
                logging.error(f"실패: {filename} ({res.stderr})")

if __name__ == "__main__":
    lock_f = open(LOCK_FILE, 'w')
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("정리 스크립트가 이미 실행 중입니다.")
        sys.exit(1)

    try:
        migrator = PDFRootLegacyOrganizer()
        migrator.migrate()
    finally:
        lock_f.close()
        if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)
