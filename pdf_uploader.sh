#!/bin/bash -l
# 쉘 스크립트로 작성된 원드라이브 고속 업로더

# 환경 변수를 제거하여 사용자 기본 환경을 따르도록 설정
LOCAL_DIR="/home/ubuntu/downloads/pdf_archive_temp"
REMOTE_DIR="onedrive:/archive/pdf"
DB_PATH="/home/ubuntu/sqlite3/telegram.db"
PYTHON_BIN="/home/ubuntu/.cache/uv/environments-v2/pdf-archiver-async-9167053ae5c63912/bin/python3"

LOCK_FILE="/home/ubuntu/prod/pdf_archiver/uploader.lock"
DOWNLOADER_LOCK="/home/ubuntu/prod/pdf_archiver/downloader.lock"

# 중복 실행 방지 및 다운로더 작업 중 여부 확인
if [ -f "$LOCK_FILE" ]; then
    echo "[$(date)] Uploader is already running. Skipping."
    exit 0
fi

if [ -f "$DOWNLOADER_LOCK" ]; then
    echo "[$(date)] Downloader is running. Skipping uploader to avoid conflict."
    exit 0
fi

# 락 파일 생성
touch "$LOCK_FILE"

# 종료 시 락 파일 삭제 (정상 종료, 에러 종료 모두 포함)
trap 'rm -f "$LOCK_FILE"' EXIT

echo "[$(date)] Starting rclone move for 2026-03 first..."

# 사용자님이 성공하신 2026-03부터 전송을 시도합니다.
rclone move "$LOCAL_DIR/2026-03" "$REMOTE_DIR/2026-03" --include "*.pdf" --transfers 10 -v

echo "[$(date)] Moving other folders..."
for dir in "$LOCAL_DIR"/*; do
    dirname=$(basename "$dir")
    if [ "$dirname" != "2026-03" ] && [ -d "$dir" ]; then
        echo "[$(date)] Moving $dirname..."
        rclone move "$dir" "$REMOTE_DIR/$dirname" \
          --include "*.pdf" \
          --transfers 10 \
          --retries 3 \
          --delete-empty-src-dirs \
          -v
    fi
done

echo "[$(date)] Rclone move finished. Updating DB status..."

# 파이썬은 오직 로컬 파일 삭제 여부를 확인해 DB를 업데이트하는 '검증/통계' 역할만 수행 (1초 컷)
$PYTHON_BIN -c "
import sqlite3, os
conn = sqlite3.connect('$DB_PATH')
rows = conn.execute('SELECT report_id, file_path FROM pdf_archive_metadata').fetchall()
success_count = 0
for r_id, path in rows:
    if not os.path.exists(path):
        conn.execute('UPDATE data_main_daily_send SET sync_status = 2 WHERE report_id = ?', (r_id,))
        success_count += 1
conn.commit()
conn.close()
print(f'DB Sync complete. {success_count} files marked as archived.')
"

# 로컬에 남은 불필요한 .tmp 파일 정리 (성공/실패 잔재물)
echo "[$(date)] Cleaning up leftover .tmp files..."
find "$LOCAL_DIR" -name "*.tmp" -delete

echo "[$(date)] Uploader Job Done."
