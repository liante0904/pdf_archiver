#!/bin/bash
# OneDrive → GDrive: 디렉토리 4개씩 병렬 복사
# 2026-06-11
set -e

LOCKFILE="/tmp/sync_onedrive_to_gdrive.lock"
exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date)] Already running, exit"; exit 0; }

LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"
LOG="$LOGDIR/rclone_sync.log"

echo "[$(date)] === Sync start ==="

# OneDrive 디렉토리 목록
DIRS=$(rclone lsd onedrive:/archive/pdf 2>/dev/null | awk '{print $5}')
echo "  Dirs: $(echo "$DIRS" | wc -l)"

echo "$DIRS" | xargs -P 4 -I {} bash -c '
  rclone copy "onedrive:/archive/pdf/{}" "gdrive:/archive/pdf/{}" \
    --transfers 4 \
    --checkers 8 \
    --no-traverse \
    --tpslimit 10 \
    --retries 3 \
    --timeout 60s \
    --contimeout 30s \
    --low-level-retries 2 \
    --log-file "'"$LOG"'" \
    --log-level INFO 2>&1 | grep -v "^$" > /dev/null
  echo "[$(date)] Done: {}"
'

echo "[$(date)] === Sync complete ==="
