#!/bin/bash
# OneDrive → GDrive 병렬 동기화 (디렉토리 3개씩 병렬)
# 2026-06-11: 자체 OAuth + xargs -P 병렬

LOCKFILE="/tmp/sync_onedrive_to_gdrive_parallel.lock"
LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"

exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date)] Already running, exit"; exit 0; }

echo "[$(date)] Starting parallel sync..."

rclone lsd onedrive:/archive/pdf 2>/dev/null | awk '{print $5}' | \
  xargs -P 3 -I {} bash -c '
    rclone copy "onedrive:/archive/pdf/{}" "gdrive:/archive/pdf/{}" \
      --transfers 3 \
      --checkers 8 \
      --no-traverse \
      --tpslimit 10 \
      --retries 3 \
      --timeout 60s \
      --contimeout 30s \
      --low-level-retries 2 \
      --log-file "'"$LOGDIR"'/rclone_sync.log" \
      --log-level INFO 2>&1 | grep -v "^$"
    echo "[$(date)] Done: {}"
  '

echo "[$(date)] All done"
