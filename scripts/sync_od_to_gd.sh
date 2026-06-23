#!/bin/bash
# OneDrive → GDrive 동기화 (잔여분 + 신규)
# crontab: */3 * * * * bash /home/ubuntu/workspace/services/pdf-archiver/scripts/sync_od_to_gd.sh
set -e

LOCKFILE="/tmp/sync_od_to_gd.lock"
LOGFILE="/home/ubuntu/logs/sync_od_to_gd.log"

mkdir -p "$(dirname "$LOGFILE")"

exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date)] Already running, exit"; exit 0; }

rclone copy onedrive:/archive/pdf gdrive:/archive/pdf \
  --transfers 8 \
  --checkers 16 \
  --no-traverse \
  --tpslimit 12 \
  --retries 5 \
  --timeout 120s \
  --contimeout 30s \
  --low-level-retries 5 \
  --log-file "$LOGFILE" \
  --log-level INFO \
  > /dev/null 2>&1

echo "[$(date)] exit=$?" >> "$LOGFILE"
