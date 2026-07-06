#!/bin/bash
# v3 archiver wrapper: SSH 터널 → v3 실행 → 정리
# crontab: */3 * * * * bash /home/ubuntu/workspace/services/pdf-archiver/scripts/run_v3.sh
#
# v3 uses cloud_store library (lib/cloud_store) for rclone abstraction.
# v2 cron과 lock/buffer가 분리되어 있어 동시 실행 가능.

set -e
cd /home/ubuntu/workspace/services/pdf-archiver
LOCKFILE="/tmp/pdf_archiver_v3_cron.lock"
LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"

exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date)] v3 Already running, exit"; exit 0; }

# SSH 터널 (이미 있으면 스킵)
if ! ss -tlnp | grep -q "127.0.0.1:5433"; then
  ssh -f -N -L 5433:10.0.0.111:5432 -o ExitOnForwardFailure=yes oci 2>/dev/null
  sleep 1
fi

# cloud_store library path 추가
export PYTHONPATH="/home/ubuntu/workspace/lib:$PYTHONPATH"

# v3 실행
POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  PATH="/home/ubuntu/.local/bin:$PATH" \
  /home/ubuntu/.local/bin/uv run --env-file .env python scripts/pdf_archiver_v3.py \
  >> "$LOGDIR/pdf_archiver_v3.log" 2>&1

echo "[$(date)] exit=$?" >> "$LOGDIR/pdf_archiver_v3_exit.log"
