#!/bin/bash
# v2 archiver wrapper: SSH 터널 → v2 실행 → 정리
# Legacy only. Do not install in crontab; production uses scripts/run_v3.sh.

set -e
cd /home/ubuntu/workspace/services/pdf-archiver
LOCKFILE="/tmp/pdf_archiver_v2_cron.lock"
LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"

exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date)] Already running, exit"; exit 0; }

# SSH 터널 (이미 있으면 스킵)
# ⚠️ 10.0.0.111:5432 로 포워딩 (127.0.0.1 아님! DB가 10.0.0.111에만 바인딩)
if ! ss -tlnp | grep -q "127.0.0.1:5433"; then
  ssh -f -N -L 5433:10.0.0.111:5432 -o ExitOnForwardFailure=yes oci 2>/dev/null
  sleep 1
fi

# v2 실행 (터널 포트로)
POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  PATH="/home/ubuntu/.local/bin:$PATH" \
  /home/ubuntu/.local/bin/uv run --env-file .env python legacy/v2/pdf_archiver_v2.py \
  >> "$LOGDIR/pdf_archiver_v2.log" 2>&1

echo "[$(date)] exit=$?" >> "$LOGDIR/pdf_archiver_v2_exit.log"
