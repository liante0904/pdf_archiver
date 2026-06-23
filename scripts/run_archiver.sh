#!/bin/bash
# PDF Archiver wrapper: SSH 터널 보장 → archiver 실행
set -e
cd /home/ubuntu/workspace/services/pdf-archiver

LOCKFILE="/tmp/pdf_archiver_cron.lock"
LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"

exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date)] Already running, exit"; exit 0; }

# SSH 터널 (이미 있으면 스킵)
# ⚠️ 10.0.0.111:5432 로 포워딩 (127.0.0.1 아님! DB가 10.0.0.111에만 바인딩)
if ! ss -tlnp 2>/dev/null | grep -q "127.0.0.1:5433"; then
  ssh -f -N -L 5433:10.0.0.111:5432 -o ExitOnForwardFailure=yes oci 2>/dev/null
  sleep 1
fi

# v1 archiver (터널 포트로)
POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  PATH="/home/ubuntu/.local/bin:$PATH" \
  /home/ubuntu/.local/bin/uv run --env-file .env python pdf_archiver_async.py \
  >> "$LOGDIR/pdf_archiver_async.log" 2>&1
