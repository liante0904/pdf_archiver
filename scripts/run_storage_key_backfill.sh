#!/bin/bash
# Historical storage-key repair: 500 uniquely matched rows per cron run.
# Production PDF downloads remain owned by scripts/run_v3.sh.
set -e

cd /home/ubuntu/workspace/services/pdf-archiver
LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"

# Reuse the arm2 -> OCI DB tunnel contract used by the v3 cron.
if ! ss -tlnp | grep -q "127.0.0.1:5433"; then
  ssh -f -N -L 5433:10.0.0.111:5432 -o ExitOnForwardFailure=yes oci 2>/dev/null
  sleep 1
fi

POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  /home/ubuntu/.local/bin/uv run --env-file .env python scripts/backfill_storage_keys.py \
    --execute --max-updates 500 \
  >> "$LOGDIR/storage_key_backfill.log" 2>&1
