#!/bin/bash
# Incremental GDrive truth reconciliation.  New uploads write an ID directly;
# this covers historical status=2 rows, newest report_id first.
set -e

cd /home/ubuntu/workspace/services/pdf-archiver
LOGDIR="$HOME/logs"
mkdir -p "$LOGDIR"

if ! ss -tlnp | grep -q "127.0.0.1:5433"; then
  ssh -f -N -L 5433:10.0.0.111:5432 -o ExitOnForwardFailure=yes oci 2>/dev/null
  sleep 1
fi

POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  /home/ubuntu/.local/bin/uv run --env-file .env python scripts/verify_gdrive_file_ids.py \
    --manifest tmp/gdrive_file_id_verify/manifest.jsonl \
    --execute --requeue-missing --limit 1000 --max-requeues 10 \
  >> "$LOGDIR/gdrive_file_id_verify.log" 2>&1
