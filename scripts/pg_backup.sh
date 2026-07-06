#!/bin/bash
# PostgreSQL backup → GDrive via cloud_store
# crontab: 0 3 * * * bash /home/ubuntu/workspace/services/pdf-archiver/scripts/pg_backup.sh
#
# Requires: SSH tunnel to DB (auto-setup), cloud_store CLI

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$REPO_ROOT/.env"
CLOUD_STORE="$HOME/workspace/lib/cloud_store/cli.py"
LOGDIR="$HOME/logs"
BACKUP_DIR="/tmp/pg_backups"
REMOTE_BASE="gdrive:archive/backups/db"
KEEP_DAYS=${BACKUP_KEEP_DAYS:-14}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/ssh_reports_hub_${TIMESTAMP}.sql.gz"

mkdir -p "$BACKUP_DIR" "$LOGDIR"

# ── load env ────────────────────────────────────────────────
export $(grep -E '^POSTGRES_(HOST|PORT|DB|USER|PASSWORD)=' "$ENV_FILE" | xargs)

# ── SSH tunnel ──────────────────────────────────────────────
if ! ss -tlnp | grep -q "127.0.0.1:5433"; then
  echo "[$(date)] Setting up SSH tunnel..."
  ssh -f -N -L 5433:10.0.0.111:5432 -o ExitOnForwardFailure=yes oci 2>/dev/null
  sleep 2
fi

# ── pg_dump ─────────────────────────────────────────────────
# pg_dump 15 client (server is 15.x, must match major version)
PG_DUMP="/usr/lib/postgresql/15/bin/pg_dump"

echo "[$(date)] Starting backup..." | tee -a "$LOGDIR/pg_backup.log"

PGPASSWORD="$POSTGRES_PASSWORD" "$PG_DUMP" \
  -h localhost -p 5433 \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --no-owner --no-acl \
  --exclude-table='tbl_dart_*' \
  --exclude-table='tbm_dart_*' \
  --exclude-table='ddl_event_log*' \
  --exclude-table='tbl_forwarded_messages*' \
  --exclude-table='tbl_global_settings*' \
  --exclude-table='tbl_user_settings*' \
  --exclude-table='tbl_user_watchlist*' \
  | gzip > "$BACKUP_FILE"

BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo "[$(date)] Dump complete: $BACKUP_FILE ($BACKUP_SIZE)" | tee -a "$LOGDIR/pg_backup.log"

# ── upload to GDrive (with retry on quota) ──────────────────
echo "[$(date)] Uploading..." | tee -a "$LOGDIR/pg_backup.log"

UPLOAD_EXIT=1
MAX_UPLOAD_RETRIES=5
RETRY_DELAY=30
for i in $(seq 1 $MAX_UPLOAD_RETRIES); do
  python3 "$CLOUD_STORE" upload "$BACKUP_FILE" "${REMOTE_BASE}/${TIMESTAMP}.sql.gz" 2>&1 | tee -a "$LOGDIR/pg_backup.log"
  UPLOAD_EXIT=$?

  if [ $UPLOAD_EXIT -eq 0 ]; then
    break
  elif [ $UPLOAD_EXIT -eq 3 ]; then
    # QUOTA_EXCEEDED → wait and retry
    echo "[$(date)] Quota hit, retrying in ${RETRY_DELAY}s (attempt $i/$MAX_UPLOAD_RETRIES)..." | tee -a "$LOGDIR/pg_backup.log"
    sleep $RETRY_DELAY
    RETRY_DELAY=$((RETRY_DELAY * 2))  # exponential backoff
  else
    # Other error → don't retry
    break
  fi
done
if [ $UPLOAD_EXIT -eq 0 ]; then
  echo "[$(date)] Upload OK. Cleaning local file." | tee -a "$LOGDIR/pg_backup.log"
  rm -f "$BACKUP_FILE"
else
  echo "[$(date)] Upload FAILED (exit=$UPLOAD_EXIT). Keeping local file: $BACKUP_FILE" | tee -a "$LOGDIR/pg_backup.log"
fi

# ── cleanup old GDrive backups ──────────────────────────────
echo "[$(date)] Cleaning backups older than ${KEEP_DAYS} days..." | tee -a "$LOGDIR/pg_backup.log"
CUTOFF=$(date -d "${KEEP_DAYS} days ago" +%Y%m%d)

# List remote backups, delete old ones
python3 "$CLOUD_STORE" list "${REMOTE_BASE}/" 2>/dev/null | while read -r fname; do
  if [ -n "$fname" ] && [[ "$fname" =~ ^ssh_reports_hub_([0-9]{8})_.* ]]; then
    fdate="${BASH_REMATCH[1]}"
    if [ "$fdate" -lt "$CUTOFF" ]; then
      echo "[$(date)] Deleting old: $fname" | tee -a "$LOGDIR/pg_backup.log"
      python3 "$CLOUD_STORE" delete "${REMOTE_BASE}/$fname" 2>&1 | tee -a "$LOGDIR/pg_backup.log" || true
    fi
  fi
done

echo "[$(date)] Backup complete." | tee -a "$LOGDIR/pg_backup.log"
