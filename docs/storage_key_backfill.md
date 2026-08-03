# Historical Google Drive storage-key backfill

This is a metadata repair for rows already marked `ARCHIVED`. It does **not**
download source PDFs or delete remote files.

The manifest step scans GDrive once and maps only filenames ending in
`_<report_id>.pdf`. The DB update is permitted only when one and only one
remote file matches a report ID. `missing` and `ambiguous` rows stay unchanged.

```bash
# 1. Read-only: create manifest and candidate plan
POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  uv run --env-file .env python scripts/backfill_storage_keys.py --refresh-manifest

# 2. Inspect tmp/storage_key_backfill/storage_key_backfill_plan.csv

# 3. Small reviewed pilot; this changes only uniquely matched rows
POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
  uv run --env-file .env python scripts/backfill_storage_keys.py --execute --max-updates 500
```

Do not run `--refresh-manifest` on every cron cycle. Refresh the manifest
manually or on a low-frequency schedule, then run
`--execute --max-updates 500` batches using that reviewed snapshot. The script
has its own lock and updates only rows whose `storage_key` is still empty, so it
does not overlap v3's newly archived rows.

The arm2 production schedule runs every five minutes:

```cron
*/5 * * * * bash /home/ubuntu/workspace/services/pdf-archiver/scripts/run_storage_key_backfill.sh
```

Each run updates at most 500 uniquely matched rows. It reuses the reviewed local
manifest and never re-lists GDrive from cron.
