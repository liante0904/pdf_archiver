# PDF Archiver — Current Operational State

> Snapshot updated: 2026-08-03 KST. Recheck DB, logs, and crontab before making operational decisions.

## Read this first

The service runs on arm2; OCI hosts PostgreSQL. Docker is not an operational path.

| Job | Actual entry point | Schedule | Purpose |
|---|---|---|---|
| New PDFs | `scripts/run_v3.sh` | every 3 minutes | download, validate `%PDF-`, upload to `gdrive:archive/pdf` |
| Historical key repair | `scripts/run_storage_key_backfill.sh` | every 5 minutes | update up to 500 DB rows from a reviewed GDrive manifest |
| PostgreSQL backup | `scripts/pg_backup.sh` | daily 03:17 KST | dump and upload to `gdrive:archive/backups/db` |

All three jobs create the OCI DB tunnel to `localhost:5433` when needed. Current crontab is the source of truth.

## Data contract

```text
tbl_sec_reports (source metadata, read by v3)
    report_id
        └── tbl_sec_reports_pdf_archive (archive state and storage metadata)
```

| Field | Meaning | Rule |
|---|---|---|
| `archive_status='ARCHIVED'`, `pdf_sync_status=2` | recorded archive success | not enough to prove the remote object still exists |
| `storage_key` | GDrive relative path, e.g. `2026-08/firm/..._123.pdf` | primary DB-to-GDrive locator |
| `file_size` | bytes recorded for the remote object | compare with `rclone lsjson` when validating |
| `file_path` | often historical brokerage source URL | never treat as a GDrive path |
| `pdf_hash` | SHA-256 when available | useful only after confirming a canonical row has a valid key |
| `retry_count >= 8` | download retry exhausted | not selected by v3 |

Do not use `tbl_sec_reports.pdf_sync_status` for current archive health: it is stale for many v3 rows.

## Verified snapshot and open data issue

At the first audit on 2026-08-03:

| Item | Count | Interpretation |
|---|---:|---|
| source reports | 314,896 | current source rows |
| archive rows | 328,125 | includes historical source-orphan rows |
| current rows with valid archived key + size | 63,173 | directly traceable before the key-repair run |
| `ARCHIVED` current rows missing `storage_key` | 239,984 | historical metadata gap, not proof of file loss |
| direct GDrive filename (`_<report_id>.pdf`) matches | 90,389 | safe automatic key-repair candidates |
| no direct GDrive filename match | 149,503 | requires separate missing/legacy-name investigation |
| ambiguous filename matches | 92 | must remain untouched automatically |
| safe same-hash aliases | 9 | too few to solve the gap |

The key-repair job has already completed a 5-row manual pilot and a successful
10-row cron run. Counts change as the job progresses; use the commands below
for current values.

## Operations

```bash
# Current queue and latest v3 behavior
tail -n 100 ~/logs/pdf_archiver_v3.log

# Key-repair progress (the job must not refresh its manifest from cron)
tail -n 100 ~/logs/storage_key_backfill.log
wc -l tmp/storage_key_backfill/gdrive_manifest.jsonl

# Backup outcome and remote destination
tail -n 100 ~/logs/pg_backup.log
rclone lsf --files-only --format 'pst' gdrive:archive/backups/db | tail

# Exact active schedules
crontab -l
```

## Storage-key backfill safety

`scripts/backfill_storage_keys.py` has three deliberate modes:

1. `--refresh-manifest`: expensive GDrive read-only listing; run manually, never from cron.
2. default: write a CSV plan only.
3. `--execute --max-updates N`: update only unique report-ID matches whose key is still empty.

It never downloads a source PDF, changes archive status/hash/source URL, or deletes a remote file. Its own lock prevents overlapping writes. See [storage_key_backfill.md](storage_key_backfill.md) for commands.

## Legacy and documentation policy

- The tag `legacy-v1-v2-before-isolation-20260803` preserves the pre-isolation layout.
- `legacy/v1/` and `legacy/v2/` are not runnable production paths.
- `docs/archive/` is retained only for historical investigation. It contains stale schemas, stale counts, and v1/v2 migration plans.
