# PDF Archiver Stability Notes

This document records the current operational direction and the fragile points
that should not be changed casually.

## Current Direction

- Operate locally with `uv run pdf_archiver_async.py` first. Docker is not the
  primary runtime until the local path stays stable.
- Keep `pdf_archiver_async.py` behavior conservative. The system is an archival
  batch job, so keeping local files on uncertain upload results is preferable to
  deleting too much.
- Fetch targets as a small, firm-diversified batch. The query should pick one
  candidate per firm before taking a second candidate from the same firm.
- Treat download failures and upload failures differently. Download failures can
  increment retry count; upload failures should keep files and avoid burning
  retry budget.

## Fetch Policy

- `BATCH_SIZE` is the total batch size, not per-firm size.
- The target query uses `ROW_NUMBER() OVER (PARTITION BY firm_nm ...)` and then
  orders by `firm_rank` first. This creates the round-robin shape.
- Do not hardcode a firm-specific exception such as `firm_nm = 'LS증권'` in the
  fetch query. If a downloader improves for one firm, prefer a general retry
  rule based on source URL availability and bounded `FETCH_RETRY_LIMIT`.
- Keep `pdf_key` fallback to `report_id::TEXT`. Without it, rows with null URL
  keys can collapse under `DISTINCT ON`.
- Use `--fetch-only` before changing fetch logic:

```bash
uv run python pdf_archiver_async.py --fetch-only
```

The output must show a diversified `대상 증권사 분포`, not one firm dominating
unless only one firm is eligible.

## Rclone Upload Rules

- Do not run `rclone cleanup` during routine upload. It can scan the remote and
  take hours.
- Do not upload the whole local buffer. Always upload only the current successful
  download set via `--files-from`.
- Keep OneDrive chunk size as `64000k` or another value that is a multiple of
  320 KiB. `64M` fails with `chunk size ... is not a multiple of 320Ki`.
- Keep `--no-traverse` for the batch copy path.
- If rclone reports auth/config errors, keep local files and stop repair work.
- Only delete stale 0-byte remote files for the current upload targets.

## Retry And DB Status

- Successful download + failed upload should not increment `retry_count`.
- Failed download should increment `retry_count`.
- Mark archive success only after upload verification accepts the file.
- Keep DB updates shared through `_update_source_workflow` and
  `_apply_workflow_update`; avoid ad hoc SQL updates in the run loop.

## Common Mistakes

- Changing `ORDER BY firm_rank, ...` back to `ORDER BY firm_nm` or a global
  `reg_dt DESC`; this makes one firm dominate the batch.
- Replacing `--files-from` with `--include "*.pdf"` against the buffer root; this
  re-uploads old failed files and mixes unrelated failures into the current run.
- Retrying upload repair after an auth error; this creates more partial remote
  objects and noisy `nameAlreadyExists` errors.
- Treating Docker image rebuilds as operational fixes while the live runtime is
  the local `uv run` cron/manual path.
- Removing `--fetch-only`; it is the cheapest safety check before real downloads
  and uploads.

## Required Checks After Changes

```bash
python3 -m py_compile pdf_archiver_async.py
uv run pytest
uv run python pdf_archiver_async.py --fetch-only
```
