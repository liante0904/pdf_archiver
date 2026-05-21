# PDF content deduplication and Google Drive migration plan

## Current problem

Some reports are distinct DB rows but point to byte-identical PDF content. Filename or
`report_id` based cleanup is not enough for this case, because two different
`report_id` values can legitimately represent the same PDF payload.

The cleanup unit must therefore be `pdf_hash`, not `report_id`, filename, URL, or
folder path.

## Target model

- Keep all source report rows in `tbl_sec_reports`.
- Store one canonical PDF object per `pdf_hash`.
- Let duplicate report rows share the canonical archive object through archive
  metadata (`storage_backend`, `storage_key`, `file_path`, `pdf_hash`).
- Delete duplicate cloud objects only after DB metadata points to the canonical
  object and a backup/migration copy has been verified.

This avoids losing report-level history while still removing duplicate PDF bytes
from storage.

## Safe order of operations

1. Keep the archiver paused.
2. Backfill missing `pdf_hash` values.
   - `uv run python scripts/backfill_pdf_hash.py`
3. Generate a non-destructive deduplication plan.
   - DB hash plan: `uv run python scripts/plan_content_dedup.py`
   - Exact `pdf_url` duplicate plan:
     `uv run python scripts/plan_content_dedup.py --include-pdf-url`
   - Exact `pdf_url` duplicate plan with only affected OneDrive prefixes scanned:
     `uv run python scripts/plan_content_dedup.py --include-pdf-url --scan-affected-prefixes`
   - Avoid full OneDrive listing for the production archive. The archive is too
     large for routine `rclone lsf -R` planning.
   - Only use `--scan-remote-full` on a narrowed test remote or a small prefix.
4. Review generated CSV files under `tmp/dedup_plan/`.
   - `db_duplicate_groups.csv`: one canonical report per `pdf_hash`.
   - `db_alias_updates.csv`: duplicate report rows that should point to the canonical PDF.
   - `remote_delete_candidates.csv`: OneDrive paths that can be deleted later
     when remote data was supplied or explicitly scanned.
   - `remote_hash_duplicate_groups.csv`: remote-only duplicate hints from rclone
     hashes when an explicit remote scan was requested.
   - `pdf_url_duplicate_groups.csv`: exact `pdf_url` duplicate groups where the
     lowest `report_id` is canonical.
   - `pdf_url_alias_updates.csv`: duplicate rows that can point to the lowest
     `report_id` archive metadata for the same `pdf_url`.
   - `pdf_url_remote_scope_prefixes.csv`: affected `YYYY-MM/firm` OneDrive
     prefixes to inspect instead of listing the whole archive.
   - `pdf_url_remote_delete_candidates.csv`: duplicate OneDrive paths found by
     scanning only affected prefixes.
   - The planner excludes the SHA-256 empty payload hash
     `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`;
     rows with this value need source/backfill investigation, not dedup deletion.
5. Apply DB alias updates in a transaction.
   - Update duplicate archive rows to the canonical `storage_backend`,
     `storage_key`, `file_path`, `file_size`, `page_count`, and `pdf_hash`.
   - Do not delete source report rows.
6. Resolve actual cloud paths for the DB duplicate groups.
   - Prefer DB `storage_key`/`file_path` if already canonical.
   - For older rows where metadata contains source URLs or local temp paths,
     repair metadata before deletion planning.
   - Query OneDrive only by known month/firm/report-id prefixes, not by scanning
     the entire archive.
7. Delete OneDrive duplicate objects from the delete-candidate CSV only after
   DB alias updates are committed and the canonical OneDrive object is verified.
8. Resume the archiver with duplicate prevention enabled.

Google Drive migration is intentionally deferred. When it resumes later, use
`rclone copy`, not `move`, until counts and checksums are verified.

## Invariants before deletion

Every row in `remote_delete_candidates.csv` must satisfy all of these:

- The row has a non-empty `pdf_hash_hex`.
- `canonical_report_id` has an existing canonical remote object.
- The duplicate report's archive metadata points to that canonical object.
- The canonical OneDrive object exists and DB alias update has already been committed.
- The source row remains in `tbl_sec_reports`.

If any invariant fails, skip that delete candidate.

## Prevention after cleanup

The archiver already computes `pdf_hash` on successful downloads. The remaining
prevention work is:

- Treat `pdf_hash` as the first-class dedupe key when selecting download targets.
- When a new download produces an existing `pdf_hash`, update the new report's
  archive metadata to the existing canonical object instead of uploading another
  copy.
- Add a partial unique index on the canonical archive object once the alias model
  is settled. Do not add a plain unique index on `pdf_hash` to source reports,
  because multiple source report rows can share the same PDF.
