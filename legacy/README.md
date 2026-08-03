# Legacy archive implementations

Production is **only** `scripts/run_v3.sh` → `scripts/pdf_archiver_v3.py` via the arm2 user crontab.

- `v1/`: deprecated OneDrive-era archiver, its helpers, tests, and historical Docker definitions.
- `v2/`: superseded Google Drive implementation and its historical wrapper/switch script.
- `../scripts/legacy/`: one-off historical migration and repair tools.

Do not add either v1 or v2 to crontab, Docker Compose, or normal CI. The tag
`legacy-v1-v2-before-isolation-20260803` identifies the repository layout before
this isolation.
