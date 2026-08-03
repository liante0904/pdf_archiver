# PDF Archiver

증권사 리포트 PDF를 arm2에서 다운로드해 Google Drive에 저장하는 배치 서비스다.

시작 전에는 [현재 운영 상태](docs/PROJECT_STATE.md)를 읽는다. 이 문서가 현재
운영 경로·DB 해석·크론·데이터 복구 상태의 단일 기준이다.

## 운영 진입점

- 신규 다운로드/업로드: `scripts/run_v3.sh` → `scripts/pdf_archiver_v3.py`
- 과거 경로 복구: `scripts/run_storage_key_backfill.sh` → `scripts/backfill_storage_keys.py`
- PostgreSQL 백업: `scripts/pg_backup.sh`

Docker는 운영하지 않는다. 호스트의 SSH 터널, WARP, rclone 설정을 사용하는 arm2
크론이 실제 실행 경로다. v1/v2는 [legacy/](legacy/README.md)에 보관한다.

## 검증

```bash
uv run pytest -q
uv run python -m py_compile scripts/pdf_archiver_v3.py scripts/backfill_storage_keys.py
```

`docs/archive/`는 이전 v1/v2 설계·마이그레이션 기록이다. 현재 운영 판단에 사용하지 않는다.
