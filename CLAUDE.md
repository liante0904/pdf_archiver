# PDF Archiver — LLM Quick Context

먼저 [docs/PROJECT_STATE.md](docs/PROJECT_STATE.md)를 읽고, 그 다음에 필요한 소스만 연다.
`docs/archive/`는 역사 기록이며 현재 수치나 런타임 계약의 근거로 사용하지 않는다.

## 절대 기준

- 운영 런타임은 arm2 사용자 crontab의 v3다. Docker, v1, v2는 운영 경로가 아니다.
- v3는 `tbl_sec_reports`를 읽고 `tbl_sec_reports_pdf_archive`를 상태의 기준으로 쓴다.
- `storage_key`는 GDrive 상대 경로다. `file_path`는 과거 증권사 다운로드 URL일 수 있으므로 저장 위치로 해석하면 안 된다.
- `ARCHIVED`만으로 실파일 존재가 증명되지 않는다. `storage_key`와 `file_size` 또는 원격 조회까지 확인한다.
- 운영 DB 변경·원격 파일 삭제·crontab 변경은 명시적 승인 후에만 한다. 계획 CSV와 소규모 파일럿을 먼저 만든다.

## 코드 경계

- `scripts/pdf_archiver_v3.py`: 신규 PDF 다운로드·GDrive 업로드.
- `scripts/backfill_storage_keys.py`: 기존 GDrive manifest로 누락 `storage_key`를 복구. 다운로드·삭제 금지.
- `downloaders/`: v3 공용 증권사별 다운로더.
- `legacy/v1`, `legacy/v2`: 복구 참고용만. 재활성화 금지.

## 빠른 검증

```bash
uv run pytest -q
tail -n 80 ~/logs/pdf_archiver_v3.log
tail -n 80 ~/logs/storage_key_backfill.log
tail -n 80 ~/logs/pg_backup.log
```
