# Changelog

## 2026-04-28

### PDF workflow status 분리
- `sync_status`와 별개로 `pdf_sync_status`를 PDF 아카이빙 전용 상태값으로 도입했다.
- 상태 의미는 다음과 같다.
  - `0`: 미처리
  - `1`: 다운로드/처리 대상
  - `2`: 완료
  - `3`: 실패/재시도 필요

### 읽기 경로 변경
- `pdf_archiver_async.py`는 이제 `TBL_SEC_REPORTS`의 `pdf_sync_status`만 기준으로 대상 레코드를 선별한다.
- 기존 `sync_status`는 더 이상 작업 대상 판단에 사용하지 않는다.
- 신규 대상 조회는 `pdf_sync_status IN (0, 3)` 기준으로 통일했다.

### 업데이트 경로 변경
- 성공, 실패, 재시도 시 `pdf_sync_status`를 먼저 갱신한다.
- `retry_count`도 함께 갱신한다.
- `tbl_sec_reports_pdf_archive`는 아카이빙 메타데이터를 보관하는 위치로 유지한다.
- `TBL_SEC_REPORTS`와 archive 테이블은 `report_id` 기준으로 같이 갱신되도록 공용 workflow helper로 묶었다.

### Legacy 호환
- `sync_status`는 당분간 유지한다.
- legacy 동기화 및 과거 데이터 백필용으로만 남기고, 새 판단 기준으로는 사용하지 않는다.
- 마이그레이션 초기에는 `sync_status -> pdf_sync_status` 복제 로직을 사용한다.

### 테이블명 분리
- 원본 테이블명은 `db_tables.py`의 공용 헬퍼로 분리했다.
- `SOURCE_REPORTS_TABLE_NAME` 환경변수로 원본 테이블명을 바꿀 수 있다.
- 기본값은 `TBL_SEC_REPORTS`다.
- archive 테이블명도 `PDF_ARCHIVE_TABLE_NAME`으로 제어한다.

### 마이그레이션 스크립트
- `scripts/full_migration_v2.py`
  - `TBL_SEC_REPORTS`와 `tbl_sec_reports_pdf_archive`에 `pdf_sync_status`를 추가
  - 기존 `sync_status`를 `pdf_sync_status`로 복제
- `scripts/migrate_status_2.py`
  - `sync_status=2` 데이터를 아카이브 테이블로 이관할 때 `pdf_sync_status=2`로 적재
- `scripts/migrate_existing_data.py`
  - 기존 아카이브 데이터 이관 시 `pdf_sync_status=2`를 함께 기록

### 실행 환경
- `docker-compose.yml`은 Postgres 기반 실행만 남겼다.
- 테이블명 교체용 환경변수와 PDF 워크플로우 상태값 기준을 명시했다.

## 2026-04-29

### `pdf_hash` 도입 TODO
- [ ] `tbl_sec_reports`와 `tbl_sec_reports_pdf_archive`에 `pdf_hash`(BINARY(32) / PostgreSQL `BYTEA`) 컬럼 추가
- [ ] 다운로드 성공 시 SHA-256 32바이트 해시를 source/archive 둘 다에 적재
- [ ] 기존 archive 파일과 source 레코드를 대상으로 `pdf_hash` 백필 스크립트 추가
- [ ] `pdf_hash` 기반 unique index 추가
- [ ] 다운로드 대상 선별은 `pdf_hash`가 있으면 우선 사용하고, 없으면 기존 `pdf_url` 보조키로 canonical row 1개 선택
- [ ] 동일 `pdf_hash`를 가진 source row 전체에 상태 업데이트 전파
- [ ] 레포트 리스트 조회는 `pdf_hash -> archive` 조인으로 page_count, file_size, checksum 노출
- [ ] 중복 PDF survivor는 최소 `report_id`로 고정
- [ ] survivor 기준 외 duplicate row와 OneDrive 파일 정리 절차는 별도 진행
