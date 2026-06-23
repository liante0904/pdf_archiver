# PDF Archiver — 현재 상태 및 진행 컨텍스트 (2026-06-11 기준)

> 이 문서는 다른 LLM과 이어서 작업하기 위한 핸드오프 프롬프트입니다.
> 전체 프로젝트 경로: `/home/ubuntu/workspace/services/pdf-archiver`
> 모든 PDF 작업은 arm2(로컬)에서 수행. OCI는 운영서버지만 PDF 로드는 arm2에서 처리.

---

## 1. 프로젝트 개요

증권사 리서치 PDF를 수집·아카이빙하는 서비스. 두 개의 PostgreSQL 테이블 사용:

| 테이블 | 역할 | 건수 |
|--------|------|------|
| `tbl_sec_reports` | 원본 리포트 메타데이터 (증권사별 등록) | ~283K |
| `tbl_sec_reports_pdf_archive` | PDF 파일 메타데이터 (storage_key, hash 등) | ~282K |

핵심 프로세스: `pdf_archiver_async.py` — `pdf_sync_status IN (0,3)` 인 레코드를 찾아 PDF 다운로드 → 해시 생성 → rclone 업로드

---

## 2. 인프라 구조

```
arm2 (로컬, 유휴)              OCI (운영)
┌──────────────────┐          ┌──────────────────┐
│ pdf_archiver     │          │ DB 스크래퍼       │
│ rclone copy      │          │ PostgreSQL        │
│ crontab          │          │ API 서버          │
│ GDrive/OneDrive  │          │                   │
└──────────────────┘          └──────────────────┘
```

rclone remotes: `onedrive:` (1TB, 772GB 사용), `gdrive:` (5TB, ~1.3GB 사용)

---

## 3. 마이그레이션 파이프라인 (Phase 0~5)

```
Phase 0 ─── OneDrive Orphan → DB Backfill          [⬜ 미구현]
Phase 1 ─── pdf_hash Backfill                       [✅ 스크립트 존재, 61K건 NULL 백필 중]
Phase 2 ─── Dedup Plan 생성                          [✅ 완료 - 2,195 hash groups + 3,777 url groups]
Phase 2a ── pdf_url 기반 Alias 적용                  [✅ 완료 - 22건]
Phase 3 ─── OneDrive 중복 파일 삭제                   [✅ 완료 - 1,159개 삭제, 996 skip]
Phase 4 ─── Google Drive 이전                        [🔄 진행 중]
Phase 5 ─── Archiver v2 (중복방지 + GDrive)          [🚧 v2 초안 있음]
```

### Phase 3 완료 상세
- 스크립트: `scripts/delete_pdf_url_duplicate_files.py`
- 결과: `deleted=1159 skipped=996`
- OneDrive: 134,744 → 133,029 객체로 감소

### Phase 4 현재 상태 (2026-06-12 완료)
- **✅ 완료**: `rclone check --one-way` → **0 differences**, 133,126 matching files
- GDrive: 133,132 객체, OneDrive: 133,126 객체 → 사실상 동일
- rateLimit: 신규 0건 (자체 OAuth 완전 해결)
- sync 로그: `~/logs/rclone_sync.log`, 병렬 로그: `~/logs/rclone_parallel.log`
- **config.py**: `RCLONE_REMOTE = "gdrive:/archive/pdf"` — 신규 다운로드는 GDrive로 직행
- **v2 코드**: `scripts/pdf_archiver_v2.py` — 더블라이트 제거 완료, archive 테이블만 쓰기
- **DB**: `storage_backend` 아직 'onedrive' → DB 연결 복구 후 `switch_to_v2.sh` 실행

---

## 4. 현재 arm2 crontab

```
# 메인 아카이버 — 3분마다 신규 PDF 다운로드 → GDrive
*/3 * * * * cd /home/ubuntu/workspace/services/pdf-archiver && uv run --env-file .env pdf_archiver_async.py >> ~/logs/... 2>&1

# hash 백필 — 매시간
0 * * * * cd /home/ubuntu/workspace/services/pdf-archiver && uv run python scripts/backfill_pdf_hash.py >> ~/logs/backfill_hash.log 2>&1

# OneDrive→GDrive health-check — 5분마다 (죽으면 재시작)
*/5 * * * * pgrep -f "rclone copy" >/dev/null || bash /home/ubuntu/workspace/services/pdf-archiver/scripts/sync_onedrive_to_gdrive.sh >> /home/ubuntu/logs/rclone_sync_cron.log 2>&1

# 주간 dedup — 일요일 03:00
0 3 * * 0 cd /home/ubuntu/workspace/services/pdf-archiver && uv run python scripts/plan_content_dedup.py ... && apply_db_aliases.py ... && apply_pdf_url_aliases.py ... && delete_pdf_url_duplicate_files.py ...
```

---

## 5. 주요 파일/스크립트

| 파일 | 용도 |
|------|------|
| `pdf_archiver_async.py` (43KB) | 메인 아카이버 (다운로드→업로드) |
| `config.py` | 설정 (RCLONE_REMOTE = gdrive) |
| `db_tables.py` | 테이블명 정의 |
| `rclone_manager.py` | rclone 업로드 관리 |
| `scripts/sync_onedrive_to_gdrive.sh` | OneDrive→GDrive 동기화 (cron) |
| `scripts/watch_migration.py` | 이전 진행상황 모니터링 |
| `scripts/delete_pdf_url_duplicate_files.py` | Phase 3 실행 스크립트 |
| `scripts/apply_pdf_url_aliases.py` | Phase 2a alias 적용 |
| `scripts/plan_content_dedup.py` | 중복 계획 생성 |
| `scripts/backfill_pdf_hash.py` | hash 백필 |
| `scripts/copy_pdf_url_canonicals_to_gdrive.py` | canonical만 GDrive 복사 |
| `scripts/pdf_archiver_v2.py` (389줄) | 차세대 아카이버 초안 |
| `docs/pdf_dedup_google_migration_plan.md` | 전체 마이그레이션 계획 (Phase 0~5) |
| `docs/PDF_ARCHIVE_TABLE_DESIGN.md` | 테이블 구조·상태값 레퍼런스 |
| `docs/v2_double_write_removal_plan.md` | 메인테이블 쓰기 제거 계획 |
| `docs/archiver_stability_notes.md` | 안정성 주의사항 |
| `docs/changelog.md` | 변경 이력 |
| `tmp/dedup_plan/` | 중복 분석 CSV 결과물 |

---

## 6. GDrive rclone 설정 이슈 (2026-06-11 최종)

- **기존**: rclone 기본 OAuth → 전 세계 공유 quota → rateLimitExceeded 229회
- **해결**: 자체 GCP OAuth 클라이언트 등록
  - Redirect URI: `http://127.0.0.1:53682/`
  - OAuth 동의 화면 → **대상(Audience)** → 테스트 사용자 `Liante0904@gmail.com` 추가
- **인증 방법**: 로컬 PC에서 `rclone authorize "drive" "client_id" "client_secret"` 실행 → 브라우저 인증 → 토큰을 arm2 rclone.conf에 붙여넣기
- **rateLimit**: 자체 OAuth 전환 후 신규 rateLimit **0건** (229에서 증가 없음)

---

## 7. 남은 작업

### 즉시 필요한 것
- [ ] **Phase 4 완료 확인**: GDrive 용량이 127 GB 근접할 때까지 대기 (수일 소요)
- [ ] **DB storage_backend UPDATE**: `UPDATE tbl_sec_reports_pdf_archive SET storage_backend = 'googledrive' WHERE storage_backend = 'onedrive'`
- [ ] **rclone check 검증**: `rclone check onedrive:/archive/pdf gdrive:/archive/pdf --one-way`

### 중기
- [ ] **Phase 0**: Orphan backfill 스크립트 작성
- [ ] **Phase 1**: 나머지 61K건 hash 백필 지속
- [ ] **Phase 5**: `pdf_archiver_v2.py` 완성 및 전환 (hash 기반 중복 방지)

### 레거시 정리 (현재 사용자가 진행 중)
- [ ] `tbl_sec_reports`에서 불필요 컬럼 제거: `download_status_yn`, `archive_path`, `sync_status`, `ATTACH_URL`
- [ ] `retry_count`는 아카이버가 활발히 사용 중 → v2 전환 후 archive 테이블로 이전해야 제거 가능
- [ ] 참고: `docs/v2_double_write_removal_plan.md` 에 메인테이블 쓰기 제거 계획 있음

---

## 8. 유용한 명령어

```bash
# 이전 진행상황 보기
python3 ~/workspace/services/pdf-archiver/scripts/watch_migration.py
watch -n10 'python3 ~/workspace/services/pdf-archiver/scripts/watch_migration.py'

# 양쪽 크기 비교
rclone size onedrive:/archive/pdf
rclone size gdrive:/archive/pdf

# 누락 파일 체크 (완료 후)
rclone check onedrive:/archive/pdf gdrive:/archive/pdf --one-way --missing-on-dst /tmp/missing.txt

# 아카이버 수동 실행 (테스트)
cd ~/workspace/services/pdf-archiver && uv run python pdf_archiver_async.py --fetch-only

# 로그 확인
tail -f ~/logs/pdf_archiver_async.log
tail -f ~/logs/rclone_sync.log
```

---

## 9. 중요한 주의사항

1. **모든 PDF 작업은 arm2에서** — OCI는 운영 부하 때문에 PDF 처리 안 함
2. **rclone copy (move 금지)** — OneDrive 원본 절대 건드리지 않음
3. **`tbl_sec_reports`는 원본 보존** — 중복 레코드도 삭제하지 않음 (archive 테이블만 alias)
4. **`retry_count` 컬럼 제거 시 주의** — `pdf_archiver_async.py`에서 WHERE, ORDER BY, UPDATE에 사용 중 (v2 전환 필요)
5. **rclone copy는 OneDrive 스로틀링에 취약** — `--tpslimit 4`, `--transfers 2` 등 보수적 설정 사용 중
6. **cron sync 스크립트는 flock으로 중복 실행 방지**, `--max-duration 55m`으로 다음 cron 실행 전 종료 보장
