# PDF 중복제거 & Google Drive 이전 — 실행 계획

> 마지막 업데이트: 2026-06-07  
> 2026-06-07 실측 기반으로 현재 상태 갱신. 202개 원격 디렉토리 스캔을 통해 미검증 694건의 실물 확인 완료.

---

## 🗺 전체 로드맵

```
Phase 0 ─── OneDrive orphan → DB backfill (신규)
Phase 1 ─── pdf_hash backfill (기존 스크립트 있음)
Phase 2 ─── Dedup plan 생성 (기존 CSV 이미 생성됨)
Phase 2a ── pdf_url 기반 중복 alias 적용
Phase 3 ─── OneDrive 중복 파일 삭제
Phase 4 ─── Google Drive 이전
Phase 5 ─── archiver 재개 + 중복 방지 로직
```

---

## Phase 0: OneDrive Orphan → DB Backfill

### 문제
OneDrive에 PDF 파일이 존재하지만 `tbl_sec_reports_pdf_archive`에 기록이 없는 고아(orphan) 파일들이 있음. archiver가 생성되기 전에 수동 업로드했거나, 과거 버그로 DB 기록이 누락된 건들.

### 접근
1. `rclone lsl onedrive:/archive/pdf/ -R` 로 OneDrive 전체 파일 리스트 추출
2. 파일명에서 `report_id` 추출 (패턴: `*_(\d+)\.pdf$`)
3. DB `tbl_sec_reports` 및 `tbl_sec_reports_pdf_archive` 와 대조
4. DB에 없는 report_id → `tbl_sec_reports_pdf_archive`에 INSERT (메타데이터는 `tbl_sec_reports`에서 조회)
5. `pdf_hash`, `page_count` 등은 NULL → Phase 1에서 채움

### 필요한 새 스크립트
```bash
# scripts/backfill_onedrive_orphans.py
```
- `rclone lsl` 출력을 파싱해서 orphan 목록 생성
- `tbl_sec_reports`에서 매칭되는 report_id 찾아 archive 메타데이터 구성
- archive 테이블에 UPSERT
- 매칭 안 되는 파일은 별도 CSV로 기록 (수동 확인용)

### 상태: ⬜ 미구현

### 2026-06-06 진단
- OneDrive: 134,744 objects, 127.6 GB, 309개 `YYYY-MM` 디렉토리
- DB archive: 282,013 rows → OneDrive 대비 약 147K건 더 많음
  - 원인: archive 테이블 1 report_id = 1 row지만, OneDrive에는 report_id 외의 파일(중복 파일명, 구버전 등)이 있을 가능성
- `rclone lsl onedrive:/archive/pdf/ -R` 전체 스캔은 시간이 걸리므로, 일단 Phase 2a 먼저 진행하고 orphan 분석은 병행

---

## Phase 1: pdf_hash Backfill

### 목적
`tbl_sec_reports_pdf_archive`와 `tbl_sec_reports`의 `pdf_hash` 컬럼이 NULL인 레코드들에 대해 SHA-256 해시를 계산하여 채움.

### 현재 커버리지 (2026-06-06)
| 항목 | 건수 |
|------|------|
| 전체 archive rows | 282,013 |
| hash 있음 | 221,108 (78.4%) |
| hash NULL | 60,905 (21.6%) |

- 22만 건은 이미 hash가 채워져 있으므로, Phase 2 hash 기반 중복 재실행 시 8 groups → 훨씬 더 큰 숫자 예상

### 실행
```bash
uv run python scripts/backfill_pdf_hash.py
```

### 스크립트
- `scripts/backfill_pdf_hash.py` — 로컬 파일 or HTTP URL에서 PDF를 읽어 해시 계산
- 아카이브 테이블 + 원본 테이블 양쪽 백필
- 동시성 제어, 파일 락, 재시도 로직 포함

### 상태: ✅ 스크립트 존재, 61K건 백필 필요

---

## Phase 2: Dedup Plan 생성

### 목적
중복 PDF를 식별하고 canonical(대표) 레코드를 결정하여 CSV 계획 파일 생성.

### 실행
```bash
# 해시 기반 중복 (가장 정확)
uv run python scripts/plan_content_dedup.py

# pdf_url 완전일치 기반 중복도 포함
uv run python scripts/plan_content_dedup.py --include-pdf-url

# 영향받는 OneDrive prefix만 스캔
uv run python scripts/plan_content_dedup.py --include-pdf-url --scan-affected-prefixes
```

### 생성되는 CSV (2026-06-07 재실행 결과)

| 파일 | 5월 6일 | **6월 7일** | 증가율 |
|------|---------|------------|--------|
| `db_duplicate_groups.csv` | 8 groups | **2,195 groups** | 274× |
| `db_alias_updates.csv` | 124 rows | **25,327 rows** | 204× |
| `pdf_url_duplicate_groups.csv` | 687 groups | **3,777 groups** | 5.5× |
| `pdf_url_alias_updates.csv` | 720 rows | **3,827 rows** | 5.3× |
| `pdf_url_remote_scope_prefixes.csv` | 22 prefixes | **202 prefixes** | 9.2× |
| `pdf_url_remote_delete_candidates.csv` | 26 files | **2,155 files** | 82.8× |
| **총 alias 대상** | **844건** | **~29,154건** | **35×** |

### canonical 결정 규칙
- **Hash 기반**: 같은 `pdf_hash` 그룹 내에서 `min-report-id` 정책 (가장 낮은 report_id) + OneDrive 파일 존재 우선
- **pdf_url 기반**: 같은 `pdf_url` 그룹 내에서 가장 낮은 `report_id` (pdf_sync_status=2인 것만)

### 상태: ✅ 2026-06-07 재실행 완료 — hash 2,195 groups, 총 29,154건 중복 식별 및 원격 2,155개 파일 매칭 검증

---

## Phase 2a: pdf_url 기반 Alias 적용

### 목적
`pdf_url`이 완전히 동일한 레코드들은 같은 PDF를 가리키므로, archive 메타데이터를 canonical로 통일.

### 실행
```bash
uv run python scripts/apply_pdf_url_aliases.py
```

### 동작
- `pdf_url_alias_updates.csv` 의 각 row에 대해:
  - alias report_id의 archive 레코드를 canonical의 storage_key/file_path/pdf_hash로 UPDATE
  - 원본 `tbl_sec_reports` row는 그대로 유지

### 상태: ✅ 2026-06-06 실행 완료

| 결과 | 건수 |
|------|------|
| 적용 성공 | 22건 |
| Skip (URL 불일치/pdf_sync≠2) | 3건: `231971011`, `231700518`, `231700511` |
| Backup | `pdf_url_alias_backup.csv` (25 rows) |

### 처리 대상 구분 (2026-06-07 업데이트)
| 구분 | 건수 | 검증 | 설명 |
|------|------|------|------|
| 검증 완료, 적용 완료 | 22건 | ✅ alias 적용 + OneDrive canonical 확인 | 기존 22건 적용 완료 |
| 검증 완료, skip | 3건 | ⚠️ 수동 확인 필요 (현대차증권×2, LS증권×1) | pdf_sync_status 조건 불일치 |
| 미검증 694건 스캔 결과 | 2,155건 | ✅ 202개 prefix 실물 파일 대조 검증 성공 | `remote_delete_candidates` 2,155개 확정 |

---

## Phase 3: OneDrive 중복 파일 삭제

### 전제조건
- Phase 2a 완료 (DB alias 업데이트 커밋됨)
- canonical OneDrive 객체가 실제로 존재함을 확인
- 삭제 대상 파일이 alias report_id만 참조하고, 다른 report_id와 공유되지 않음을 확인

### 실행
```bash
uv run python scripts/delete_pdf_url_duplicate_files.py
```

### 안전장치
- `pdf_url_remote_delete_candidates.csv` 목록만 삭제
- 각 파일에 대해: canonical 존재 확인 → 삭제
- 실패 시 skip, 로그만 남김

### 상태: ✅ 2026-06-06 확인 완료 — 22건 모두 OneDrive에서 이미 부재 (중복 파일 기정리됨)
→ canonical path 파일들 정상 존재 확인 완료. 실제 `rclone deletefile` 건수: 0건.

---

## Phase 4: Google Drive 이전

### 목적
현재 OneDrive에 저장된 PDF 아카이브 전체를 Google Drive로 이전.

### 방법
```bash
# 1. rclone config에 Google Drive remote 추가
rclone config  # → 'gdrive' remote 생성

# 2. canonical 파일만 Google Drive로 복사
uv run python scripts/copy_pdf_url_canonicals_to_gdrive.py

# 3. 전체 복사 (canonical + alias 모두)
rclone copy onedrive:/archive/pdf gdrive:/archive/pdf \
  --transfers 8 --checkers 16 --no-traverse \
  --onedrive-chunk-size 64000k --retries 3

# 4. 검증: 파일 수/크기 비교
rclone size onedrive:/archive/pdf
rclone size gdrive:/archive/pdf

# 5. config.py 변경
RCLONE_REMOTE = "gdrive:/archive/pdf"

# 6. DB storage_backend 업데이트
UPDATE tbl_sec_reports_pdf_archive SET storage_backend = 'googledrive'
WHERE storage_backend = 'onedrive';
```

### Google Drive 용량
- **2026-06-06**: 5TB Google Drive 계정 보유 확인 → 용량 문제 해결
- OneDrive 총 127.6 GB → GDrive 5TB로 충분히 이전 가능

### 상태: ✅ GDrive 5TB 확보, rclone remote `gdrive:` 이미 설정됨

---

## Phase 5: Archiver 재개 + 중복 방지

### 중복 방지 로직 (archiver에 추가)
```python
# download_task() 성공 후, pdf_hash 확인
if pdf_hash:
    existing = await db.fetchrow(
        "SELECT storage_key, file_size FROM tbl_sec_reports_pdf_archive "
        "WHERE pdf_hash = $1 AND archive_status = 'ARCHIVED' LIMIT 1",
        pdf_hash
    )
    if existing:
        # 기존 canonical 객체 재사용 → 업로드 스킵
        storage_key = existing['storage_key']
        file_size = existing['file_size']
```

### DB 인덱스 추가
```sql
-- hash 기반 빠른 중복 조회
CREATE INDEX IF NOT EXISTS idx_pdf_archive_hash 
ON tbl_sec_reports_pdf_archive(pdf_hash) 
WHERE pdf_hash IS NOT NULL;
```

### 상태: 🚧 `pdf_archiver_v2.py` 389줄 초안 존재 (commit `ddd3f83`)

v2는 이미 hash 기반 중복 방지 + GDrive 업로드 로직이 구현되어 있음. GDrive 유료 플랜 결정 후 v2로 전환 가능.

---

## 📊 현재 인벤토리

### 스크립트 (`scripts/`)

| 스크립트 | 용도 | 상태 |
|----------|------|------|
| `backfill_pdf_hash.py` | hash 백필 | ✅ |
| `plan_content_dedup.py` | 중복 계획 생성 (22KB) | ✅ |
| `apply_pdf_url_aliases.py` | URL 기반 alias 적용 (backup/dry-run 지원) | ✅ |
| `apply_db_aliases.py` | **Hash 기반 alias 적용 (25K건)** — canonical의 storage_key/pdf_hash 참조 | ✅ 신규 |
| `delete_pdf_url_duplicate_files.py` | OneDrive 중복 삭제 | ✅ |
| `copy_pdf_url_canonicals_to_gdrive.py` | GDrive canonical 복사 | ✅ |
| `pdf_duplicate_manager.py` | 중복 관리 orchestration | ✅ |
| `checksum_scan.py` | hash 스캔 | ✅ |
| `verify_onedrive_files.py` | OneDrive 파일 검증 | ✅ |
| `pdf_archiver_v2.py` | **차세대 아카이버** (389줄, hash 중복방지 + GDrive) | 🚧 초안 |

### 현재 DB 상태 (2026-06-06)

| 항목 | 건수 |
|------|------|
| 전체 리포트 (`tbl_sec_reports`) | 283,152 |
| 아카이브 레코드 (`tbl_sec_reports_pdf_archive`) | 282,013 |
| 아카이브 미시도 (archive 테이블에 없음) | 1,139 |
| archive 중 `archive_status='ARCHIVED'` | ~280K |
| archive 중 `pdf_hash` 있음 | 221,108 (78.4%) |
| archive 중 `pdf_hash` NULL | 60,905 (21.6%) |

### OneDrive 상태 (2026-06-06)

| 항목 | 값 |
|------|-----|
| 총 객체 수 | 134,744 |
| 총 용량 | 127.6 GB |
| YYYY-MM 디렉토리 수 | 309개 |
| 최신 디렉토리 | 2026-06 (21 files) |
| 최고(最古) 확인 | rclone lsl 전체 스캔 필요 |

### 생성된 Plan CSV (`tmp/dedup_plan/`) — 2026-06-07 기준

| 파일 | 건수 |
|------|------|
| `db_duplicate_groups.csv` | **2,195 groups** (hash 기반) |
| `db_alias_updates.csv` | **25,327 rows** |
| `pdf_url_duplicate_groups.csv` | **3,777 groups** |
| `pdf_url_alias_updates.csv` | **3,827 rows** |
| `pdf_url_remote_delete_candidates.csv` | **2,155 files** (실물 검증 완료) |
| `pdf_url_alias_backup.csv` | 25 rows (Phase 2a 백업) |

### 누락된 것

| 항목 | 상태 |
|------|------|
| OneDrive orphan backfill 스크립트 | ❌ 없음 |
| Google Drive 용량 확인 | ✅ 완료 — GDrive 5TB, 127.6GB 이전 충분 |
| Archiver 중복 방지 로직 | 🚧 v2 초안 존재 |
| DB 인덱스 (`pdf_hash`) | ❌ 확인 필요 |
| GDrive 이전 | ⬜ 중복제거 완료 후 진행 |

---

## 🔄 자동화 파이프라인 (arm2 crontab)

### 상시 실행
```
*/3 * * * *  pdf_archiver_async.py    # PDF 다운로드 → OneDrive 업로드
0   * * * *  backfill_pdf_hash.py      # NULL hash 채우기 (락 파일로 중복 방지)
```

### 매주 일요일 03:00 (순차 실행)
```
plan_content_dedup.py --include-pdf-url --scan-affected-prefixes
    │  220K건 hash + 7.6K건 pdf_url 중복 분석, 202개 OneDrive prefix 스캔
    │  출력: db_alias_updates.csv (25,323건) + pdf_url_alias_updates.csv (3,827건)
    ▼
apply_db_aliases.py --execute
    │  25,323건 hash 기반 alias 적용 (canonical의 storage_key/pdf_hash로 UPDATE)
    │  hash가 같으므로 내용 동일 보장 → OneDrive 검증 불필요
    ▼
apply_pdf_url_aliases.py --execute
    │  3,827건 중 OneDrive keep/delete 경로 검증된 건만 alias 적용
    ▼
delete_pdf_url_duplicate_files.py --execute
    │  alias 완료된 중복 파일 OneDrive에서 삭제
```

### 전체 흐름도
```
새 리포트 입수 → pdf_archiver 다운로드 → OneDrive 업로드 → pdf_hash 기록
                                                              │
                                     ┌────────────────────────┘
                                     ▼
                              backfill_pdf_hash (매시간)
                              : 신규 PDF hash 채움
                                     │
                                     ▼ (일요일)
                              plan_content_dedup
                              : hash/URL 중복 그룹 발견
                                     │
                                     ▼
                              apply_db_aliases
                              : 중복 레코드 → canonical 참조로 통일
                              (tbl_sec_reports 원본은 유지, archive만 alias)
```

### 핵심 원칙
- **`tbl_sec_reports` (원본)**: 절대 수정하지 않음. 증권사별 중복 등록 보존
- **`tbl_sec_reports_pdf_archive` (PDF)**: 중복 제거. canonical 하나만 실물 보관
- **Hash 기반 중복**: PDF 내용 완전 동일 → 가장 안전한 중복 판정
- **URL 기반 중복**: 같은 URL로 등록된 건 → 게시판만 다른 경우

---

## ⚠️ 주의사항

1. **원본 `tbl_sec_reports`는 절대 건드리지 않음** — 중복 레코드도 그대로 유지 (리포트 이력 보존)
2. **삭제 전 항상 canonical 존재 확인** — alias가 가리키는 canonical 파일이 실제로 OneDrive에 있는지 검증
3. **`rclone copy` 사용 (move 금지)** — OneDrive 원본 유지한 채로 GDrive 이전
4. **archiver 일시 정지** — Phase 1~4 진행 중에는 pdf-archiver 중단
5. **트랜잭션** — DB 변경은 트랜잭션으로 묶어서, 문제 시 롤백 가능하게
