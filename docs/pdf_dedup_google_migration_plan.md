# PDF 중복제거 & Google Drive 이전 — 실행 계획

> 마지막 업데이트: 2026-05-28  
> 지금까지 논의한 모든 내용 + 기존 스크립트/CSV 기반으로 재정리

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

---

## Phase 1: pdf_hash Backfill

### 목적
`tbl_sec_reports_pdf_archive`와 `tbl_sec_reports`의 `pdf_hash` 컬럼이 NULL인 레코드들에 대해 SHA-256 해시를 계산하여 채움.

### 실행
```bash
uv run python scripts/backfill_pdf_hash.py
```

### 스크립트
- `scripts/backfill_pdf_hash.py` — 로컬 파일 or HTTP URL에서 PDF를 읽어 해시 계산
- 아카이브 테이블 + 원본 테이블 양쪽 백필
- 동시성 제어, 파일 락, 재시도 로직 포함

### 상태: ✅ 스크립트 존재, 실행 필요할 수 있음

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

### 생성되는 CSV
| 파일 | 설명 | 현재 |
|------|------|------|
| `db_duplicate_groups.csv` | hash별 canonical report_id | ✅ 8 groups |
| `db_alias_updates.csv` | canonical을 가리키도록 업데이트할 row들 | ✅ 124 rows |
| `pdf_url_duplicate_groups.csv` | pdf_url 완전일치 그룹 | ✅ 687 groups |
| `pdf_url_alias_updates.csv` | URL 기반 alias 업데이트 대상 | ✅ 720 rows |
| `pdf_url_remote_scope_prefixes.csv` | OneDrive 스캔 범위 (prefix) | ✅ 22 prefixes |
| `pdf_url_remote_delete_candidates.csv` | OneDrive에서 삭제할 중복 파일 | ✅ 26 files |
| `remote_hash_duplicate_groups.csv` | 원격 hash 중복 그룹 | ✅ 1 group |

### canonical 결정 규칙
- 같은 `pdf_hash` 그룹 내에서 가장 낮은 `report_id`를 canonical로 선택
- canonical의 `storage_key`/`file_path`를 모든 alias가 참조

### 상태: ✅ CSV 생성 완료, 검토 필요

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

### 상태: ⬜ 실행 대기 (CSV는 생성됨)

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

### 상태: ⬜ Phase 2a 이후 실행

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

### Google Drive 용량 제한
- Google Drive 무료: 15GB
- 281K PDF 파일의 총 용량 확인 필요
- 용량 부족 시 Google Workspace 또는 Google One 업그레이드 고려
- 또는 hash 기반 canonical만 Google Drive로 보내고, OneDrive는 보관/백업용으로 유지

### 상태: ⬜ Phase 4a: 용량 확인부터

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

### 상태: ⬜ archiver 코드 수정 필요

---

## 📊 현재 인벤토리

### 스크립트 (`scripts/`)

| 스크립트 | 용도 | 상태 |
|----------|------|------|
| `backfill_pdf_hash.py` | hash 백필 | ✅ |
| `plan_content_dedup.py` | 중복 계획 생성 (22KB) | ✅ |
| `apply_pdf_url_aliases.py` | URL 기반 alias 적용 | ✅ |
| `delete_pdf_url_duplicate_files.py` | OneDrive 중복 삭제 | ✅ |
| `copy_pdf_url_canonicals_to_gdrive.py` | GDrive canonical 복사 | ✅ |
| `pdf_duplicate_manager.py` | 중복 관리 orchestration | ✅ |
| `checksum_scan.py` | hash 스캔 | ✅ |
| `verify_onedrive_files.py` | OneDrive 파일 검증 | ✅ |

### 생성된 Plan CSV (`tmp/dedup_plan/`)

| 파일 | 건수 |
|------|------|
| `pdf_url_duplicate_groups.csv` | 687 groups |
| `pdf_url_alias_updates.csv` | 720 rows |
| `db_duplicate_groups.csv` | 8 groups (hash 기반) |
| `db_alias_updates.csv` | 124 rows |
| `pdf_url_remote_delete_candidates.csv` | 26 files |

### 누락된 것

| 항목 | 상태 |
|------|------|
| OneDrive orphan backfill 스크립트 | ❌ 없음 |
| Google Drive 용량 확인 | ❌ 안 함 |
| Archiver 중복 방지 로직 | ❌ 미구현 |
| DB 인덱스 (`pdf_hash`) | ❌ 확인 필요 |

---

## 🚀 지금 당장 할 수 있는 것

### 1순위: DB alias 적용 (안전, 가역적)
```bash
cd ~/workspace/services/pdf-archiver
uv run python scripts/apply_pdf_url_aliases.py
```
→ 720건의 archive 레코드가 canonical을 참조하도록 업데이트됨

### 2순위: OneDrive orphan 분석
```bash
rclone lsl onedrive:/archive/pdf/ -R > tmp/onedrive_full_list.txt
wc -l tmp/onedrive_full_list.txt
```
→ OneDrive 파일 수 파악 (DB 281K와 비교)

### 3순위: Google Drive 용량 확인
```bash
rclone size onedrive:/archive/pdf
```
→ 전체 용량이 15GB 이하면 무료 GDrive 가능

---

## ⚠️ 주의사항

1. **원본 `tbl_sec_reports`는 절대 건드리지 않음** — 중복 레코드도 그대로 유지 (리포트 이력 보존)
2. **삭제 전 항상 canonical 존재 확인** — alias가 가리키는 canonical 파일이 실제로 OneDrive에 있는지 검증
3. **`rclone copy` 사용 (move 금지)** — OneDrive 원본 유지한 채로 GDrive 이전
4. **archiver 일시 정지** — Phase 1~4 진행 중에는 pdf-archiver 중단
5. **트랜잭션** — DB 변경은 트랜잭션으로 묶어서, 문제 시 롤백 가능하게
