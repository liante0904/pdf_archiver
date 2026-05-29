# PDF 중복제거 파이프라인 — 단계별 실행 가이드

> 마지막 업데이트: 2026-05-28  
> 이 문서 하나만 보면 순서대로 실행할 수 있게 정리

---

## 🗺 전체 흐름도

```
┌─────────────────────────────────────────────────────────────────────┐
│                        현재 DB 상태                                  │
│                                                                     │
│  tbl_sec_reports (282K)  ──LEFT JOIN──  tbl_sec_reports_pdf_archive │
│  │                                       (281K, 1,003건 누락)       │
│  │ sync_status=2: 241K                   │ archive_status            │
│  │ pdf_sync_status: 0/2/3/9              │ pdf_sync_status: 0/2/3/9  │
│  │                                       │ storage_key → OneDrive    │
│  └───────────────────────────────────────┘                          │
│                                                                     │
│  OneDrive: /archive/pdf/YYYY-MM/firm/＊.pdf (281K+ 파일, 일부 중복) │
│  중복 원인: 같은 리포트가 여러 게시판에 등록 → report_id 다름,      │
│            pdf_url 같음, PDF 내용 동일, OneDrive에 중복 저장         │
└─────────────────────────────────────────────────────────────────────┘

                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Phase 1: Hash Backfill                                             │
│  ─────────────────────                                               │
│  scripts/backfill_pdf_hash.py                                       │
│                                                                     │
│  입력: archive 테이블 (pdf_hash IS NULL)                             │
│  출력: pdf_hash 컬럼 채워짐                                          │
│                                                                     │
│  상태: ✅ 스크립트 존재 (실행 필요 시)                                │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Phase 2: Dedup Plan 생성                                            │
│  ───────────────────────                                             │
│  scripts/plan_content_dedup.py [--include-pdf-url] [--scan-...]     │
│                                                                     │
│  ┌─ pdf_url 완전일치 기반 ─────────────────────────────┐            │
│  │ pdf_url_duplicate_groups.csv    687 groups          │            │
│  │ pdf_url_alias_updates.csv       720 rows            │            │
│  │   → POINT_ARCHIVE_METADATA_TO_MIN_REPORT_ID         │            │
│  └────────────────────────────────────────────────────┘            │
│                                                                     │
│  ┌─ pdf_hash 기반 ───────────────────────────────────┐              │
│  │ db_duplicate_groups.csv          8 groups          │              │
│  │ db_alias_updates.csv           124 rows            │              │
│  └────────────────────────────────────────────────────┘            │
│                                                                     │
│  상태: ✅ CSV 이미 생성됨 (tmp/dedup_plan/)                          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Phase 2a: OneDrive 검증                                              │
│  ──────────────────────                                              │
│  scripts/plan_content_dedup.py --include-pdf-url --scan-affected-...│
│                                                                     │
│  rclone lsl 로 OneDrive 실제 파일 존재 확인                           │
│                                                                     │
│  ┌─ 검증된 26건 ─────────────────────────────────────┐              │
│  │ pdf_url_remote_delete_candidates.csv  26 files    │              │
│  │   delete_after: DB_ALIAS_UPDATE_AND_               │              │
│  │                 CANONICAL_ONEDRIVE_VERIFIED         │              │
│  │ → keep_path + delete_path 둘 다 실재 확인           │              │
│  └────────────────────────────────────────────────────┘              │
│                                                                     │
│  ┌─ 미검증 694건 ───────────────────────────────────┐              │
│  │ pdf_url_alias_updates.csv 의 나머지              │              │
│  │ canonical_remote_path 가 URL임 (파일경로 아님)    │              │
│  │ → 추가 scan 필요                                  │              │
│  └────────────────────────────────────────────────────┘              │
│                                                                     │
│  상태: ✅ 26건 검증 완료 / ⬜ 694건 검증 필요                         │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Phase 3: DB Alias 적용 ← 【지금 여기】                               │
│  ──────────────────────                                              │
│  scripts/apply_pdf_url_aliases.py --execute                          │
│                                                                     │
│  대상: pdf_url_remote_delete_candidates.csv (26건)                   │
│                                                                     │
│  하는 일:                                                            │
│    duplicate_report_id=231953596                                     │
│         │                                                            │
│         │  storage_key ──→ canonical의 OneDrive 경로로 덮어씀        │
│         │  pdf_hash     ──→ canonical의 hash로 덮어씀               │
│         │  archive_status = 'ARCHIVED'                               │
│         │  pdf_sync_status = 2                                       │
│         ▼                                                            │
│    canonical_report_id=231953595                                     │
│    (기존 canonical 파일은 그대로 유지)                                │
│                                                                     │
│  ┌─ 적용 전 ────────────────────────────────────────┐               │
│  │ report 596 → storage_key: .../231953596.pdf      │               │
│  │ report 595 → storage_key: .../231953595.pdf      │               │
│  │ OneDrive: 596.pdf, 595.pdf 둘 다 존재 (중복)     │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                     │
│  ┌─ 적용 후 ────────────────────────────────────────┐               │
│  │ report 596 → storage_key: .../231953595.pdf  ←── 같은 파일 참조  │
│  │ report 595 → storage_key: .../231953595.pdf      │               │
│  │ OneDrive: 596.pdf → 삭제 가능 (아무도 참조 안 함) │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                     │
│  안전장치:                                                           │
│    ✅ dry-run backup 이미 생성됨 (pdf_url_alias_backup.csv)          │
│    ✅ pdf_sync_status=2 양쪽 확인                                    │
│    ✅ pdf_url 완전일치 재확인 (SQL 내 WHERE)                         │
│    ✅ OneDrive keep/delete 경로 실재 확인 완료                        │
│    ✅ 트랜잭션 (all-or-nothing)                                      │
│                                                                     │
│  상태: ⬜ --execute 실행 대기                                        │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Phase 4: OneDrive 중복 파일 삭제                                     │
│  ─────────────────────────────                                       │
│  scripts/delete_pdf_url_duplicate_files.py                           │
│                                                                     │
│  대상: pdf_url_remote_delete_candidates.csv 의 delete_remote_path    │
│  전제: Phase 3 완료 (DB가 canonical만 참조 중)                       │
│                                                                     │
│  상태: ⬜ Phase 3 이후 실행                                          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Phase 5: Google Drive 이전                                           │
│  ────────────────────────                                             │
│  rclone copy onedrive:/archive/pdf gdrive:/archive/pdf               │
│                                                                     │
│  config.py: RCLONE_REMOTE = "gdrive:/archive/pdf"                    │
│  DB: UPDATE storage_backend = 'googledrive'                          │
│                                                                     │
│  상태: ⬜ Phase 4 이후 실행                                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 지금 당장 실행할 명령어

```bash
# 1. DB alias 적용 (26건, dry-run 백업 이미 있음)
cd ~/workspace/services/pdf-archiver
uv run python scripts/apply_pdf_url_aliases.py --execute

# 2. (Phase 4) 문제없으면 OneDrive 중복파일 삭제
uv run python scripts/delete_pdf_url_duplicate_files.py
```

---

## 📋 나머지 694건 처리

```bash
# 영향받는 OneDrive prefix만 스캔해서 검증
uv run python scripts/plan_content_dedup.py --include-pdf-url --scan-affected-prefixes

# → pdf_url_remote_delete_candidates.csv 에 검증된 건 추가됨

# 검증된 건들에 대해 Phase 3~4 반복
uv run python scripts/apply_pdf_url_aliases.py --execute
uv run python scripts/delete_pdf_url_duplicate_files.py
```

---

## 🔁 크론탭 (기존 + 신규)

```
*/3 * * * * ... pdf_archiver_async.py ...     # v1 (OneDrive, 현행)
0 * * * * ... backfill_pdf_hash.py ...         # hash 백필
0 3 * * 0 ... plan_content_dedup ...           # 주간 중복 계획
```

---

## 🆕 pdf_archiver_v2.py — 차세대 아카이버

### v1 vs v2

```
v1 (pdf_archiver_async.py, 3천줄)          v2 (pdf_archiver_v2.py, ~500줄)
─────────────────────────────────          ─────────────────────────────
OneDrive 저장                                Google Drive 저장
중복 그냥 또 업로드                           hash 검사 → canonical 재사용
증권사별 if-else 인라인                       downloader registry 패턴
config.py 하드코딩                            env 기반 설정
```

### v2 핵심 로직

```python
# 다운로드 → hash 계산 → 중복 검사
pdf_bytes = download(url)
pdf_hash = sha256(pdf_bytes)

existing = db.find_by_hash(pdf_hash)
if existing:
    # 중복: canonical의 storage_key 참조만 복사, 업로드 스킵
    db.update_alias(report_id, existing.storage_key, pdf_hash)
else:
    # 신규: rclone 업로드
    path = f"{date}/{firm}/{filename}.pdf"
    rclone.upload(pdf_bytes, path)
    db.insert_archive(report_id, path, pdf_hash)
```

### 중복 레코드 처리 방식

```
tbl_sec_reports (원본 — 절대 삭제 안 함)
┌────────────┬──────────┬────────────────────────┐
│ 231970965  │ LS증권   │ 게시판A (먼저 insert)   │ ← canonical
│ 231971011  │ LS증권   │ 게시판B (나중 insert)   │ ← duplicate
└────────────┴──────────┴────────────────────────┘

tbl_sec_reports_pdf_archive (v2 처리 후)
┌────────────┬─────────────────────────────────┬──────────┐
│ 231970965  │ gdrive:/.../231970965.pdf       │ abc123   │ ← 실물 파일
│ 231971011  │ gdrive:/.../231970965.pdf       │ abc123   │ ← 같은거 참조!
└────────────┴─────────────────────────────────┴──────────┘

→ 두 report_id 모두 원본 유지, PDF는 1개만 저장
```

### v2 실행 순서

```
1. tbl_sec_reports 조회 (pdf_sync_status IN (0,3))
2. downloader registry에서 증권사별 다운로더 선택
3. PDF 다운로드 → SHA-256 hash
4. DB에서 hash 검색
   ├─ 있음 → canonical의 storage_key 참조, archive_status='ARCHIVED'
   └─ 없음 → rclone GDrive 업로드 → storage_key 저장
5. tbl_sec_reports.pdf_sync_status 업데이트
```

### 상태: 🚧 코드 작성 중 (scripts/pdf_archiver_v2.py)
