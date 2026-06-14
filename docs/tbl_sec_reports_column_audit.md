# tbl_sec_reports 컬럼 사용 현황 감사 (2026-06-14)

> **목적**: `tbl_sec_reports` 테이블의 37개 컬럼 중 실제 pdf-archiver 코드에서 사용하는 컬럼과 미사용 컬럼을 분류하여 정리 대상 식별.
> **분석 범위**: `/home/ubuntu/workspace/services/pdf-archiver` 전체 소스코드 + 크론잡
> **분석 방법**: 각 컬럼명으로 `grep -rn` 전수 검색 후 실제 로직에서의 역할 추적

---

## 분류 기준

| 등급 | 의미 |
|------|------|
| 🔴 **ACTIVE** | pdf-archiver가 SELECT + 비즈니스 로직에 직접 사용. **절대 삭제 불가** |
| 🟡 **PASSIVE** | archiver가 읽기만 함. 실사용은 스크래퍼/텔레그램봇 등 외부 서비스. **삭제 시 외부 장애 가능성** |
| 🟢 **LEGACY** | archiver 코드에서 참조하나 과도기적/진단용. **v2 전환 후 제거 가능** |
| ⚪ **UNUSED** | pdf-archiver 코드에서 참조 0건. **외부 서비스 확인 후 제거 가능** |

---

## 🔴 ACTIVE (12개) — pdf-archiver 핵심 의존

| # | 컬럼명 | 타입 | 용도 | 증거 |
|---|--------|------|------|------|
| 1 | `report_id` | bigint PK | 모든 쿼리의 기본키, JOIN, WHERE, archive 테이블 FK | `pdf_archiver_async.py:582,628` / `pdf_archiver_v2.py:88` |
| 2 | `sec_firm_order` | integer | DBfi(19) 판별, 증권사별 분기 | `pdf_archiver_async.py:181` (`if sec_firm_order == Config.DBFI_FIRM_ORDER`) |
| 3 | `firm_nm` | text | WHERE (EXCLUDED_FIRMS), 파일 경로 생성, 로깅, ORDER BY | `pdf_archiver_async.py:582,608,618` |
| 4 | `article_title` | text | SELECT → title로 매핑, 파일명 생성, 로깅 | `pdf_archiver_async.py:582` (`article_title` → `title`) |
| 5 | `reg_dt` | text | ORDER BY, 파일 경로 생성 (YYYY-MM), 로깅 | `pdf_archiver_async.py:623` / `_make_file_path:155-161` |
| 6 | `pdf_url` | text | **Primary 다운로드 URL**. WHERE (URL 유무 체크), UPDATE 매칭조건, pdf_key 생성 | `pdf_archiver_async.py:586-587,592,601` / `pdf_archiver_v2.py:95,255` |
| 7 | `report_unique_key` | text | **Fallback 다운로드 URL** (key 대체, 100% 동일값). DBfi primary source, referer 구성 | `pdf_archiver_async.py:582,589,604,628` / `pdf_archiver_v2.py:88,98,255` |
| 8 | `telegram_url` | text | **Fallback 다운로드 URL**. DBfi descRsh fallback. ⚠️ DS증권 DB 트리거 의존 | `pdf_archiver_async.py:183,587-588,602` / `pdf_archiver_v2.py:96,255` |
| 9 | `download_url` | text | **Fallback 다운로드 URL**. ⚠️ 유안타증권 1,024건 (pdf_url NULL)의 유일한 소스 | `pdf_archiver_async.py:588,603` / `pdf_archiver_v2.py:97,255` |
| 10 | `pdf_sync_status` | integer | **메인 상태 컬럼**. WHERE `IN (0,3)` 대상 선별, UPDATE 성공(2)/실패(3) 기록 | `pdf_archiver_async.py:308-311,594` / `pdf_archiver_v2.py:93` |
| 11 | `retry_count` | integer | WHERE (retry limit), ORDER BY, UPDATE (COALESCE + 1) | `pdf_archiver_async.py:310,596-599` / `pdf_archiver_v2.py:94,155` |
| 12 | `pdf_hash` | bytea | 중복제거 키 (`DISTINCT ON (COALESCE(ENCODE(pdf_hash, 'hex'), ...))`), UPDATE 기록 | `pdf_archiver_async.py:311,592` / `pdf_archiver_v2.py:112` |

---

## 🟡 PASSIVE (3개) — archiver가 안 쓰지만 외부 서비스가 사용 중일 가능성 높음

| # | 컬럼명 | 타입 | 추정 사용처 | 증거 (archiver 불사용) |
|---|--------|------|-------------|------------------------|
| 13 | `key` | text | ⚠️ **DEPRECATED** (2026-06-14). `report_unique_key`로 전환 완료. pdf-archiver 모든 참조 이전됨. DROP 예정 | commit `43dbe91` — 8개 파일에서 `report_unique_key`로 전환 |
| 14 | `sync_status` | integer | **스크래퍼**가 기록하는 원본 수집 동기화 상태 (0/1/2/-1). archiver는 `ensure_pdf_sync_status_schema()`에서 초기 백필 시에만 참조 | `docs/changelog.md:15`: "기존 sync_status는 더 이상 작업 대상 판단에 사용하지 않는다" |
| 15 | `download_status_yn` | text | **스크래퍼**가 기록하는 다운로드 성공 여부 (Y/N). `deep_analyze_db.py`에서 진단용으로만 SELECT | `pdf_archiver_async.py` 전체에 참조 없음 |
| 16 | `writer` | text | **스크래퍼**가 기록하는 작성자. `backfill_pdf_hash.py:303`에서 archive 테이블 author 백필 시 fallback으로만 참조 | `backfill_pdf_hash.py:303`: `("writer", "author")` 폴백 체인 |

---

## 🟢 LEGACY (1개) — 과도기적 존재, 곧 제거 가능

| # | 컬럼명 | 타입 | 현황 | 제거 조건 |
|---|--------|------|------|-----------|
| 16 | `archive_path` | text | **스크래퍼**가 기록하던 legacy 파일 경로. `deep_analyze_db.py:44`에서 진단용으로만 SELECT하고 archiver는 전혀 사용 안 함 | archive 테이블로 완전 이관 확인 후 DROP |

---

## ⚪ UNUSED (20개) — pdf-archiver 코드에서 참조 0건

> ⚠️ **주의**: 이 컬럼들은 pdf-archiver repo 기준 미사용이지만, **스크래퍼 / 텔레그램봇 / 대시보드 API / FnGuide 연동** 등 외부 서비스가 사용 중일 수 있음. 삭제 전 반드시 외부 서비스 담당자에게 확인할 것.

| # | 컬럼명 | 타입 | 추정 원본 용도 | archiver 참조 |
|---|--------|------|---------------|---------------|
| 17 | `article_board_order` | integer | 스크래퍼: 게시판 내 순서 | 0건 |
| 18 | `article_url` | text | 스크래퍼: 게시글 URL (과거 `key`와 동일?) | 0건 |
| 19 | `main_ch_send_yn` | text | 텔레그램봇: 메인채널 발송 여부 | 0건 |
| 20 | `save_time` | text | 스크래퍼: 저장 시간 | 0건 |
| 21 | `mkt_tp` | text | 스크래퍼: 시장 구분 | 0건 |
| 22 | `gemini_summary` | text | Gemini AI: 요약 결과 | 0건 |
| 23 | `summary_time` | text | Gemini AI: 요약 생성 시간 | 0건 |
| 24 | `summary_model` | text | Gemini AI: 사용 모델명 | 0건 |
| 25 | `tags` | jsonb | 스크래퍼: 키워드 태그 | 0건 |
| 26 | `stock_names` | jsonb | 스크래퍼: 관련 종목명 | 0건 |
| 27 | `sector` | text | 스크래퍼: 섹터 분류 | 0건 |
| 28 | `fnguide_summary_id` | bigint | FnGuide: 외부 연동 ID | 0건 |
| 29 | `target_price` | numeric | 스크래퍼: 목표 주가 | 0건 |
| 30 | `rating` | text | 스크래퍼: 투자 의견 | 0건 |
| 31 | `revision_type` | text | 스크래퍼: 리포트 수정 유형 | 0건 |
| 32 | `report_type` | text | 스크래퍼: 리포트 분류 | 0건 |
| 33 | `stock_tickers` | jsonb | 스크래퍼: 종목 티커 | 0건 |
| 34 | `saved_at` | timestamptz | 스크래퍼: 적재 시간 | 0건 |
| 35 | `report_date` | date | 스크래퍼: 리포트 기준일 | 0건 |
| 36 | `telegram_sent` | boolean | 텔레그램봇: 발송 완료 플래그 | 0건 |

---

## 종합 요약

```
Total columns: 37
├── 🔴 ACTIVE  (12개): archiver 직접 사용 → 절대 삭제 불가 (report_unique_key 포함)
├── 🟡 PASSIVE ( 4개): key(DEPRECATED) + sync_status + download_status_yn + writer
├── 🟢 LEGACY  ( 1개): archive_path → 곧 제거 가능
└── ⚪ UNUSED  (20개): archiver 미사용 → 외부 확인 후 제거 가능
```

### URL 컬럼 특이사항
- `pdf_url`, `report_unique_key`, `telegram_url`, `download_url` 4개 모두 ACTIVE (다운로드 fallback 체인)
- `key`: ⚠️ **DEPRECATED** — `report_unique_key`로 전환 완료 (2026-06-14). DROP 대기 중
- `telegram_url`: DB 트리거 `trg_set_ds_share_telegram_url` (DS투자증권) 의존 → 함부로 삭제 시 트리거 오류
- `download_url`: 유안타증권 1,024건 (pdf_url=NULL) 의 유일한 다운로드 소스 → 정리하려면 archive 테이블로 이관 선행 필요
- `article_url`: `report_unique_key`와 중복 가능성 있으나 archiver는 사용 안 함 (0건 참조)

### 정리 우선순위 추천
1. **완료** ✅: `key` → `report_unique_key` 코드 전환 (pdf-archiver)
2. **즉시**: `archive_path` → archive 테이블 이관 확인 후 DROP
3. **단기**: `key` DROP (스크래퍼 전환 + DB 제약조건 이전 후)
4. **중기**: URL 컬럼 정규화 (`telegram_url`, `download_url` → archive 테이블로 이관 후 DROP) - `docs/url_column_cleanup_plan.md` 참조
5. **장기**: 20개 UNUSED 컬럼 → 스크래퍼/텔레그램봇 담당자 확인 후 일괄 정리
