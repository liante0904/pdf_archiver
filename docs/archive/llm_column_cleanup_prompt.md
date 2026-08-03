# [LLM Prompt] tbl_sec_reports 컬럼 정리 태스크

당신은 PostgreSQL 데이터베이스 정리 전문가입니다.
아래 정보를 바탕으로 `tbl_sec_reports` 테이블의 미사용 컬럼 정리 계획을 수립하고 실행하세요.

## 환경
- **DB**: PostgreSQL, 호스트 `ssh_reports_hub`
- **메인 테이블**: `tbl_sec_reports` (증권사 리포트 메타데이터, ~283K rows)
- **아카이브 테이블**: `tbl_sec_reports_pdf_archive` (PDF 파일 메타데이터, ~282K rows)
- **관계**: `tbl_sec_reports.report_id` 1:1 `tbl_sec_reports_pdf_archive.report_id`

## 지침
각 컬럼을 아래 분류에 따라 처리하세요:
- **ACTIVE**: 현재 pdf-archiver가 읽고 쓰는 컬럼 → 손대지 말 것
- **PASSIVE**: pdf-archiver는 안 쓰지만 외부 서비스(스크래퍼/텔레그램봇)가 사용 중일 수 있음 → 확인 후 결정
- **LEGACY**: 과도기적 존재 → 먼저 의존성 확인 후 제거
- **UNUSED**: pdf-archiver에서 참조 0건 → 외부 서비스 확인 후 제거

## 컬럼 목록 및 분류

### ACTIVE (12개) — 제거 금지
```
report_id           bigint PK    → 모든 쿼리 기본키
sec_firm_order      integer      → 증권사 식별 (DBfi=19 체크 등)
firm_nm             text         → 파일 경로 생성, WHERE 필터, 로깅
article_title       text         → 파일명 생성, 로깅
reg_dt              text         → ORDER BY, 파일 경로 생성 (YYYY-MM)
pdf_url             text         → Primary 다운로드 URL (적재율 99.64%)
key                 text         → Fallback URL, DBfi primary source, referer
telegram_url        text         → Fallback URL, DS증권 DB 트리거 의존
download_url        text         → Fallback URL, 유안타증권 1,024건의 유일 소스
pdf_sync_status     integer      → archiver 메인 상태 컬럼 (WHERE IN(0,3), UPDATE)
retry_count         integer      → 재시도 제한, ORDER BY
pdf_hash            bytea        → 중복 제거 키
```

### PASSIVE (3개) — 외부 확인 필요
```
sync_status         integer      → 스크래퍼가 기록. archiver는 초기 백필 시에만 참조
download_status_yn  text         → 스크래퍼가 기록(Y/N). archiver는 진단 스크립트만 참조
writer              text         → 스크래퍼가 기록. backfill_pdf_hash.py가 author 백필 시 fallback
```

### LEGACY (1개)
```
archive_path        text         → 스크래퍼의 legacy 파일 경로. archive 테이블로 이관 완료 시 DROP 가능
```

### UNUSED (21개) — archiver 참조 0건
```
article_board_order  integer      → 추정: 게시판 순서 (스크래퍼)
article_url          text         → 추정: 게시글 URL, key와 중복 가능성
main_ch_send_yn      text         → 추정: 텔레그램 메인채널 발송 여부
save_time            text         → 추정: 스크래퍼 저장 시간
mkt_tp               text         → 추정: 시장 구분
gemini_summary       text         → 추정: Gemini AI 요약
summary_time         text         → 추정: AI 요약 시간
summary_model        text         → 추정: AI 모델명
tags                 jsonb        → 추정: 키워드 태그
stock_names          jsonb        → 추정: 관련 종목명
sector               text         → 추정: 섹터 분류
fnguide_summary_id   bigint       → 추정: FnGuide 연동 ID
target_price         numeric      → 추정: 목표 주가
rating               text         → 추정: 투자 의견
revision_type        text         → 추정: 리포트 수정 유형
report_type          text         → 추정: 리포트 분류
stock_tickers        jsonb        → 추정: 종목 티커
saved_at             timestamptz  → 추정: 적재 시간
report_date          date         → 추정: 리포트 기준일
telegram_sent        boolean      → 추정: 텔레그램 발송 완료 플래그
report_unique_key    text         → 추정: 중복 방지 unique key
```

## 예상 질문 및 검증 항목

### 1. `article_url` vs `key`
두 컬럼이 완전히 동일한 값을 갖는지 검증:
```sql
SELECT COUNT(*) AS total,
       SUM(CASE WHEN article_url = key THEN 1 ELSE 0 END) AS identical,
       SUM(CASE WHEN article_url IS NULL AND key IS NOT NULL THEN 1 ELSE 0 END) AS url_null_only,
       SUM(CASE WHEN key IS NULL AND article_url IS NOT NULL THEN 1 ELSE 0 END) AS key_null_only
FROM tbl_sec_reports;
```

### 2. `sync_status` vs `pdf_sync_status`
`sync_status`의 값 분포 확인 (스크래퍼가 계속 쓰는지):
```sql
SELECT sync_status, COUNT(*) FROM tbl_sec_reports GROUP BY sync_status ORDER BY sync_status;
```

### 3. `download_status_yn` 사용처 확인
```sql
SELECT download_status_yn, COUNT(*) FROM tbl_sec_reports GROUP BY download_status_yn;
```

### 4. `archive_path` 잔여 데이터 확인
```sql
SELECT COUNT(*) FROM tbl_sec_reports WHERE archive_path IS NOT NULL;
```

### 5. UNUSED 컬럼의 실제 데이터 존재 여부
각 UNUSED 컬럼이 실제 데이터를 가지고 있는지 확인 (NULL이면 거의 확실히 미사용):
```sql
SELECT
    'article_board_order' AS col, COUNT(*) FILTER (WHERE article_board_order IS NOT NULL) AS non_null FROM tbl_sec_reports
UNION ALL SELECT 'article_url', COUNT(*) FILTER (WHERE article_url IS NOT NULL) FROM tbl_sec_reports
UNION ALL SELECT 'main_ch_send_yn', COUNT(*) FILTER (WHERE main_ch_send_yn IS NOT NULL) FROM tbl_sec_reports
-- ... 나머지 UNUSED 컬럼도 동일 패턴
```

## 안전한 제거 절차

각 컬럼 제거는 이 순서로 진행:
1. 해당 컬럼이 `non_null_count = 0` 또는 오직 한 서비스만 기록하는지 확인
2. 해당 컬럼을 참조하는 DB 뷰/트리거/함수 존재 여부 확인
3. `ALTER TABLE tbl_sec_reports RENAME COLUMN x TO _z_x;` (즉시 DROP 대신 rename)
4. 1주일 모니터링 후 문제 없으면 `ALTER TABLE ... DROP COLUMN _z_x;`

## 참고 문서
- `docs/url_column_cleanup_plan.md`: URL 컬럼 정규화 Phase 1~3 상세 계획
- `docs/PDF_ARCHIVE_TABLE_DESIGN.md`: 테이블 구조 및 상태값 레퍼런스
- `docs/v2_double_write_removal_plan.md`: 메인 테이블 쓰기 제거 계획
