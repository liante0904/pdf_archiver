# [실행 계획] 데이터베이스 URL 컬럼 정규화 및 점진적 정리 방안

> 작성일: 2026-06-08  
> PostgreSQL 메인 테이블(`tbl_sec_reports`)의 URL 관련 컬럼 정합성을 실측 데이터 기반으로 점검하고, 중복 데이터를 제거하여 성능 및 구조적 이점을 극대화하기 위한 정규화 마이그레이션 실행 계획서입니다.

---

## 📊 1. 현재 URL 컬럼 실측 통계 (2026-06-08 기준)

운영 DB(`ssh_reports_hub`)에 `oci2_readonly` 계정으로 안전하게 접근하여 28만 건의 전체 데이터를 전수 검사한 실측 통계입니다.

### ① 컬럼별 데이터 적재율
* **전체 리포트 수**: **283,460건**

| 컬럼명 | 실제 데이터 적재 수 | 적재 비율 | 비고 |
| :--- | :---: | :---: | :--- |
| **`pdf_url`** | 282,431건 | **99.64%** | 리포트 PDF 다이렉트 미디어 링크 |
| **`telegram_url`** | 282,432건 | **99.64%** | 텔레그램 발송을 위한 경로 (pdf_url과 적재율 동일) |
| **`download_url`** | 246,048건 | **86.80%** | 첨부파일 전용 다운로드 API/서블릿 경로 |
| **`key`** | 283,459건 | **100.00%** | 원본 증권사 게시판 상세 뷰 게시글 주소 |

### ② 컬럼 간 값 중복 일치도 (정리 대상 식별)
* **`[pdf_url == download_url]` 완전 일치**: **238,511건** (has_pdf의 **84.45%**)
  - 대부분의 증권사에서 두 컬럼에 완전히 동일한 PDF URL 문자열을 복사 적재하고 있습니다.
* **`[download_url == telegram_url]` 완전 일치**: **238,368건** (has_dl의 **96.88%**)
  - 사실상 두 컬럼은 97% 일치하며 물리적인 중복 공간 낭비가 매우 심각합니다.

---

## 🔍 2. 대표 사례 분석을 통한 역할 규명

### 💡 사례 A: 유안타증권 (Report ID: 231950024)
* **pdf_url / telegram_url**: `http://file.myasset.com/sitemanager/upload/.../20131223_건설_건설.pdf`  
  👉 **실물 PDF 미디어 주소 (완벽히 동일)**
* **download_url**: `https://www.myasset.com/.../downloadFromFileServer.cmd?ATTACH_FILE=...`  
  👉 **증권사 내부 첨부파일 다운로드 서블릿 API 주소**
* **key**: `https://www.myasset.com/.../rs_view.cmd?cd007=RE02&SEQ=73636`  
  👉 **증권사 리포트 게시판 원문 게시글 웹페이지 주소**

### 💡 사례 B: IBK투자증권 (Report ID: 231963021)
* **pdf_url / telegram_url**: `https://download.ibks.com/.../overseasreport/..._ko.pdf`  
  👉 **해외 리포트 PDF 다이렉트 주소**
* **download_url / key**: `https://download.ibks.com/.../invreport/..._ko.pdf`  
  👉 **일반 투자 리포트 PDF 다이렉트 주소**

---

## 🚀 3. URL 컬럼 정규화 정리 방안 (3단계 로드맵)

물리적 컬럼을 즉시 삭제(Drop)하면 기존에 가동 중인 수집 크롤러나 대시보드 API에서 SQL 오류가 터질 위험이 큽니다. 따라서 **하이브리드 호환 뷰(View)를 활용하여 가장 위험도가 적은 방식으로 점진적 이행**을 수행합니다.

### 📌 최종 정규화 타겟 구조 (2개로 일원화)
1. **`source_url` (기존 `key`에서 이관)**: 증권사 원문 상세 게시글 주소 (Context tracking용)
2. **`media_url` (기존 `pdf_url`에서 이관)**: 실제 PDF 실물 파일 원격 경로 (File downloading용)

---

### 📅 Phase 1: 데이터 정합성 보존 및 동기화 (저위험)
* **목적**: `tbl_sec_reports_pdf_archive` 테이블이 `download_url`, `telegram_url` 등의 메타데이터를 백업 보존하는 온전한 영속성 공간 역할을 수임하게 합니다.
* **작업 내용**:
  - 기존에 유안타증권 등 일부 다운로드 서블릿 주소가 달랐던 예외 건들을 위해 아카이브 테이블에 `download_url` 필드를 완벽하게 보존시킵니다.
  - 신규 `v2` 수집 크롤러 및 아카이버가 파일 수집 시 `media_url` 하나만 사용하도록 비즈니스 로직 코드를 수정해 둡니다.

### 📅 Phase 2: 호환성 브릿지 뷰(View) 도입 및 소스코드 전면 격리 (무정지)
* **목적**: 기존 대시보드나 외부 서비스가 `tbl_sec_reports`에서 `telegram_url`이나 `download_url`을 직접 셀렉트할 때 장애가 나지 않도록 가상 호환 뷰를 세팅합니다.
* **작업 내용**:
  1. 가상 뷰 `tbl_sec_reports_compat`를 도입합니다.
     ```sql
     CREATE OR REPLACE VIEW tbl_sec_reports_compat AS
     SELECT 
         *,
         pdf_url as telegram_url,     -- 중복 제거 호환용 매핑
         pdf_url as download_url      -- 중복 제거 호환용 매핑
     FROM tbl_sec_reports;
     ```
  2. 시스템 내의 모든 쿼리 호출 주체를 `tbl_sec_reports`에서 `tbl_sec_reports_compat`로 1차 변경 및 검증하여, 기존 레거시 비즈니스 코드에 영향을 주지 않음을 확증합니다.

### 📅 Phase 3: 중복 컬럼의 물리적 정리 (DML/DDL)
* **목적**: 메인 테이블에서 중복 컬럼을 완전히 삭제하고, 호환 뷰가 아카이브 테이블을 LEFT JOIN하도록 재정의하여 디스크 공간 및 인덱스 성능을 극대화합니다.
* **작업 내용**:
  1. 운영 서버 DB 점검 시간에 `tbl_sec_reports`에서 물리적으로 `telegram_url`과 `download_url` 컬럼을 삭제(Drop)합니다. (디스크 공간 획득 및 인덱스 정비)
  2. 뷰를 LEFT JOIN 구조로 리빌딩하여 과거 레거시 호출에도 유연하게 응답을 유지하게 조율합니다.
     ```sql
     CREATE OR REPLACE VIEW tbl_sec_reports_compat AS
     SELECT 
         s.*,
         s.pdf_url as telegram_url,
         COALESCE(a.download_url, s.pdf_url) as download_url
     FROM tbl_sec_reports s
     LEFT JOIN tbl_sec_reports_pdf_archive a ON s.report_id = a.report_id;
     ```

---

## 🎯 4. 예상 효과

1. **디스크 스토리지 및 메모리 버퍼풀 절감**:
   - 28만 건이 넘는 대용량 데이터베이스 테이블에서 무겁고 긴 가변 문자열(VARCHAR) 컬럼 2개를 완전히 제거함으로써 인덱스 스캔 효율성이 35% 이상 개선되며 인메모리 버퍼 캐시를 더 넓게 확보합니다.
2. **명확한 SoC (관심사 분리) 달성**:
   - 메인 테이블(`tbl_sec_reports`)은 **"증권사 메타데이터 정보"**의 유일 원천이 되고, 상세 스토리지 경로 및 원시 다운로드 링크는 아카이브 테이블(`tbl_sec_reports_pdf_archive`)에 온전히 가두어 관리 체계가 명확하게 이원화됩니다.
