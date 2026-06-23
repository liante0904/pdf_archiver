# [보고서] 데이터베이스 정밀 실측 기반 URL 및 레거시 플래그 분석서

> **작성자**: Antigravity AI Coding Assistant  
> **작성일**: 2026-06-08  
> **대상 데이터베이스**: PostgreSQL 운영 DB (`ssh_reports_hub`)  
> **대상 테이블**: `tbl_sec_reports` (메인 리포트 테이블, 총 283,460건)

---

## 📊 1. [pdf_url IS NULL] 0.36% (1,029건) 정밀 실측 분석

`tbl_sec_reports` 테이블에서 `pdf_url` 적재율은 **99.64%**입니다. 나머지 **0.36% (1,029건)**이 비어 있는(NULL) 데이터들을 전수 분석하여 비즈니스적인 수집 배경`과 정합성 실체를 파악하였습니다.

### ① 데이터 결합 분포 상태
* **전체 pdf_url NULL 수량**: **1,029건**
* `telegram_url`이 동시에 존재하는 건: **단 1건** (유형 A에 해당)
* `download_url`이 존재하는 건: **1,020건**
* 원문 게시글 주소(`key`)가 존재하는 건: **1,028건**

### ② 증권사별 데이터 분포
* **유안타증권**: **1,024건 (99.5%)**
* **LS증권**: **3건**
* **NH투자증권**: **1건** (유형 A)
* **한화투자증권**: **1건**

### ③ 연도별 등록 분포 (`reg_dt` 기준)
```mermaid
gantt
    title pdf_url 누락 데이터 연도별 집중도
    dateFormat  YYYY
    axisFormat %Y
    section 과거 레거시 데이터 (99.3%)
    2002~2013년 (1,022건) : active, 2002, 2013
    section 현대 간헐적 예외 (0.7%)
    2019~2026년 (7건)     : crit, 2019, 2026
```

### 💡 0.36% 누락 원인의 비즈니스 비밀
1. **유안타증권 과거 백필의 한계 (1,024건)**:  
   * 누락 데이터의 **99.5%가 유안타증권의 2013년 이전(2002~2013) 과거 데이터**입니다.
   * 유안타증권(과거 동양증권 시절 포함)은 과거 리포트 다운로드 시 직접적인 PDF 원본 링크(`pdf_url`) 형태를 대외 오픈하지 않았고, 내부 첨부파일 다운로드 서블릿 경로(`download_url` = `downloadFromFileServer.cmd?ATTACH_FILE=...`)만을 제공했습니다.
   * 따라서 이 데이터들은 직접 미디어 주소(`pdf_url`, `telegram_url`)가 NULL인 것이 비즈니스상 자연스러우며, **`download_url`에만 실제 유효한 다운로드 경로가 담긴 정상 아카이브 데이터**입니다.
2. **LS/한화증권 원본 누락 (4건)**:  
   * `key`(상세글 주소)만 존재하고 세 가지 URL이 모두 비어 있습니다. 증권사 게시판에 본문 텍스트만 게시되었거나 PDF 첨부파일 자체가 업로드되지 않은 특수 케이스입니다.

---

## 🔒 2. 유형 A (NH투자증권) 단 1건 처리 방안

* **대상 데이터**: `report_id = 231969915` (NH투자증권, 등록일: `2026-03-20`)
* **특이 현상**: `pdf_url`은 NULL이지만 `telegram_url`만 유독 값(`http://download.nhqv.com/...`)이 채워져 있어 불일치하는 유일무이한 **유형 A** 데이터입니다.

> [!IMPORTANT]  
> 사용자 가이드라인에 따라 운영 DB에 직접 DML 수정 권한을 쓸 수 없으므로, 운영 서버(`oci`)에 직접 접속하셔서 `docker exec`를 활용한 postgres 컨테이너 내부 psql 혹은 SQL 클라이언트에서 아래 업데이트 쿼리를 직접 수행해 주시면 완벽하게 보정됩니다.

```sql
-- [1단계] 보정 대상 1건 검증 조회
SELECT report_id, firm_nm, reg_dt, pdf_url, telegram_url 
FROM tbl_sec_reports 
WHERE report_id = 231969915;

-- [2단계] telegram_url NULL 보정 (두 필드 모두 NULL로 정합성 일치)
UPDATE tbl_sec_reports 
SET telegram_url = NULL 
WHERE report_id = 231969915;

-- [3단계] 반영 결과 검증
SELECT report_id, firm_nm, reg_dt, pdf_url, telegram_url 
FROM tbl_sec_reports 
WHERE report_id = 231969915;
```

---

## 📡 3. 유형 C (DS투자증권) Netlify 우회 트리거 규명

* **대상 데이터**: DS투자증권 (유형 C - 179건)
* **불일치 실체**: 
  * `pdf_url`: `https://www.ds-sec.co.kr/bbs/download.php?bo_table=sub03_03&wr_id=2073&no=0` (그누보드 다운로드)
  * `telegram_url`: `https://ssh-oci.netlify.app/share?id=246935027` (자체 모바일 브릿지)

### ⚙️ 데이터베이스 트리거 연동 작동 원리
메인 테이블 스키마 DDL 검증 도중, 이 우회 주소가 **데이터베이스 트리거 함수를 통해 자동으로 생성 적재**되고 있음이 완벽하게 규명되었습니다.

```sql
CREATE OR REPLACE FUNCTION public.set_ds_share_telegram_url()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- sec_firm_order = 11 (DS투자증권)인 신규 데이터 적재 시,
    -- telegram_url이 비어 있다면 자동으로 Netlify 공유 주소로 변환하여 적재함
    IF NEW.sec_firm_order = 11
       AND (NEW.telegram_url IS NULL OR NEW.telegram_url = '') THEN
        NEW.telegram_url := 'https://ssh-oci.netlify.app/share?id=' || NEW.report_id::text;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_set_ds_share_telegram_url 
BEFORE INSERT OR UPDATE ON public.tbl_sec_reports 
FOR EACH ROW EXECUTE FUNCTION public.set_ds_share_telegram_url();
```

* **비즈니스적 혜택**: 그누보드 첨부파일 다운로드 서블릿은 모바일 텔레그램 인앱 브라우저에서 먹통을 유발하기 때문에, 사용자님이 구축하신 Netlify 모바일 프리뷰 브릿지(`ssh-oci.netlify.app`)를 연동하여 모바일 사용자 경험(UX)을 완벽하게 수호하는 정교한 설계 구조입니다.

---

## 🛠️ 4. 리팩토링 타당성 진단: 2번(증권사명) vs 4번(레거시 플래그)

정규화 설계 시 소스코드 내 사용처를 전수 수색(`grep_search`)하여, 가장 무장애로 간단히 처리할 수 있는 우선순위를 도출했습니다.

### ① 소스코드 내 사용 빈도 비교
* **`firm_nm` (2번 - 증권사 명칭 정규화)**: **수십 개 파일 이상**
  * 수집기 크롤러의 직접 적재 구문, 텔레그램 발송 메시지 빌더, 조회 API 등 비즈니스 코어 로직 깊숙이 한글 문자열로 침투되어 있습니다. 물리 컬럼을 드랍하고 FK 조인으로 변경할 경우 공사가 매우 커지며 알림 누락 등의 장애 리스크가 따릅니다.
* **`download_status_yn` (4번 - 레거시 상태 플래그 제거)**: **단 5개 파일**
  ```bash
  # 수색 결과 위치
  - PostgreSQLManager.py (L30, L283, L325)
  - SQLiteManager.py (L340, L387)
  - migrate_sqlite_to_postgres.py (L109, L126, L142, L152)
  - sync_recent_sqlite_to_postgres.py (L32, L56)
  ```
  * 실질 사용처가 단 몇 줄에 불과하며, 텔레그램 발송 비즈니스 로직과는 전혀 무관한 백엔드 내부 "다운로드 성공 여부 기록용" 과거의 흔적입니다.

### ② 안전한 4단계 과도기 마이그레이션 시나리오 (정석 이행안)

실서비스 운영 환경에서의 무장애 릴리즈 정석에 따라, 기존 레거시 컬럼을 무작정 즉시 삭제(Drop)하지 않고 **"신규 컬럼 및 테이블 영역 선구축(ADD) ➡️ 데이터 이관(BACKFILL) ➡️ 소스코드 점진적 전환(SWAP) ➡️ 레거시 정리(DROP)"**의 4단계 롤링 마이그레이션 절차를 수립하여 안전성을 극대화합니다.

```mermaid
graph TD
    A["[Step 1] 신규 상태 공간 보존 및 DB 추가 (ADD)"] --> B["[Step 2] 기존 누적 데이터 동기화 (BACKFILL)"]
    B --> C["[Step 3] 소스코드 신규 영역 전면 전환 및 뷰 세팅 (SWAP)"]
    C --> D["[Step 4] 무장애 최종 모니터링 후 구 컬럼 정리 (DROP)"]
```

#### 📅 Step 1: 신규 상태 관리 및 아카이브 전용 컬럼 확보 (ADD)
* 메인 테이블의 `download_status_yn` 및 URL 정보들을 완벽하게 격리 보관할 `tbl_sec_reports_pdf_archive` 테이블 측에 완벽한 스키마 및 상태 컬럼(`pdf_sync_status` 등)이 선구축되어 있는지 확인하고 미비한 컬럼이 있다면 우선 추가합니다.

#### 📅 Step 2: 레거시 데이터 동기화 및 백필 (BACKFILL)
* 기존 메인 테이블에 적재되어 있던 `download_status_yn = 'Y'` 상태와 기존 PDF 가공 주소 데이터들을 아카이브 전용 테이블(`tbl_sec_reports_pdf_archive`)의 대응 컬럼에 한 건의 유실도 없도록 동기화(UPSERT/UPDATE) 배치를 가동해 완벽하게 정합성을 맞춰 둡니다.

#### 📅 Step 3: 소스코드 전면 전환 및 호환성 뷰 설정 (SWAP)
* `PostgreSQLManager.py`를 포함한 5개 핵심 소스코드 파일에서 `download_status_yn` 및 구 URL 컬럼을 참조/업데이트하던 쿼리문을, 신규 정규화 컬럼 및 아카이브 테이블을 바라보도록 전면 수정 및 빌드 배포합니다.
* 이때, 데이터베이스 전면에 가상 호환 뷰(`tbl_sec_reports_compat`)를 먼저 배치하여 배포 과도기 동안 신구 버전의 애플리케이션 코드가 공존하더라도 쿼리 에러가 나지 않도록 격리(Bridge)합니다.

#### 📅 Step 4: 무장애 최종 모니터링 후 레거시 삭제 (DROP)
* 신규 릴팩토링 소스코드가 실시간 수집 및 텔레그램 발송 환경에서 며칠간 완벽하게 무장애로 가동되는지 집중 모니터링을 거칩니다.
* 데이터 누수 및 로직 오류가 0%임을 최종 확증한 뒤, 점검 시간을 활용해 비로소 메인 테이블(`tbl_sec_reports`)에서 쓸모 없어진 레거시 컬럼(`download_status_yn` 등)을 안전하게 물리적으로 제거(Drop)하고 호환 뷰를 LEFT JOIN 형태로 완전 리빌딩합니다.

---
*본 보고서는 시스템 인프라 아키텍처 자산 보존을 위해 로컬 문서로 생성되었습니다.*
