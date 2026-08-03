# PDF Archiver

증권사 리서치 리포트 PDF 다운로드 → Google Drive 아카이빙 → Static PDF 서빙

## 아키텍처

```
tbl_sec_reports (source) → v3 (pdf_archiver_v3.py) → GDrive (archive/pdf)
         ↓                                                    ↓
tbl_sec_reports_pdf_archive (meta + gdrive_file_id)    nginx proxy → https://ssh-oci.duckdns.org/pdf/{file_id}
```

- **v1** (`legacy/v1/`): OneDrive 업로드. **운영 금지, Git 보관용**.
- **v2** (`legacy/v2/`): 이전 GDrive 구현. **운영 금지, Git 보관용**.
- **v3** (`scripts/pdf_archiver_v3.py`): `lib/cloud_store` 기반 GDrive 업로드. **현재 운영 중**.
- arm2 사용자 크론: `*/3 * * * * bash /home/ubuntu/workspace/services/pdf-archiver/scripts/run_v3.sh`

Docker는 운영 경로가 아니다. v1 Docker 정의는 `legacy/v1/docker/`에만 보관하며, 새 Docker 실행 경로를 만들지 않는다.

## Google Drive

### OAuth (v3 업로드용)
- rclone remote: `gdrive:archive/pdf` (개인 계정 OAuth)
- 토큰: `~/.config/rclone/rclone.conf` → `[gdrive]`
- refresh_token으로 자동 갱신. 만료 시 Mac에서:
  ```bash
  rclone authorize drive "570770616379-..." "GOCSPX-..."
  ```
  결과 JSON을 `rclone.conf`의 `[gdrive]` token에 덮어쓰기

### 서비스 계정 (API 조회용)
- 파일: `/home/ubuntu/workspace/gcp-key.json`
- 이메일: `ag-cli-worker@gen-lang-client-0035351125.iam.gserviceaccount.com`
- ⚠️ 업로드 불가 (서비스 계정은 storage quota 없음). Shared Drive 필요.
- API 조회 / 파일 링크 생성은 가능

### Static PDF URL
- nginx proxy: `https://ssh-oci.duckdns.org/pdf/{gdrive_file_id}`
- Content-Type: `application/pdf` (GDrive는 `octet-stream`만 주므로 nginx에서 변환)
- IOS Safari / PWA에서 PDF 뷰어로 바로 열림
- 설정 위치: OCI `~/workspace/main-infra/gateway/main-nginx/public/default.conf`의 `/pdf/` location
- GDrive file_id 획득: `rclone link` 또는 Drive API `files.get`

## DB 연결

### 스키마 (2026-08-03 운영 기준)

**`tbl_sec_reports`** (소스 리포트):
| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_id` | bigint PK | 자동증가 |
| `firm_nm` | text | 증권사명 (예: 하나증권, KB증권) |
| `report_unique_key` | text UNIQUE | 리포트 식별키 |
| `article_title` | text | 리포트 제목 |
| `writer` | text | 작성자 |
| `pdf_url` | text | PDF 다운로드 URL |
| `telegram_url` | text | 텔레그램 URL |
| `report_date` | date | 리포트 발행일 |
| `save_at` | timestamptz | 저장 시각 |
| `mkt_tp` | text | 마켓 구분 (기본 'KR') |

**`tbl_sec_reports_pdf_archive`** (아카이브 메타):
| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_id` | bigint PK | `tbl_sec_reports.report_id` 와 동일값 |
| `firm_nm` | text | 증권사명 |
| `archive_status` | text | `ARCHIVED` / `INIT` |
| `storage_backend` | text | `googledrive` (v3), `onedrive` (v1 legacy) |
| `storage_key` | text | GDrive file ID |
| `file_path` | text | GDrive 경로 |
| `file_size` | bigint | 바이트 |
| `page_count` | integer | 페이지 수 |
| `file_name` | text | 파일명 |
| `pdf_hash` | bytea | SHA-256 |
| `created_at` | timestamptz | 아카이빙 완료 시각 |

**Join**: `tbl_sec_reports.report_id = tbl_sec_reports_pdf_archive.report_id`
- ⚠️ `report_unique_key`는 archive 테이블에 **없음**. join에 사용 불가.
- 두 테이블 모두 `download_url`, `download_status_yn` 컬럼 없음 (deprecated).

**참고**: archive 테이블 row 수가 소스보다 많을 수 있음 (~13K). v1/v2 시절 레거시 데이터가 소스에서 삭제되었으나 archive에 남아있는 건.

### ⚠️ SSH 터널 필수 (arm2 → OCI)
```bash
ssh -f -N -L 5433:10.0.0.111:5432 -o ServerAliveInterval=30 oci
```
- `POSTGRES_HOST=localhost POSTGRES_PORT=5433` (터널 통해서)
- ⚠️ `127.0.0.1:5432`가 아니라 `10.0.0.111:5432`로 포워딩! (DB가 10.0.0.111에만 바인딩)
- ⚠️ asyncpg는 `ssl=False` 필수 (SSL 시도하다 실패함. psql은 자동 fallback)

## 다운로더

`downloaders/` — 증권사별 전용 PDF 다운로드. 현재 v3 registry에 등록됨.

| 파일 | 증권사 | 방식 |
|------|--------|------|
| `hana.py` | 하나증권 | Board page → PDF 링크 추출 |
| `ds.py` | DS투자증권 | Board page → 다운로드 |
| `kyobo.py` | 교보증권 | board.php → PDF URL |
| `ls.py` | LS증권 | WARP SOCKS5 → View.jsp |
| `mirae.py` | 미래에셋증권 | 게시판 → PDF |
| `dbfi.py` | DB증권/DBFi | WARP → vgate → streamdocs (candidates에서 appData 우선) |
| `heungkuk.py` | 흥국증권 | Board list → key → download.do |
| `meritz.py` | 메리츠증권 | BbsRead.go → PDF 링크 + Referer |

## WARP 프록시
- SOCKS5: `127.0.0.1:9091`
- DBFi, LS 다운로더에서 사용 (`aiohttp_socks.ProxyConnector`)
- 해당 증권사 서버 WARP 통해서만 접근 가능

## v2 주요 버그 수정 내역 (2026-06-16)

| 버그 | 증상 | 수정 |
|------|------|------|
| `upsert_archive` INSERT `$16` 누락 | 전건 실패 (0 ok) | `$15,NOW(),NOW(),$16` → `NOW(),NOW(),$15` |
| downloader 미연동 | 6개 증권사 wget만 사용 | registry 수동 등록 + `_select_downloader` 연동 |
| `_is_pdf` await 누락 | RuntimeWarning | `async def` + `await` |
| wget→curl | Referer 없는 요청으로 차단 | curl + Referer + URL 인코딩 |
| `_clean_url` | URL 끝 `')` 가비지 (교보 101건) | 정규식으로 trailing garbage 제거 |
| DB tunnel IP | `127.0.0.1` → DB 연결 안됨 | `10.0.0.111`로 수정 |
| asyncpg SSL | SSL handshake 실패 | `ssl=False` |
| zombie lock | v2 4일간 정지 | lock 파일 PID 체크 후 정리 |
| GDrive 403 Quota | 전건 실패 (0 ok), 무한루프 | rclone stderr 파싱 → exponential backoff + max runtime guard |
| InterfaceError | "another operation in progress" | asyncio.Lock으로 DB 작업 직렬화 (asyncpg conn은 단일 작업만 가능) |
| DataError (date) | report_date `expected str, got date` | asyncpg가 DATE 컬럼을 `datetime.date`로 반환 → str 변환 |
| stale lock | kill -9 후 cron 진입 불가 | `acquire_lock()`에서 PID 존재 여부 확인 후 정리 |

## v2 Quota / Rate-limit 설계 (2026-07-06)

GDrive 개인 계정은 100 QPM 제한 → `--drive-pacer-min-sleep 200ms --drive-pacer-burst 2`로 rclone 호출 간격 조절.
Quota 초과 감지 시:
- retry_count 증가하지 않음 (DB_RETRY_LIMIT 소진 방지)
- exponential backoff: `QUOTA_BACKOFF_BASE * 2^min(consecutive_failures, 6)` (기본 5s → 최대 320s)
- `MAX_CONSECUTIVE_QUOTA_FAILURES` (기본 12) 도달 시 종료 → 다음 cron run에서 재시도
- 성공적인 업로드 발생 시 counter reset

## 주요 환경변수 (`.env`)
```
RCLONE_REMOTE=gdrive:archive/pdf
WARP_PROXY=localhost:9091
POSTGRES_HOST=10.0.0.111
```
