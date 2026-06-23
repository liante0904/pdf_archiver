# PDF Archiver

증권사 리서치 리포트 PDF 다운로드 → Google Drive 아카이빙 → Static PDF 서빙

## 아키텍처

```
tbl_sec_reports (source) → v2 (pdf_archiver_v2.py) → GDrive (archive/pdf)
         ↓                                                    ↓
tbl_sec_reports_pdf_archive (meta + gdrive_file_id)    nginx proxy → https://ssh-oci.duckdns.org/pdf/{file_id}
```

- **v1** (`pdf_archiver_async.py`): OneDrive 업로드. **deprecated**.
- **v2** (`scripts/pdf_archiver_v2.py`): GDrive 업로드. **현재 운영 중**.
- 크론: `*/3 * * * * bash run_v2.sh`

## Google Drive

### OAuth (v2 업로드용)
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

### ⚠️ SSH 터널 필수 (arm2 → OCI)
```bash
ssh -f -N -L 5433:10.0.0.111:5432 -o ServerAliveInterval=30 oci
```
- `POSTGRES_HOST=localhost POSTGRES_PORT=5433` (터널 통해서)
- ⚠️ `127.0.0.1:5432`가 아니라 `10.0.0.111:5432`로 포워딩! (DB가 10.0.0.111에만 바인딩)
- ⚠️ asyncpg는 `ssl=False` 필수 (SSL 시도하다 실패함. psql은 자동 fallback)

## 다운로더

`downloaders/` — 증권사별 전용 PDF 다운로드. v2 registry에 등록됨.

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

## 주요 환경변수 (`.env`)
```
RCLONE_REMOTE=gdrive:archive/pdf
WARP_PROXY=localhost:9091
POSTGRES_HOST=10.0.0.111
```
