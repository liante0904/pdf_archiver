# pdf_archiver

DB증권(DBfi) 레코드는 `pdf_url` 문자열만 직접 받아서 내려받는 방식이 아니라,
`key -> descRsh JSON -> pv/auth -> pv/viewer -> streamdocs PDF GET` 순서를 같은
`aiohttp` 세션 안에서 타야 합니다. `pdf_url`은 사용자가 볼 수 있는 URL로 저장되지만,
쿠키 없는 새 프로세스나 `wget`으로 직접 호출하면 실패할 수 있습니다.

현재 v3 아카이버는 DBfi를 이 방식으로 처리하고, 일반 증권사는 기존 URL 후보 다운로드 경로를 유지합니다.

운영/분석/마이그레이션용 단발 스크립트는 `scripts/` 아래로 분리해 두었습니다.
운영 실행 본체는 `scripts/pdf_archiver_v3.py`이며 arm2 사용자 crontab이 3분마다
`scripts/run_v3.sh`를 호출합니다. `pdf_archiver_async.py`는 deprecated v1 구현입니다.

2026-07-10 운영 DB 기준 source URL/키는 `tbl_sec_reports`의 `pdf_url`,
`telegram_url`, `report_unique_key`입니다. archive 테이블은 `pdf_url`,
`telegram_url`만 사용하며, `download_url`과 `download_status_yn`은 두 테이블에 없습니다.

현재 안정화 방향과 수정 시 주의사항은 `docs/archiver_stability_notes.md`를 먼저 확인하세요.

환경변수는 기본적으로 `~/secrets/pdf_archiver/.json`에서 읽습니다.
필요하면 `WORKSPACE_SECRET_FILE`로 다른 JSON 경로를 지정할 수 있습니다.
Docker Compose는 호스트의 `/home/ubuntu/secrets/pdf_archiver`를 `/secrets/pdf_archiver`로 마운트해 같은 파일을 읽습니다.
