# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiohttp",
#     "asyncpg",
#     "aiohttp-socks",
# ]
# ///

import asyncio
import aiohttp
import ssl
import os
import signal
import time
import logging
import fcntl
import sys
import re
import shutil
import unicodedata
import json
import html
import tempfile
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse, unquote, urljoin

try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None

from config import Config
from utils import (
    _truncate, _normalize_pdf_url_value, _normalize_pdf_url_sql,
    _pdf_hash_bytes, _is_pdf_payload, _report_prefix,
    _download_sources_for_firm, _browser_like_headers,
    _cookie_header_from_response, safe_encode_url,
    _encode_url_euc_kr, _has_korean, _origin_referer,
    _firm_base_domain, _get_first_url, _decode_mirae_html,
    _normalize_match_text, _find_mirae_board_download_url,
    _FIRMS_NEEDING_COOKIE_SESSION, extract_dbfi_retry_candidates,
    build_candidate_urls, get_pdf_page_count,
    _ensure_session_cookies_aiohttp
)
from downloaders import (
    download_ds_pdf, download_mirae_pdf, download_kyobo_pdf,
    download_hana_pdf, download_ls_pdf, download_dbfi_pdf
)

LOCAL_BUFFER_DIR = Config.LOCAL_BUFFER_DIR

from db_manager import DBManager, get_db_connection, ensure_pdf_sync_status_schema

def _row_payload(row):
    return {
        "row_id": row[0],
        "report_id": row[1],
        "sec_firm_order": row[2],
        "report_unique_key": row[3],
        "pdf_url": row[4],
        "telegram_url": row[5],
        "download_url": row[6],
        "firm_nm": row[7],
        "title": row[8],
        "report_date": row[9],
    }


class WorkflowRecord(dict):
    ORDER = (
        "row_id",
        "report_id",
        "sec_firm_order",
        "report_unique_key",
        "pdf_url",
        "telegram_url",
        "download_url",
        "firm_nm",
        "title",
        "report_date",
        "pdf_hash",
    )

    def __getitem__(self, key):
        if isinstance(key, int):
            return tuple(dict.__getitem__(self, field) for field in self.ORDER)[key]
        return super().__getitem__(key)

async def check_and_restart_warp():
    # 도커 환경에서는 별도의 warp 컨테이너나 호스트의 warp를 사용함
    try:
        # 1단계: 프록시 포트 오픈 확인 (단순 소켓 연결)
        host, port = Config.WARP_PROXY.split(":")
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=3)
        writer.close()
        await writer.wait_closed()
        logging.info(f"WARP proxy ({Config.WARP_PROXY}) is reachable.")
    except Exception:
        logging.warning(f"WARP proxy ({Config.WARP_PROXY}) seems down or unreachable. (Docker 'warp' container check needed)")

from rclone_manager import RcloneManager

class PDFArchiver(RcloneManager):
    def __init__(self):
        super().__init__()
        self.local_dir = Path(Config.LOCAL_BUFFER_DIR)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(Config.DOWNLOAD_CONCURRENCY)
        self.dbfi_semaphore = asyncio.Semaphore(Config.DBFI_DOWNLOAD_CONCURRENCY)
        self.success_downloads = []
        self.total_targets = 0
        self.processed_count = 0
        self._counter_lock = asyncio.Lock()

    async def _increment_processed(self, ok, firm, title, report_id, pdf_url=None):
        async with self._counter_lock:
            self.processed_count += 1
            total = self.total_targets if self.total_targets > 0 else 1
            pct = (self.processed_count / total * 100)
            
            # 상태바 생성 [##########----------]
            bar_length = 20
            filled_length = int(bar_length * self.processed_count // total)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            emoji = "✅" if ok else "❌"
            short_url = _truncate(pdf_url, 60) if pdf_url else "N/A"
            
            # 로그 출력: 상태바 + 퍼센트 + 결과 이모티콘 + report_id + 증권사 | 제목
            logging.info(f"|{bar}| {pct:5.1f}% {emoji} [{self.processed_count:3}/{self.total_targets:3}] report_id={report_id} {firm} | {title[:25]}... | {short_url}")

    def _add_success_record(self, row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, report_date, target_path, size, pages, pdf_hash):
        self.success_downloads.append(WorkflowRecord({
            "row_id": row_id, "report_id": report_id, "sec_firm_order": sec_firm_order,
            "report_unique_key": key_url, "pdf_url": pdf_url, "telegram_url": tel_url, "download_url": dw_url,
            "firm_nm": firm, "title": title, "report_date": report_date, "path": target_path,
            "size": size, "pages": pages, "pdf_hash": pdf_hash,
        }))

    async def _try_await_record_download(self, row_meta, target_path, coro):
        """비동기 다운로드 래퍼: await coro → 성공 시 success_downloads 에 기록하고 True."""
        result = await coro
        if result:
            row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, report_date = row_meta
            self._add_success_record(
                row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url,
                firm, title, report_date, target_path,
                result["size"], result["pages"], result.get("pdf_hash"),
            )
            return True
        return False

    def _make_file_path(self, firm, title, report_date, report_id):
        clean_dt = re.sub(r'[^0-9]', '', str(report_date)) if report_date else "00000000"
        y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
        yy_mm_dd = clean_dt[2:8]
        normalized = unicodedata.normalize('NFC', title or '')
        safe_title = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]\(\)]', ' ', normalized)
        safe_title = '_'.join(safe_title.split())[:60].strip('_') or 'untitled'
        filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
        return self.local_dir / y_m / firm / filename

    async def download_task(self, row):
        # row: (id, report_id, sec_firm_order, report_unique_key, pdf_url, telegram_url, download_url, firm, title, report_date)
        row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, report_date = row
        row_meta = (row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, report_date)
        raw_urls = _download_sources_for_firm(key_url, pdf_url, tel_url, dw_url)
        candidates = build_candidate_urls(firm, raw_urls)
        
        proxy_url = os.getenv("WARP_PROXY")
        use_proxy = proxy_url and any(k in firm for k in ("LS",))

        target_path = self._make_file_path(firm, title, report_date, report_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix('.tmp')

        ok = False
        async with self.semaphore:
            # 1. DBfi 특수 처리
            if not ok and sec_firm_order == Config.DBFI_FIRM_ORDER:
                dbfi_source_url = key_url or pdf_url
                if not dbfi_source_url and tel_url and "/appData/descRsh/" in str(tel_url):
                    dbfi_source_url = tel_url
                async with self.dbfi_semaphore:
                    ok = await self._try_await_record_download(
                        row_meta, target_path,
                        download_dbfi_pdf(dbfi_source_url, target_path, title, report_id, firm, report_date),
                    )
                    if Config.DBFI_REQUEST_DELAY_SECONDS > 0:
                        await asyncio.sleep(Config.DBFI_REQUEST_DELAY_SECONDS)

            # 2. 미래에셋증권 특수 처리
            if not ok and firm == "미래에셋증권":
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_mirae_pdf(candidates, target_path, title, report_id, firm, report_date),
                )

            # 3. DS투자증권 특수 처리
            if not ok and firm == "DS투자증권":
                for url in candidates:
                    ok = await self._try_await_record_download(
                        row_meta, target_path,
                        download_ds_pdf(url, target_path, title, report_id, firm, report_date),
                    )
                    if ok:
                        break


            # LS증권 특수 처리 (View.jsp 2-step 파싱)
            if not ok and "LS" in firm:
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_ls_pdf(candidates, target_path, title, report_id, firm, report_date),
                )
            # 3b. 교보증권 특수 처리 (게시판 뷰 페이지에서 실제 PDF URL 추출)
            if not ok and firm == "교보증권":
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_kyobo_pdf(candidates, target_path, title, report_id, firm, report_date),
                )

            # 3c. 하나증권 특수 처리 (게시판에서 유효한 다운로드 URL 재추출)
            if not ok and firm == "하나증권":
                ok = await self._try_await_record_download(
                    row_meta, target_path,
                    download_hana_pdf(candidates, target_path, title, report_id, firm, report_date),
                )

            # 4. 일반 다운로드 (wget) — 4개 증권사(대신/IBK/삼성/다올/교보)는 쿠키+Referer 처리
            if not ok and firm not in ("미래에셋증권", "DS투자증권", "교보증권", "하나증권") and sec_firm_order != Config.DBFI_FIRM_ORDER:
                # 4a. aiohttp로 세션 쿠키 사전 획득
                cookie_string = ""
                if firm in _FIRMS_NEEDING_COOKIE_SESSION:
                    first_url = _get_first_url(pdf_url, key_url, tel_url, dw_url, candidates)
                    if first_url:
                        cookie_string = await _ensure_session_cookies_aiohttp(firm, first_url)
                        if cookie_string:
                            logging.info(
                                "%s session cookies acquired: %s",
                                _report_prefix(firm, title, report_id, report_date),
                                cookie_string,
                            )

                for url in candidates:
                    # 4b. wget 명령어 구성
                    # 4개 대상 증권사는 Referer를 다운로드 URL 자체로 설정 (서블릿/Referer 검증 대응)
                    if firm in _FIRMS_NEEDING_COOKIE_SESSION:
                        referer = pdf_url or key_url or url
                    else:
                        referer = _origin_referer(pdf_url or key_url or url)
                    cmd = [
                        "wget", "-q", "-O", str(tmp_path),
                        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                        "--referer=" + referer,
                        "--timeout=30", "--tries=2",
                        "--no-check-certificate",
                    ]
                    if cookie_string:
                        cmd.insert(1, "--header=Cookie: " + cookie_string)
                    cmd.append(url)

                    if use_proxy:
                        os.environ["all_proxy"] = f"socks5h://{proxy_url}"

                    try:
                        proc = await asyncio.create_subprocess_exec(*cmd, env=os.environ)
                        await proc.wait()

                        if proc.returncode == 0 and tmp_path.exists() and tmp_path.stat().st_size > 1024:
                            body = tmp_path.read_bytes()
                            if _is_pdf_payload(body):
                                if target_path.exists(): target_path.unlink()
                                tmp_path.rename(target_path)
                                pages = await get_pdf_page_count(target_path)
                                self._add_success_record(row_id, report_id, sec_firm_order, key_url, pdf_url, tel_url, dw_url, firm, title, report_date, target_path, target_path.stat().st_size, pages, _pdf_hash_bytes(body))
                                ok = True
                                break
                        else:
                            logging.warning(
                                "%s download failed returncode=%s size=%s url=%s",
                                _report_prefix(firm, title, report_id, report_date),
                                proc.returncode,
                                tmp_path.stat().st_size if tmp_path.exists() else 0,
                                _truncate(url, 220),
                            )
                    except Exception as e:
                        logging.warning(
                            "%s download exception %s: %r url=%s",
                            _report_prefix(firm, title, report_id, report_date),
                            type(e).__name__,
                            e,
                            _truncate(url, 220),
                        )
                    finally:
                        if use_proxy: os.environ.pop("all_proxy", None)
                        if tmp_path.exists(): tmp_path.unlink(missing_ok=True)
        
        await self._increment_processed(ok, firm, title, report_id, pdf_url=pdf_url or key_url)
        return ok

    async def _update_source_workflow(self, conn, payload, pdf_status, retry_delta=0):
        pdf_url_norm = _normalize_pdf_url_value(payload.get("pdf_url"))
        pdf_hash = payload.get("pdf_hash")
        await conn.execute(
            f'''
            UPDATE {Config.SOURCE_TABLE}
            SET {Config.PDF_STATUS_COL} = $2,
                retry_count = COALESCE(retry_count, 0) + $3,
                {Config.PDF_HASH_COL} = COALESCE($4, {Config.PDF_HASH_COL})
            WHERE report_id = $1
               OR (NULLIF(BTRIM(pdf_url), '') = $5 AND $5 IS NOT NULL)
            ''',
            int(payload["report_id"]), pdf_status, retry_delta, pdf_hash, pdf_url_norm,
        )

    async def _upsert_archive_workflow(self, conn, payload, pdf_status, retry_delta=0, file_path=None, file_size=None, page_count=None, archive_status=None, download_status_yn=None, storage_key=None):
        file_name = Path(file_path).name if file_path else None
        await conn.execute(
            f'''
            INSERT INTO {Config.META_TABLE} (
                report_id, firm_nm, title, author, reg_dt, pdf_url, {Config.PDF_HASH_COL},
                has_text, is_encrypted, storage_backend, storage_key, download_url, telegram_url,
                key, archive_status, file_name, download_status_yn, file_path, file_size, page_count,
                last_accessed_at, {Config.PDF_STATUS_COL}, created_at, updated_at, retry_count
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, COALESCE($23, NOW()), NOW(), $24
            )
            ON CONFLICT (report_id) DO UPDATE SET
                firm_nm = EXCLUDED.firm_nm, title = EXCLUDED.title, author = COALESCE(EXCLUDED.author, {Config.META_TABLE}.author),
                reg_dt = EXCLUDED.reg_dt, pdf_url = EXCLUDED.pdf_url, {Config.PDF_HASH_COL} = COALESCE(EXCLUDED.{Config.PDF_HASH_COL}, {Config.META_TABLE}.{Config.PDF_HASH_COL}),
                storage_backend = COALESCE(EXCLUDED.storage_backend, {Config.META_TABLE}.storage_backend),
                storage_key = COALESCE(EXCLUDED.storage_key, {Config.META_TABLE}.storage_key),
                archive_status = COALESCE(EXCLUDED.archive_status, {Config.META_TABLE}.archive_status),
                file_name = COALESCE(EXCLUDED.file_name, {Config.META_TABLE}.file_name),
                download_status_yn = COALESCE(EXCLUDED.download_status_yn, {Config.META_TABLE}.download_status_yn),
                file_path = COALESCE(EXCLUDED.file_path, {Config.META_TABLE}.file_path),
                file_size = COALESCE(EXCLUDED.file_size, {Config.META_TABLE}.file_size),
                page_count = COALESCE(EXCLUDED.page_count, {Config.META_TABLE}.page_count),
                {Config.PDF_STATUS_COL} = EXCLUDED.{Config.PDF_STATUS_COL},
                updated_at = NOW(), retry_count = COALESCE({Config.META_TABLE}.retry_count, 0) + $24
            ''',
            int(payload["report_id"]), payload.get("firm_nm"), payload.get("title"), payload.get("author"),
            payload.get("reg_dt"), payload.get("pdf_url"), payload.get("pdf_hash"), payload.get("has_text"),
            payload.get("is_encrypted"), payload.get("storage_backend") or "onedrive",
            storage_key or payload.get("storage_key") or (str(file_path) if file_path else None),
            payload.get("download_url"), payload.get("telegram_url"), payload.get("report_unique_key"),
            archive_status, file_name, download_status_yn, str(file_path) if file_path else None,
            file_size, page_count, payload.get("last_accessed_at"), pdf_status, None, retry_delta,
        )

    async def _apply_workflow_update(self, conn, payload, pdf_status, **kwargs):
        """Helper to update both source and archive tables."""
        await self._update_source_workflow(conn, payload, pdf_status, retry_delta=kwargs.get("retry_delta", 0))
        await self._upsert_archive_workflow(conn, payload, pdf_status, **kwargs)

    async def upload_to_onedrive(self) -> list[WorkflowRecord]:
        """
        0-byte target cleanup → rclone copy → nameAlreadyExists 체크 → 디렉토리 배치 lsl 검증 → 로컬 정리.

        nameAlreadyExists 처리:
          - 파일 하나하나 lsl 호출하지 않고, 부모 디렉토리별로 rclone lsl 한 번씩만 호출
          - 10개 파일이 3개 디렉토리에 있으면 lsl 3번 = API 3번 (기존 방식은 10번)
          - 전체 rclone cleanup은 너무 느려 기본 흐름에서는 사용하지 않음
        """
        total = len(self.success_downloads)
        logging.info("Upload %d files...", total)

        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        deleted_zero_byte = await self._delete_stale_zero_byte_upload_targets(self.success_downloads, self.local_dir)
        if deleted_zero_byte:
            logging.info("Deleted %d stale 0-byte remote upload target(s).", deleted_zero_byte)

        # 로컬 매핑: relative_path → WorkflowRecord
        local_map: dict[str, WorkflowRecord] = {}
        for p in self.success_downloads: local_map[str(p["path"].relative_to(self.local_dir))] = p

        files_from_path = None
        files_from_fp = tempfile.NamedTemporaryFile("w", encoding="utf-8", prefix="pdf_archiver_rclone_files_", suffix=".txt", delete=False)
        try:
            files_from_path = files_from_fp.name
            for rel_path in local_map: files_from_fp.write(rel_path + "\n")
        finally:
            files_from_fp.close()

        # --- rclone copy (move 대신 copy 사용 후 수동 검증+삭제가 더 안전함) ---
        try:
            proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "copy", Config.LOCAL_BUFFER_DIR, Config.RCLONE_REMOTE, "--files-from", files_from_path, "--transfers", str(Config.RCLONE_TRANSFERS), "--checkers", str(Config.RCLONE_CHECKERS), "--no-traverse", "--onedrive-chunk-size", Config.ONEDRIVE_CHUNK_SIZE, "--retries", str(Config.RCLONE_RETRIES), "--low-level-retries", str(Config.RCLONE_LOW_LEVEL_RETRIES), "--onedrive-no-versions", env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
            _, stderr = await proc.communicate()
            stderr_text = stderr.decode("utf-8", errors="replace")
        finally:
            if files_from_path:
                try: os.unlink(files_from_path)
                except OSError: pass

        if proc.returncode != 0 and stderr_text.strip():
            stderr_lines = [line for line in stderr_text.splitlines() if line.strip()]
            logging.warning("rclone copy stderr tail: %s", " | ".join(stderr_lines[-8:])[:1200])
            if "Failed to create file system" in stderr_text:
                logging.error("rclone configuration error during batch copy. Local files will be kept for next run.")
                return []

        # 에러가 발생한 파일 경로 수집 (중복 또는 기타 에러)
        error_paths: list[str] = []
        for line in stderr_text.splitlines():
            if "ERROR" in line:
                m = re.search(r"ERROR\s*:\s*(.+?\.pdf):", line, re.I)
                if m: error_paths.append(m.group(1).strip())
                elif "Failed to copy" in line or "nameAlreadyExists" in line:
                    m = re.search(r"(?:Failed to copy|nameAlreadyExists).*?:\s+(.+?\.pdf)", line, re.I)
                    if m: error_paths.append(m.group(1).strip())
                if proc.returncode != 0 and not any(k in line for k in ("nameAlreadyExists", "Couldn't delete", "no such file or directory")):
                    logging.error("rclone: %s", line)

        unique_errors = list(dict.fromkeys(error_paths))

        if self._rclone_is_auth_error(stderr_text):
            logging.error("rclone auth failed during batch copy. Skipping verification/repair; local files will be kept for next run.")
            return []

        verified: set[str] = set()

        if proc.returncode == 0:
            for p in self.success_downloads: verified.add(str(p["report_id"]))
            logging.info("Upload OK (%d files).", total)
        else:
            logging.warning("rclone reported errors (code=%d). Starting verification...", proc.returncode)

        if (proc.returncode != 0) or unique_errors:
            candidates = list(set(unique_errors + [rel for rel in local_map.keys() if str(local_map[rel]["report_id"]) not in verified]))
            if candidates:
                dir_groups: dict[str, list[str]] = {}
                for rel_path in candidates:
                    d = os.path.dirname(rel_path)
                    dir_groups.setdefault(d, []).append(os.path.basename(rel_path))
                logging.info("Verifying %d files in %d dirs...", len(candidates), len(dir_groups))
                for sub_dir, filenames in dir_groups.items():
                    remote_dir = f"{Config.RCLONE_REMOTE}/{sub_dir}"
                    remote_files = await self._rclone_lsl_dir(remote_dir)
                    for fname in filenames:
                        rel_path = f"{sub_dir}/{fname}" if sub_dir else fname
                        payload = local_map.get(rel_path)
                        if not payload: continue
                        exact_remote_name = self._find_remote_filename(fname, remote_files)
                        if exact_remote_name is None: continue
                        rs = remote_files[exact_remote_name]
                        ls = payload.get("size", 0)
                        if rs and rs > 0 and rs == ls:
                            logging.info("Verification match (size=%d): %s", ls, rel_path)
                            verified.add(str(payload["report_id"]))
                        elif rs is not None and rs != ls:
                            remote_full = f"{remote_dir}/{exact_remote_name}"
                            lf = os.path.join(Config.LOCAL_BUFFER_DIR, rel_path)
                            if not os.path.exists(lf):
                                logging.warning("Local file missing for %s, skipping retry", rel_path)
                                continue
                            max_retries = 5
                            retry_delays = [3, 6, 12, 24, 48]
                            success = False
                            last_error = ""
                            auth_failed = False
                            for attempt in range(1, max_retries + 1):
                                logging.warning("Size mismatch (remote=%s local=%s): %s. Retry %d/%d: delete remote and re-upload...", rs, ls, rel_path, attempt, max_retries)
                                delete_ok = False
                                for del_attempt in range(3):
                                    ok, err = await self._rclone_delete_remote(remote_full, remote_dir=remote_dir, filename=exact_remote_name)
                                    if self._rclone_is_auth_error(err):
                                        auth_failed = True
                                        last_error = f"delete auth failed: {err[:200]}"
                                        logging.error("rclone auth failed while deleting %s. Stop retrying this file; local file will be kept.", rel_path)
                                        break
                                    if ok:
                                        await asyncio.sleep(2)
                                        after_size, stat_err = await self._rclone_stat_remote(remote_full)
                                        if self._rclone_is_auth_error(stat_err):
                                            auth_failed = True
                                            last_error = f"stat after delete auth failed: {stat_err[:200]}"
                                            logging.error("rclone auth failed while verifying delete for %s. Stop retrying this file; local file will be kept.", rel_path)
                                            break
                                        if after_size is None:
                                            delete_ok = True
                                            break
                                        logging.warning("Delete returned ok but file still exists (size=%s) for %s, stderr=%s", after_size, rel_path, err)
                                    else:
                                        after_size, stat_err = await self._rclone_stat_remote(remote_full)
                                        if self._rclone_is_auth_error(stat_err):
                                            auth_failed = True
                                            last_error = f"stat after failed delete auth failed: {stat_err[:200]}"
                                            logging.error("rclone auth failed while checking failed delete for %s. Stop retrying this file; local file will be kept.", rel_path)
                                            break
                                        if after_size is None and not stat_err:
                                            delete_ok = True
                                            logging.info("Remote already absent after failed delete command: %s", rel_path)
                                            break
                                        logging.warning("Delete attempt %d/3 failed for %s: %s", del_attempt + 1, rel_path, err or "(no stderr)")
                                    await asyncio.sleep(2 ** del_attempt)
                                if auth_failed: break
                                if not delete_ok:
                                    last_error = "delete failed after 3 attempts (file still on remote)"
                                    logging.warning("Delete ultimately failed for %s. Skip copyto until the remote file is actually gone.", rel_path)
                                    await asyncio.sleep(retry_delays[attempt - 1] if attempt <= len(retry_delays) else 60)
                                    continue
                                await asyncio.sleep(retry_delays[attempt - 1] if attempt <= len(retry_delays) else 60)
                                rp = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "copyto", lf, remote_full, "--onedrive-chunk-size", Config.ONEDRIVE_CHUNK_SIZE, "--retries", str(max(Config.RCLONE_RETRIES, 5)), "--low-level-retries", str(Config.RCLONE_LOW_LEVEL_RETRIES), "--onedrive-no-versions", env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
                                _, rp_stderr = await rp.communicate()
                                rp_stderr_text = rp_stderr.decode("utf-8", errors="replace")
                                upload_ok = rp.returncode == 0
                                if rp.returncode != 0 or "nameAlreadyExists" in rp_stderr_text or "ERROR" in rp_stderr_text:
                                    upload_ok = False
                                    last_error = f"copyto: {rp_stderr_text.strip()[:200]}"
                                    logging.warning("Retry %d copyto problem for %s (rc=%d): %s", attempt, rel_path, rp.returncode, last_error)
                                    if self._rclone_is_auth_error(rp_stderr_text):
                                        auth_failed = True
                                        logging.error("rclone auth failed while re-uploading %s. Stop retrying this file; local file will be kept.", rel_path)
                                        break
                                if not upload_ok: continue
                                await asyncio.sleep(1)
                                uploaded_size, stat_err = await self._rclone_stat_remote(remote_full)
                                if self._rclone_is_auth_error(stat_err):
                                    auth_failed = True
                                    last_error = f"stat after upload auth failed: {stat_err[:200]}"
                                    logging.error("rclone auth failed while verifying upload for %s. Stop retrying this file; local file will be kept.", rel_path)
                                    break
                                if uploaded_size == ls:
                                    logging.info("Retry %d successful (size=%d): %s", attempt, ls, rel_path)
                                    verified.add(str(payload["report_id"]))
                                    success = True
                                    break
                                elif uploaded_size is not None:
                                    rs = uploaded_size
                                    last_error = f"size mismatch (remote={uploaded_size} local={ls})"
                                    logging.warning("Retry %d %s for %s", attempt, last_error, rel_path)
                                else:
                                    last_error = "remote file not found after upload"
                                    logging.warning("Retry %d remote empty for %s", attempt, rel_path)
                            if not success:
                                logging.error("Giving up on %s after %d retries. Last error: %s", rel_path, attempt if auth_failed else max_retries, last_error)

        deleted = 0
        kept = 0
        for p in self.success_downloads:
            rid = str(p["report_id"])
            lp = p["path"]
            if rid in verified:
                try:
                    if lp.exists(): os.remove(str(lp))
                    deleted += 1
                except OSError as e: logging.warning("rm fail: %s: %s", lp, e)
            else: kept += 1

        for root, dirs, files in os.walk(Config.LOCAL_BUFFER_DIR, topdown=False):
            for d in dirs:
                try: os.rmdir(os.path.join(root, d))
                except OSError: pass

        result = [p for p in self.success_downloads if str(p["report_id"]) in verified]
        logging.info("Upload: %d ok, %d kept. (%d dir lsl calls)", len(result), kept, len(dir_groups) if unique_errors else 0)
        return result

    @staticmethod
    def _target_firm_counts(targets) -> dict[str, int]:
        firm_counts: dict[str, int] = {}
        for target in targets:
            firm = target.get("firm_nm") or "UNKNOWN"
            firm_counts[firm] = firm_counts.get(firm, 0) + 1
        return firm_counts

    def _build_target_query(self, excluded: str) -> str:
        """
        Fetch a small, firm-diversified batch.

        Ordering by firm_rank first gives a round-robin shape:
        each eligible firm contributes its first candidate before any firm contributes
        its second candidate.
        """
        return f"""
            WITH base AS (
                SELECT report_id, sec_firm_order, report_unique_key, pdf_url, telegram_url, download_url, firm_nm, article_title, report_date,
                       {Config.PDF_STATUS_COL} as status,
                       retry_count,
                       CASE
                           WHEN NULLIF(BTRIM(pdf_url), '') IS NOT NULL
                             OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
                             OR NULLIF(BTRIM(download_url), '') IS NOT NULL
                             OR NULLIF(BTRIM(report_unique_key), '') IS NOT NULL
                           THEN 1 ELSE 0
                       END AS has_source_url,
                       COALESCE(ENCODE({Config.PDF_HASH_COL}, 'hex'), NULLIF(BTRIM(pdf_url), ''), report_id::TEXT) AS pdf_key
                FROM {Config.SOURCE_TABLE}
                WHERE {Config.PDF_STATUS_COL} IN (0, 3)
                  AND (
                      COALESCE(retry_count, 0) < 5
                      OR (
                          {Config.PDF_STATUS_COL} = 3
                          AND COALESCE(retry_count, 0) < {Config.FETCH_RETRY_LIMIT}
                          AND (
                              NULLIF(BTRIM(pdf_url), '') IS NOT NULL
                              OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
                              OR NULLIF(BTRIM(download_url), '') IS NOT NULL
                              OR NULLIF(BTRIM(report_unique_key), '') IS NOT NULL
                          )
                      )
                  )
                  AND firm_nm NOT IN ({excluded})
                  AND report_id IS NOT NULL
            ),
            distinct_targets AS (
                SELECT DISTINCT ON (pdf_key) * FROM base
                ORDER BY pdf_key, report_id ASC
            ),
            ranked_targets AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY firm_nm
                           ORDER BY
                               (CASE WHEN status = 0 THEN 0 ELSE 1 END),
                               COALESCE(retry_count, 0) ASC,
                               has_source_url DESC,
                               report_date DESC,
                               report_id DESC
                       ) AS firm_rank
                FROM distinct_targets
            )
            SELECT report_id as row_id, report_id, sec_firm_order, report_unique_key, pdf_url, telegram_url, download_url, firm_nm, article_title, report_date
            FROM ranked_targets
            ORDER BY firm_rank, report_date DESC, firm_nm, report_id DESC
            LIMIT {Config.BATCH_SIZE}
        """

    async def _fetch_targets(self, conn):
        excluded = ', '.join(f"'{f}'" for f in Config.EXCLUDED_FIRMS)
        return await conn.fetch(self._build_target_query(excluded))

    def _print_target_summary(self, targets) -> None:
        self.total_targets = len(targets)
        print(f"DEBUG: 조회된 대상 수 = {self.total_targets}개")
        if targets:
            print(f"DEBUG: 대상 증권사 분포 = {self._target_firm_counts(targets)}")

    async def _mark_download_failures(self, conn, failed_targets) -> None:
        for target in failed_targets:
            payload = _row_payload(target)
            logging.warning(
                "DOWNLOAD FAILED report_id=%s firm=%s title=%s pdf_url=%s",
                payload["report_id"], payload["firm_nm"],
                _truncate(payload["title"], 60),
                _truncate(payload["pdf_url"], 120),
            )
            await self._update_source_workflow(conn, payload, 3, retry_delta=1)

    async def _apply_upload_results(self, conn, uploaded_payloads) -> None:
        if uploaded_payloads:
            print(f"DEBUG: 업로드 성공 ({len(uploaded_payloads)}건). DB 메타데이터 업데이트 중...")
            for payload in uploaded_payloads:
                local_path = payload["path"]
                relative_path = local_path.relative_to(self.local_dir)
                storage_key = str(relative_path)

                await self._apply_workflow_update(
                    conn, payload, 2, retry_delta=0, file_path=str(local_path),
                    file_size=payload["size"], page_count=payload["pages"],
                    archive_status="ARCHIVED", download_status_yn="Y",
                    storage_key=storage_key
                )

        uploaded_ids = {str(p["report_id"]) for p in (uploaded_payloads or [])}
        failed_payloads = [p for p in self.success_downloads if str(p["report_id"]) not in uploaded_ids]
        if failed_payloads:
            print(f"DEBUG: {len(failed_payloads)}건 업로드 실패. 상태 롤백 중...")
            for payload in failed_payloads:
                await self._update_source_workflow(conn, payload, 3, retry_delta=0)

    async def run(self):
        print("DEBUG: [1/4] DB 연결 준비 중...")
        # (WARP 체크는 나중에 LS증권 대상이 있을 때만 수행하도록 위치 변경됨)
        
        print("DEBUG: [2/4] DB 연결 중...")
        conn = await DBManager.get_conn()
        
        try:
            print("DEBUG: [3/4] 스키마 확인 및 대상 쿼리 실행 중...")
            if Config.FETCH_ONLY:
                print("DEBUG: fetch-only 모드: 스키마 변경, 다운로드, 업로드를 건너뜁니다.")
            else:
                await ensure_pdf_sync_status_schema(conn)

            targets = await self._fetch_targets(conn)
            self._print_target_summary(targets)

            if not targets:
                # 0개인 원인 분석을 위해 전체 대기 건수 확인
                wait_count = await conn.fetchval(f"SELECT COUNT(*) FROM {Config.SOURCE_TABLE} WHERE {Config.PDF_STATUS_COL} IN (0, 3)")
                print(f"DEBUG: DB내 총 대기 레코드(필터 전) = {wait_count}개")
                logging.info("No pending targets.")
                return

            if Config.FETCH_ONLY:
                for idx, target in enumerate(targets, 1):
                    print(
                        "DEBUG: fetch-only target "
                        f"{idx:02d}: {target.get('firm_nm')} report_id={target.get('report_id')} "
                        f"report_date={target.get('report_date')} title={_truncate(target.get('article_title'), 80)}"
                    )
                return

            # LS증권이 포함된 경우에만 WARP 프록시 체크
            if any("LS" in (target.get("firm_nm") or "") for target in targets):
                print(f"DEBUG: LS증권이 포함되어 WARP 체크 중...")
                await check_and_restart_warp()
            else:
                print("DEBUG: LS증권이 없어 WARP 체크를 건너뜁니다.")

            print(f"DEBUG: [4/4] 다운로드 시작 ({self.total_targets}건)...")
            results = await asyncio.gather(*[self.download_task(t) for t in targets])
            failed_targets = [target for target, ok in zip(targets, results) if not ok]
            
            success_count = len(self.success_downloads)
            print(f"DEBUG: 다운로드 완료 (성공: {success_count}건, 실패: {len(failed_targets)}건)")

            if failed_targets:
                await self._mark_download_failures(conn, failed_targets)

            if self.success_downloads:
                print(f"DEBUG: OneDrive 업로드 시작 ({success_count}건)...")
                uploaded_payloads = await self.upload_to_onedrive()
                await self._apply_upload_results(conn, uploaded_payloads)
            
            print("DEBUG: 모든 작업 완료.")

        except Exception as e:
            print(f"DEBUG: 실행 중 에러 발생: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await DBManager.close()

def _get_process_elapsed(pid: int) -> float | None:
    """Return elapsed seconds since process started, or None if undetermined."""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            stat = f.read()
        # find end of comm field (enclosed in parens, may contain spaces)
        comm_end = stat.rfind(")")
        fields = stat[comm_end + 2:].split()
        starttime_ticks = int(fields[19])               # field 19 = starttime
        with open("/proc/uptime", "r") as f:
            uptime_sec = float(f.read().split()[0])
        clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return uptime_sec - (starttime_ticks / clk_tck)
    except Exception:
        return None


def _acquire_lock(lock_path: str, timeout: int) -> tuple[bool, object | None]:
    """Try to acquire a fcntl advisory lock.

    Returns (True, file_handle) on success.
    If the lock is held by a zombie (elapsed > *timeout* seconds), kill it
    and re-acquire.  Otherwise returns (False, None).
    """
    # attempt without truncating so we can read the old PID on failure
    try:
        lock_f = open(lock_path, "r+")
    except FileNotFoundError:
        lock_f = open(lock_path, "w+")

    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # success — stamp our PID
        lock_f.seek(0)
        lock_f.truncate()
        lock_f.write(str(os.getpid()))
        lock_f.flush()
        return True, lock_f
    except (IOError, OSError):
        # lock held by another process
        lock_f.seek(0)
        old_pid_str = lock_f.read().strip()
        lock_f.close()

        if not old_pid_str:
            return False, None

        try:
            old_pid = int(old_pid_str)
        except ValueError:
            return False, None

        elapsed = _get_process_elapsed(old_pid)
        if elapsed is None or elapsed <= timeout:
            if elapsed is not None:
                print(f"DEBUG: 다른 인스턴스 실행 중 (PID={old_pid}, 경과={elapsed:.0f}초 < {timeout}초). 종료합니다.")
            return False, None

        # zombie detected — kill and retry
        print(f"DEBUG: 좀비 프로세스 발견 (PID={old_pid}, 실행시간={elapsed:.0f}초 > {timeout}초). SIGKILL 전송.")
        try:
            os.kill(old_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        time.sleep(1)

        # retry lock acquisition
        lock_f = open(lock_path, "w")
        try:
            fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_f.write(str(os.getpid()))
            lock_f.flush()
            print(f"DEBUG: 좀비 종료 후 락 획득 성공 (PID={os.getpid()}).")
            return True, lock_f
        except (IOError, OSError):
            lock_f.close()
            print(f"DEBUG: 좀비 종료했으나 락 획득 실패. 종료합니다.")
            return False, None


if __name__ == "__main__":
    if "--fetch-only" in sys.argv:
        Config.FETCH_ONLY = True

    ok, lock_f = _acquire_lock(Config.LOCK_FILE, Config.ZOMBIE_TIMEOUT_SECONDS)
    if not ok:
        sys.exit(0)
    try:
        asyncio.run(PDFArchiver().run())
    finally:
        try:
            lock_f.close()
        except Exception:
            pass
