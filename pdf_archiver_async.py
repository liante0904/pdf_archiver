# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiohttp",
# ]
# ///

import asyncio
import sqlite3
import os
import time
import logging
import subprocess
import fcntl
import sys
import re
import shutil
import unicodedata
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

# --- 설정 ---
DB_PATH = os.path.expanduser("~/sqlite3/telegram.db")
LOCAL_BUFFER_DIR = os.path.expanduser("~/downloads/pdf_archive_temp")
RCLONE_BIN = shutil.which("rclone") or os.path.expanduser("~/.local/bin/rclone")
RCLONE_REMOTE = "onedrive:/archive/pdf"
LOCK_FILE = os.path.expanduser("~/prod/pdf_archiver/pdf_archiver_async.lock")

BATCH_SIZE = 200
DOWNLOAD_CONCURRENCY = 10
RCLONE_TRANSFERS = 5

# 다운로드 실패가 많은 증권사 제외 (쿠키/인증 필요)
EXCLUDED_FIRMS = ('DB금융투자', '현대차증권', '유진투자증권', '상상인증권')

LOG_FILE = os.path.expanduser("~/log/pdf_archiver_async.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)


def safe_encode_url(url):
    """URL 경로/쿼리를 안전하게 인코딩. 이중/삼중 인코딩 방지."""
    try:
        from urllib.parse import unquote
        current = url
        prev = None
        while prev != current:
            prev = current
            current = unquote(current)
        parts = urlparse(current)
        return urlunparse((
            parts.scheme, parts.netloc,
            quote(parts.path, safe='/:@'),
            parts.params,
            quote(parts.query, safe='&='),
            parts.fragment
        ))
    except Exception:
        return url


def build_candidate_urls(firm, urls):
    """증권사별 URL 후보 목록 생성 (대체 경로, 인코딩 변형 포함)."""
    candidates = list(urls)

    if firm == "IBK투자증권" and urls:
        base = urls[0]
        for path in ("invrespect", "invreport", "indreport", "comment"):
            alt = re.sub(r'(tradeinfo/)[^/]+(/)', rf'\1{path}\2', base)
            if alt not in candidates:
                candidates.append(alt)
        # JSP 패턴 (2026년 이후 404 대응)
        try:
            fname = os.path.basename(urlparse(base).path)
            if fname.endswith('.pdf'):
                jsp = "https://www.ibks.com/company/common/download.jsp?filepath=/files/tradeinfo/{cat}&filename={fname}"
                for path in ("invrespect", "invreport", "indreport", "comment"):
                    u = jsp.format(cat=path, fname=fname)
                    if u not in candidates:
                        candidates.append(u)
        except Exception:
            pass

    if firm == "유안타증권":
        for u in list(urls):
            if "ATTACH_FILE=" in u:
                seq = u.split("ATTACH_FILE=")[-1]
                alt = f"http://file.myasset.com/sitemanager/upload/{seq}"
            elif "upload/" in u:
                seq = u.split("upload/")[-1]
                alt = f"https://www.myasset.com/myasset/common/commonFile/downloadFromFileServer.cmd?ATTACH_FILE={seq}"
            else:
                continue
            if alt not in candidates:
                candidates.append(alt)

    # 한글 URL은 UTF-8 / EUC-KR 인코딩 후보 추가
    final = []
    seen = set()
    for u in candidates:
        encoded = safe_encode_url(u)
        for variant in (u, encoded):
            if variant not in seen:
                seen.add(variant)
                final.append(variant)
        if any(ord(c) > 127 for c in u):
            try:
                parts = urlparse(u)
                euckr = urlunparse((parts.scheme, parts.netloc,
                                    quote(parts.path.encode('euc-kr'), safe='/:@'),
                                    parts.params,
                                    quote(parts.query.encode('euc-kr'), safe='&='),
                                    parts.fragment))
                if euckr not in seen:
                    seen.add(euckr)
                    final.append(euckr)
            except Exception:
                pass
    return final


async def check_and_restart_warp():
    """WARP 프록시 상태 확인, 다운 시 docker 재시작 (10분 이내 재시작 방지)."""
    lock_file = "/tmp/warp_restart.lock"
    if os.path.exists(lock_file) and (time.time() - os.path.getmtime(lock_file)) < 600:
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            "curl", "-o", "/dev/null", "-s", "-w", "%{http_code}",
            "--connect-timeout", "10", "--socks5-hostname", "localhost:9091",
            "https://www.ls-sec.co.kr",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await proc.communicate()
        if stdout.decode().strip() == "200":
            return
        # 이중 확인: 구글
        proc2 = await asyncio.create_subprocess_exec(
            "curl", "-o", "/dev/null", "-s", "-w", "%{http_code}",
            "--connect-timeout", "10", "--socks5-hostname", "localhost:9091",
            "https://www.google.com",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        stdout2, _ = await proc2.communicate()
        if stdout2.decode().strip() != "200":
            logging.warning("WARP proxy down. Restarting docker container...")
            with open(lock_file, "w") as f:
                f.write(str(time.time()))
            await (await asyncio.create_subprocess_shell(
                "docker restart cloudflare-warp && sleep 15"
            )).wait()
        else:
            logging.warning("LS-Sec unreachable, but proxy is OK. Skipping WARP restart.")
    except Exception as e:
        logging.error(f"WARP check error: {e}")


async def get_pdf_page_count(file_path):
    try:
        proc = await asyncio.create_subprocess_shell(
            f"grep -a /Count {file_path} | head -n 10 | grep -oE '/Count [0-9]+' | head -n 1 | grep -oE '[0-9]+'",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await proc.communicate()
        return int(stdout.decode().strip()) if stdout.strip() else 0
    except Exception:
        return 0


class PDFArchiver:
    def __init__(self):
        self.local_dir = Path(LOCAL_BUFFER_DIR)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
        self.success_downloads = []

    def _make_file_path(self, firm, title, reg_dt, report_id):
        clean_dt = re.sub(r'[^0-9]', '', str(reg_dt)) if reg_dt else "00000000"
        y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
        yy_mm_dd = clean_dt[2:8]
        normalized = unicodedata.normalize('NFC', title or '')
        safe_title = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]\(\)]', ' ', normalized)
        safe_title = '_'.join(safe_title.split())[:60].strip('_') or 'untitled'
        filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
        return self.local_dir / y_m / firm / filename

    async def download_task(self, row):
        row = [str(v).replace('\x00', '') if v is not None else '' for v in row]
        row_id, report_id, tel_url, dw_url, att_url, firm, title, reg_dt = row

        raw_urls = [u for u in (tel_url, dw_url, att_url) if u.startswith('http')]
        candidates = build_candidate_urls(firm, raw_urls)
        use_proxy = any(k in firm for k in ("LS", "이베스트"))

        target_path = self._make_file_path(firm, title, reg_dt, report_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix('.tmp')

        async with self.semaphore:
            for url in candidates:
                cmd = [
                    "curl", "-L", "-s", "-w", "%{http_code}",
                    "--connect-timeout", "15", "--max-time", "60",
                    "-o", str(tmp_path), url,
                    "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                ]
                if use_proxy:
                    cmd += ["--socks5-hostname", "localhost:9091"]
                try:
                    proc = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL
                    )
                    stdout, _ = await proc.communicate()
                    http_code = stdout.decode().strip()

                    if http_code == "403":
                        logging.warning(f"403 Forbidden [{firm}]: {url}")

                    if proc.returncode == 0 and tmp_path.exists() and tmp_path.stat().st_size > 1024:
                        with open(tmp_path, 'rb') as f:
                            header = f.read(4)
                        if header == b'%PDF':
                            if target_path.exists():
                                target_path.unlink()
                            tmp_path.rename(target_path)
                            pages = await get_pdf_page_count(target_path)
                            self.success_downloads.append(
                                (row_id, report_id, firm, title, target_path,
                                 target_path.stat().st_size, pages, reg_dt)
                            )
                            return True
                        else:
                            logging.debug(f"Not a PDF (HTTP {http_code}) [{firm}]: {url}")
                    elif http_code not in ("000", ""):
                        logging.debug(f"HTTP {http_code} [{firm}]: {url}")
                except Exception as e:
                    logging.debug(f"Exception [{firm}]: {e}")
                finally:
                    if tmp_path.exists():
                        tmp_path.unlink(missing_ok=True)
        return False

    async def run(self):
        if shutil.disk_usage("/").free < 2 * 1024 ** 3:
            logging.warning("Disk space < 2GB. Skipping.")
            return

        await check_and_restart_warp()

        excluded = ', '.join(f"'{f}'" for f in EXCLUDED_FIRMS)
        with sqlite3.connect(DB_PATH) as conn:
            targets = conn.execute(f"""
                SELECT id, report_id, TELEGRAM_URL, DOWNLOAD_URL, ATTACH_URL,
                       FIRM_NM, ARTICLE_TITLE, REG_DT
                FROM data_main_daily_send
                WHERE sync_status IN (0, 3)
                  AND retry_count < 5
                  AND FIRM_NM NOT IN ({excluded})
                  AND report_id IS NOT NULL
                  AND report_id NOT IN (SELECT report_id FROM pdf_archive_metadata)
                  AND (TELEGRAM_URL LIKE 'http%' OR DOWNLOAD_URL LIKE 'http%' OR ATTACH_URL LIKE 'http%')
                ORDER BY (CASE WHEN sync_status = 3 THEN 0 ELSE 1 END), REG_DT DESC
                LIMIT {BATCH_SIZE}
            """).fetchall()

        if not targets:
            logging.info("No pending targets.")
            return

        logging.info(f"Batch start: {len(targets)} targets")
        await asyncio.gather(*[self.download_task(t) for t in targets])

        if not self.success_downloads:
            failed_ids = [t[0] for t in targets]
            with sqlite3.connect(DB_PATH) as conn:
                conn.executemany(
                    "UPDATE data_main_daily_send SET sync_status=3, retry_count=retry_count+1 WHERE id=?",
                    [(fid,) for fid in failed_ids]
                )
            logging.info("Batch done: 0 downloaded.")
            return

        logging.info(f"Downloaded {len(self.success_downloads)} files. Updating DB...")
        with sqlite3.connect(DB_PATH) as conn:
            for row_id, r_id, firm, title, path, size, pages, reg_dt in self.success_downloads:
                conn.execute(
                    "UPDATE data_main_daily_send SET sync_status=1 WHERE id=?",
                    (row_id,)
                )
                conn.execute(
                    "INSERT OR REPLACE INTO pdf_archive_metadata "
                    "(report_id, firm_nm, title, file_path, file_size, page_count, reg_dt) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (r_id, firm, title, str(path), size, pages, reg_dt)
                )

        logging.info("Uploading via rclone move...")
        rclone_cmd = [
            RCLONE_BIN, "move", LOCAL_BUFFER_DIR, RCLONE_REMOTE,
            "--transfers", str(RCLONE_TRANSFERS),
            "--retries", "3",
            "--delete-empty-src-dirs",
            "--fast-list",
        ]
        proc = await asyncio.create_subprocess_exec(*rclone_cmd)
        await proc.wait()

        success_count = 0
        with sqlite3.connect(DB_PATH) as conn:
            for row_id, _, _, _, path, _, _, _ in self.success_downloads:
                if not path.exists():
                    conn.execute(
                        "UPDATE data_main_daily_send SET sync_status=2 WHERE id=?",
                        (row_id,)
                    )
                    success_count += 1
                else:
                    # rclone 실패 - 다음 배치에서 재시도
                    conn.execute(
                        "UPDATE data_main_daily_send SET sync_status=0, retry_count=retry_count+1 WHERE id=?",
                        (row_id,)
                    )

        logging.info(f"Batch done: {success_count}/{len(self.success_downloads)} archived.")


if __name__ == "__main__":
    lock_f = open(LOCK_FILE, 'w')
    try:
        fcntl.lockf(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        asyncio.run(PDFArchiver().run())
    except (IOError, OSError):
        sys.exit(0)
    finally:
        try:
            lock_f.close()
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        except Exception:
            pass
