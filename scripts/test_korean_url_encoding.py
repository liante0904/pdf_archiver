#!/usr/bin/env python3
"""
한글 URL 인코딩 문제 진단 및 재처리 테스트 스크립트
───────────────────────────────────────────────────
문제: pdf_archiver_async.py 의 safe_encode_url() 이 UTF-8 percent-encoding 만 사용함.
      iprovest.com(교보증권) 등 일부 서버는 EUC-KR percent-encoding 을 기대함.
      → 현재 UTF-8 인코딩된 URL과 raw 한글 URL 모두 다운로드 실패.

해결: URL path 부분의 한글을 EUC-KR percent-encoding 으로도 시도하여 다운로드.

사용법:
  1. dry-run (다운로드 없이 URL 변환만 확인):
     python scripts/test_korean_url_encoding.py --dry-run --limit 5

  2. 실제 다운로드 테스트 (기본 5건):
     python scripts/test_korean_url_encoding.py --limit 5

  3. 전체 교보증권 대상 처리:
     python scripts/test_korean_url_encoding.py --firm 교보증권
"""

import asyncio
import hashlib
import os
import re
import sys
import json
import logging
import unicodedata
from pathlib import Path
from urllib.parse import quote, unquote, urlparse, urlunparse

import asyncpg
import aiohttp

# 프로젝트 루트 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from secret_env import load_workspace_secret_env_defaults

load_workspace_secret_env_defaults()

# --- Config ---
LOCAL_DIR = Path(os.getenv("LOCAL_BUFFER_DIR", os.path.expanduser("~/downloads/pdf_archive_temp")))
LOG_FILE = os.path.expanduser("~/logs/test_korean_url_encoding.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# --- DB ---
async def get_conn():
    return await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        password=os.getenv("POSTGRES_PASSWORD"),
        timeout=30,
    )


# --- URL 인코딩 유틸리티 ---

def has_korean(text: str) -> bool:
    """문자열에 한글(가-힣)이 포함되어 있는지"""
    if not text:
        return False
    return bool(re.search(r'[가-힣]', text))


def encode_url_euc_kr(url: str) -> str:
    """
    URL 의 path 부분을 EUC-KR percent-encoding 으로 변환.
    이미 percent-encoded 된 URL 도 처리 가능.
    """
    try:
        # 완전히 decode
        current = url
        prev = None
        while prev != current:
            prev = current
            current = unquote(current)

        parts = urlparse(current)

        # path 를 EUC-KR 로 인코딩
        # quote() 는 safe 에 지정된 문자 외에는 모두 percent-encode
        euc_kr_path = quote(parts.path, safe='/:@', encoding='euc-kr')

        # query 도 EUC-KR 로 (파라미터 값에 한글이 있을 수 있음)
        euc_kr_query = quote(parts.query, safe='&=', encoding='euc-kr') if parts.query else ''

        return urlunparse((
            parts.scheme, parts.netloc,
            euc_kr_path,
            parts.params,
            euc_kr_query,
            parts.fragment,
        ))
    except Exception as e:
        log.warning(f"  EUC-KR encoding failed: {e}")
        return url


def encode_url_utf8(url: str) -> str:
    """
    URL 의 path 부분을 UTF-8 percent-encoding 으로 변환.
    (기존 safe_encode_url 과 동일한 로직)
    """
    try:
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
            parts.fragment,
        ))
    except Exception:
        return url


def generate_candidate_urls(url: str) -> list[tuple[str, str]]:
    """
    주어진 URL 로부터 다운로드 후보 목록을 생성.
    반환: [(url, encoding_label), ...]
      - encoding_label: 'raw', 'utf-8', 'euc-kr'
    """
    candidates = []
    seen = set()

    # 1. 원본 URL
    if url and url not in seen:
        seen.add(url)
        candidates.append((url, "raw"))

    # 2. UTF-8 인코딩
    utf8_url = encode_url_utf8(url)
    if utf8_url and utf8_url not in seen:
        seen.add(utf8_url)
        candidates.append((utf8_url, "utf-8"))

    # 3. EUC-KR 인코딩 (한글이 있을 때만)
    if has_korean(url):
        euc_url = encode_url_euc_kr(url)
        if euc_url and euc_url not in seen:
            seen.add(euc_url)
            candidates.append((euc_url, "euc-kr"))

    return candidates


# --- 다운로드 ---

def _is_pdf(data: bytes) -> bool:
    """pdf_archiver_async 와 동일한 시그니처 검출 방식 (상위 2048바이트 스캔)"""
    if not data:
        return False
    head = data[:2048]
    for marker in (b"%PDF", b"\xef\xbb\xbf%PDF"):
        if head.find(marker) != -1:
            return True
    stripped = head.lstrip(b"\x00\t\r\n\x0c\x20")
    if stripped.startswith(b"%PDF"):
        return True
    # Fasoo DRM 등으로 암호화된 PDF도 유효한 다운로드로 간주 (200KB 이상)
    if len(data) > 200 * 1024:
        return True
    return False


def _safe_filename(title: str, max_len: int = 60) -> str:
    norm = unicodedata.normalize("NFC", title or "")
    safe = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]()]', ' ', norm)
    safe = "_".join(safe.split())[:max_len].strip("_") or "untitled"
    return safe


def _make_path(firm: str, title: str, reg_dt: str, report_id: int) -> Path:
    cd = re.sub(r'[^0-9]', '', str(reg_dt)) or "00000000"
    ym = f"{cd[:4]}-{cd[4:6]}"
    fn = f"{cd[2:8]}_{_safe_filename(title)}_{report_id}.pdf"
    return LOCAL_DIR / ym / firm / fn


async def try_download(url: str, encoding_label: str = "raw") -> tuple[bool, int, str]:
    """
    aiohttp 로 URL 다운로드 시도.
    반환: (성공여부, byte_size, 비고)
    """
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/pdf,application/octet-stream,*/*",
    }
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                body = await resp.read()
                content_type = resp.headers.get("content-type", "")
                size = len(body)

                if _is_pdf(body) and size > 1024:
                    return True, size, f"PDF ok ({encoding_label})"
                elif "text/html" in content_type.lower():
                    # HTML 에러 페이지 - 내용 미리보기
                    try:
                        snippet = body.decode("euc-kr", errors="replace")[:120]
                    except:
                        snippet = body.decode("utf-8", errors="ignore")[:120]
                    return False, size, f"HTML error page ({encoding_label}): {snippet}"
                else:
                    return False, size, f"Not PDF ({encoding_label}): content-type={content_type}, size={size}"
    except Exception as e:
        return False, 0, f"Exception ({encoding_label}): {type(e).__name__}: {e!r}"


async def process_record(conn, rec: dict, dry_run: bool = False) -> dict | None:
    """
    단일 레코드 처리: 모든 URL 필드에서 후보 URL 생성 → 다운로드 시도.
    """
    rid = rec["report_id"]
    firm = rec["firm_nm"]
    title = str(rec.get("article_title") or "")
    reg_dt = str(rec.get("reg_dt") or "")

    target_path = _make_path(firm, title, reg_dt, rid)

    # 이미 다운로드 성공했으면 skip
    if target_path.exists() and target_path.stat().st_size > 1024 and _is_pdf(target_path.read_bytes()):
        log.info(f"  report_id={rid} [{firm}] SKIP (already downloaded): {title[:40]}...")
        return {"report_id": rid, "status": "skipped", "reason": "already exists"}

    # 모든 URL 필드에서 후보 수집
    all_candidates: list[tuple[str, str]] = []
    seen_urls = set()
    for field in ("pdf_url", "report_unique_key", "telegram_url", "download_url"):
        url = str(rec.get(field) or "").strip()
        if url and url.startswith("http"):
            candidates = generate_candidate_urls(url)
            for cand_url, enc_label in candidates:
                if cand_url not in seen_urls:
                    seen_urls.add(cand_url)
                    all_candidates.append((cand_url, enc_label, field))

    if not all_candidates:
        return {"report_id": rid, "status": "no_candidates", "reason": "no valid URLs"}

    if dry_run:
        log.info(f"  report_id={rid} [{firm}] DRY-RUN: {title[:40]}...")
        for cand_url, enc_label, field in all_candidates[:5]:
            log.info(f"    [{enc_label:5s}] ({field}) {cand_url[:150]}")
        return {"report_id": rid, "status": "dry_run", "candidates": len(all_candidates)}

    # 다운로드 시도
    log.info(f"  report_id={rid} [{firm}] {title[:50]}... ({len(all_candidates)} candidates)")

    success = False
    final_size = 0
    final_enc = ""

    for cand_url, enc_label, field in all_candidates:
        ok, size, note = await try_download(cand_url, enc_label)
        if ok:
            success = True
            final_size = size
            final_enc = enc_label
            log.info(f"    ✅ SUCCESS [{enc_label}] ({field}) size={size:,} url={cand_url[:120]}")
            break
        else:
            log.debug(f"    ❌ FAIL [{enc_label}] ({field}) {note[:100]}")

    if success:
        return {
            "report_id": rid,
            "status": "success",
            "encoding": final_enc,
            "size": final_size,
            "url_hint": cand_url[:200] if 'cand_url' in dir() else "",
        }
    else:
        return {
            "report_id": rid,
            "status": "failed",
            "reason": "all candidates failed",
            "candidates_tried": len(all_candidates),
        }


# --- 메인 ---

async def main():
    import argparse

    parser = argparse.ArgumentParser(description="한글 URL 인코딩 테스트 & 재처리")
    parser.add_argument("--dry-run", action="store_true", help="다운로드 없이 URL 변환만 출력")
    parser.add_argument("--limit", type=int, default=5, help="처리할 최대 레코드 수 (기본: 5)")
    parser.add_argument("--firm", type=str, default=None, help="특정 증권사만 처리 (예: 교보증권, 유안타증권)")
    parser.add_argument("--concurrency", type=int, default=3, help="동시 다운로드 수 (기본: 3)")
    args = parser.parse_args()

    log.info("=" * 60)
    mode = "DRY-RUN" if args.dry_run else "DOWNLOAD TEST"
    log.info(f"한글 URL 인코딩 {mode} 시작")
    log.info(f"  limit={args.limit}, firm={args.firm or 'ALL'}, concurrency={args.concurrency}")
    log.info("=" * 60)

    conn = await get_conn()
    try:
        # 대상 레코드 조회
        where_clauses = [
            "pdf_sync_status = 3",
            "(pdf_url ~ '[가-힣]' OR report_unique_key ~ '[가-힣]' OR telegram_url ~ '[가-힣]' OR download_url ~ '[가-힣]')",
        ]
        if args.firm:
            where_clauses.append(f"firm_nm = '{args.firm}'")

        where_sql = " AND ".join(where_clauses)
        query = f"""
            SELECT report_id, firm_nm, article_title, pdf_url, report_unique_key,
                   telegram_url, download_url, reg_dt
            FROM tbl_sec_reports
            WHERE {where_sql}
            ORDER BY reg_dt DESC
            LIMIT {args.limit}
        """

        rows = await conn.fetch(query)
        total = len(rows)
        log.info(f"대상 레코드: {total}건")

        if total == 0:
            log.info("처리할 레코드가 없습니다.")
            return

        # 펌별 분포 출력
        firm_counts = {}
        for r in rows:
            f = r["firm_nm"]
            firm_counts[f] = firm_counts.get(f, 0) + 1
        log.info(f"증권사 분포: {firm_counts}")

        # 동시 처리
        sem = asyncio.Semaphore(args.concurrency)

        async def limited(rec):
            async with sem:
                return await process_record(conn, rec, dry_run=args.dry_run)

        results = await asyncio.gather(*[limited(dict(r)) for r in rows])

        # 결과 요약
        success = [r for r in results if r and r.get("status") == "success"]
        skipped = [r for r in results if r and r.get("status") == "skipped"]
        failed = [r for r in results if r and r.get("status") in ("failed", "no_candidates")]
        dry = [r for r in results if r and r.get("status") == "dry_run"]

        log.info("\n" + "=" * 60)
        log.info("결과 요약")
        log.info("=" * 60)
        log.info(f"  전체: {total}건")
        log.info(f"  ✅ 성공: {len(success)}건")
        log.info(f"  ⏭️  스킵 (이미존재): {len(skipped)}건")
        log.info(f"  ❌ 실패: {len(failed)}건")
        if args.dry_run:
            log.info(f"  🔍 dry-run: {len(dry)}건")

        if success:
            log.info("\n성공 건 인코딩 분류:")
            enc_counts = {}
            for r in success:
                enc = r.get("encoding", "unknown")
                enc_counts[enc] = enc_counts.get(enc, 0) + 1
            for enc, cnt in enc_counts.items():
                log.info(f"  [{enc}]: {cnt}건")

            for r in success:
                log.info(f"  report_id={r['report_id']} [{r.get('encoding', '?')}] size={r.get('size', 0):,}")

        if failed:
            log.info("\n실패 건:")
            for r in failed:
                log.info(f"  report_id={r['report_id']} reason={r.get('reason', '?')}")

    finally:
        await conn.close()

    log.info("\n완료.")


if __name__ == "__main__":
    asyncio.run(main())
