"""
tbl_sec_reports PDF 다운로드 대상 현황 조회 스크립트.
사용법: uv run python3 check_download_targets.py
"""
import asyncio
import asyncpg
import os
from pathlib import Path


# ── DB 접속 정보 (.env 우선, 없으면 OCI1 기본값) ──────────────────────────
def _load_env(path: str = ".env") -> None:
    """.env 파일을 읽어 환경변수로 적용 (이미 설정된 변수는 덮어쓰지 않음)."""
    env_path = Path(__file__).resolve().parent / path
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip("\"'")
        if key not in os.environ:
            os.environ[key] = val


_load_env()

DB_HOST = os.getenv("POSTGRES_HOST", "10.0.0.111")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "ssh_reports_hub")
DB_USER = os.getenv("POSTGRES_USER", "ssh_reports_hub")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")

EXCLUDED_FIRMS = ("미래에셋증권", "유진투자증권", "상상인증권", "BNK투자증권")


async def main() -> None:
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )
    print(f"Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}\n")

    excluded_sql = ", ".join(f"'{f}'" for f in EXCLUDED_FIRMS)

    # ── 1. 전체 + status 분포 ───────────────────────────────────────────
    total = await conn.fetchval("SELECT COUNT(*) FROM tbl_sec_reports")
    dist = await conn.fetch("""
        SELECT pdf_sync_status, COUNT(*) AS cnt
        FROM tbl_sec_reports
        GROUP BY pdf_sync_status
        ORDER BY pdf_sync_status
    """)

    status_labels = {0: "신규", 2: "완료", 3: "실패", 9: "기타"}

    print("=" * 55)
    print("  tbl_sec_reports PDF 다운로드 대상")
    print("=" * 55)
    print(f"  {'전체 로우':<30} {total:>10,}")
    for row in dist:
        s = row["pdf_sync_status"]
        label = status_labels.get(s, f"status={s}")
        print(f"  pdf_sync_status={s} ({label:<4})     {row['cnt']:>10,}")

    # ── 2. 다운로드 대상 (retry limit + 제외증권사) ──────────────────────
    target_sql = f"""
        SELECT COUNT(*) FROM tbl_sec_reports
        WHERE pdf_sync_status IN (0, 3)
          AND firm_nm NOT IN ({excluded_sql})
          AND report_id IS NOT NULL
          AND (
              (pdf_sync_status = 0 AND COALESCE(retry_count, 0) < 5)
              OR
              (pdf_sync_status = 3 AND COALESCE(retry_count, 0) < 8)
          )
    """
    count_raw = await conn.fetchval(target_sql)

    # 중복제거 (pdf_key 기준)
    dedup_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (
                COALESCE(ENCODE(pdf_hash, 'hex'), NULLIF(BTRIM(pdf_url), ''), report_id::TEXT)
            ) report_id
            FROM tbl_sec_reports
            WHERE pdf_sync_status IN (0, 3)
              AND firm_nm NOT IN ({excluded_sql})
              AND report_id IS NOT NULL
              AND (
                  (pdf_sync_status = 0 AND COALESCE(retry_count, 0) < 5)
                  OR
                  (pdf_sync_status = 3 AND COALESCE(retry_count, 0) < 8)
              )
        ) sub
    """
    count_dedup = await conn.fetchval(dedup_sql)

    # source_url 유무
    has_url_sql = f"""
        SELECT COUNT(*) FROM tbl_sec_reports
        WHERE pdf_sync_status IN (0, 3)
          AND firm_nm NOT IN ({excluded_sql})
          AND report_id IS NOT NULL
          AND (
              NULLIF(BTRIM(pdf_url), '') IS NOT NULL
              OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
              OR NULLIF(BTRIM(download_url), '') IS NOT NULL
              OR NULLIF(BTRIM(key), '') IS NOT NULL
          )
    """
    no_url_sql = f"""
        SELECT COUNT(*) FROM tbl_sec_reports
        WHERE pdf_sync_status IN (0, 3)
          AND firm_nm NOT IN ({excluded_sql})
          AND report_id IS NOT NULL
          AND NOT (
              NULLIF(BTRIM(pdf_url), '') IS NOT NULL
              OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
              OR NULLIF(BTRIM(download_url), '') IS NOT NULL
              OR NULLIF(BTRIM(key), '') IS NOT NULL
          )
    """
    cnt_has_url = await conn.fetchval(has_url_sql)
    cnt_no_url = await conn.fetchval(no_url_sql)

    print(f"\n  {'필터링 후 (status 0/3, retry < limit, 제외증권사)':<30} {count_raw:>10,}")
    print(f"  {'pdf_key 중복제거 후':<30} {count_dedup:>10,}")
    print(f"  {'- source URL 있음':<30} {cnt_has_url:>10,}")
    print(f"  {'- source URL 없음':<30} {cnt_no_url:>10,}")

    # ── 3. 증권사별 분포 ────────────────────────────────────────────────
    firms = await conn.fetch(f"""
        SELECT firm_nm, COUNT(*) AS cnt
        FROM tbl_sec_reports
        WHERE pdf_sync_status IN (0, 3)
          AND firm_nm NOT IN ({excluded_sql})
          AND report_id IS NOT NULL
          AND (
              (pdf_sync_status = 0 AND COALESCE(retry_count, 0) < 5)
              OR
              (pdf_sync_status = 3 AND COALESCE(retry_count, 0) < 8)
          )
        GROUP BY firm_nm
        ORDER BY cnt DESC
    """)

    print(f"\n  {'증권사별 분포':─<45}")
    for f in firms:
        print(f"  {f['firm_nm']:<20} {f['cnt']:>10,}")

    print("=" * 55)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
