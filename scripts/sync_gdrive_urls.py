#!/usr/bin/env python3
"""
pdf-archiver post-upload + backfill:
GDrive에 업로드된 PDF의 file_id → gdrive_pdf_url 기록

사용법:
  # 신규 업로드 동기화 (기본)
  RCLONE_REMOTE=gdrive:archive/pdf python sync_gdrive_urls.py

  # 백필: 전체 GDrive 파일 스캔 → 매칭되는 레코드 모두 업데이트
  RCLONE_REMOTE=gdrive:archive/pdf python sync_gdrive_urls.py --backfill
"""
import argparse, json, os, re, subprocess, sys, time
import psycopg2
import psycopg2.extras

PROXY_BASE = "https://ssh-oci.duckdns.org/pdf"

def get_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST_REPORTS", os.getenv("POSTGRES_HOST", "10.0.0.111")),
        port=os.getenv("POSTGRES_PORT_REPORTS", os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER_REPORTS", os.getenv("POSTGRES_USER", "ssh_reports_hub")),
        password=os.getenv("POSTGRES_PASSWORD_REPORTS", os.getenv("POSTGRES_PASSWORD", "")),
        dbname=os.getenv("POSTGRES_DB_REPORTS", os.getenv("POSTGRES_DB", "ssh_reports_hub")),
    )

def rclone_lsjson(remote_path: str) -> list[dict]:
    """rclone lsjson → 파일 목록"""
    remote = os.getenv("RCLONE_REMOTE", "gdrive:archive/pdf")
    full = f"{remote}/{remote_path}" if remote_path else remote
    result = subprocess.run(
        ["rclone", "lsjson", full, "--files-only", "--recursive"],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        print(f"rclone failed: {result.stderr[:200]}")
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []

def extract_report_id(filename: str) -> int | None:
    """파일명에서 report_id 추출: {date}_{title}_{report_id}.pdf"""
    m = re.search(r'_(\d{6,})\.pdf$', filename)
    return int(m.group(1)) if m else None

def sync_new(conn):
    """최근 업로드된 PDF → gdrive_pdf_url 기록"""
    # pdf_sync_status=2 (archive 완료) + gdrive_pdf_url IS NULL 인 레코드
    cur = conn.cursor()
    cur.execute("""
        SELECT report_id, pdf_sync_status, download_url
        FROM tbl_sec_reports
        WHERE pdf_sync_status >= 2 AND gdrive_pdf_url IS NULL
        LIMIT 200
    """)
    rows = cur.fetchall()
    if not rows:
        print("No pending records")
        return

    print(f"Scanning GDrive for {len(rows)} records...")
    files = rclone_lsjson("")
    if not files:
        print("No GDrive files found (rate limited?)")
        return

    # Build filename → ID map
    id_map = {f["Name"]: f["ID"] for f in files}

    updated = 0
    for report_id, status, dl_url in rows:
        # Try to find matching GDrive file by report_id pattern
        for name, fid in id_map.items():
            rid = extract_report_id(name)
            if rid == report_id:
                url = f"{PROXY_BASE}/{fid}"
                cur.execute(
                    "UPDATE tbl_sec_reports SET gdrive_pdf_url = %s WHERE report_id = %s",
                    [url, report_id],
                )
                updated += 1
                break

    conn.commit()
    print(f"Updated {updated} records with gdrive_pdf_url")

def backfill(conn):
    """전체 GDrive 스캔 → 모든 매칭 레코드 백필"""
    print("Backfill: scanning all GDrive files...")
    files = rclone_lsjson("")
    if not files:
        print("No GDrive files found")
        return

    print(f"Found {len(files)} files on GDrive")

    # Extract report_ids from filenames
    id_map = {}
    for f in files:
        rid = extract_report_id(f["Name"])
        if rid:
            id_map[rid] = f["ID"]

    if not id_map:
        print("No report_ids extracted from filenames")
        return

    print(f"Extracted {len(id_map)} report_ids. Updating DB...")

    cur = conn.cursor()
    updated = 0
    for report_id, fid in id_map.items():
        url = f"{PROXY_BASE}/{fid}"
        cur.execute(
            "UPDATE tbl_sec_reports SET gdrive_pdf_url = %s WHERE report_id = %s AND gdrive_pdf_url IS NULL",
            [url, report_id],
        )
        updated += cur.rowcount

    conn.commit()
    print(f"Backfill complete: {updated} records updated")

    # Summary
    cur.execute("SELECT COUNT(*) FROM tbl_sec_reports WHERE gdrive_pdf_url IS NOT NULL")
    total = cur.fetchone()[0]
    print(f"Total records with gdrive_pdf_url: {total}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--backfill", action="store_true")
    args = p.parse_args()

    conn = get_db()
    try:
        if args.backfill:
            backfill(conn)
        else:
            sync_new(conn)
    finally:
        conn.close()
