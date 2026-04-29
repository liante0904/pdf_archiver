import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn


async def analyze_pdf_url_duplicates():
    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        query = """
            WITH dup_groups AS (
                SELECT
                    BTRIM(pdf_url) AS pdf_url,
                    COUNT(*) AS group_count
                FROM tbl_sec_reports
                WHERE NULLIF(BTRIM(pdf_url), '') IS NOT NULL
                GROUP BY BTRIM(pdf_url)
                HAVING COUNT(*) > 1
            )
            SELECT
                r.report_id,
                r.firm_nm,
                r.article_title,
                r.reg_dt,
                r.key,
                r.pdf_url,
                r.download_url,
                r.telegram_url,
                d.group_count
            FROM tbl_sec_reports r
            JOIN dup_groups d
              ON BTRIM(r.pdf_url) = d.pdf_url
            ORDER BY d.pdf_url, r.reg_dt DESC, r.report_id ASC
        """

        rows = await conn.fetch(query)
        if not rows:
            print("No duplicate pdf_url groups found.")
            return

        print(f"--- pdf_url exact duplicate analysis ---")
        print(f"Duplicate rows: {len(rows)}")

        current_url = None
        group_rows = []

        def flush_group(url, items):
            if not url or not items:
                return
            survivor = min(items, key=lambda x: int(x["report_id"]))
            print(f"\npdf_url: {url}")
            print(f"  count: {len(items)}")
            print(f"  survivor candidate: report_id={survivor['report_id']} firm={survivor['firm_nm']} title={survivor['article_title']}")
            for item in items:
                marker = "KEEP" if item["report_id"] == survivor["report_id"] else "DROP?"
                print(
                    f"  - {marker} ID={item['report_id']} | reg_dt={item['reg_dt']} | firm={item['firm_nm']} | "
                    f"key={item['key']} | download_url={item['download_url']} | telegram_url={item['telegram_url']}"
                )

        for row in rows:
            url = row["pdf_url"].strip() if row["pdf_url"] else None
            if url != current_url and group_rows:
                flush_group(current_url, group_rows)
                group_rows = []
            current_url = url
            group_rows.append(row)

        flush_group(current_url, group_rows)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(analyze_pdf_url_duplicates())
