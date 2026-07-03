import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn


async def analyze_pdf_hash_duplicates():
    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        query = """
            WITH dup_groups AS (
                SELECT
                    encode(pdf_hash, 'hex') AS pdf_hash_hex,
                    COUNT(*) AS group_count
                FROM tbl_sec_reports
                WHERE pdf_hash IS NOT NULL
                GROUP BY pdf_hash
                HAVING COUNT(*) > 1
            )
            SELECT
                r.report_id,
                r.firm_nm,
                r.article_title,
                r.report_date,
                r.report_unique_key,
                r.pdf_url,
                encode(r.pdf_hash, 'hex') AS pdf_hash_hex,
                r.download_url,
                r.telegram_url,
                d.group_count
            FROM tbl_sec_reports r
            JOIN dup_groups d
              ON r.pdf_hash = decode(d.pdf_hash_hex, 'hex')
            ORDER BY d.pdf_hash_hex, r.report_date DESC, r.report_id ASC
        """

        rows = await conn.fetch(query)
        if not rows:
            print("No duplicate pdf_hash groups found.")
            return

        print(f"--- pdf_hash duplicate analysis ---")
        print(f"Duplicate rows: {len(rows)}")

        current_hash = None
        group_rows = []

        def flush_group(pdf_hash_hex, items):
            if not pdf_hash_hex or not items:
                return
            survivor = min(items, key=lambda x: int(x["report_id"]))
            print(f"\npdf_hash: {pdf_hash_hex}")
            print(f"  count: {len(items)}")
            print(f"  survivor candidate: report_id={survivor['report_id']} firm={survivor['firm_nm']} title={survivor['article_title']}")
            for item in items:
                marker = "KEEP" if item["report_id"] == survivor["report_id"] else "DROP?"
                print(
                    f"  - {marker} ID={item['report_id']} | report_date={item['report_date']} | firm={item['firm_nm']} | "
                    f"report_unique_key={item['report_unique_key']} | pdf_url={item['pdf_url']} | download_url={item['download_url']} | telegram_url={item['telegram_url']}"
                )

        for row in rows:
            pdf_hash_hex = row["pdf_hash_hex"]
            if pdf_hash_hex != current_hash and group_rows:
                flush_group(current_hash, group_rows)
                group_rows = []
            current_hash = pdf_hash_hex
            group_rows.append(row)

        flush_group(current_hash, group_rows)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(analyze_pdf_hash_duplicates())
