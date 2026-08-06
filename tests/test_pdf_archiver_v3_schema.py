import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPTS_PATH = Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_PATH))


def load_archiver():
    module_path = SCRIPTS_PATH / "pdf_archiver_v3.py"
    spec = importlib.util.spec_from_file_location("pdf_archiver_v3", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeConnection:
    def __init__(self):
        self.query = None
        self.args = None

    async def fetch(self, query, *args):
        self.query = query
        self.args = args
        return []

    async def execute(self, query, *args):
        self.query = query
        self.args = args
        return "INSERT 0 1"

    async def fetchrow(self, query, *args):
        self.query = query
        self.args = args
        return None


class FetchTargetsSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_queries_use_only_current_source_url_columns(self):
        conn = FakeConnection()
        archiver = load_archiver()

        rows = await archiver.fetch_targets(conn, 25)

        self.assertEqual(rows, [])
        self.assertEqual(conn.args, (25,))
        self.assertNotIn("download_url", conn.query)
        self.assertIn("s.pdf_url", conn.query)
        self.assertIn("s.telegram_url", conn.query)
        self.assertIn("s.report_unique_key", conn.query)

    async def test_upserts_use_only_current_archive_status_columns(self):
        conn = FakeConnection()
        archiver = load_archiver()

        await archiver.upsert_archive(
            conn,
            report_id=1,
            firm_nm="firm",
            title="title",
            report_date="20260710",
            pdf_url="https://example.com/report.pdf",
            storage_key="2026-07/firm/report.pdf",
            file_size=100,
            page_count=1,
            pdf_hash="00",
            pdf_hash_bytes=b"hash",
            success=True,
        )

        self.assertNotIn("download_status_yn", conn.query)
        self.assertIn("pdf_sync_status", conn.query)
        self.assertIn("sync_status", conn.query)
        self.assertIn("gdrive_file_id", conn.query)
        self.assertEqual(len(conn.args), 15)

    async def test_hash_dedup_requires_verified_remote_canonical(self):
        conn = FakeConnection()
        archiver = load_archiver()

        self.assertIsNone(await archiver.find_by_hash(conn, "abc"))
        self.assertIn("gdrive_file_id", conn.query)
        self.assertIn("storage_key", conn.query)
        self.assertIn("COALESCE(file_size, 0) > 0", conn.query)


if __name__ == "__main__":
    unittest.main()
