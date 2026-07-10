import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "pdf_archiver_v2.py"
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("pdf_archiver_v2", MODULE_PATH)
pdf_archiver_v2 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pdf_archiver_v2)


class FakeConnection:
    def __init__(self):
        self.query = None
        self.args = None

    async def fetch(self, query, *args):
        self.query = query
        self.args = args
        return []


class FetchTargetsSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_uses_only_current_source_url_columns(self):
        conn = FakeConnection()

        rows = await pdf_archiver_v2.fetch_targets(conn, 25)

        self.assertEqual(rows, [])
        self.assertEqual(conn.args, (25,))
        self.assertNotIn("download_url", conn.query)
        self.assertIn("s.pdf_url", conn.query)
        self.assertIn("s.telegram_url", conn.query)
        self.assertIn("s.report_unique_key", conn.query)


if __name__ == "__main__":
    unittest.main()
