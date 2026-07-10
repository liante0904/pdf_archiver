import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPTS_PATH = Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_PATH))


def load_archiver(version):
    module_path = SCRIPTS_PATH / f"pdf_archiver_{version}.py"
    spec = importlib.util.spec_from_file_location(f"pdf_archiver_{version}", module_path)
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


class FetchTargetsSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_queries_use_only_current_source_url_columns(self):
        for version in ("v2", "v3"):
            with self.subTest(version=version):
                conn = FakeConnection()
                archiver = load_archiver(version)

                rows = await archiver.fetch_targets(conn, 25)

                self.assertEqual(rows, [])
                self.assertEqual(conn.args, (25,))
                self.assertNotIn("download_url", conn.query)
                self.assertIn("s.pdf_url", conn.query)
                self.assertIn("s.telegram_url", conn.query)
                self.assertIn("s.report_unique_key", conn.query)


if __name__ == "__main__":
    unittest.main()
