import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sys

os.environ.setdefault("LOG_FILE", "/tmp/pdf_archiver_test.log")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pdf_archiver_async as archiver  # noqa: E402


class FakeResponse:
    def __init__(self, status=200, body=b"", json_data=None):
        self.status = status
        self._body = body
        self._json_data = json_data

    async def read(self):
        return self._body

    async def json(self):
        if self._json_data is None:
            raise ValueError("No json data configured")
        return self._json_data

    async def text(self):
        return self._body.decode("utf-8", errors="ignore")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def _next_response(self, method, url):
        self.calls.append((method, url))
        queue = self.responses.get((method, url))
        if not queue:
            raise AssertionError(f"No fake response configured for {method} {url}")
        return queue.pop(0)

    def post(self, url, headers=None, data=None):
        return self._next_response("POST", url)

    def get(self, url, headers=None):
        return self._next_response("GET", url)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class TestDbfiRetryCandidates(unittest.TestCase):
    def test_extract_dbfi_retry_candidates_dedupes_and_resolves_relative_urls(self):
        html = """
        <html>
          <body>
            <a href="/streamdocs/v4/documents/abc123">download</a>
            <div data-url="/files/report.pdf"></div>
            <script>var url="/files/report.pdf";</script>
            <a href="/streamdocs/v4/documents/abc123">duplicate</a>
          </body>
        </html>
        """

        candidates = archiver.extract_dbfi_retry_candidates(
            html,
            "https://whub.dbsec.co.kr/streamdocs/v4/documents/base",
        )

        self.assertEqual(
            candidates,
            [
                "https://whub.dbsec.co.kr/streamdocs/v4/documents/abc123",
                "https://whub.dbsec.co.kr/files/report.pdf",
            ],
        )


class TestDbfiDownload(unittest.IsolatedAsyncioTestCase):
    async def test_download_dbfi_pdf_resolves_detail_json_and_downloads_in_same_session(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / "sample.pdf"
            detail_url = "https://m.db-fi.com/appData/descRsh/35364.json"
            encoded_url = "gate-token"
            auth_url = "https://whub.dbsec.co.kr/pv/auth"
            viewer_url = "https://whub.dbsec.co.kr/pv/viewer"
            pdf_url = "https://whub.dbsec.co.kr/streamdocs/v4/documents/doc123"
            viewer_html = b'<div id="doc123" class="item"><span>research</span></div>'

            fake_session = FakeSession(
                {
                    ("POST", detail_url): [
                        FakeResponse(status=200, json_data={"data": {"url": encoded_url}})
                    ],
                    ("POST", auth_url): [
                        FakeResponse(status=200, body=b"ok")
                    ],
                    ("POST", viewer_url): [
                        FakeResponse(status=200, body=viewer_html)
                    ],
                    ("GET", pdf_url): [
                        FakeResponse(
                            status=200,
                            body=b"%PDF-1.4\n" + (b"dbfi-pdf-content\n" * 128),
                        )
                    ],
                }
            )

            with patch.object(archiver.aiohttp, "ClientSession", return_value=fake_session), patch.object(
                archiver.aiohttp, "TCPConnector", return_value=None
            ), patch.object(archiver, "get_pdf_page_count", new=AsyncMock(return_value=9)):
                result = await archiver.download_dbfi_pdf(
                    detail_url,
                    target_path,
                    title="DBfi Test",
                    report_id="233375322",
                    firm="DB증권",
                    reg_dt="20260422",
                )

            self.assertIsNotNone(result)
            self.assertTrue(target_path.exists())
            self.assertEqual(result["pages"], 9)
            self.assertEqual(
                fake_session.calls,
                [
                    ("POST", detail_url),
                    ("POST", auth_url),
                    ("POST", viewer_url),
                    ("GET", pdf_url),
                ],
            )

    async def test_download_task_uses_dbfi_key_before_pdf_url_and_records_success(self):
        archiver_instance = archiver.PDFArchiver()
        row = (
            10,
            "233375322",
            19,
            "https://m.db-fi.com/appData/descRsh/35364.json",
            "https://whub.dbsec.co.kr/streamdocs/v4/documents/from_pdf_url",
            "",
            "",
            "DB증권",
            "DBfi title",
            "20260422",
        )

        async def fake_download(source_url, target_path, title, report_id, firm, reg_dt):
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n" + (b"dbfi-pdf-content\n" * 128))
            return {
                "report_id": report_id,
                "firm": firm,
                "title": title,
                "path": target_path,
                "size": target_path.stat().st_size,
                "pages": 7,
                "reg_dt": reg_dt,
            }

        download_mock = AsyncMock(side_effect=fake_download)
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(archiver, "LOCAL_BUFFER_DIR", tmpdir), patch.object(
            archiver, "download_dbfi_pdf", new=download_mock
        ):
            archiver_instance.local_dir = Path(tmpdir)
            ok = await archiver_instance.download_task(row)

        self.assertTrue(ok)
        download_mock.assert_awaited_once()
        self.assertEqual(
            download_mock.await_args.args[0],
            "https://m.db-fi.com/appData/descRsh/35364.json",
        )
        self.assertEqual(len(archiver_instance.success_downloads), 1)
        self.assertEqual(archiver_instance.success_downloads[0][0], 10)
        self.assertEqual(archiver_instance.success_downloads[0][1], "233375322")

    async def test_download_dbfi_pdf_retries_html_candidate_until_pdf(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / "sample.pdf"
            source_url = "https://whub.dbsec.co.kr/streamdocs/v4/documents/base"
            final_url = "https://whub.dbsec.co.kr/streamdocs/v4/documents/final"

            fake_session = FakeSession(
                {
                    ("GET", source_url): [
                        FakeResponse(
                            status=200,
                            body=b'<html><a href="/streamdocs/v4/documents/final">open</a></html>',
                        )
                    ],
                    ("GET", final_url): [
                        FakeResponse(
                            status=200,
                            body=b"%PDF-1.4\n" + (b"fake-pdf-content\n" * 128),
                        )
                    ],
                }
            )

            with patch.object(archiver.aiohttp, "ClientSession", return_value=fake_session), patch.object(
                archiver.aiohttp, "TCPConnector", return_value=None
            ), patch.object(archiver, "get_pdf_page_count", new=AsyncMock(return_value=12)):
                result = await archiver.download_dbfi_pdf(
                    source_url,
                    target_path,
                    title="DBfi Test",
                    report_id="123",
                    firm="DB증권",
                    reg_dt="20260422",
                )

            self.assertIsNotNone(result)
            self.assertTrue(target_path.exists())
            self.assertEqual(result["pages"], 12)
            self.assertEqual(result["size"], target_path.stat().st_size)
            self.assertEqual(result["path"], target_path)
            self.assertEqual(fake_session.calls[0], ("GET", source_url))
            self.assertEqual(fake_session.calls[1], ("GET", final_url))


if __name__ == "__main__":
    unittest.main()
