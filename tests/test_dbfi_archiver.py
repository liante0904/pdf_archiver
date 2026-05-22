import asyncio
import json
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


class FakeProcess:
    def __init__(self, returncode=0, stdout=b"", stderr=b""):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    async def communicate(self):
        return self._stdout, self._stderr


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


class TestRcloneRepair(unittest.IsolatedAsyncioTestCase):
    async def test_upload_aborts_before_repair_when_batch_copy_has_auth_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archiver_instance = archiver.PDFArchiver()
            archiver_instance.local_dir = Path(tmpdir)
            local_path = Path(tmpdir) / "2017-10" / "하나증권" / "sample.pdf"
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(b"%PDF-1.4\nbody")
            archiver_instance.success_downloads = [
                archiver.WorkflowRecord({
                    "row_id": 1,
                    "report_id": "237896530",
                    "sec_firm_order": 1,
                    "key": "",
                    "pdf_url": "",
                    "telegram_url": "",
                    "download_url": "",
                    "firm_nm": "하나증권",
                    "title": "sample",
                    "reg_dt": "20171019",
                    "path": local_path,
                    "size": local_path.stat().st_size,
                    "pages": 1,
                    "pdf_hash": None,
                })
            ]
            calls = []
            processes = [
                FakeProcess(returncode=0, stdout=b"[]", stderr=b""),
                FakeProcess(
                    returncode=1,
                    stderr=b"Attempt 1/2 failed with 23 errors and: unauthenticated: Unauthenticated",
                ),
            ]

            async def fake_exec(*args, **kwargs):
                calls.append(args)
                return processes.pop(0)

            with patch.object(archiver.Config, "LOCAL_BUFFER_DIR", tmpdir), patch.object(
                archiver.asyncio, "create_subprocess_exec", new=fake_exec
            ):
                result = await archiver_instance.upload_to_onedrive()

            self.assertEqual(result, [])
            self.assertTrue(local_path.exists())
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0][3], "lsjson")
            self.assertEqual(calls[1][3], "copy")
            self.assertFalse(processes)

    async def test_delete_remote_falls_back_to_filtered_parent_delete(self):
        archiver_instance = archiver.PDFArchiver()
        remote_dir = "onedrive:/archive/pdf/2017-10/하나증권"
        filename = "171026_BondTalk_ECB와_BOE_Preview_기대와_현실_237775291.pdf"
        remote_path = f"{remote_dir}/{filename}"
        calls = []
        processes = [
            FakeProcess(
                returncode=1,
                stderr=(
                    f"Failed to deletefile: {remote_path} is a directory or doesn't exist"
                ).encode(),
            ),
            FakeProcess(returncode=0, stderr=b""),
            FakeProcess(returncode=0, stdout=b"[]", stderr=b""),
        ]

        async def fake_exec(*args, **kwargs):
            calls.append(args)
            return processes.pop(0)

        with patch.object(archiver.asyncio, "create_subprocess_exec", new=fake_exec):
            ok, err = await archiver_instance._rclone_delete_remote(
                remote_path,
                remote_dir=remote_dir,
                filename=filename,
            )

        self.assertTrue(ok, err)
        self.assertEqual(calls[0][3], "deletefile")
        self.assertEqual(calls[1][3], "delete")
        self.assertIn("--max-depth", calls[1])
        self.assertIn("--include", calls[1])
        self.assertIn(f"/{filename}", calls[1])
        self.assertEqual(calls[2][3], "lsjson")
        self.assertFalse(processes)

    async def test_delete_remote_does_not_report_success_when_filtered_delete_leaves_file(self):
        archiver_instance = archiver.PDFArchiver()
        remote_dir = "onedrive:/archive/pdf/2017-10/하나증권"
        filename = "stale[1].pdf"
        remote_path = f"{remote_dir}/{filename}"
        calls = []
        listing = json.dumps([{"Name": filename, "Size": 0, "IsDir": False}]).encode()
        processes = [
            FakeProcess(returncode=1, stderr=b"Failed to deletefile: object doesn't exist"),
            FakeProcess(returncode=0, stderr=b""),
            FakeProcess(returncode=0, stdout=listing, stderr=b""),
        ]

        async def fake_exec(*args, **kwargs):
            calls.append(args)
            return processes.pop(0)

        with patch.object(archiver.asyncio, "create_subprocess_exec", new=fake_exec):
            ok, err = await archiver_instance._rclone_delete_remote(
                remote_path,
                remote_dir=remote_dir,
                filename=filename,
            )

        self.assertFalse(ok)
        self.assertIn("file still exists", err)
        self.assertIn("/stale\\[1\\].pdf", calls[1])
        self.assertFalse(processes)


class TestFetchPolicy(unittest.TestCase):
    def test_target_query_is_firm_diversified_without_firm_specific_retry_exception(self):
        archiver_instance = archiver.PDFArchiver()

        query = archiver_instance._build_target_query("'미래에셋증권'")

        self.assertIn("ROW_NUMBER() OVER", query)
        self.assertIn("PARTITION BY firm_nm", query)
        self.assertIn("ORDER BY firm_rank, reg_dt DESC, firm_nm, report_id DESC", query)
        self.assertIn(f"LIMIT {archiver.Config.BATCH_SIZE}", query)
        self.assertIn(f"< {archiver.Config.FETCH_RETRY_LIMIT}", query)
        self.assertNotIn("firm_nm = 'LS증권'", query)
        self.assertNotIn("View.jsp", query)

    def test_target_firm_counts_summarizes_fetch_distribution(self):
        targets = [
            {"firm_nm": "LS증권"},
            {"firm_nm": "하나증권"},
            {"firm_nm": "LS증권"},
            {"firm_nm": None},
        ]

        counts = archiver.PDFArchiver._target_firm_counts(targets)

        self.assertEqual(counts, {"LS증권": 2, "하나증권": 1, "UNKNOWN": 1})


class TestWorkflowUpdates(unittest.IsolatedAsyncioTestCase):
    async def test_upload_failures_do_not_increment_retry_count(self):
        archiver_instance = archiver.PDFArchiver()
        payload = archiver.WorkflowRecord({
            "row_id": 1,
            "report_id": "100",
            "sec_firm_order": 1,
            "key": "",
            "pdf_url": "https://example.test/report.pdf",
            "telegram_url": "",
            "download_url": "",
            "firm_nm": "테스트증권",
            "title": "sample",
            "reg_dt": "20260509",
            "path": Path("/tmp/sample.pdf"),
            "size": 123,
            "pages": 1,
            "pdf_hash": None,
        })
        archiver_instance.success_downloads = [payload]
        conn = AsyncMock()

        await archiver_instance._apply_upload_results(conn, [])

        self.assertEqual(conn.execute.await_count, 1)
        args = conn.execute.await_args.args
        self.assertEqual(args[1], 100)
        self.assertEqual(args[2], 3)
        self.assertEqual(args[3], 0)


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
            ), patch("downloaders.dbfi.get_pdf_page_count", new=AsyncMock(return_value=9)):
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
            ), patch("downloaders.dbfi.get_pdf_page_count", new=AsyncMock(return_value=12)):
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
