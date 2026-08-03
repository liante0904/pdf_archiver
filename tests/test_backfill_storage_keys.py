import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from backfill_storage_keys import extract_report_id, parse_lsf_line


def test_extract_report_id_only_accepts_archiver_filename_suffix():
    assert extract_report_id("2026-08/firm/260803_title_123.pdf") == 123
    assert extract_report_id("2026-08/firm/title_123.PDF") == 123
    assert extract_report_id("2026-08/firm/title_123.pdf.bak") is None
    assert extract_report_id("2026-08/firm/title.pdf") is None


def test_parse_lsf_line_discards_unusable_remote_entries():
    item = parse_lsf_line("2026-08/firm/title_123.pdf;4567\n")
    assert item is not None
    assert item.report_id == 123
    assert item.remote_path == "2026-08/firm/title_123.pdf"
    assert item.remote_size == 4567
    assert parse_lsf_line("2026-08/firm/title_123.pdf;0\n") is None
    assert parse_lsf_line("2026-08/firm/title.pdf;4567\n") is None
