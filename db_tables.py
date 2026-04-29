import os


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


SOURCE_REPORTS_TABLE_NAME = os.getenv("SOURCE_REPORTS_TABLE_NAME", "tbl_sec_reports")
PDF_ARCHIVE_TABLE_NAME = os.getenv("PDF_ARCHIVE_TABLE_NAME", "tbl_sec_reports_pdf_archive")

SOURCE_REPORTS_TABLE = quote_ident(SOURCE_REPORTS_TABLE_NAME)
PDF_ARCHIVE_TABLE = quote_ident(PDF_ARCHIVE_TABLE_NAME)
