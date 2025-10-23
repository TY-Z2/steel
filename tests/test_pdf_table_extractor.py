import sys
import types
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from parsers import pdf_table_extractor


def _write_dummy_pdf(tmp_path):
    pdf_path = tmp_path / "dummy.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                        b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
                        b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]/Contents 4 0 R>>endobj\n"
                        b"4 0 obj<</Length 15>>stream\nBT /F1 12 Tf ET\nendstream endobj\n"
                        b"xref\n0 5\n0000000000 65535 f \n0000000010 00000 n \n0000000053 00000 n \n"
                        b"0000000100 00000 n \n0000000190 00000 n \ntrailer<</Size 5/Root 1 0 R>>\nstartxref\n260\n%%EOF")
    return pdf_path


def test_extract_tables_uses_camelot(monkeypatch, tmp_path):
    pdf_path = _write_dummy_pdf(tmp_path)

    dummy_df = object()

    class DummyTable:
        def __init__(self):
            self.df = dummy_df
            self.page = 2
            self.parsing_report = {"page": 2}

    def fake_read_pdf(path, pages="all", flavor=None, strip_text="\n"):
        assert flavor == "lattice"
        return [DummyTable()]

    monkeypatch.setattr(
        pdf_table_extractor,
        "_dataframe_to_rows",
        lambda df: [["Element", "Value"], ["C", "0.5"]] if df is dummy_df else [],
    )
    monkeypatch.setattr(pdf_table_extractor, "camelot", types.SimpleNamespace(read_pdf=fake_read_pdf))
    monkeypatch.setattr(pdf_table_extractor, "tabula", None)
    monkeypatch.setattr(pdf_table_extractor, "convert_from_path", None)

    tables = pdf_table_extractor.extract_tables_from_pdf(str(pdf_path))

    assert tables
    assert tables[0]["extraction_method"] == "camelot-lattice"
    assert tables[0]["page"] == 2


def test_extract_tables_falls_back_to_ocr(monkeypatch, tmp_path, caplog):
    pdf_path = _write_dummy_pdf(tmp_path)

    class DummyPDF:
        def __init__(self, pages):
            self.pages = [None] * pages

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_pdf_module = types.SimpleNamespace(open=lambda path: DummyPDF(1))

    def fake_convert_from_path(path, dpi=300):
        return [object()]

    def fake_image_to_data(image, output_type=None):
        return {
            "text": ["Col1", "Col2", "1", "2"],
            "block_num": [1, 1, 1, 1],
            "line_num": [1, 1, 2, 2],
            "left": [10, 80, 10, 80],
        }

    def fake_read_pdf(*args, **kwargs):
        return []

    pytesseract_stub = types.SimpleNamespace(
        image_to_data=fake_image_to_data,
        image_to_string=lambda image: "",
    )

    Output_stub = types.SimpleNamespace(DICT="DICT")

    monkeypatch.setattr(pdf_table_extractor, "camelot", None)
    monkeypatch.setattr(pdf_table_extractor, "tabula", types.SimpleNamespace(read_pdf=fake_read_pdf))
    monkeypatch.setattr(pdf_table_extractor, "pdfplumber", fake_pdf_module)
    monkeypatch.setattr(pdf_table_extractor, "convert_from_path", fake_convert_from_path)
    monkeypatch.setattr(pdf_table_extractor, "pytesseract", pytesseract_stub)
    monkeypatch.setattr(pdf_table_extractor, "Output", Output_stub)

    with caplog.at_level("INFO"):
        tables = pdf_table_extractor.extract_tables_from_pdf(str(pdf_path))

    assert tables
    assert tables[0]["extraction_method"] == "ocr"
    assert any("OCR" in record.message for record in caplog.records)

