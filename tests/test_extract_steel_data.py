import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from data_extraction import extract_steel_data


def _write_dummy_pdf(tmp_path):
    pdf_path = tmp_path / "dummy.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                        b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
                        b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]/Contents 4 0 R>>endobj\n"
                        b"4 0 obj<</Length 15>>stream\nBT /F1 12 Tf ET\nendstream endobj\n"
                        b"xref\n0 5\n0000000000 65535 f \n0000000010 00000 n \n0000000053 00000 n \n"
                        b"0000000100 00000 n \n0000000190 00000 n \ntrailer<</Size 5/Root 1 0 R>>\nstartxref\n260\n%%EOF")
    return pdf_path


def test_process_pdf_merges_table_results(monkeypatch, tmp_path):
    pdf_path = _write_dummy_pdf(tmp_path)

    monkeypatch.setattr(extract_steel_data, "extract_text", lambda path: "C 0.5%")

    fake_tables = [
        {
            "caption": "Table 1",
            "data": [["Element", "Value"], ["C", "0.6"]],
            "page": 1,
            "extraction_method": "camelot-lattice",
        }
    ]

    fake_table_results = {
        "composition": {"C": 0.6},
        "process": {"T1": 850},
        "properties": {},
    }

    monkeypatch.setattr(extract_steel_data.table_extractor, "extract_tables", lambda *_args, **_kwargs: fake_tables)
    monkeypatch.setattr(
        extract_steel_data.table_data_processor,
        "extract_data_from_tables",
        lambda tables: fake_table_results,
    )

    result = extract_steel_data.process_pdf(str(pdf_path))

    assert result["composition"]["C"] == 0.6
    assert result["heat_treatment"]["T1"] == 850
    assert result["tables"] == fake_tables
    assert result["table_results"] == fake_table_results
    assert result["text_source"] == "pdfminer"

