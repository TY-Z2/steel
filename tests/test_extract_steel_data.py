import logging
import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from data_extraction.extract_steel_data import (  # noqa: E402
    NLPExtractor,
    extract_steel_data_from_text,
)


@pytest.fixture(autouse=True)
def _restore_logging_level():
    previous = logging.getLogger("data_extraction.extract_steel_data").level
    yield
    logging.getLogger("data_extraction.extract_steel_data").setLevel(previous)


def test_extracts_multilingual_schema_and_ranges():
    text = (
        "碳含量约为 0.12 wt.% ，硅含量为 0.25~0.30%。"
        "The specimens were austenitized at 980 ℃ for 30 min, quenched in oil, tempered at 200 °C for 2 h."
        "屈服强度 Rp0.2 达到 1.2×10^3 MPa，抗拉强度Rm为1.35×10³MPa，延伸率为12 %。"
    )

    data = extract_steel_data_from_text(text)

    carbon = data["composition"]["C"][0]
    assert pytest.approx(carbon["value"], rel=1e-6) == 0.12

    silicon = data["composition"]["Si"][0]
    assert silicon["range"]["min"] == pytest.approx(0.25, rel=1e-6)
    assert silicon["range"]["max"] == pytest.approx(0.30, rel=1e-6)

    temper_times = [item["value"] for item in data["heat_treatment"]["tempering_time"]]
    assert any(pytest.approx(value, rel=1e-6) == 120.0 for value in temper_times)

    quench = data["heat_treatment"]["quenching_medium"][0]
    assert quench["value"] == "water" or quench["value"] == "oil"

    yield_strength = data["mechanical_properties"]["yield_strength"][0]
    assert yield_strength["unit"] == "MPa"
    assert yield_strength["value"] == pytest.approx(1200.0, rel=1e-6)
    assert "屈服强度" in yield_strength["metadata"]["sentence"]

    tensile_strength = data["mechanical_properties"]["tensile_strength"][0]
    assert tensile_strength["value"] == pytest.approx(1350.0, rel=1e-6)

    elongation = data["mechanical_properties"]["elongation"][0]
    assert elongation["value"] == pytest.approx(12.0, rel=1e-6)
    assert elongation["unit"] == "%"


def test_nlp_extractor_finds_abbreviations():
    text = "YS of the alloy reached 950 MPa while tensile strength remained higher."
    extractor = NLPExtractor()
    heat, properties = extractor.extract(text)
    assert properties["yield_strength"], "NLP extractor should capture YS abbreviation"
    method = properties["yield_strength"][0]["metadata"]["method"]
    assert method in {"nlp_dependency", "heuristic_sentencizer", "regex_sentences"}


def test_unknown_unit_emits_warning(caplog):
    caplog.set_level(logging.WARNING)
    extract_steel_data_from_text("Yield strength was measured as 500 psi.")
    assert any("psi" in record.message for record in caplog.records)


def test_metadata_contains_required_fields():
    text = "Impact toughness KV reached 45 J and fatigue strength σ-1 was 800 MPa."
    data = extract_steel_data_from_text(text)
    impact = data["mechanical_properties"]["impact_toughness"][0]
    assert "method" in impact["metadata"]
    assert "sentence" in impact["metadata"]
    assert impact["metadata"]["method"] == "rule_regex"


def test_range_normalization_handles_scientific_notation():
    text = "Silicon content ranged 2.5×10^-1 to 3.0×10^-1 wt.%"
    data = extract_steel_data_from_text(text)
    silicon = data["composition"]["Si"][0]
    assert silicon["range"]["min"] == pytest.approx(0.25, rel=1e-6)
    assert silicon["range"]["max"] == pytest.approx(0.30, rel=1e-6)
