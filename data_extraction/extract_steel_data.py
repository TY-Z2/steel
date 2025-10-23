"""Extraction and persistence utilities for steel-related data."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError

from data_extraction.data_quality import validate_dataset


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def extract_composition(text: str) -> Dict[str, float]:
    """Extract chemical composition from free text."""

    composition: Dict[str, float] = {}
    elements = {
        "C": r"C[\s:]*([\d.]+)\s*%?",
        "Si": r"Si[\s:]*([\d.]+)\s*%?",
        "Mn": r"Mn[\s:]*([\d.]+)\s*%?",
        "Cr": r"Cr[\s:]*([\d.]+)\s*%?",
        "Mo": r"Mo[\s:]*([\d.]+)\s*%?",
        "Ni": r"Ni[\s:]*([\d.]+)\s*%?",
        "V": r"V[\s:]*([\d.]+)\s*%?",
        "Ti": r"Ti[\s:]*([\d.]+)\s*%?",
        "Al": r"Al[\s:]*([\d.]+)\s*%?",
        "Cu": r"Cu[\s:]*([\d.]+)\s*%?",
        "Nb": r"Nb[\s:]*([\d.]+)\s*%?",
        "B": r"B[\s:]*([\d.]+)\s*%?",
        "P": r"P[\s:]*([\d.]+)\s*%?",
        "S": r"S[\s:]*([\d.]+)\s*%?",
        "N": r"N[\s:]*([\d.]+)\s*%?",
    }

    table_pattern = r"composition[^.]*?(\bC\b[\s\S]*?)\n\n"
    table_match = re.search(table_pattern, text, re.IGNORECASE)
    table_text = table_match.group(1) if table_match else text

    for element, pattern in elements.items():
        matches = re.findall(pattern, table_text, re.IGNORECASE)
        if not matches:
            continue
        try:
            value = float(matches[-1])
        except ValueError:
            continue
        if 0 < value < 100:
            composition[element] = value

    return composition


def extract_heat_treatment(text: str) -> Dict[str, float | str]:
    """Extract heat treatment parameters from text."""

    heat_treatment: Dict[str, float | str] = {}
    patterns = {
        "austenitizing_temperature": r"austenitiz(?:ation|ing|ed)?\s*at\s*(\d+)\s*°?C",
        "austenitizing_time": r"austenitiz(?:ation|ing|ed)?\s*[^\d]*\s*(\d+)\s*min",
        "isothermal_temperature": r"isothermal\s*(?:treatment|transformation)\s*at\s*(\d+)\s*°?C",
        "isothermal_time": r"isothermal\s*(?:treatment|transformation)\s*[^\d]*\s*(\d+)\s*min",
        "quenching_medium": r"quench(?:ing|ed)?\s*(?:in|to)?\s*(oil|water|air)",
        "tempering_temperature": r"temper(?:ing|ed)?\s*at\s*(\d+)\s*°?C",
        "tempering_time": r"temper(?:ing|ed)?\s*[^\d]*\s*(\d+)\s*(?:min|h)",
        "cooling_rate": r"cooling rate\s*[:=]?\s*([\d.]+)\s*°C/s",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        if key == "quenching_medium":
            heat_treatment[key] = match.group(1).lower()
        else:
            try:
                heat_treatment[key] = float(match.group(1))
            except ValueError:
                continue

    return heat_treatment


def extract_mechanical_properties(text: str) -> Dict[str, float | str]:
    """Extract mechanical property metrics from text."""

    properties: Dict[str, float | str] = {}
    patterns = {
        "tensile_strength": r"tensile strength\s*[:=]?\s*(\d+)\s*MPa",
        "yield_strength": r"yield strength\s*[:=]?\s*(\d+)\s*MPa",
        "elongation": r"elongation\s*[:=]?\s*([\d.]+)\s*%",
        "reduction_of_area": r"reduction of area\s*[:=]?\s*([\d.]+)\s*%",
        "hardness_value": r"hardness\s*[:=]?\s*(\d+)\s*(HV|HRC)",
        "impact_toughness": r"impact toughness\s*[:=]?\s*(\d+)\s*J",
        "fatigue_strength": r"fatigue strength\s*[:=]?\s*(\d+)\s*MPa",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            if key == "hardness_value":
                properties["hardness_value"] = float(match.group(1))
                properties["hardness_unit"] = match.group(2)
            else:
                properties[key] = float(match.group(1))
        except ValueError:
            continue

    return properties


def extract_microstructure(text: str) -> Dict[str, float]:
    """Extract microstructure-related fields from text."""

    microstructure: Dict[str, float] = {}
    patterns = {
        "bainite_fraction": r"bainite\s*fraction\s*[:=]?\s*([\d.]+)\s*%",
        "martensite_fraction": r"martensite\s*fraction\s*[:=]?\s*([\d.]+)\s*%",
        "ferrite_fraction": r"ferrite\s*fraction\s*[:=]?\s*([\d.]+)\s*%",
        "austenite_fraction": r"austenite\s*fraction\s*[:=]?\s*([\d.]+)\s*%",
        "grain_size": r"grain size\s*[:=]?\s*([\d.]+)\s*μm",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            microstructure[key] = float(match.group(1))
        except ValueError:
            continue

    return microstructure


def extract_steel_data_from_text(text: str) -> Dict[str, Dict[str, float]]:
    """Normalize text and extract structured data."""

    normalized_text = re.sub(r"-\s*\n", "", text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)

    return {
        "composition": extract_composition(normalized_text),
        "heat_treatment": extract_heat_treatment(normalized_text),
        "mechanical_properties": extract_mechanical_properties(normalized_text),
        "microstructure": extract_microstructure(normalized_text),
    }


def process_pdf(pdf_path: str) -> Optional[Dict[str, object]]:
    """Process a single PDF and return extracted data."""

    try:
        text = extract_text(pdf_path)
        steel_data = extract_steel_data_from_text(text)
        return {
            "file_path": pdf_path,
            "composition": steel_data["composition"],
            "heat_treatment": steel_data["heat_treatment"],
            "mechanical_properties": steel_data["mechanical_properties"],
            "microstructure": steel_data["microstructure"],
            "text_snippet": (text[:1000] + "...") if text else "",
        }
    except PDFSyntaxError as exc:
        logger.error("PDF解析错误: %s: %s", pdf_path, exc)
        return None
    except Exception as exc:  # pragma: no cover - protective logging
        logger.error("处理PDF失败: %s: %s", pdf_path, exc)
        return None


def process_papers_from_directory(papers_dir: str) -> List[Dict[str, object]]:
    """Iterate over PDF files in a directory and build a dataset."""

    dataset: List[Dict[str, object]] = []
    processed_files = 0
    pdf_files = [f for f in os.listdir(papers_dir) if f.lower().endswith(".pdf")]

    for filename in pdf_files:
        pdf_path = os.path.join(papers_dir, filename)
        logger.info("处理: %s", filename)
        paper_data = process_pdf(pdf_path)
        processed_files += 1

        if not paper_data:
            continue

        if any(paper_data[key] for key in ("composition", "heat_treatment", "mechanical_properties", "microstructure")):
            dataset.append(paper_data)
        else:
            logger.warning("未提取到数据: %s", filename)

    logger.info("成功处理 %s/%s 个文件", len(dataset), processed_files)
    return dataset


def _prepare_excel_sheets(dataset: List[Dict[str, object]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Prepare DataFrames for data, derived metrics, and quality metadata."""

    data_rows: List[Dict[str, object]] = []
    metrics_rows: List[Dict[str, object]] = []
    metadata_rows: List[Dict[str, object]] = []

    for item in dataset:
        file_name = os.path.basename(str(item.get("file_path", "")))
        data_rows.append(
            {
                "file": file_name,
                **{f"composition_{k}": v for k, v in (item.get("composition") or {}).items()},
                **{
                    f"heat_treatment_{k}": v
                    for k, v in (item.get("heat_treatment") or {}).items()
                    if k != "quenching_medium"
                },
                **{
                    f"mechanical_{k}": v
                    for k, v in (item.get("mechanical_properties") or {}).items()
                },
                **{
                    f"microstructure_{k}": v
                    for k, v in (item.get("microstructure") or {}).items()
                },
            }
        )

        if item.get("heat_treatment", {}).get("quenching_medium") is not None:
            data_rows[-1]["heat_treatment_quenching_medium"] = item["heat_treatment"]["quenching_medium"]

        metrics_rows.append(
            {
                "file": file_name,
                **(item.get("derived_metrics") or {}),
            }
        )

        metadata_rows.append(
            {
                "file": file_name,
                **(item.get("quality_metadata") or {}),
            }
        )

    data_df = pd.DataFrame(data_rows)
    metrics_df = pd.DataFrame(metrics_rows)
    metadata_df = pd.DataFrame(metadata_rows)

    return data_df, metrics_df, metadata_df


def save_steel_data(dataset: List[Dict[str, object]], output_dir: str) -> Tuple[str, str]:
    """Validate and persist the extracted dataset as JSON and Excel files."""

    os.makedirs(output_dir, exist_ok=True)

    validated_dataset, errors = validate_dataset(dataset)
    if errors:
        logger.warning("存在校验错误: %s", errors)

    json_path = os.path.join(output_dir, "steel_data.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(validated_dataset, fh, indent=2, ensure_ascii=False)

    excel_path = os.path.join(output_dir, "steel_data.xlsx")
    data_df, metrics_df, metadata_df = _prepare_excel_sheets(validated_dataset)

    with pd.ExcelWriter(excel_path) as writer:
        data_df.to_excel(writer, sheet_name="steel_data", index=False)
        metrics_df.to_excel(writer, sheet_name="derived_metrics", index=False)
        metadata_df.to_excel(writer, sheet_name="quality_metadata", index=False)

    return json_path, excel_path


__all__ = [
    "extract_composition",
    "extract_heat_treatment",
    "extract_mechanical_properties",
    "extract_microstructure",
    "extract_steel_data_from_text",
    "process_pdf",
    "process_papers_from_directory",
    "save_steel_data",
]

