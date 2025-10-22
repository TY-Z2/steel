import os
import re
import json
from typing import Any, Dict, List, Tuple

import pandas as pd
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError
import logging

from parsers.pdf_table_extractor import extract_tables_from_pdf
from data_extraction.table_data_processor import extract_data_from_tables

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 词表与正则常量
# --------------------
ELEMENT_SYNONYMS: Dict[str, List[str]] = {
    "C": [r"\\bC\\b", "Carbon"],
    "Si": [r"\\bSi\\b", "Silicon"],
    "Mn": [r"\\bMn\\b", "Manganese"],
    "Cr": [r"\\bCr\\b", "Chromium"],
    "Mo": [r"\\bMo\\b", "Molybdenum"],
    "Ni": [r"\\bNi\\b", "Nickel"],
    "V": [r"\\bV\\b", "Vanadium"],
    "Ti": [r"\\bTi\\b", "Titanium"],
    "Al": [r"\\bAl\\b", "Aluminum", "Aluminium"],
    "Cu": [r"\\bCu\\b", "Copper"],
    "Nb": [r"\\bNb\\b", "Niobium"],
    "B": [r"\\bB\\b", "Boron"],
    "P": [r"\\bP\\b", "Phosphorus"],
    "S": [r"\\bS\\b", "Sulfur", "Sulphur"],
    "N": [r"\\bN\\b", "Nitrogen"],
    "W": [r"\\bW\\b", "Tungsten"],
    "Co": [r"\\bCo\\b", "Cobalt"],
    "Fe": [r"\\bFe\\b", "Iron"],
    "Zr": [r"\\bZr\\b", "Zirconium"],
    "Ta": [r"\\bTa\\b", "Tantalum"],
    "Mg": [r"\\bMg\\b", "Magnesium"],
    "Ca": [r"\\bCa\\b", "Calcium"],
    "La": [r"\\bLa\\b", "Lanthanum"],
    "Ce": [r"\\bCe\\b", "Cerium"],
}

ELEMENT_UNIT_REGEX = r"wt\\.?\\s*%|mass\\.?\\s*%|weight\\s*%|vol\\.?\\s*%|体积分数|质量分数|wt%|mass%|%"
VALUE_RANGE_PATTERN = r"(?P<value>\\d+(?:\\.\\d+)?)(?:\\s*[-–~]\\s*(?P<upper>\\d+(?:\\.\\d+)?))?"

QUENCHING_KEYWORDS: Dict[str, List[str]] = {
    "water": [r"water", r"H2O", r"water bath"],
    "oil": [r"oil", r"oil bath"],
    "air": [r"air", r"air cooling", r"air-cooled"],
    "salt bath": [r"salt bath", r"nitrate bath", r"salt solution"],
    "polymer": [r"polymer", r"PAG", r"polyalkylene glycol"],
}

STRESS_UNIT_FACTORS: Dict[str, float] = {
    "mpa": 1.0,
    "gpa": 1000.0,
    "pa": 1e-6,
    "n/mm2": 1.0,
    "n/mm^2": 1.0,
    "n/mm²": 1.0,
    "nmm-2": 1.0,
    "nmm^-2": 1.0,
    "kg/mm2": 9.80665,
    "kgf/mm2": 9.80665,
    "kg/mm^2": 9.80665,
    "ksi": 6.89476,
    "psi": 0.00689476,
}

HEAT_TREATMENT_PATTERNS: Dict[str, Dict[str, Any]] = {
    "austenitizing_temperature": {
        "unit_type": "temperature",
        "patterns": [
            rf"(?:austenitiz(?:ation|ing|ed)|solution(?: |-)?treated|solution treatment|solutionized)\\s*(?:at|temperature(?:\\s*of)?|temp\\.?|temperature:)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>\\u00b0C|\\u2103|C|℃|K|Kelvin|\\u00b0F|F|Fahrenheit)?",
            rf"(?:heated|heating)\\s*(?:to|at)\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>\\u00b0C|\\u2103|C|℃|K|Kelvin|\\u00b0F|F|Fahrenheit)?\\s*(?:for|prior to)?\\s*(?:austenitiz|solution)",
        ],
    },
    "austenitizing_time": {
        "unit_type": "time",
        "patterns": [
            rf"(?:austenitiz(?:ation|ing|ed)|solution(?: |-)?treated|solution treatment|solutionized)\\s*(?:for|held for|time(?:\\s*of)?|duration(?:\\s*of)?)\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>h|hr|hrs|hour|hours|min|mins|minute|minutes|s|sec|secs|second|seconds)",
        ],
    },
    "isothermal_temperature": {
        "unit_type": "temperature",
        "patterns": [
            rf"(?:isothermal|bainit(?:e|ic)\\s*(?:treatment|holding|transformation)|austemper(?:ed|ing))\\s*(?:at|temperature(?:\\s*of)?|temp\\.?|temperature:)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>\\u00b0C|\\u2103|C|℃|K|Kelvin|\\u00b0F|F|Fahrenheit)?",
        ],
    },
    "isothermal_time": {
        "unit_type": "time",
        "patterns": [
            rf"(?:isothermal|bainit(?:e|ic)\\s*(?:treatment|holding|transformation)|austemper(?:ed|ing))\\s*(?:for|held for|time(?:\\s*of)?|duration(?:\\s*of)?)\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>h|hr|hrs|hour|hours|min|mins|minute|minutes|s|sec|secs|second|seconds)",
        ],
    },
    "tempering_temperature": {
        "unit_type": "temperature",
        "patterns": [
            rf"(?:temper(?:ed|ing)|aging|aged|anneal(?:ed|ing))\\s*(?:at|temperature(?:\\s*of)?|temp\\.?|temperature:)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>\\u00b0C|\\u2103|C|℃|K|Kelvin|\\u00b0F|F|Fahrenheit)?",
        ],
    },
    "tempering_time": {
        "unit_type": "time",
        "patterns": [
            rf"(?:temper(?:ed|ing)|aging|aged|anneal(?:ed|ing))\\s*(?:for|held for|time(?:\\s*of)?|duration(?:\\s*of)?)\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>h|hr|hrs|hour|hours|min|mins|minute|minutes|s|sec|secs|second|seconds)",
        ],
    },
    "cooling_rate": {
        "unit_type": "rate",
        "patterns": [
            rf"cool(?:ing)?\\s*(?:rate|speed)?\\s*(?:of|:)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>[\\u00b0C\\u2103CKF\\/\\-\\sperminsec\.]+)",
        ],
    },
}

MECHANICAL_PROPERTY_PATTERNS: Dict[str, Dict[str, Any]] = {
    "yield_strength": {
        "unit_type": "stress",
        "patterns": [
            rf"(?:yield(?:\\s*(?:strength|stress))?|0\\.?2\\s*%?\\s*(?:proof|offset)\\s*strength|proof\\s*stress|Rp0\\.?2|\\u03c30\\.?2|\\u03c3_y|Re0\\.?2)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>MPa|GPa|N/mm\\u00b2|N/mm2|N\\s*/\\s*mm2|kgf/mm2|kg/mm2|ksi|psi)?",
        ],
    },
    "tensile_strength": {
        "unit_type": "stress",
        "patterns": [
            rf"(?:ultimate\\s*tensile\\s*strength|UTS|tensile\\s*strength|Rm|\\u03c3_u|\\u03c3_b)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>MPa|GPa|N/mm\\u00b2|N/mm2|N\\s*/\\s*mm2|kgf/mm2|kg/mm2|ksi|psi)?",
        ],
    },
    "elongation": {
        "unit_type": "percent",
        "patterns": [
            rf"(?:elongation|EL|El\\.?|\\u03b4|percent\\s*elongation|elongation\\s*to\\s*fracture)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>%|percent|pct)?",
        ],
    },
    "reduction_of_area": {
        "unit_type": "percent",
        "patterns": [
            rf"(?:reduction\\s*of\\s*area|RA|\\u03c8)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>%|percent|pct)?",
        ],
    },
    "impact_toughness": {
        "unit_type": "impact",
        "patterns": [
            rf"(?:impact\\s*(?:toughness|energy)|Charpy|CVN|KCV|KCU)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>J/cm2|J/cm\\^2|J|kJ/m2|kJ/m\\^2|kJ\\s*/\\s*m2|ft\\.?-?lb|ft\\.?\\s*lbf)?",
        ],
    },
    "fatigue_strength": {
        "unit_type": "stress",
        "patterns": [
            rf"(?:fatigue\\s*(?:strength|limit)|\\u03c3-1|endurance\\s*limit)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>MPa|GPa|N/mm\\u00b2|N/mm2|N\\s*/\\s*mm2|kgf/mm2|kg/mm2|ksi|psi)?",
        ],
    },
    "hardness_value": {
        "unit_type": "hardness",
        "patterns": [
            rf"(?:hardness|HV|HRC|HRB|HBW|Brinell|Vickers|Rockwell)\\s*(?:[:=]|is|of)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>HV\\d*|HRC|HRB|HBW|HB|HV|VHN|HR15N|HR15T)?",
        ],
    },
}

# --------------------
# 工具函数
# --------------------

def normalize_text(text: str) -> str:
    """对原始PDF文本做基本清洗，减少换行和特殊字符影响"""
    if not text:
        return ""

    normalized = text.replace("\r", " ")
    normalized = normalized.replace("℃", "°C").replace("ºC", "°C").replace("℉", "°F")
    normalized = normalized.replace("–", "-").replace("—", "-")
    normalized = re.sub(r"-\\s*\n", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def parse_numeric_range(value: str, upper: str = None) -> float:
    """根据上下限返回代表数值，若给出区间则取区间平均"""
    try:
        base = float(value)
    except (TypeError, ValueError):
        return None

    if upper:
        try:
            upper_value = float(upper)
            return (base + upper_value) / 2.0
        except ValueError:
            pass
    return base


def normalize_unit_text(unit: str) -> str:
    if not unit:
        return ""
    cleaned = unit.replace("²", "2").replace("·", "").replace("^", "")
    cleaned = cleaned.replace("\\", "/").replace(" ", "")
    cleaned = cleaned.replace("per", "/")
    cleaned = cleaned.replace("minute", "min").replace("minutes", "min").replace("mins", "min")
    cleaned = cleaned.replace("second", "s").replace("seconds", "s").replace("secs", "s")
    cleaned = cleaned.lower()
    return cleaned


def convert_temperature(value: float, unit: str) -> Tuple[float, str]:
    unit_norm = normalize_unit_text(unit)
    if unit_norm in {"", "c", "°c", "℃"}:
        return value, "°C"
    if unit_norm in {"k"}:
        return value - 273.15, "°C"
    if unit_norm in {"°f", "f", "fahrenheit"}:
        return (value - 32) * 5.0 / 9.0, "°C"
    return value, "°C"


def convert_time(value: float, unit: str) -> Tuple[float, str]:
    unit_norm = normalize_unit_text(unit)
    if unit_norm in {"h", "hr", "hrs", "hour", "hours"}:
        return value * 60.0, "min"
    if unit_norm in {"s", "sec"}:
        return value / 60.0, "min"
    return value, "min"


def convert_cooling_rate(value: float, unit: str) -> Tuple[float, str]:
    unit_norm = normalize_unit_text(unit)
    if not unit_norm:
        return value, "°C/s"
    if unit_norm in {"k/s", "k/s", "k s-1", "ks-1"}:
        return value, "°C/s"
    if unit_norm in {"°c/s", "c/s"}:
        return value, "°C/s"
    if unit_norm in {"°f/s", "f/s"}:
        return value * 5.0 / 9.0, "°C/s"
    if unit_norm in {"°c/min", "c/min"}:
        return value / 60.0, "°C/s"
    if unit_norm in {"k/min"}:
        return value / 60.0, "°C/s"
    return value, unit or "°C/s"


def convert_stress(value: float, unit: str) -> Tuple[float, str]:
    unit_norm = normalize_unit_text(unit)
    if not unit_norm:
        return value, "MPa"
    factor = STRESS_UNIT_FACTORS.get(unit_norm)
    if factor is None and unit_norm.endswith("mpa"):
        factor = 1.0
    if factor is None and unit_norm.endswith("gpa"):
        factor = 1000.0
    if factor is None and unit_norm in {"n/m2", "n/m^2", "pa"}:
        factor = STRESS_UNIT_FACTORS.get("pa")
    if factor is None:
        factor = 1.0
    return value * factor, "MPa"


def convert_impact_energy(value: float, unit: str) -> Tuple[float, str]:
    unit_norm = normalize_unit_text(unit)
    if not unit_norm or unit_norm in {"j", "joule", "joules"}:
        return value, "J"
    if unit_norm in {"kj", "kilojoule", "kilojoules"}:
        return value * 1000.0, "J"
    if unit_norm in {"mj"}:
        return value * 1_000_000.0, "J"
    if unit_norm in {"j/cm2", "j/cm^2"}:
        return value, "J/cm^2"
    if unit_norm in {"kj/m2", "kj/m^2", "kj/m-2", "kj/m2"}:
        return value, "kJ/m^2"
    if unit_norm in {"ft-lb", "ftlb", "ft.lb", "ftlbf"}:
        return value * 1.35582, "J"
    return value, unit or "J"


def normalize_hardness_unit(unit: str) -> str:
    if not unit:
        return "HV"
    unit_norm = unit.strip().upper()
    # 常见附带载荷的Vickers硬度格式，如HV10
    if unit_norm.startswith("HV"):
        return unit_norm
    if unit_norm in {"HRC", "HRB", "HBW", "HB", "HV", "VHN", "HR15N", "HR15T"}:
        return unit_norm
    return unit_norm


def convert_by_unit_type(value: float, unit: str, unit_type: str, default_unit: str = "") -> Tuple[float, str]:
    if unit_type == "temperature":
        return convert_temperature(value, unit or default_unit)
    if unit_type == "time":
        return convert_time(value, unit or default_unit)
    if unit_type == "rate":
        return convert_cooling_rate(value, unit or default_unit)
    if unit_type == "stress":
        return convert_stress(value, unit or default_unit)
    if unit_type == "percent":
        return value, "%"
    if unit_type == "impact":
        return convert_impact_energy(value, unit or default_unit)
    if unit_type == "hardness":
        return value, normalize_hardness_unit(unit or default_unit or "HV")
    return value, unit or default_unit


def build_source_map(data: Dict[str, Dict[str, Any]], source_label: str) -> Dict[str, Dict[str, str]]:
    sources: Dict[str, Dict[str, str]] = {}
    for category, values in data.items():
        category_sources: Dict[str, str] = {}
        for key in values.keys():
            category_sources[key] = source_label
        sources[category] = category_sources
    return sources


# --------------------
# 文本抽取
# --------------------

def extract_composition(text: str) -> Dict[str, float]:
    """从文本中提取化学成分，支持更多同义词和单位写法"""
    composition: Dict[str, float] = {}

    for element, patterns in ELEMENT_SYNONYMS.items():
        best_value = None
        for pattern in patterns:
            alias_pattern = f"(?:{pattern})"
            forward_regex = re.compile(
                rf"{alias_pattern}(?:\\s*(?:content|含量|{ELEMENT_UNIT_REGEX}))?\\s*(?:[:=]|is|为|约|about)?\\s*{VALUE_RANGE_PATTERN}\\s*(?P<unit>{ELEMENT_UNIT_REGEX})?",
                re.IGNORECASE,
            )
            backward_regex = re.compile(
                rf"{VALUE_RANGE_PATTERN}\\s*(?P<unit>{ELEMENT_UNIT_REGEX})?\\s*(?:of\\s*)?{alias_pattern}",
                re.IGNORECASE,
            )

            for regex in (forward_regex, backward_regex):
                for match in regex.finditer(text):
                    value = parse_numeric_range(match.group("value"), match.group("upper"))
                    if value is None:
                        continue
                    if 0 < value <= 100:
                        best_value = value
        if best_value is not None:
            composition[element] = best_value

    return composition


def extract_quenching_medium(text: str) -> str:
    for medium, keywords in QUENCHING_KEYWORDS.items():
        for keyword in keywords:
            pattern = rf"quench(?:ed|ing)?[^.]*{keyword}"
            if re.search(pattern, text, re.IGNORECASE):
                return medium
    return ""


def extract_heat_treatment(text: str) -> Dict[str, Any]:
    """提取热处理参数并进行单位归一化"""
    heat_treatment: Dict[str, Any] = {}

    for key, descriptor in HEAT_TREATMENT_PATTERNS.items():
        unit_type = descriptor.get("unit_type", "")
        patterns = descriptor.get("patterns", [])
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.search(text)
            if match:
                value = parse_numeric_range(match.group("value"), match.group("upper"))
                if value is None:
                    continue
                normalized_value, normalized_unit = convert_by_unit_type(
                    value, match.groupdict().get("unit"), unit_type
                )
                heat_treatment[key] = normalized_value
                if normalized_unit:
                    heat_treatment[f"{key}_unit"] = normalized_unit
                break

    quenching_medium = extract_quenching_medium(text)
    if quenching_medium:
        heat_treatment["quenching_medium"] = quenching_medium

    return heat_treatment


def extract_mechanical_properties(text: str) -> Dict[str, Any]:
    """提取力学性能并统一单位"""
    properties: Dict[str, Any] = {}

    for key, descriptor in MECHANICAL_PROPERTY_PATTERNS.items():
        unit_type = descriptor.get("unit_type", "")
        patterns = descriptor.get("patterns", [])
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.search(text)
            if match:
                value = parse_numeric_range(match.group("value"), match.group("upper"))
                if value is None:
                    continue
                normalized_value, normalized_unit = convert_by_unit_type(
                    value, match.groupdict().get("unit"), unit_type
                )
                properties[key] = normalized_value
                if key == "hardness_value":
                    properties["hardness_unit"] = normalized_unit
                elif key == "impact_toughness":
                    properties["impact_unit"] = normalized_unit
                else:
                    unit_key = f"{key}_unit"
                    properties[unit_key] = normalized_unit
                break

    return properties


def extract_microstructure(text: str) -> Dict[str, float]:
    """提取简单的组织学指标"""
    microstructure: Dict[str, float] = {}

    patterns = {
        "bainite_fraction": r"bainite\\s*(?:fraction|content)\\s*[:=]?\\s*([\\d.]+)\\s*%",
        "martensite_fraction": r"martensite\\s*(?:fraction|content)\\s*[:=]?\\s*([\\d.]+)\\s*%",
        "ferrite_fraction": r"ferrite\\s*(?:fraction|content)\\s*[:=]?\\s*([\\d.]+)\\s*%",
        "austenite_fraction": r"retained\\s*austenite\\s*(?:fraction|content)?\\s*[:=]?\\s*([\\d.]+)\\s*%",
        "grain_size": r"grain\\s*size\\s*[:=]?\\s*([\\d.]+)\\s*(?:μm|um)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                microstructure[key] = float(match.group(1))
            except ValueError:
                continue

    return microstructure


def extract_steel_data_from_text(text: str) -> Dict[str, Dict[str, Any]]:
    """从纯文本提取钢铁材料数据"""
    normalized_text = normalize_text(text)

    return {
        "composition": extract_composition(normalized_text),
        "heat_treatment": extract_heat_treatment(normalized_text),
        "mechanical_properties": extract_mechanical_properties(normalized_text),
        "microstructure": extract_microstructure(normalized_text),
    }


# --------------------
# 表格结果整合
# --------------------

def map_table_process_data(process_data: Dict[str, float]) -> Dict[str, Any]:
    mapping = {
        "T1": ("austenitizing_temperature", "temperature", "°C"),
        "t1": ("austenitizing_time", "time", "min"),
        "T2": ("isothermal_temperature", "temperature", "°C"),
        "t2": ("isothermal_time", "time", "min"),
        "T3": ("tempering_temperature", "temperature", "°C"),
        "t3": ("tempering_time", "time", "min"),
        "Δt2": ("delta_t2", "temperature", "°C"),
    }

    results: Dict[str, Any] = {}
    for key, value in process_data.items():
        target = mapping.get(key)
        if not target:
            continue
        target_key, unit_type, default_unit = target
        normalized_value, normalized_unit = convert_by_unit_type(value, default_unit, unit_type, default_unit)
        results[target_key] = normalized_value
        if normalized_unit:
            results[f"{target_key}_unit"] = normalized_unit
    return results


def map_table_property_data(property_data: Dict[str, float]) -> Dict[str, Any]:
    mapping = {
        "YS": ("yield_strength", "stress", "MPa"),
        "UTS": ("tensile_strength", "stress", "MPa"),
        "EL": ("elongation", "percent", "%"),
        "HRC": ("hardness_value", "hardness", "HRC"),
        "impact_toughness": ("impact_toughness", "impact", "J"),
    }

    results: Dict[str, Any] = {}
    for key, value in property_data.items():
        target = mapping.get(key)
        if not target:
            continue
        target_key, unit_type, default_unit = target
        normalized_value, normalized_unit = convert_by_unit_type(value, default_unit, unit_type, default_unit)
        results[target_key] = normalized_value
        if target_key == "hardness_value":
            results["hardness_unit"] = normalized_unit
        elif target_key == "impact_toughness":
            results["impact_unit"] = normalized_unit
        else:
            results[f"{target_key}_unit"] = normalized_unit
    return results


def extract_table_driven_data(pdf_path: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, str]], List[str]]:
    tables = extract_tables_from_pdf(pdf_path)
    if not tables:
        return {
            "composition": {},
            "heat_treatment": {},
            "mechanical_properties": {},
        }, {}, []

    table_results = extract_data_from_tables(tables)
    mapped_results = {
        "composition": table_results.get("composition", {}),
        "heat_treatment": map_table_process_data(table_results.get("process", {})),
        "mechanical_properties": map_table_property_data(table_results.get("properties", {})),
    }

    sources = build_source_map(mapped_results, "table")
    table_captions = [table.get("caption", "") for table in tables]
    return mapped_results, sources, table_captions


def merge_data_sources(
    text_data: Dict[str, Dict[str, Any]],
    text_sources: Dict[str, Dict[str, str]],
    table_data: Dict[str, Dict[str, Any]],
    table_sources: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, str]]]:
    merged_data: Dict[str, Dict[str, Any]] = {}
    merged_sources: Dict[str, Dict[str, str]] = {}

    categories = {"composition", "heat_treatment", "mechanical_properties", "microstructure"}
    for category in categories:
        merged_category: Dict[str, Any] = {}
        merged_category_sources: Dict[str, str] = {}

        text_category = text_data.get(category, {})
        table_category = table_data.get(category, {})

        # 先写入文本结果
        for key, value in text_category.items():
            merged_category[key] = value
            if text_sources.get(category):
                merged_category_sources[key] = text_sources[category].get(key, "text")
            else:
                merged_category_sources[key] = "text"

        # 表格结果覆盖或新增
        for key, value in table_category.items():
            merged_category[key] = value
            if table_sources.get(category):
                merged_category_sources[key] = table_sources[category].get(key, "table")
            else:
                merged_category_sources[key] = "table"

        merged_data[category] = merged_category
        merged_sources[category] = merged_category_sources

    return merged_data, merged_sources


# --------------------
# 数据质量检查
# --------------------

def validate_steel_data(steel_data: Dict[str, Dict[str, Any]]) -> List[str]:
    warnings: List[str] = []

    composition = steel_data.get("composition", {})
    if composition:
        total = sum(v for v in composition.values() if isinstance(v, (int, float)))
        if total > 100.0:
            warnings.append(f"Composition sum exceeds 100 wt.% ({total:.2f}).")
        for element, value in composition.items():
            if value <= 0 or value > 100:
                warnings.append(f"Composition value for {element} out of range: {value}.")

    heat_treatment = steel_data.get("heat_treatment", {})
    temperature_keys = ["austenitizing_temperature", "isothermal_temperature", "tempering_temperature"]
    for key in temperature_keys:
        if key in heat_treatment and isinstance(heat_treatment[key], (int, float)):
            value = heat_treatment[key]
            if not 100 <= value <= 1500:
                warnings.append(f"Heat-treatment temperature '{key}' has unusual value: {value} °C.")

    time_keys = ["austenitizing_time", "isothermal_time", "tempering_time"]
    for key in time_keys:
        if key in heat_treatment and isinstance(heat_treatment[key], (int, float)):
            value = heat_treatment[key]
            if value <= 0 or value > 10_000:
                warnings.append(f"Heat-treatment time '{key}' has unusual value: {value} min.")

    if "cooling_rate" in heat_treatment and isinstance(heat_treatment["cooling_rate"], (int, float)):
        value = heat_treatment["cooling_rate"]
        if value <= 0 or value > 1_000:
            warnings.append(f"Cooling rate appears unusual: {value} °C/s.")

    mechanical = steel_data.get("mechanical_properties", {})
    if "yield_strength" in mechanical and "tensile_strength" in mechanical:
        y = mechanical["yield_strength"]
        t = mechanical["tensile_strength"]
        if isinstance(y, (int, float)) and isinstance(t, (int, float)) and y > t:
            warnings.append("Yield strength exceeds tensile strength, please verify.")

    stress_keys = ["yield_strength", "tensile_strength", "fatigue_strength"]
    for key in stress_keys:
        if key in mechanical and isinstance(mechanical[key], (int, float)):
            value = mechanical[key]
            if value <= 0 or value > 5_000:
                warnings.append(f"Mechanical property '{key}' has unusual value: {value} MPa.")

    percent_keys = ["elongation", "reduction_of_area"]
    for key in percent_keys:
        if key in mechanical and isinstance(mechanical[key], (int, float)):
            value = mechanical[key]
            if value < 0 or value > 100:
                warnings.append(f"Mechanical property '{key}' has unusual value: {value} %.")

    if "hardness_value" in mechanical and isinstance(mechanical["hardness_value"], (int, float)):
        value = mechanical["hardness_value"]
        if value <= 0 or value > 1_500:
            warnings.append(f"Hardness value appears unusual: {value}.")

    if "impact_toughness" in mechanical and isinstance(mechanical["impact_toughness"], (int, float)):
        value = mechanical["impact_toughness"]
        if value <= 0:
            warnings.append("Impact toughness is non-positive, please verify.")

    return warnings


# --------------------
# 主流程
# --------------------

def process_pdf(pdf_path: str) -> Dict[str, Any]:
    """处理单个PDF文件并提取数据，同时整合表格信息"""
    try:
        text = extract_text(pdf_path)
    except PDFSyntaxError as exc:
        logger.error(f"PDF解析错误: {pdf_path}: {exc}")
        return None
    except Exception as exc:
        logger.error(f"处理PDF失败: {pdf_path}: {exc}")
        return None

    text_data = extract_steel_data_from_text(text)
    text_sources = build_source_map(text_data, "text")

    table_data, table_sources, table_captions = extract_table_driven_data(pdf_path)

    merged_data, merged_sources = merge_data_sources(text_data, text_sources, table_data, table_sources)
    warnings = validate_steel_data(merged_data)

    return {
        "file_path": pdf_path,
        "composition": merged_data.get("composition", {}),
        "heat_treatment": merged_data.get("heat_treatment", {}),
        "mechanical_properties": merged_data.get("mechanical_properties", {}),
        "microstructure": merged_data.get("microstructure", {}),
        "sources": merged_sources,
        "warnings": warnings,
        "table_summary": table_captions,
        "text_snippet": (text[:1000] + "...") if text else "",
    }


def process_papers_from_directory(papers_dir: str) -> List[Dict[str, Any]]:
    """处理目录中的所有PDF文件"""
    dataset: List[Dict[str, Any]] = []
    valid_files = 0
    total_files = 0

    for filename in os.listdir(papers_dir):
        if not filename.lower().endswith(".pdf"):
            continue
        total_files += 1
        pdf_path = os.path.join(papers_dir, filename)
        logger.info(f"处理: {filename}")

        paper_data = process_pdf(pdf_path)
        if not paper_data:
            continue

        if (
            paper_data["composition"]
            or paper_data["heat_treatment"]
            or paper_data["mechanical_properties"]
            or paper_data["microstructure"]
        ):
            dataset.append(paper_data)
            valid_files += 1
        else:
            logger.warning(f"未提取到数据: {filename}")

    if total_files:
        logger.info(f"成功提取数据 {valid_files}/{total_files} 个文件")
    else:
        logger.info("未在目录中找到PDF文件")

    return dataset


def save_steel_data(dataset: List[Dict[str, Any]], output_dir: str) -> Tuple[str, str]:
    """保存提取的钢铁数据为 JSON 和 Excel"""
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, "steel_data.json")
    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(dataset, file, indent=2, ensure_ascii=False)

    excel_path = os.path.join(output_dir, "steel_data.xlsx")

    excel_rows: List[Dict[str, Any]] = []
    for item in dataset:
        row: Dict[str, Any] = {
            "file": os.path.basename(item.get("file_path", "")),
        }

        for prefix, data in [
            ("composition", item.get("composition", {})),
            ("heat_treatment", item.get("heat_treatment", {})),
            ("mechanical", item.get("mechanical_properties", {})),
            ("microstructure", item.get("microstructure", {})),
        ]:
            for key, value in data.items():
                row[f"{prefix}_{key}"] = value

        sources = item.get("sources", {})
        for category, mapping in sources.items():
            for key, source_label in mapping.items():
                row[f"source_{category}_{key}"] = source_label

        warnings = item.get("warnings", [])
        if warnings:
            row["warnings"] = "; ".join(warnings)

        table_summary = item.get("table_summary", [])
        if table_summary:
            row["table_summary"] = "; ".join(summary for summary in table_summary if summary)

        composition_total = sum(
            value for value in item.get("composition", {}).values() if isinstance(value, (int, float))
        )
        if composition_total:
            row["composition_total"] = composition_total

        excel_rows.append(row)

    if excel_rows:
        df = pd.DataFrame(excel_rows)
        df.to_excel(excel_path, index=False)
    else:
        logger.warning("没有可保存的数据")

    return json_path, excel_path
