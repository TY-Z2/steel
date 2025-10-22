# steel_miner/data_extraction/table_data_processor.py
import pandas as pd
import re
from parsers.table_extractor import is_materials_table

ELEMENT_PATTERNS = {
    "C": r"\bC\b|Carbon",
    "Si": r"\bSi\b|Silicon",
    "Mn": r"\bMn\b|Manganese",
    "Ni": r"\bNi\b|Nickel",
    "Cr": r"\bCr\b|Chromium",
    "Mo": r"\bMo\b|Molybdenum",
    "Al": r"\bAl\b|Aluminum",
    "V": r"\bV\b|Vanadium",
    "B": r"\bB\b|Boron",
    "Co": r"\bCo\b|Cobalt"
}

PROPERTY_PATTERNS = {
    "YS": r"Yield\s*Strength|YS|\σ_y",
    "UTS": r"Tensile\s*Strength|UTS|Ultimate\s*Strength|\σ_u",
    "EL": r"Elongation|EL|\δ",
    "HRC": r"Hardness|HRC",
    "impact_toughness": r"Impact\s*Toughness|CVN|Charpy"
}

HEAT_TREATMENT_PATTERNS = {
    "T1": r"Austenitization|T1|Heating\s*Temperature",
    "t1": r"Austenitization\s*Time|t1|Holding\s*Time",
    "T2": r"Isothermal\s*Temperature|T2|Bainite\s*Transformation",
    "Δt2": r"ΔT|Ms\s*difference",
    "t2": r"Isothermal\s*Time|t2",
    "T3": r"Tempering\s*Temperature|T3",
    "t3": r"Tempering\s*Time|t3"
}


def process_table(table_data):
    """处理单个表格数据，提取有用信息"""
    results = {
        "composition": {},
        "process": {},
        "properties": {}
    }

    if not table_data["data"]:
        return results

    # 将表格数据转换为DataFrame
    df = pd.DataFrame(table_data["data"][1:], columns=table_data["data"][0])

    # 1. 识别列类型
    col_types = {}
    for col in df.columns:
        col_text = str(col).lower()

        # 检查是否为元素列
        for element, pattern in ELEMENT_PATTERNS.items():
            if re.search(pattern, col_text, re.IGNORECASE):
                col_types[col] = ("composition", element)
                break

        # 检查是否为性能列
        if not col_types.get(col):
            for prop, pattern in PROPERTY_PATTERNS.items():
                if re.search(pattern, col_text, re.IGNORECASE):
                    col_types[col] = ("properties", prop)
                    break

        # 检查是否为工艺参数列
        if not col_types.get(col):
            for param, pattern in HEAT_TREATMENT_PATTERNS.items():
                if re.search(pattern, col_text, re.IGNORECASE):
                    col_types[col] = ("process", param)
                    break

    # 2. 提取数据
    for col, (category, key) in col_types.items():
        for value in df[col]:
            # 跳过空值
            if pd.isna(value) or value == "":
                continue

            # 提取数值部分
            num_match = re.search(r"(\d+\.?\d*)", str(value))
            if num_match:
                num_value = float(num_match.group(1))

                if category == "composition":
                    results["composition"][key] = num_value
                elif category == "process":
                    results["process"][key] = num_value
                elif category == "properties":
                    results["properties"][key] = num_value

    return results


def extract_data_from_tables(tables):
    """从所有表格中提取材料数据"""
    all_results = {
        "composition": {},
        "process": {},
        "properties": {}
    }

    for table in tables:
        if is_materials_table(table):
            table_results = process_table(table)

            # 合并结果
            for category in ["composition", "process", "properties"]:
                for key, value in table_results[category].items():
                    all_results[category][key] = value

    return all_results