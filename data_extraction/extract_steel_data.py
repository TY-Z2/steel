import os
import re
import json
import logging

import pandas as pd
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError

from data_extraction.table_data_processor import extract_data_from_tables
from parsers.pdf_table_extractor import extract_tables_from_pdf

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_composition(text):
    """从文本中提取化学成分"""
    composition = {}

    # 常见元素及其正则表达式模式
    elements = {
        "C": r'C[\s:]*([\d.]+)\s*%?',
        "Si": r'Si[\s:]*([\d.]+)\s*%?',
        "Mn": r'Mn[\s:]*([\d.]+)\s*%?',
        "Cr": r'Cr[\s:]*([\d.]+)\s*%?',
        "Mo": r'Mo[\s:]*([\d.]+)\s*%?',
        "Ni": r'Ni[\s:]*([\d.]+)\s*%?',
        "V": r'V[\s:]*([\d.]+)\s*%?',
        "Ti": r'Ti[\s:]*([\d.]+)\s*%?',
        "Al": r'Al[\s:]*([\d.]+)\s*%?',
        "Cu": r'Cu[\s:]*([\d.]+)\s*%?',
        "Nb": r'Nb[\s:]*([\d.]+)\s*%?',
        "B": r'B[\s:]*([\d.]+)\s*%?',
        "P": r'P[\s:]*([\d.]+)\s*%?',
        "S": r'S[\s:]*([\d.]+)\s*%?',
        "N": r'N[\s:]*([\d.]+)\s*%?'
    }

    # 尝试提取化学成分表
    table_pattern = r'composition[^.]*?(\bC\b[\s\S]*?)\n\n'
    table_match = re.search(table_pattern, text, re.IGNORECASE)
    table_text = table_match.group(1) if table_match else text

    for element, pattern in elements.items():
        matches = re.findall(pattern, table_text, re.IGNORECASE)
        if matches:
            # 取最后一个匹配值（通常最准确）
            try:
                value = float(matches[-1])
                if 0 < value < 100:  # 合理范围检查
                    composition[element] = value
            except ValueError:
                continue

    return composition


def extract_heat_treatment(text):
    """从文本中提取热处理工艺"""
    heat_treatment = {}

    # 热处理参数模式
    patterns = {
        "austenitizing_temperature": r'austenitiz(?:ation|ing|ed)?\s*at\s*(\d+)\s*°?C',
        "austenitizing_time": r'austenitiz(?:ation|ing|ed)?\s*[^\d]*\s*(\d+)\s*min',
        "isothermal_temperature": r'isothermal\s*(?:treatment|transformation)\s*at\s*(\d+)\s*°?C',
        "isothermal_time": r'isothermal\s*(?:treatment|transformation)\s*[^\d]*\s*(\d+)\s*min',
        "quenching_medium": r'quench(?:ing|ed)?\s*(?:in|to)?\s*(oil|water|air)',
        "tempering_temperature": r'temper(?:ing|ed)?\s*at\s*(\d+)\s*°?C',
        "tempering_time": r'temper(?:ing|ed)?\s*[^\d]*\s*(\d+)\s*(?:min|h)',
        "cooling_rate": r'cooling rate\s*[:=]?\s*([\d.]+)\s*°C/s'
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if key == "quenching_medium":
                heat_treatment[key] = match.group(1).lower()
            else:
                try:
                    value = float(match.group(1))
                    heat_treatment[key] = value
                except ValueError:
                    continue

    return heat_treatment


def extract_mechanical_properties(text):
    """从文本中提取力学性能"""
    properties = {}

    # 力学性能模式
    patterns = {
        "tensile_strength": r'tensile strength\s*[:=]?\s*(\d+)\s*MPa',
        "yield_strength": r'yield strength\s*[:=]?\s*(\d+)\s*MPa',
        "elongation": r'elongation\s*[:=]?\s*([\d.]+)\s*%',
        "reduction_of_area": r'reduction of area\s*[:=]?\s*([\d.]+)\s*%',
        "hardness_value": r'hardness\s*[:=]?\s*(\d+)\s*(HV|HRC)',
        "impact_toughness": r'impact toughness\s*[:=]?\s*(\d+)\s*J',
        "fatigue_strength": r'fatigue strength\s*[:=]?\s*(\d+)\s*MPa'
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                if key == "hardness_value":
                    properties["hardness_value"] = float(match.group(1))
                    properties["hardness_unit"] = match.group(2)
                else:
                    properties[key] = float(match.group(1))
            except ValueError:
                continue

    return properties


def extract_microstructure(text):
    """从文本中提取微观结构信息"""
    microstructure = {}

    # 微观结构模式
    patterns = {
        "bainite_fraction": r'bainite\s*fraction\s*[:=]?\s*([\d.]+)\s*%',
        "martensite_fraction": r'martensite\s*fraction\s*[:=]?\s*([\d.]+)\s*%',
        "ferrite_fraction": r'ferrite\s*fraction\s*[:=]?\s*([\d.]+)\s*%',
        "austenite_fraction": r'austenite\s*fraction\s*[:=]?\s*([\d.]+)\s*%',
        "grain_size": r'grain size\s*[:=]?\s*([\d.]+)\s*μm'
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                microstructure[key] = float(match.group(1))
            except ValueError:
                continue

    return microstructure


def extract_steel_data_from_text(text):
    """从文本中提取钢铁材料数据"""
    # 标准化文本：移除连字符和多余空格
    text = re.sub(r'-\s*\n', '', text)
    text = re.sub(r'\s+', ' ', text)

    return {
        "composition": extract_composition(text),
        "heat_treatment": extract_heat_treatment(text),
        "mechanical_properties": extract_mechanical_properties(text),
        "microstructure": extract_microstructure(text)
    }


def merge_with_preference(text_values, table_values, table_sources):
    """合并文本和表格提取结果，优先使用表格数值，并记录来源"""
    combined = dict(text_values)
    sources = {key: {"source": "text"} for key in text_values}

    for key, value in table_values.items():
        combined[key] = value
        source_info = {"source": "table"}
        if table_sources.get(key):
            source_info.update({k: v for k, v in table_sources[key].items() if v is not None})
        sources[key] = source_info

    return combined, sources


def process_pdf(pdf_path):
    """处理单个PDF文件并提取数据"""
    try:
        # 使用pdfminer提取文本
        text = extract_text(pdf_path)

        # 提取钢铁数据（文本）
        text_data = extract_steel_data_from_text(text)

        # 提取表格数据
        tables = extract_tables_from_pdf(pdf_path)
        table_data = extract_data_from_tables(tables)
        table_sources = table_data.get("sources", {"composition": {}, "process": {}, "properties": {}})

        # 合并数据，表格优先
        composition, composition_sources = merge_with_preference(
            text_data.get("composition", {}),
            table_data.get("composition", {}),
            table_sources.get("composition", {})
        )
        heat_treatment, heat_treatment_sources = merge_with_preference(
            text_data.get("heat_treatment", {}),
            table_data.get("process", {}),
            table_sources.get("process", {})
        )
        mechanical_properties, mechanical_sources = merge_with_preference(
            text_data.get("mechanical_properties", {}),
            table_data.get("properties", {}),
            table_sources.get("properties", {})
        )

        return {
            "file_path": pdf_path,
            "composition": composition,
            "composition_sources": composition_sources,
            "heat_treatment": heat_treatment,
            "heat_treatment_sources": heat_treatment_sources,
            "mechanical_properties": mechanical_properties,
            "mechanical_properties_sources": mechanical_sources,
            "microstructure": text_data.get("microstructure", {}),
            "microstructure_sources": {key: {"source": "text"} for key in text_data.get("microstructure", {})},
            "table_extraction": {
                "composition": table_data.get("composition", {}),
                "process": table_data.get("process", {}),
                "properties": table_data.get("properties", {}),
                "sources": table_sources,
            },
            "text_extraction": text_data,
            "text_snippet": text[:1000] + "..." if text else ""
        }
    except PDFSyntaxError as e:
        logger.error(f"PDF解析错误: {pdf_path}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"处理PDF失败: {pdf_path}: {str(e)}")
        return None


def process_papers_from_directory(papers_dir):
    """处理目录中的所有PDF文件"""
    dataset = []
    valid_files = 0

    pdf_files = [f for f in os.listdir(papers_dir) if f.endswith(".pdf")]

    for filename in pdf_files:
        pdf_path = os.path.join(papers_dir, filename)
        logger.info(f"处理: {filename}")

        paper_data = process_pdf(pdf_path)
        if paper_data:
            # 检查是否提取到任何数据
            if (paper_data["composition"] or
                    paper_data["heat_treatment"] or
                    paper_data["mechanical_properties"] or
                    paper_data["microstructure"]):
                dataset.append(paper_data)
                valid_files += 1
            else:
                logger.warning(f"未提取到数据: {filename}")

    total_files = len(pdf_files)
    logger.info(f"成功处理 {valid_files}/{total_files} 个文件")
    return dataset


def save_steel_data(dataset, output_dir):
    """保存提取的钢铁数据"""
    os.makedirs(output_dir, exist_ok=True)

    # 保存为JSON
    json_path = os.path.join(output_dir, "steel_data.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)

    # 保存为Excel
    excel_path = os.path.join(output_dir, "steel_data.xlsx")

    # 准备Excel数据
    excel_data = []
    for item in dataset:
        row = {
            "file": os.path.basename(item["file_path"]),
        }

        category_mappings = [
            ("composition", "composition"),
            ("heat_treatment", "heat_treatment"),
            ("mechanical_properties", "mechanical"),
            ("microstructure", "microstructure"),
        ]

        for category, prefix in category_mappings:
            values = item.get(category, {})
            for key, value in values.items():
                row[f"{prefix}_{key}"] = value

            sources = item.get(f"{category}_sources", {})
            for key, source_info in sources.items():
                row[f"{prefix}_{key}_source"] = source_info.get("source")
                if source_info.get("page_number") is not None:
                    row[f"{prefix}_{key}_page"] = source_info.get("page_number")
                if source_info.get("caption"):
                    row[f"{prefix}_{key}_caption"] = source_info.get("caption")

        table_extraction = item.get("table_extraction", {})
        table_sources = table_extraction.get("sources", {})

        table_category_mappings = [
            ("composition", "table_composition"),
            ("process", "table_process"),
            ("properties", "table_properties"),
        ]

        for category, prefix in table_category_mappings:
            for key, value in table_extraction.get(category, {}).items():
                row[f"{prefix}_{key}"] = value

            for key, source_info in table_sources.get(category, {}).items():
                if source_info.get("page_number") is not None:
                    row[f"{prefix}_{key}_page"] = source_info.get("page_number")
                if source_info.get("caption"):
                    row[f"{prefix}_{key}_caption"] = source_info.get("caption")

        excel_data.append(row)

    # 转换为DataFrame并保存
    if excel_data:
        df = pd.DataFrame(excel_data)
        df.to_excel(excel_path, index=False)
    else:
        logger.warning("没有可保存的数据")

    return json_path, excel_path
