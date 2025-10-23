import os
import re
import json
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - 允许在无 pandas 环境下运行部分功能
    pd = None
try:
    from pdfminer.high_level import extract_text  # type: ignore
    from pdfminer.pdfparser import PDFSyntaxError  # type: ignore
except Exception:  # pragma: no cover - 在测试环境缺失 pdfminer 时提供兜底
    def extract_text(_path):  # type: ignore
        return ""

    class PDFSyntaxError(Exception):  # type: ignore
        pass
import logging

from parsers import table_extractor
from parsers.pdf_table_extractor import extract_text_via_ocr
from data_extraction import table_data_processor

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


def process_pdf(pdf_path):
    """处理单个PDF文件并提取数据"""
    try:
        # 使用pdfminer提取文本
        text = extract_text(pdf_path)
        text_source = "pdfminer"

        if not text.strip():
            logger.info("PDF 文本提取为空，尝试 OCR: %s", pdf_path)
            text = extract_text_via_ocr(pdf_path)
            text_source = "ocr"

        if not text.strip():
            logger.warning("OCR 后仍未获得文本: %s", pdf_path)

        tables = table_extractor.extract_tables({}, filepath=pdf_path)
        table_results = table_data_processor.extract_data_from_tables(tables)

        # 提取钢铁数据
        steel_data = extract_steel_data_from_text(text)

        composition = {
            **steel_data["composition"],
            **table_results.get("composition", {}),
        }
        heat_treatment = {
            **steel_data["heat_treatment"],
            **table_results.get("process", {}),
        }
        mechanical_properties = {
            **steel_data["mechanical_properties"],
            **table_results.get("properties", {}),
        }

        return {
            "file_path": pdf_path,
            "composition": composition,
            "heat_treatment": heat_treatment,
            "mechanical_properties": mechanical_properties,
            "microstructure": steel_data["microstructure"],
            "text_snippet": text[:1000] + "..." if text else "",
            "tables": tables,
            "table_results": table_results,
            "text_source": text_source,
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

    for filename in os.listdir(papers_dir):
        if filename.endswith(".pdf"):
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

    logger.info(f"成功处理 {valid_files}/{len(dataset)} 个文件")
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

    if pd is None:
        logger.warning("缺少 pandas，跳过 Excel 导出")
        return json_path, excel_path

    # 准备Excel数据
    excel_data = []
    for item in dataset:
        row = {
            "file": os.path.basename(item["file_path"]),
            **{f"composition_{k}": v for k, v in item["composition"].items()},
            **{f"heat_treatment_{k}": v for k, v in item["heat_treatment"].items()},
            **{f"mechanical_{k}": v for k, v in item["mechanical_properties"].items()},
            **{f"microstructure_{k}": v for k, v in item["microstructure"].items()}
        }
        excel_data.append(row)

    # 转换为DataFrame并保存
    if excel_data:
        df = pd.DataFrame(excel_data)
        df.to_excel(excel_path, index=False)
    else:
        logger.warning("没有可保存的数据")

    return json_path, excel_path