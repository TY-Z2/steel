# steel_miner/parsers/table_extractor.py
import re
import pandas as pd
from bs4 import BeautifulSoup
from . import xml_table_extractor, html_table_extractor, pdf_table_extractor


def extract_tables(paper_data, filepath=None):
    """主函数：从文献数据中提取所有表格"""
    tables = paper_data.get("tables", [])

    # 如果已有表格数据，直接返回
    if tables:
        return tables

    # 根据文件类型调用专用提取器
    if filepath:
        if filepath.endswith(".xml"):
            return xml_table_extractor.extract_tables_from_xml(filepath)
        elif filepath.endswith(".html"):
            return html_table_extractor.extract_tables_from_html(filepath)
        elif filepath.endswith(".pdf"):
            return pdf_table_extractor.extract_tables_from_pdf(filepath)

    # 从文本内容中尝试提取表格
    return extract_tables_from_text(paper_data.get("body", ""))


def extract_tables_from_text(text):
    """从纯文本中识别表格结构"""
    tables = []
    table_pattern = r"Table \d+[.:]? (.+?)\n(.+?)(?=\n\n|$)"

    # 尝试匹配表格结构
    for match in re.finditer(table_pattern, text, re.DOTALL):
        caption = match.group(1).strip()
        table_text = match.group(2).strip()

        # 尝试解析表格内容
        table_data = []
        for line in table_text.split("\n"):
            # 分割行：假设列由2个以上空格分隔
            row = [cell.strip() for cell in re.split(r"\s{2,}", line) if cell.strip()]
            if row:
                table_data.append(row)

        if table_data:
            tables.append({"caption": caption, "data": table_data})

    return tables


def is_materials_table(table_data):
    """判断是否为材料数据表"""
    caption = table_data.get("caption", "").lower()
    headers = table_data["data"][0] if table_data["data"] else []

    # 检查标题关键词
    caption_keywords = ["composition", "chemical", "element", "properties", "mechanical", "heat treatment"]
    if any(kw in caption for kw in caption_keywords):
        return True

    # 检查表头关键词
    header_keywords = ["C", "Si", "Mn", "Cr", "Ni", "YS", "UTS", "EL", "temperature", "time"]
    for header in headers:
        if any(kw in header.lower() for kw in header_keywords):
            return True

    return False