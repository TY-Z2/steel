# steel_miner/parsers/xml_table_extractor.py
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd


def extract_tables_from_xml(filepath):
    """从Elsevier XML中提取表格"""
    tables = []

    with open(filepath, "rb") as f:
        content = f.read()

    soup = BeautifulSoup(content, "xml")

    for table_elem in soup.find_all("table"):
        table_data = []
        caption = ""

        # 提取标题
        if caption_elem := table_elem.find_previous("caption"):
            caption = caption_elem.get_text(strip=True)

        # 提取表头
        headers = []
        if thead := table_elem.find("thead"):
            for th in thead.find_all("th"):
                headers.append(th.get_text(strip=True))

        # 提取表格内容
        for row in table_elem.find_all("tr"):
            row_data = []
            for cell in row.find_all(["td", "th"]):
                # 合并跨行跨列单元格
                rowspan = int(cell.get("rowspan", 1))
                colspan = int(cell.get("colspan", 1))
                cell_text = cell.get_text(strip=True)

                # 添加占位符
                for _ in range(colspan):
                    row_data.append(cell_text)

            if row_data:
                table_data.append(row_data)

        # 确保所有行长度一致
        max_cols = max(len(row) for row in table_data) if table_data else 0
        for row in table_data:
            while len(row) < max_cols:
                row.append("")

        tables.append({"caption": caption, "data": table_data})

    return tables