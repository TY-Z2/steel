# steel_miner/parsers/pdf_table_extractor.py
import pdfplumber
import pandas as pd
import re


def extract_tables_from_pdf(filepath):
    """从PDF文献中提取表格"""
    tables = []

    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                # 尝试提取表格
                page_tables = page.extract_tables({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "explicit_vertical_lines": page.curves + page.edges,
                    "explicit_horizontal_lines": page.curves + page.edges,
                    "snap_tolerance": 3,
                })

                # 尝试查找表格标题
                text = page.extract_text()
                table_captions = re.findall(r"Table\s+\d+[.:]?\s*(.+?)\n", text)

                # 处理提取的表格
                for i, table in enumerate(page_tables):
                    caption = table_captions[i] if i < len(table_captions) else f"Table on page {page.page_number}"

                    # 清理表格数据
                    cleaned_table = []
                    for row in table:
                        cleaned_row = [clean_cell(cell) for cell in row]
                        cleaned_table.append(cleaned_row)

                    tables.append(
                        {
                            "caption": caption,
                            "data": cleaned_table,
                            "page_number": page.page_number,
                        }
                    )
    except Exception as e:
        print(f"PDF table extraction error: {str(e)}")

    return tables


def clean_cell(cell):
    """清理表格单元格内容"""
    if cell is None:
        return ""

    # 移除多余的换行和空格
    cleaned = re.sub(r"\s+", " ", str(cell).strip())

    # 处理特殊字符
    cleaned = cleaned.replace("\uf0b7", "•")  # 项目符号

    return cleaned