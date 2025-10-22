# parsers/xml_parser.py
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup


def parse_elsevier_xml(content):
    """解析Elsevier XML格式文献"""
    try:
        soup = BeautifulSoup(content, "xml")

        # 提取标题
        title = soup.find("ce:title").text if soup.find("ce:title") else ""

        # 提取摘要
        abstract = ""
        if abs_sec := soup.find("ce:abstract"):
            for p in abs_sec.find_all("ce:para"):
                abstract += p.text + "\n"

        # 提取正文
        body_text = ""
        if body := soup.find("ce:body"):
            for sec in body.find_all("ce:section"):
                sec_title = sec.find("ce:section-title").text if sec.find("ce:section-title") else ""
                body_text += f"\n\n{sec_title}\n"
                for para in sec.find_all("ce:para"):
                    body_text += para.text + "\n"

        # 提取表格数据
        tables = []
        for table in soup.find_all("ce:table"):
            table_data = []
            caption = table.find("ce:caption").text if table.find("ce:caption") else ""

            # 尝试解析HTML表格
            if table_html := table.find("table"):
                for row in table_html.find_all("tr"):
                    cols = [col.get_text(strip=True) for col in row.find_all(["th", "td"])]
                    if cols:
                        table_data.append(cols)

            tables.append({"caption": caption, "data": table_data})

        return {
            "title": title.strip(),
            "abstract": abstract.strip(),
            "body": body_text.strip(),
            "tables": tables
        }

    except Exception as e:
        print(f"XML parsing error: {str(e)}")
        return None


# parsers/html_parser.py
from bs4 import BeautifulSoup
import re


def parse_html(content):
    """解析HTML格式文献"""
    try:
        soup = BeautifulSoup(content, "html.parser")

        # 提取标题
        title = soup.find("h1").text if soup.find("h1") else ""

        # 提取摘要
        abstract = ""
        for element in soup.find_all(["div", "section"]):
            if "abstract" in element.get("class", []) or "abstract" in element.get("id", "").lower():
                abstract = element.get_text(strip=True)
                break

        # 提取正文
        body_text = ""
        body_sections = soup.find_all(["section", "div"], class_=re.compile(r"(content|body|text)"))
        for section in body_sections:
            body_text += section.get_text() + "\n\n"

        # 提取表格
        tables = []
        for table in soup.find_all("table"):
            caption = table.find_previous_sibling(["caption", "p", "div"])
            caption = caption.text if caption else ""

            table_data = []
            for row in table.find_all("tr"):
                cols = [col.get_text(strip=True) for col in row.find_all(["th", "td"])]
                if cols:
                    table_data.append(cols)

            tables.append({"caption": caption, "data": table_data})

        return {
            "title": title.strip(),
            "abstract": abstract.strip(),
            "body": body_text.strip(),
            "tables": tables
        }

    except Exception as e:
        print(f"HTML parsing error: {str(e)}")
        return None