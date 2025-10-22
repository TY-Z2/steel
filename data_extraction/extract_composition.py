# data_extraction/extract_composition.py
import re

ELEMENTS = ["C", "Si", "Mn", "Ni", "Cr", "Mo", "Al", "V", "B", "Co"]


def extract_composition(paper_data):
    """从文献数据中提取合金成分"""
    composition = {}

    # 搜索范围：摘要、正文和表格
    search_text = paper_data["abstract"] + "\n" + paper_data["body"]

    # 策略1：表格数据提取
    for table in paper_data.get("tables", []):
        # 检查表格标题是否包含成分关键词
        if any(keyword in table["caption"].lower() for keyword in ["composition", "chemical", "element"]):
            # 查找包含元素名的行或列
            header_row = None
            for i, row in enumerate(table["data"]):
                if any(element in cell for element in ELEMENTS):
                    header_row = i
                    break

            if header_row is not None:
                # 提取数据行
                for row in table["data"][header_row + 1:]:
                    for j, cell in enumerate(row):
                        if any(element in cell for element in ELEMENTS):
                            # 提取元素和数值
                            for element in ELEMENTS:
                                if element in cell:
                                    # 尝试从相邻单元格提取数值
                                    if j + 1 < len(row) and re.match(r"\d+\.?\d*", row[j + 1]):
                                        try:
                                            value = float(row[j + 1])
                                            composition[element] = value
                                        except:
                                            continue

    # 策略2：文本模式匹配
    patterns = [
        r"(\b" + "|".join(ELEMENTS) + r")\s*[:=]?\s*(\d+\.?\d*)\s*(?:wt\.?%|%)",
        r"(\d+\.?\d*)\s*(?:wt\.?%|%)\s*(\b" + "|".join(ELEMENTS) + r")\b"
    ]

    for pattern in patterns:
        matches = re.finditer(pattern, search_text)
        for match in matches:
            # 确定元素和数值的位置
            if match.group(1) in ELEMENTS:
                element = match.group(1)
                value = match.group(2)
            elif match.group(2) in ELEMENTS:
                element = match.group(2)
                value = match.group(1)
            else:
                continue

            try:
                composition[element] = float(value)
            except:
                continue

    return composition




def extract_heat_treatment(paper_data):
    """提取热处理工艺参数"""
    params = {
        "T1": None, "t1": None,  # 奥氏体化
        "T2": None, "Δt2": None, "t2": None,  # 等温处理
        "T3": None, "t3": None  # 回火
    }

    # 聚焦"实验"部分
    search_text = ""
    for section_title in ["experimental", "methods", "materials and methods"]:
        if section_title in paper_data["body"].lower():
            start_idx = paper_data["body"].lower().index(section_title)
            end_idx = paper_data["body"].find("\n\n", start_idx)
            search_text = paper_data["body"][start_idx:end_idx]
            break

    if not search_text:
        search_text = paper_data["body"]

    # 温度-时间模式提取
    patterns = [
        (r"austenit(?:ization|izing)\s*at\s*(\d+)\s*°?C\s*for\s*(\d+)\s*(min|h)", ["T1", "t1"]),
        (r"heated\s*to\s*(\d+)\s*°?C\s*for\s*(\d+)\s*(min|h)", ["T1", "t1"]),
        (r"isothermal\s*(?:treatment|holding)\s*at\s*(\d+)\s*°?C\s*for\s*(\d+)\s*min", ["T2", "t2"]),
        (r"tempered?\s*at\s*(\d+)\s*°?C\s*for\s*(\d+)\s*h", ["T3", "t3"])
    ]

    for pattern, keys in patterns:
        matches = re.findall(pattern, search_text, re.IGNORECASE)
        if matches:
            values = matches[0]
            for i, key in enumerate(keys):
                if i < len(values):
                    try:
                        # 处理时间单位转换
                        value = float(values[i])
                        if keys[i] in ["t1", "t2", "t3"] and values[2] == "h" and i == 1:
                            value *= 60  # 小时转分钟
                        params[key] = value
                    except:
                        continue

    # 特殊参数：ΔT2 (与Ms温度的差值)
    delta_match = re.search(r"ΔT\s*=\s*(\d+)\s*°?C", search_text)
    if delta_match:
        params["Δt2"] = float(delta_match.group(1))

    # 从表格中提取
    for table in paper_data.get("tables", []):
        if "heat treatment" in table["caption"].lower():
            for row in table["data"]:
                for i, cell in enumerate(row):
                    if "austenit" in cell.lower():
                        if i + 1 < len(row) and re.match(r"\d+", row[i + 1]):
                            params["T1"] = float(row[i + 1])
                    elif "isothermal" in cell.lower():
                        if i + 1 < len(row) and re.match(r"\d+", row[i + 1]):
                            params["T2"] = float(row[i + 1])
                    elif "temper" in cell.lower():
                        if i + 1 < len(row) and re.match(r"\d+", row[i + 1]):
                            params["T3"] = float(row[i + 1])

    return {k: v for k, v in params.items() if v is not None}


# data_extraction/extract_properties.py
import re


def extract_mechanical_properties(paper_data):
    """提取力学性能数据"""
    properties = {}

    # 聚焦"结果"部分
    search_text = ""
    for section_title in ["results", "mechanical properties", "properties"]:
        if section_title in paper_data["body"].lower():
            start_idx = paper_data["body"].lower().index(section_title)
            end_idx = paper_data["body"].find("\n\n", start_idx)
            search_text = paper_data["body"][start_idx:end_idx]
            break

    if not search_text:
        search_text = paper_data["body"]

    # 定义提取模式
    patterns = {
        "YS": [
            r"yield\s*strength\s*\(?YS\)?\s*[:=]?\s*(\d+)\s*MPa",
            r"σ_(?:y|0\.2)\s*=\s*(\d+)\s*MPa",
            r"\bYS\b\s*[:=]?\s*(\d+)\s*MPa"
        ],
        "UTS": [
            r"tensile\s*strength\s*\(?UTS\)?\s*[:=]?\s*(\d+)\s*MPa",
            r"ultimate\s*tensile\s*strength\s*[:=]?\s*(\d+)\s*MPa",
            r"σ_(?:u|b)\s*=\s*(\d+)\s*MPa",
            r"\bUTS\b\s*[:=]?\s*(\d+)\s*MPa"
        ],
        "EL": [
            r"elongation\s*\(?EL\)?\s*[:=]?\s*(\d+\.?\d*)\s*%",
            r"ductility\s*[:=]?\s*(\d+\.?\d*)\s*%",
            r"δ\s*=\s*(\d+\.?\d*)\s*%",
            r"\bEL\b\s*[:=]?\s*(\d+\.?\d*)\s*%"
        ],
        "impact_toughness": [
            r"impact\s*toughness\s*[:=]?\s*(\d+)\s*J/cm2",
            r"Charpy\s*V-notch\s*[:=]?\s*(\d+)\s*J/cm2",
            r"CVN\s*[:=]?\s*(\d+)\s*J/cm2"
        ]
    }

    for prop, prop_patterns in patterns.items():
        for pattern in prop_patterns:
            match = re.search(pattern, search_text, re.IGNORECASE)
            if match:
                try:
                    properties[prop] = float(match.group(1))
                    break  # 找到第一个匹配即停止
                except:
                    continue

    # 从表格中提取
    for table in paper_data.get("tables", []):
        if "mechanical properties" in table["caption"].lower():
            headers = table["data"][0] if table["data"] else []
            for row in table["data"][1:]:
                for i, header in enumerate(headers):
                    if i >= len(row):
                        continue

                    header = header.lower()
                    if "yield" in header or "ys" in header:
                        if re.match(r"\d+", row[i]):
                            properties["YS"] = float(row[i])
                    elif "tensile" in header or "uts" in header:
                        if re.match(r"\d+", row[i]):
                            properties["UTS"] = float(row[i])
                    elif "elongation" in header or "el" in header:
                        if re.match(r"\d+\.?\d*", row[i]):
                            properties["EL"] = float(row[i])
                    elif "impact" in header or "charpy" in header:
                        if re.match(r"\d+", row[i]):
                            properties["impact_toughness"] = float(row[i])

    return properties