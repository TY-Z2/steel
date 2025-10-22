import re


def extract_mechanical_properties(text):
    """提取力学性能数据"""
    properties = {}

    # 屈服强度提取模式
    ys_patterns = [
        r"yield\s*strength\s*\(?YS\)?\s*[:=]?\s*(\d+)\s*MPa",
        r"σ_(?:y|0\.2)\s*=\s*(\d+)\s*MPa"
    ]

    # 抗拉强度提取模式
    uts_patterns = [
        r"tensile\s*strength\s*\(?UTS\)?\s*[:=]?\s*(\d+)\s*MPa",
        r"σ_(?:u|b)\s*=\s*(\d+)\s*MPa"
    ]

    # 延伸率提取模式
    el_patterns = [
        r"elongation\s*\(?EL\)?\s*[:=]?\s*(\d+\.?\d*)\s*%",
        r"δ\s*=\s*(\d+\.?\d*)\s*%"
    ]

    # 冲击韧性提取模式
    impact_pattern = r"impact\s*toughness\s*[:=]?\s*(\d+)\s*J/cm2"

    patterns = {
        "YS": ys_patterns,
        "UTS": uts_patterns,
        "EL": el_patterns,
        "impact_toughness": [impact_pattern]
    }

    for prop, prop_patterns in patterns.items():
        for pattern in prop_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    properties[prop] = float(match.group(1))
                    break  # 找到第一个匹配即停止
                except:
                    continue

    return properties