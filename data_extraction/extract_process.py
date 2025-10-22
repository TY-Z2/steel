import re

PROCESS_KEYWORDS = {
    "austenitizing": ["austenitization", "T1", "γ phase"],
    "isothermal": ["isothermal treatment", "bainite transformation", "T2"],
    "tempering": ["temper", "T3", "martensite decomposition"]
}


def extract_heat_treatment(text):
    """提取热处理工艺参数"""
    params = {
        "T1": None, "t1": None,  # 奥氏体化
        "T2": None, "Δt2": None, "t2": None,  # 等温处理
        "T3": None, "t3": None  # 回火
    }

    # 温度-时间模式提取
    patterns = [
        (r"(\d+)\s*°?C\s*for\s*(\d+)\s*(min|h)", ["T1", "t1"]),  # 奥氏体化
        (r"isothermal\s*at\s*(\d+)\s*°?C\s*for\s*(\d+)\s*min", ["T2", "t2"]),  # 等温
        (r"tempered?\s*at\s*(\d+)\s*°?C\s*for\s*(\d+)\s*h", ["T3", "t3"])  # 回火
    ]

    for pattern, keys in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            values = matches[0]
            for i in range(len(keys)):
                if i < len(values):
                    try:
                        params[keys[i]] = float(values[i])
                    except:
                        continue

    # 特殊参数：ΔT2 (与Ms温度的差值)
    delta_match = re.search(r"ΔT\s*=\s*(\d+)\s*°?C", text)
    if delta_match:
        params["Δt2"] = float(delta_match.group(1))

    return {k: v for k, v in params.items() if v is not None}