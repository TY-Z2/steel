import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================
# 文本与数值规范化工具
# ============================

_SUPERSCRIPT_TRANSLATION = {
    ord("⁰"): "^0",
    ord("¹"): "^1",
    ord("²"): "^2",
    ord("³"): "^3",
    ord("⁴"): "^4",
    ord("⁵"): "^5",
    ord("⁶"): "^6",
    ord("⁷"): "^7",
    ord("⁸"): "^8",
    ord("⁹"): "^9",
    ord("₀"): "0",
    ord("₁"): "1",
    ord("₂"): "2",
    ord("₃"): "3",
    ord("₄"): "4",
    ord("₅"): "5",
    ord("₆"): "6",
    ord("₇"): "7",
    ord("₈"): "8",
    ord("₉"): "9",
    ord("⁻"): "-",
}

RANGE_SEPARATOR_REGEX = re.compile(r"\s*(?:-|–|—|~|～|to|至)\s*", re.IGNORECASE)
NUMBER_TOKEN_REGEX = re.compile(
    r"[−\-+]?\d+(?:[.,]\d+)?(?:\s*[×xX]\s*10\s*(?:\^|[-−])?\s*[−\-+]?\d+|[eE][−\-+]?\d+)?",
    re.UNICODE,
)
UNIT_TOKEN = r"[A-Za-z°/%μ·\^0-9.\-]+(?:/[A-Za-z°/%μ·\^0-9.\-]+)?"
VALUE_TOKEN_PATTERN = (
    r"[<≥≤>~≈]?\s*-?\d+(?:[.,]\d+)?"
    r"(?:\s*[×xX]\s*10\s*(?:\^|[-−])?\s*-?\d+|[eE][-−+]?\d+)?"
    r"(?:\s*(?:-|–|—|~|～|to|至)\s*-?\d+(?:[.,]\d+)?"
    r"(?:\s*[×xX]\s*10\s*(?:\^|[-−])?\s*-?\d+|[eE][-−+]?\d+)?)*"
)
SENTENCE_DELIMITERS = ["。", ".", "!", "！", "?", "？", ";", "；", "\n"]


@dataclass
class Measurement:
    field: str
    value: Any
    unit: Optional[str]
    raw: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    range: Optional[Tuple[float, float]] = None

    def as_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "field": self.field,
            "value": self.value,
            "unit": self.unit,
            "raw": self.raw,
            "metadata": self.metadata,
        }
        if self.range:
            data["range"] = {"min": self.range[0], "max": self.range[1]}
        return data


# ============================
# 单位与字段 Schema 定义
# ============================

UNIT_CATALOG: Dict[str, Dict[str, Any]] = {
    "wt.%": {
        "type": "fraction",
        "aliases": [
            "wt.%",
            "wt%",
            "wtpct",
            "质量%",
            "质量百分数",
            "质量分数",
            "重量%",
            "重量分数",
            "wt.-%",
            "mass%",
        ],
    },
    "%": {"type": "percent", "aliases": ["%", "％", "pct", "percent"]},
    "at.%": {"type": "fraction", "aliases": ["at.%", "at%", "原子%", "原子分数"]},
    "°C": {
        "type": "temperature",
        "aliases": ["°c", "℃", "degc", "degrees c", "摄氏度", "摄氏"],
    },
    "K": {"type": "temperature", "aliases": ["k", "kelvin", "开尔文"]},
    "°F": {"type": "temperature", "aliases": ["°f", "fahrenheit"]},
    "min": {
        "type": "time",
        "aliases": ["min", "mins", "minute", "minutes", "min.", "分钟"],
    },
    "h": {"type": "time", "aliases": ["h", "hr", "hrs", "hour", "hours", "小时"]},
    "s": {"type": "time", "aliases": ["s", "sec", "secs", "second", "seconds", "秒"]},
    "MPa": {"type": "stress", "aliases": ["mpa", "兆帕"]},
    "GPa": {"type": "stress", "aliases": ["gpa", "吉帕"]},
    "Pa": {"type": "stress", "aliases": ["pa"]},
    "kPa": {"type": "stress", "aliases": ["kpa"]},
    "HV": {"type": "hardness", "aliases": ["hv"]},
    "HRC": {"type": "hardness", "aliases": ["hrc"]},
    "HB": {"type": "hardness", "aliases": ["hb", "brinell", "布氏"]},
    "J": {"type": "energy", "aliases": ["j", "焦", "焦耳"]},
    "kJ": {"type": "energy", "aliases": ["kj", "千焦"]},
    "kJ/m^2": {
        "type": "energy_density",
        "aliases": [
            "kj/m^2",
            "kj·m^-2",
            "kj/m2",
            "kj·m-2",
            "kj m^-2",
            "kJ/m²",
            "kJ·m^-2",
        ],
    },
    "J/cm^2": {
        "type": "energy_density",
        "aliases": ["j/cm^2", "j·cm^-2", "j/cm2", "J/cm²"],
    },
    "°C/s": {"type": "cooling_rate", "aliases": ["°c/s", "℃/s", "k/s"]},
    "°C/min": {"type": "cooling_rate", "aliases": ["°c/min", "℃/min"]},
    "μm": {"type": "length", "aliases": ["μm", "um", "micron", "微米"]},
    "mm": {"type": "length", "aliases": ["mm", "毫米"]},
}

UNIT_ALIAS_TO_CANONICAL: Dict[str, str] = {}
for canonical, info in UNIT_CATALOG.items():
    aliases = set(info.get("aliases", [])) | {canonical}
    for alias in aliases:
        normalized = (
            alias.strip()
            .lower()
            .replace("℃", "°c")
            .replace("％", "%")
            .replace("·", "")
            .replace("⋅", "")
            .replace("／", "/")
            .replace(" ", "")
            .replace(" ", "")
        )
        if not normalized:
            continue
        UNIT_ALIAS_TO_CANONICAL[normalized] = canonical
UNIT_ALIAS_SORTED = sorted(UNIT_ALIAS_TO_CANONICAL.items(), key=lambda item: -len(item[0]))

UNIT_CONVERTERS: Dict[str, Dict[str, Any]] = {
    "temperature": {
        "base": "°C",
        "to_base": {
            "°C": lambda v: v,
            "K": lambda v: v - 273.15,
            "°F": lambda v: (v - 32) / 1.8,
        },
    },
    "time": {
        "base": "min",
        "to_base": {
            "min": lambda v: v,
            "h": lambda v: v * 60.0,
            "s": lambda v: v / 60.0,
        },
    },
    "stress": {
        "base": "MPa",
        "to_base": {
            "MPa": lambda v: v,
            "GPa": lambda v: v * 1000.0,
            "Pa": lambda v: v / 1_000_000.0,
            "kPa": lambda v: v / 1000.0,
        },
    },
    "fraction": {
        "base": "wt.%",
        "to_base": {
            "wt.%": lambda v: v,
            "%": lambda v: v,
            "at.%": lambda v: v,
        },
    },
    "percent": {"base": "%", "to_base": {"%": lambda v: v}},
    "energy_density": {
        "base": "kJ/m^2",
        "to_base": {
            "kJ/m^2": lambda v: v,
            "J/cm^2": lambda v: v * 10.0,
        },
    },
    "energy": {
        "base": "J",
        "to_base": {
            "J": lambda v: v,
            "kJ": lambda v: v * 1000.0,
        },
    },
    "cooling_rate": {
        "base": "°C/s",
        "to_base": {
            "°C/s": lambda v: v,
            "°C/min": lambda v: v / 60.0,
        },
    },
    "length": {
        "base": "μm",
        "to_base": {
            "μm": lambda v: v,
            "mm": lambda v: v * 1000.0,
        },
    },
    "hardness": {"base": None, "to_base": {}},
}
COMPOSITION_FIELDS: Dict[str, Dict[str, Any]] = {
    "C": {
        "aliases": ["C", "carbon", "碳", "C含量", "碳含量"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Si": {
        "aliases": ["Si", "silicon", "硅"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Mn": {
        "aliases": ["Mn", "manganese", "锰"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Cr": {
        "aliases": ["Cr", "chromium", "铬"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Mo": {
        "aliases": ["Mo", "molybdenum", "钼"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Ni": {
        "aliases": ["Ni", "nickel", "镍"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "V": {
        "aliases": ["V", "vanadium", "钒"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Ti": {
        "aliases": ["Ti", "titanium", "钛"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Al": {
        "aliases": ["Al", "aluminium", "铝", "aluminum"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Cu": {
        "aliases": ["Cu", "copper", "铜"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Nb": {
        "aliases": ["Nb", "niobium", "铌"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "B": {
        "aliases": ["B", "boron", "硼"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "P": {
        "aliases": ["P", "phosphorus", "磷"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "S": {
        "aliases": ["S", "sulfur", "硫"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "N": {
        "aliases": ["N", "nitrogen", "氮"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "W": {
        "aliases": ["W", "tungsten", "钨"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Co": {
        "aliases": ["Co", "cobalt", "钴"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
    "Fe": {
        "aliases": ["Fe", "iron", "铁"],
        "unit_type": "fraction",
        "default_unit": "wt.%",
        "allowed_units": ["wt.%", "%", "at.%"],
    },
}

HEAT_TREATMENT_FIELDS: Dict[str, Dict[str, Any]] = {
    "austenitizing_temperature": {
        "aliases": [
            "austenitizing temperature",
            "austenitized at",
            "奥氏体化温度",
            "奥氏体化在",
            "austenitizing",
        ],
        "unit_type": "temperature",
        "default_unit": "°C",
        "allowed_units": ["°C", "K"],
    },
    "austenitizing_time": {
        "aliases": [
            "austenitizing time",
            "austenitized for",
            "for",
            "奥氏体化时间",
            "保温时间",
            "保温",
        ],
        "unit_type": "time",
        "default_unit": "min",
        "allowed_units": ["min", "h", "s"],
        "context": ["austenitized", "austenitizing", "奥氏体化"],
    },
    "tempering_temperature": {
        "aliases": [
            "tempering temperature",
            "tempered at",
            "回火温度",
            "tempering",
        ],
        "unit_type": "temperature",
        "default_unit": "°C",
        "allowed_units": ["°C", "K"],
    },
    "tempering_time": {
        "aliases": ["tempering time", "tempered for", "for", "回火时间"],
        "unit_type": "time",
        "default_unit": "min",
        "allowed_units": ["min", "h", "s"],
        "context": ["tempered", "tempering", "回火"],
    },
    "isothermal_temperature": {
        "aliases": ["isothermal temperature", "等温温度", "isothermal at"],
        "unit_type": "temperature",
        "default_unit": "°C",
        "allowed_units": ["°C", "K"],
    },
    "isothermal_time": {
        "aliases": ["isothermal time", "等温时间", "等温保持", "for"],
        "unit_type": "time",
        "default_unit": "min",
        "allowed_units": ["min", "h", "s"],
        "context": ["isothermal", "等温"],
    },
    "cooling_rate": {
        "aliases": ["cooling rate", "冷却速度", "冷却速率"],
        "unit_type": "cooling_rate",
        "default_unit": "°C/s",
        "allowed_units": ["°C/s", "°C/min"],
    },
    "quenching_medium": {
        "aliases": ["quench", "quenching", "淬火", "冷却介质", "quenched"],
        "value_type": "categorical",
        "choices": {
            "water": ["water", "水淬", "water quench"],
            "oil": ["oil", "油淬", "oil quench"],
            "air": ["air", "空冷", "air cooled"],
            "salt bath": ["salt bath", "盐浴"],
            "polymer": ["polymer", "聚合物"],
        },
        "default_unit": None,
    },
}

MECHANICAL_PROPERTY_FIELDS: Dict[str, Dict[str, Any]] = {
    "tensile_strength": {
        "aliases": [
            "tensile strength",
            "ultimate tensile strength",
            "UTS",
            "抗拉强度",
            "Rm",
        ],
        "unit_type": "stress",
        "default_unit": "MPa",
        "allowed_units": ["MPa", "GPa", "Pa", "kPa"],
    },
    "yield_strength": {
        "aliases": [
            "yield strength",
            "yield stress",
            "0.2% proof stress",
            "Rp0.2",
            "YS",
            "Rp0,2",
            "屈服强度",
            "屈服点",
        ],
        "unit_type": "stress",
        "default_unit": "MPa",
        "allowed_units": ["MPa", "GPa", "Pa", "kPa"],
    },
    "elongation": {
        "aliases": ["elongation", "elongation to failure", "伸长率", "延伸率", "El"],
        "unit_type": "percent",
        "default_unit": "%",
        "allowed_units": ["%"],
    },
    "reduction_of_area": {
        "aliases": ["reduction of area", "断面收缩率", "RA"],
        "unit_type": "percent",
        "default_unit": "%",
        "allowed_units": ["%"],
    },
    "hardness": {
        "aliases": ["hardness", "硬度", "HV", "HRC", "HB"],
        "unit_type": "hardness",
        "default_unit": None,
        "allowed_units": ["HV", "HRC", "HB"],
    },
    "impact_toughness": {
        "aliases": ["impact toughness", "冲击韧性", "冲击吸收功", "KV"],
        "unit_type": None,
        "default_unit": "J",
        "allowed_units": ["J", "kJ/m^2", "J/cm^2"],
    },
    "fatigue_strength": {
        "aliases": ["fatigue strength", "fatigue limit", "疲劳强度", "疲劳极限", "σ-1"],
        "unit_type": "stress",
        "default_unit": "MPa",
        "allowed_units": ["MPa", "GPa", "Pa", "kPa"],
    },
}

MICROSTRUCTURE_FIELDS: Dict[str, Dict[str, Any]] = {
    "bainite_fraction": {
        "aliases": ["bainite fraction", "贝氏体含量", "贝氏体体积分数"],
        "unit_type": "percent",
        "default_unit": "%",
        "allowed_units": ["%"],
    },
    "martensite_fraction": {
        "aliases": ["martensite fraction", "马氏体含量", "马氏体体积分数"],
        "unit_type": "percent",
        "default_unit": "%",
        "allowed_units": ["%"],
    },
    "ferrite_fraction": {
        "aliases": ["ferrite fraction", "铁素体含量", "铁素体体积分数"],
        "unit_type": "percent",
        "default_unit": "%",
        "allowed_units": ["%"],
    },
    "austenite_fraction": {
        "aliases": ["austenite fraction", "奥氏体含量", "残余奥氏体"],
        "unit_type": "percent",
        "default_unit": "%",
        "allowed_units": ["%"],
    },
    "grain_size": {
        "aliases": ["grain size", "晶粒尺寸", "晶粒大小"],
        "unit_type": "length",
        "default_unit": "μm",
        "allowed_units": ["μm", "mm"],
    },
}
# ============================
# 基础工具函数
# ============================

def preprocess_text(text: Optional[str]) -> str:
    if not text:
        return ""
    cleaned = text.replace("\r", "\n").replace("\u00a0", " ")
    cleaned = re.sub(r"-\s*\n", "", cleaned)
    cleaned = cleaned.translate(_SUPERSCRIPT_TRANSLATION)
    cleaned = cleaned.replace("−", "-").replace("–", "-").replace("—", "-").replace("﹣", "-")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def normalize_unit_alias(unit: Optional[str]) -> str:
    if not unit:
        return ""
    token = unit.strip().lower()
    token = token.translate(_SUPERSCRIPT_TRANSLATION)
    replacements = {
        "℃": "°c",
        "％": "%",
        "·": "",
        "⋅": "",
        "／": "/",
        " ": "",
        " ": "",
        "μ": "μ",
        "um": "μm",
    }
    for src, dst in replacements.items():
        token = token.replace(src, dst)
    token = token.replace("m²", "m^2").replace("cm²", "cm^2")
    token = token.replace("m⁻²", "m^-2").replace("cm⁻²", "cm^-2")
    token = token.replace(" ", "")
    return token


def canonicalize_unit(unit: Optional[str]) -> Optional[str]:
    if not unit:
        return None
    normalized = normalize_unit_alias(unit)
    if not normalized:
        return None
    if normalized in UNIT_ALIAS_TO_CANONICAL:
        return UNIT_ALIAS_TO_CANONICAL[normalized]
    # 某些单位写法可能缺少分隔符，这里尝试一次性匹配
    for alias, canonical in UNIT_ALIAS_SORTED:
        if not alias:
            continue
        if normalized.startswith(alias):
            return canonical
    return None


def build_alias_pattern(alias: str) -> str:
    if re.fullmatch(r"[A-Za-z]", alias):
        return rf"(?<![A-Za-z0-9°]){re.escape(alias)}(?![A-Za-z0-9])"
    if re.fullmatch(r"[A-Za-z0-9]+", alias):
        return rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])"
    return re.escape(alias)


def find_alias_matches(text: str, alias: str) -> List[Tuple[int, int]]:
    pattern = re.compile(build_alias_pattern(alias), re.IGNORECASE)
    return [match.span() for match in pattern.finditer(text)]


def parse_single_numeric(value_str: str) -> Optional[float]:
    token = value_str.strip()
    if not token:
        return None
    token = token.replace("−", "-").replace("–", "-").replace("—", "-").replace("﹣", "-")
    token = token.replace("＋", "+")
    token = token.translate(_SUPERSCRIPT_TRANSLATION)
    token = token.replace("×", "x").replace("X", "x")
    token = token.replace("·", ".").replace("⋅", ".")
    token = token.replace("、", ".")
    token = re.sub(r"x10-(?=\d)", "x10^-", token)
    token = re.sub(r"x10\+(?=\d)", "x10^", token)
    token = re.sub(r"10\s*-(?=\d)", "10^-", token)
    token = re.sub(r"10\s*\+(?=\d)", "10^", token)
    token = token.replace(" ", "")
    token = token.replace("e+", "e").replace("E", "e")
    token = re.sub(r"x10\^?([-+]?\d+)", lambda m: f"e{m.group(1)}", token)
    token = token.replace("--", "-")
    if "," in token and "." not in token:
        token = token.replace(",", ".")
    else:
        token = token.replace(",", "")
    try:
        return float(token)
    except ValueError:
        return None


def split_range_parts(text: str) -> Optional[Tuple[str, str]]:
    normalized = re.sub(r"\s*(?:to|至|～|〜|~)\s*", "|", text, flags=re.IGNORECASE)
    normalized = re.sub(r"(?<![eE\^])\s*[-–—]\s*(?=[0-9])", "|", normalized)
    parts = [part.strip() for part in normalized.split("|") if part.strip()]
    if len(parts) == 2:
        return parts[0], parts[1]
    return None


def parse_numeric_expression(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    qualifiers: Dict[str, Any] = {}
    if text[0] in {"~", "≈"}:
        qualifiers["approximate"] = True
        text = text[1:].strip()
    operator = None
    for symbol in ("<=", ">=", "≥", "≤", "<", ">"):
        if text.startswith(symbol):
            operator = symbol
            text = text[len(symbol):].strip()
            break
    range_parts = split_range_parts(text)
    if range_parts:
        first = parse_single_numeric(range_parts[0])
        second = parse_single_numeric(range_parts[1])
        if first is None or second is None:
            return None
        minimum = min(first, second)
        maximum = max(first, second)
        value = (minimum + maximum) / 2.0
        result = {"value": value, "range": (minimum, maximum)}
    else:
        number = parse_single_numeric(text)
        if number is None:
            return None
        result = {"value": number}
    if qualifiers:
        result["qualifiers"] = qualifiers
    if operator:
        result.setdefault("qualifiers", {})["operator"] = operator
    return result


def find_numeric_spans(sentence: str) -> List[Dict[str, Any]]:
    spans: List[Dict[str, Any]] = []
    for match in NUMBER_TOKEN_REGEX.finditer(sentence):
        start, end = match.span()
        prefix_start = start
        while prefix_start > 0 and sentence[prefix_start - 1] in {"≈", "~", "<", ">", "≥", "≤"}:
            prefix_start -= 1
        raw = sentence[prefix_start:end]
        spans.append({"start": prefix_start, "end": end, "raw": raw})
    combined: List[Dict[str, Any]] = []
    i = 0
    while i < len(spans):
        current = spans[i]
        if i + 1 < len(spans):
            between = sentence[current["end"]:spans[i + 1]["start"]]
            if RANGE_SEPARATOR_REGEX.fullmatch(between or ""):
                combined.append({
                    "start": current["start"],
                    "end": spans[i + 1]["end"],
                    "raw": sentence[current["start"]:spans[i + 1]["end"]],
                })
                i += 2
                continue
        combined.append(current)
        i += 1
    return combined


def select_closest_span(
    alias_start: int, alias_end: int, spans: Sequence[Dict[str, Any]], used_indices: Iterable[int]
) -> Optional[int]:
    used = set(used_indices)
    best_idx: Optional[int] = None
    best_distance: Optional[int] = None
    for idx, span in enumerate(spans):
        if idx in used:
            continue
        base_distance = min(abs(alias_start - span["start"]), abs(alias_end - span["end"]))
        penalty = 0 if span["start"] >= alias_end else 10000
        distance = base_distance + penalty
        if best_distance is None or distance < best_distance:
            best_idx = idx
            best_distance = distance
    return best_idx


def detect_unit_for_span(
    sentence: str, start: int, end: int, meta: Dict[str, Any]
) -> Tuple[Optional[str], Optional[str]]:
    allowed_units = meta.get("allowed_units")
    default_unit = meta.get("default_unit")

    after_segment = sentence[end:]
    after_match = re.match(rf"\s*(?P<unit>{UNIT_TOKEN})", after_segment)
    if after_match:
        raw_unit = after_match.group("unit")
        canonical = canonicalize_unit(raw_unit)
        if canonical and (not allowed_units or canonical in allowed_units or canonical == default_unit):
            return canonical, raw_unit
        if raw_unit:
            return None, raw_unit

    before_segment = sentence[:start]
    before_match = re.search(rf"(?P<unit>{UNIT_TOKEN})\s*$", before_segment)
    if before_match:
        raw_unit = before_match.group("unit")
        canonical = canonicalize_unit(raw_unit)
        if canonical and (not allowed_units or canonical in allowed_units or canonical == default_unit):
            return canonical, raw_unit
        if raw_unit:
            return None, raw_unit

    return default_unit, None


def normalize_measurement_values(
    parsed: Dict[str, Any],
    canonical_unit: Optional[str],
    raw_unit: Optional[str],
    meta: Dict[str, Any],
    field_name: str,
) -> Optional[Tuple[Any, Optional[str], Optional[Tuple[float, float]]]]:
    unit_type = meta.get("unit_type")
    default_unit = meta.get("default_unit")
    allowed_units = meta.get("allowed_units")
    if raw_unit and canonical_unit is None:
        logger.warning("无法识别字段 %s 的单位: %s", field_name, raw_unit)
        return None
    if not canonical_unit and default_unit:
        canonical_unit = default_unit
    if allowed_units and canonical_unit not in allowed_units:
        logger.warning("字段 %s 的单位 %s 不在允许列表中", field_name, canonical_unit)
        return None
    if unit_type and unit_type in UNIT_CONVERTERS:
        converters = UNIT_CONVERTERS[unit_type]
        base_unit = converters.get("base")
        to_base = converters.get("to_base", {})
        source_unit = canonical_unit or base_unit
        if source_unit not in to_base and source_unit != base_unit:
            logger.warning(
                "字段 %s 的单位无法转换: %s",
                field_name,
                raw_unit or canonical_unit,
            )
            return None
        converter = to_base.get(source_unit, lambda v: v)
        value = parsed.get("value")
        range_info = parsed.get("range")
        converted_value = converter(value) if isinstance(value, (int, float)) else value
        converted_range = (
            (converter(range_info[0]), converter(range_info[1])) if range_info else None
        )
        return converted_value, base_unit, converted_range
    value = parsed.get("value")
    return value, canonical_unit or default_unit, parsed.get("range")


def get_sentence_context(text: str, start: int, end: int) -> str:
    begin = 0
    for delimiter in SENTENCE_DELIMITERS:
        pos = text.rfind(delimiter, 0, start)
        if pos != -1 and pos + 1 > begin:
            begin = pos + 1
    finish = len(text)
    for delimiter in SENTENCE_DELIMITERS:
        pos = text.find(delimiter, end)
        if pos != -1 and pos < finish:
            finish = pos
    snippet = text[begin:finish].strip()
    return snippet or text[max(0, start - 50):min(len(text), end + 50)].strip()


def split_sentences(text: str) -> List[str]:
    if not text:
        return []
    candidates = re.split(r"(?<=[。.!?？；;])\s+", text)
    sentences = [candidate.strip() for candidate in candidates if candidate.strip()]
    if not sentences:
        sentences = [text.strip()]
    return sentences


def measurement_key(measurement: Dict[str, Any]) -> Tuple[Any, ...]:
    value = measurement.get("value")
    if isinstance(value, float):
        value_key = round(value, 6)
    else:
        value_key = value
    unit = measurement.get("unit")
    range_info = measurement.get("range") or {}
    if range_info:
        range_key = (
            round(range_info.get("min", 0.0), 6) if range_info.get("min") is not None else None,
            round(range_info.get("max", 0.0), 6) if range_info.get("max") is not None else None,
        )
    else:
        range_key = None
    raw = measurement.get("raw")
    return (measurement.get("field"), value_key, unit, range_key, raw)


def merge_measurement_groups(*groups: Optional[Dict[str, List[Dict[str, Any]]]]) -> Dict[str, List[Dict[str, Any]]]:
    merged: Dict[str, List[Dict[str, Any]]] = {}
    for group in groups:
        if not group:
            continue
        for field, measurements in group.items():
            if not measurements:
                continue
            merged.setdefault(field, [])
            for measurement in measurements:
                if not measurement:
                    continue
                if any(measurement_key(existing) == measurement_key(measurement) for existing in merged[field]):
                    continue
                merged[field].append(measurement)
    return merged


def flatten_measurements_for_excel(measurements: List[Dict[str, Any]]) -> str:
    if not measurements:
        return ""
    entries: List[str] = []
    for item in measurements:
        value = item.get("value")
        if isinstance(value, float):
            value_repr = f"{value:.4g}"
        else:
            value_repr = str(value)
        range_info = item.get("range")
        if isinstance(range_info, dict) and range_info.get("min") is not None and range_info.get("max") is not None:
            min_val = range_info.get("min")
            max_val = range_info.get("max")
            range_repr = f"{min_val:.4g}–{max_val:.4g}"
        else:
            range_repr = None
        unit = item.get("unit") or ""
        if range_repr:
            entry = f"{range_repr} {unit}".strip()
        else:
            entry = f"{value_repr} {unit}".strip()
        entries.append(entry)
    return "; ".join(entries)
# ============================
# 具体抽取逻辑
# ============================

def extract_categorical_measurement(
    sentence: str,
    field: str,
    meta: Dict[str, Any],
    method_label: str,
    alias_matches: List[Tuple[str, Tuple[int, int]]],
) -> Optional[Dict[str, Any]]:
    choices = meta.get("choices", {})
    lowered = sentence.lower()
    for canonical, tokens in choices.items():
        for token in tokens:
            if token.lower() in lowered:
                metadata = {
                    "method": method_label,
                    "sentence": sentence.strip(),
                    "trigger": alias_matches[0][0],
                    "page": None,
                }
                return {
                    "field": field,
                    "value": canonical,
                    "unit": meta.get("default_unit"),
                    "raw": token,
                    "metadata": metadata,
                }
    logger.warning("未能从句子中解析 %s 的分类取值: %s", field, sentence)
    return None


def extract_schema_from_sentences(
    sentences: Sequence[str],
    schema: Dict[str, Dict[str, Any]],
    method_label: str,
) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {field: [] for field in schema}
    for sentence in sentences:
        if not sentence.strip():
            continue
        sentence_lower = sentence.lower()
        for field, meta in schema.items():
            context_keywords = [kw.lower() for kw in meta.get("context", [])]
            if context_keywords and not any(keyword in sentence_lower for keyword in context_keywords):
                continue
            alias_matches: List[Tuple[str, Tuple[int, int]]] = []
            for alias in meta.get("aliases", []):
                for span in find_alias_matches(sentence, alias):
                    alias_matches.append((alias, span))
            if not alias_matches:
                continue
            if meta.get("value_type") == "categorical":
                measurement = extract_categorical_measurement(
                    sentence, field, meta, method_label, alias_matches
                )
                if measurement:
                    results[field].append(measurement)
                continue
            numeric_spans = find_numeric_spans(sentence)
            if not numeric_spans:
                logger.warning("未能在句子中发现数值: %s", sentence)
                continue
            used_indices: List[int] = []
            for alias, span in alias_matches:
                idx = select_closest_span(span[0], span[1], numeric_spans, used_indices)
                if idx is None:
                    continue
                used_indices.append(idx)
                numeric_span = numeric_spans[idx]
                parsed = parse_numeric_expression(numeric_span["raw"])
                if not parsed:
                    logger.warning("无法解析数值表达式: %s", numeric_span["raw"])
                    continue
                canonical_unit, raw_unit = detect_unit_for_span(
                    sentence, numeric_span["start"], numeric_span["end"], meta
                )
                normalized = normalize_measurement_values(parsed, canonical_unit, raw_unit, meta, field)
                if not normalized:
                    continue
                value, base_unit, range_info = normalized
                metadata: Dict[str, Any] = {
                    "method": method_label,
                    "sentence": sentence.strip(),
                    "trigger": alias,
                    "page": None,
                }
                if parsed.get("qualifiers"):
                    metadata["qualifiers"] = parsed["qualifiers"]
                measurement = {
                    "field": field,
                    "value": value,
                    "unit": base_unit,
                    "raw": numeric_span["raw"].strip(),
                    "metadata": metadata,
                }
                if range_info:
                    measurement["range"] = {"min": range_info[0], "max": range_info[1]}
                results[field].append(measurement)
    return results


def extract_composition(text: str) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {field: [] for field in COMPOSITION_FIELDS}
    for element, meta in COMPOSITION_FIELDS.items():
        seen_spans: Set[Tuple[int, int]] = set()
        for alias in meta.get("aliases", []):
            for start, end in find_alias_matches(text, alias):
                # 向后查找
                tail = text[end:end + 80]
                match_after = re.search(
                    rf"(?P<value>{VALUE_TOKEN_PATTERN})(?:\s*(?P<unit>{UNIT_TOKEN}))?",
                    tail,
                    re.IGNORECASE,
                )
                if match_after:
                    value_start = end + match_after.start("value")
                    value_end = end + match_after.end("value")
                    if (value_start, value_end) not in seen_spans:
                        seen_spans.add((value_start, value_end))
                        raw_value = match_after.group("value")
                        parsed = parse_numeric_expression(raw_value)
                        if parsed:
                            raw_unit = match_after.group("unit")
                            canonical = canonicalize_unit(raw_unit) if raw_unit else None
                            normalized = normalize_measurement_values(
                                parsed, canonical, raw_unit, meta, element
                            )
                            if normalized:
                                value, base_unit, range_info = normalized
                                sentence = get_sentence_context(text, value_start, value_end)
                                metadata = {
                                    "method": "rule_regex",
                                    "sentence": sentence,
                                    "trigger": alias,
                                    "page": None,
                                }
                                measurement: Dict[str, Any] = {
                                    "field": element,
                                    "value": value,
                                    "unit": base_unit,
                                    "raw": raw_value.strip(),
                                    "metadata": metadata,
                                }
                                if range_info:
                                    measurement["range"] = {"min": range_info[0], "max": range_info[1]}
                                results[element].append(measurement)
                # 向前查找
                head = text[max(0, start - 80):start]
                matches_before = list(
                    re.finditer(
                        rf"(?P<value>{VALUE_TOKEN_PATTERN})(?:\s*(?P<unit>{UNIT_TOKEN}))?",
                        head,
                        re.IGNORECASE,
                    )
                )
                match_before = matches_before[-1] if matches_before else None
                if match_before:
                    value_start = start - (len(head) - match_before.start("value"))
                    value_end = start - (len(head) - match_before.end("value"))
                    if (value_start, value_end) not in seen_spans:
                        seen_spans.add((value_start, value_end))
                        raw_value = match_before.group("value")
                        parsed = parse_numeric_expression(raw_value)
                        if parsed:
                            raw_unit = match_before.group("unit")
                            canonical = canonicalize_unit(raw_unit) if raw_unit else None
                            normalized = normalize_measurement_values(
                                parsed, canonical, raw_unit, meta, element
                            )
                            if normalized:
                                value, base_unit, range_info = normalized
                                sentence = get_sentence_context(text, value_start, value_end)
                                metadata = {
                                    "method": "rule_regex",
                                    "sentence": sentence,
                                    "trigger": alias,
                                    "page": None,
                                }
                                measurement = {
                                    "field": element,
                                    "value": value,
                                    "unit": base_unit,
                                    "raw": raw_value.strip(),
                                    "metadata": metadata,
                                }
                                if range_info:
                                    measurement["range"] = {"min": range_info[0], "max": range_info[1]}
                                results[element].append(measurement)
    return results
def extract_heat_treatment(text: str, sentences: Optional[Sequence[str]] = None, method_label: str = "rule_regex") -> Dict[str, List[Dict[str, Any]]]:
    sentences = sentences or split_sentences(text)
    return extract_schema_from_sentences(sentences, HEAT_TREATMENT_FIELDS, method_label)


def extract_mechanical_properties(text: str, sentences: Optional[Sequence[str]] = None, method_label: str = "rule_regex") -> Dict[str, List[Dict[str, Any]]]:
    sentences = sentences or split_sentences(text)
    return extract_schema_from_sentences(sentences, MECHANICAL_PROPERTY_FIELDS, method_label)


def extract_microstructure(text: str, sentences: Optional[Sequence[str]] = None, method_label: str = "rule_regex") -> Dict[str, List[Dict[str, Any]]]:
    sentences = sentences or split_sentences(text)
    return extract_schema_from_sentences(sentences, MICROSTRUCTURE_FIELDS, method_label)


class NLPExtractor:
    def __init__(self) -> None:
        self.pipeline = None
        self.method = "nlp_dependency"
        try:
            import spacy  # type: ignore

            try:
                self.pipeline = spacy.load("en_core_web_sm")
            except Exception:
                self.pipeline = spacy.blank("en")
                if "sentencizer" not in self.pipeline.pipe_names:
                    self.pipeline.add_pipe("sentencizer")
                self.method = "heuristic_sentencizer"
                logger.warning("spaCy 模型不可用，已回退到启发式分句。")
        except Exception:
            self.pipeline = None
            self.method = "regex_sentences"
            logger.warning("spaCy 未安装，使用正则分句作为 NLP 回退方案。")

    def sentences_with_keywords(self, text: str, keywords: Sequence[str]) -> List[str]:
        lowered_keywords = [kw.lower() for kw in keywords]
        sentences: List[str] = []
        if self.pipeline:
            doc = self.pipeline(text)
            for sent in doc.sents:  # type: ignore[attr-defined]
                sentence_text = sent.text.strip()
                if not sentence_text:
                    continue
                lowered = sentence_text.lower()
                if any(keyword in lowered for keyword in lowered_keywords):
                    sentences.append(sentence_text)
        else:
            for sentence in split_sentences(text):
                lowered = sentence.lower()
                if any(keyword in lowered for keyword in lowered_keywords):
                    sentences.append(sentence)
        return sentences

    def extract(self, text: str) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
        keywords: List[str] = []
        for schema in (HEAT_TREATMENT_FIELDS, MECHANICAL_PROPERTY_FIELDS):
            for meta in schema.values():
                keywords.extend(meta.get("aliases", []))
        sentences = self.sentences_with_keywords(text, keywords)
        heat = extract_schema_from_sentences(sentences, HEAT_TREATMENT_FIELDS, self.method)
        properties = extract_schema_from_sentences(sentences, MECHANICAL_PROPERTY_FIELDS, self.method)
        return heat, properties
# ============================
# 对外接口
# ============================

def extract_steel_data_from_text(text: str) -> Dict[str, Any]:
    normalized_text = preprocess_text(text)
    sentences = split_sentences(normalized_text)
    composition = extract_composition(normalized_text)
    heat_rule = extract_heat_treatment(normalized_text, sentences, "rule_regex")
    properties_rule = extract_mechanical_properties(normalized_text, sentences, "rule_regex")
    microstructure = extract_microstructure(normalized_text, sentences, "rule_regex")

    nlp_extractor = NLPExtractor()
    heat_nlp, properties_nlp = nlp_extractor.extract(normalized_text)

    heat_treatment = merge_measurement_groups(heat_rule, heat_nlp)
    mechanical_properties = merge_measurement_groups(properties_rule, properties_nlp)

    return {
        "composition": composition,
        "heat_treatment": heat_treatment,
        "mechanical_properties": mechanical_properties,
        "microstructure": microstructure,
    }


def process_pdf(pdf_path: str) -> Optional[Dict[str, Any]]:
    from pdfminer.high_level import extract_text
    from pdfminer.pdfparser import PDFSyntaxError

    try:
        raw_text = extract_text(pdf_path)
        if not raw_text:
            logger.warning("未从 PDF 提取到文本: %s", pdf_path)
            return None
        normalized_text = preprocess_text(raw_text)
        steel_data = extract_steel_data_from_text(normalized_text)
        return {
            "file_path": pdf_path,
            **steel_data,
            "text_snippet": normalized_text[:1000] + "..." if normalized_text else "",
        }
    except PDFSyntaxError as exc:
        logger.error("PDF 解析错误 %s: %s", pdf_path, exc)
        return None
    except Exception as exc:
        logger.error("处理 PDF 失败 %s: %s", pdf_path, exc)
        return None


def process_papers_from_directory(papers_dir: str) -> List[Dict[str, Any]]:
    dataset: List[Dict[str, Any]] = []
    pdf_files = [name for name in os.listdir(papers_dir) if name.lower().endswith(".pdf")]
    valid_files = 0

    for filename in pdf_files:
        pdf_path = os.path.join(papers_dir, filename)
        logger.info("处理: %s", filename)
        paper_data = process_pdf(pdf_path)
        if not paper_data:
            continue
        if (
            any(paper_data.get(key) for key in ("composition", "heat_treatment", "mechanical_properties", "microstructure"))
        ):
            dataset.append(paper_data)
            valid_files += 1
        else:
            logger.warning("未提取到数据: %s", filename)
    total_files = len(pdf_files)
    logger.info("成功处理 %s/%s 个文件", valid_files, total_files)
    return dataset


def save_steel_data(dataset: List[Dict[str, Any]], output_dir: str) -> Tuple[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, "steel_data.json")
    with open(json_path, "w", encoding="utf-8") as stream:
        json.dump(dataset, stream, indent=2, ensure_ascii=False)

    excel_path = os.path.join(output_dir, "steel_data.xlsx")
    excel_rows: List[Dict[str, Any]] = []
    for item in dataset:
        row: Dict[str, Any] = {"file": os.path.basename(item.get("file_path", ""))}
        for section_key in ("composition", "heat_treatment", "mechanical_properties", "microstructure"):
            section = item.get(section_key, {}) or {}
            for field, measurements in section.items():
                column_name = f"{section_key}_{field}"
                row[column_name] = flatten_measurements_for_excel(measurements)
        excel_rows.append(row)

    if excel_rows:
        import pandas as pd

        pd.DataFrame(excel_rows).to_excel(excel_path, index=False)
    else:
        logger.warning("没有可保存的数据")
    return json_path, excel_path
