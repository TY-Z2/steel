# steel_miner/parsers/pdf_table_extractor.py
import logging
import re
from typing import Dict, Iterable, List, Optional

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - 依赖缺失时允许继续
    pd = None

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover - 依赖缺失时允许继续
    pdfplumber = None

try:  # Camelot（可能未安装）
    import camelot  # type: ignore
except Exception:  # pragma: no cover - 依赖缺失时允许继续
    camelot = None

try:  # tabula（依赖 Java）
    import tabula  # type: ignore
except Exception:  # pragma: no cover - 依赖缺失时允许继续
    tabula = None

try:  # OCR 依赖
    from pdf2image import convert_from_path  # type: ignore
except Exception:  # pragma: no cover - 依赖缺失时允许继续
    convert_from_path = None

try:  # pytesseract 及输出类型
    import pytesseract  # type: ignore
    from pytesseract import Output  # type: ignore
except Exception:  # pragma: no cover - 依赖缺失时允许继续
    pytesseract = None
    Output = None


logger = logging.getLogger(__name__)


def extract_tables_from_pdf(filepath: str) -> List[Dict[str, Iterable[str]]]:
    """从PDF文献中提取表格（含多种后备策略）"""

    tables: List[Dict[str, Iterable[str]]] = []

    # 1. Camelot（优先 lattice）
    tables.extend(_extract_with_camelot(filepath, flavor="lattice"))

    # 2. Camelot stream 作为后备
    if not tables:
        tables.extend(_extract_with_camelot(filepath, flavor="stream"))

    # 3. tabula 再次尝试
    if not tables:
        tables.extend(_extract_with_tabula(filepath))

    # 4. OCR 路径（扫描件）
    if not tables:
        tables.extend(_extract_with_ocr(filepath))

    if not tables:
        logger.warning("未从 PDF 中提取到表格: %s", filepath)

    return tables


def extract_text_via_ocr(filepath: str) -> str:
    """使用 OCR 从扫描 PDF 中提取文本。"""

    if convert_from_path is None or pytesseract is None or Output is None:
        logger.warning("缺少 OCR 依赖，无法从扫描 PDF 提取文本: %s", filepath)
        return ""

    try:
        images = convert_from_path(filepath, dpi=300)
    except Exception as exc:  # pragma: no cover - 与实际环境相关
        logger.error("将 PDF 渲染为图片失败: %s (%s)", filepath, exc)
        return ""

    texts: List[str] = []
    for index, image in enumerate(images, start=1):
        try:
            page_text = pytesseract.image_to_string(image)
        except Exception as exc:  # pragma: no cover - OCR 失败时记录
            logger.error("OCR 文本提取失败（第 %s 页）: %s", index, exc)
            continue
        if page_text.strip():
            texts.append(page_text)

    return "\n".join(texts)


def _extract_with_camelot(filepath: str, flavor: str) -> List[Dict[str, Iterable[str]]]:
    if camelot is None:
        logger.debug("Camelot 未安装，跳过 %s 提取", flavor)
        return []

    try:
        camelot_tables = camelot.read_pdf(
            filepath,
            pages="all",
            flavor=flavor,
            strip_text="\n",
        )
    except Exception as exc:  # pragma: no cover - 与 Camelot 环境相关
        logger.warning("Camelot 提取失败（%s）: %s", flavor, exc)
        return []

    results: List[Dict[str, Iterable[str]]] = []
    for table in camelot_tables:
        cleaned = _dataframe_to_rows(table.df)
        if not cleaned:
            continue

        page_number = _infer_camelot_page(table)
        results.append(
            {
                "caption": f"Camelot table on page {page_number}" if page_number else "Camelot table",
                "data": cleaned,
                "page": page_number,
                "extraction_method": f"camelot-{flavor}",
            }
        )

    if results:
        logger.info("Camelot(%s) 成功提取 %d 个表格: %s", flavor, len(results), filepath)
    else:
        logger.debug("Camelot(%s) 未检出表格: %s", flavor, filepath)

    return results


def _extract_with_tabula(filepath: str) -> List[Dict[str, Iterable[str]]]:
    if tabula is None:
        logger.debug("tabula-py 未安装，跳过提取: %s", filepath)
        return []

    if pdfplumber is None:
        logger.warning("缺少 pdfplumber，tabula 将在单次调用中尝试全部页面: %s", filepath)
        page_numbers = ["all"]
    else:
        try:
            with pdfplumber.open(filepath) as pdf:
                page_numbers = list(range(1, len(pdf.pages) + 1))
        except Exception as exc:  # pragma: no cover - PDF 打开失败
            logger.warning("计算 PDF 页数失败，tabula 将尝试全部页面: %s (%s)", filepath, exc)
            page_numbers = ["all"]

    results: List[Dict[str, Iterable[str]]] = []
    for page_number in page_numbers:
        try:
            frames = tabula.read_pdf(
                filepath,
                pages=page_number,
                multiple_tables=True,
                pandas_options={"dtype": str},
            )
        except Exception as exc:  # pragma: no cover - tabula 失败
            logger.warning("tabula 提取失败（第 %s 页）: %s", page_number, exc)
            continue

        for frame in frames or []:
            cleaned = _dataframe_to_rows(frame)
            if not cleaned:
                continue

            results.append(
                {
                    "caption": f"Tabula table on page {page_number}",
                    "data": cleaned,
                    "page": page_number if isinstance(page_number, int) else None,
                    "extraction_method": "tabula",
                }
            )

    if results:
        logger.info("tabula 成功提取 %d 个表格: %s", len(results), filepath)
    else:
        logger.debug("tabula 未检出表格: %s", filepath)

    return results


def _extract_with_ocr(filepath: str) -> List[Dict[str, Iterable[str]]]:
    if convert_from_path is None or pytesseract is None or Output is None:
        logger.warning("缺少 OCR 依赖，无法执行扫描表格识别: %s", filepath)
        return []

    try:
        images = convert_from_path(filepath, dpi=300)
    except Exception as exc:  # pragma: no cover - 依赖环境相关
        logger.error("PDF 渲染为图像失败: %s (%s)", filepath, exc)
        return []

    results: List[Dict[str, Iterable[str]]] = []
    for page_index, image in enumerate(images, start=1):
        try:
            ocr_data = pytesseract.image_to_data(image, output_type=Output.DICT)
        except Exception as exc:  # pragma: no cover - OCR 失败
            logger.error("OCR 数据提取失败（第 %s 页）: %s", page_index, exc)
            continue

        page_tables = _tables_from_ocr_data(ocr_data)
        for table_rows in page_tables:
            results.append(
                {
                    "caption": f"OCR table on page {page_index}",
                    "data": table_rows,
                    "page": page_index,
                    "extraction_method": "ocr",
                }
            )

    if results:
        logger.info("OCR 识别到 %d 个表格: %s", len(results), filepath)
    else:
        logger.warning("OCR 路径仍未检出表格: %s", filepath)

    return results


def _infer_camelot_page(table) -> Optional[int]:
    page_number = getattr(table, "page", None)
    if page_number:
        try:
            return int(page_number)
        except ValueError:  # pragma: no cover - 非数字
            return None

    parsing_report = getattr(table, "parsing_report", None)
    if parsing_report:
        page = parsing_report.get("page")
        if page:
            try:
                return int(page)
            except (TypeError, ValueError):  # pragma: no cover
                return None

    return None


def _tables_from_ocr_data(ocr_data: Dict[str, List]) -> List[List[List[str]]]:
    tables: List[List[List[str]]] = []
    if not ocr_data:
        return tables

    n = len(ocr_data.get("text", []))
    for block_index in sorted(set(ocr_data.get("block_num", []))):
        rows: Dict[int, List[tuple]] = {}
        for i in range(n):
            if ocr_data["block_num"][i] != block_index:
                continue
            text = ocr_data["text"][i]
            if not text or not text.strip():
                continue
            line = ocr_data["line_num"][i]
            rows.setdefault(line, []).append(
                (
                    ocr_data["left"][i],
                    clean_cell(text),
                )
            )

        if not rows:
            continue

        ordered_rows: List[List[str]] = []
        for line_number in sorted(rows.keys()):
            row = [text for _, text in sorted(rows[line_number], key=lambda item: item[0])]
            if row:
                ordered_rows.append(row)

        if _looks_like_table(ordered_rows):
            tables.append(ordered_rows)

    return tables


def _looks_like_table(rows: List[List[str]]) -> bool:
    if len(rows) < 2:
        return False

    widths = [len(row) for row in rows if row]
    if not widths:
        return False

    most_common_width = max(set(widths), key=widths.count)
    wide_rows = sum(1 for width in widths if width == most_common_width)

    return most_common_width >= 2 and wide_rows >= 2


def _dataframe_to_rows(frame) -> List[List[str]]:
    if pd is None or frame is None:
        return []

    if getattr(frame, "empty", True):
        return []

    df = frame.copy()
    df = df.applymap(clean_cell)

    rows = df.values.tolist()
    rows = [row for row in rows if any(cell for cell in row)]

    return rows


def clean_cell(cell):
    """清理表格单元格内容"""
    if cell is None:
        return ""

    cleaned = re.sub(r"\s+", " ", str(cell).strip())
    cleaned = cleaned.replace("\uf0b7", "•")

    return cleaned

