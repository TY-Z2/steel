"""Quality checking and manual review utilities for extracted steel datasets."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd


logger = logging.getLogger(__name__)


DEFAULT_RULES: Dict[str, float] = {
    "max_austenitizing_temperature": 1200.0,
    "max_isothermal_temperature": 900.0,
    "max_tempering_temperature": 1500.0,
    "max_tempering_time": 600.0,
    "max_austenitizing_time": 180.0,
    "max_isothermal_time": 600.0,
    "max_carbon_equivalent": 1.0,
}


def load_dataset(dataset_path: Path) -> List[Dict[str, object]]:
    with dataset_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_quality_rules(rules_path: Path | None) -> Dict[str, float]:
    if not rules_path or not rules_path.exists():
        return dict(DEFAULT_RULES)
    with rules_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return {**DEFAULT_RULES, **data}


def calculate_missing_rates(dataset: Iterable[Dict[str, object]]) -> Dict[str, float]:
    sections = ["composition", "heat_treatment", "mechanical_properties", "microstructure"]
    counts = {section: 0 for section in sections}
    dataset_list = list(dataset)
    total = len(dataset_list) or 1

    for record in dataset_list:
        for section in sections:
            if not record.get(section):
                counts[section] += 1

    return {section: round(count / total, 3) for section, count in counts.items()}


def detect_anomalies(
    record: Dict[str, object], rules: Dict[str, float]
) -> List[Dict[str, object]]:
    anomalies: List[Dict[str, object]] = []
    heat_treatment = record.get("heat_treatment", {}) or {}
    derived = record.get("derived_metrics", {}) or {}

    def _check(field: str, value: float, limit_key: str):
        limit = rules.get(limit_key)
        if limit is not None and value > limit:
            anomalies.append(
                {
                    "file_path": record.get("file_path"),
                    "field": field,
                    "value": value,
                    "limit": limit,
                    "type": "range",
                }
            )

    for field, limit_key in (
        ("austenitizing_temperature", "max_austenitizing_temperature"),
        ("isothermal_temperature", "max_isothermal_temperature"),
        ("tempering_temperature", "max_tempering_temperature"),
        ("tempering_time", "max_tempering_time"),
        ("austenitizing_time", "max_austenitizing_time"),
        ("isothermal_time", "max_isothermal_time"),
    ):
        if field in heat_treatment:
            try:
                value = float(heat_treatment[field])
            except (TypeError, ValueError):
                continue
            _check(field, value, limit_key)

    carbon_equivalent = derived.get("carbon_equivalent")
    if isinstance(carbon_equivalent, (int, float)):
        _check("carbon_equivalent", float(carbon_equivalent), "max_carbon_equivalent")

    return anomalies


def detect_inconsistent_combinations(record: Dict[str, object]) -> List[Dict[str, object]]:
    inconsistencies: List[Dict[str, object]] = []
    heat_treatment = record.get("heat_treatment", {}) or {}

    zero_time_fields = [
        key for key in ("tempering_time", "austenitizing_time", "isothermal_time")
        if heat_treatment.get(key) == 0
    ]
    for field in zero_time_fields:
        inconsistencies.append(
            {
                "file_path": record.get("file_path"),
                "field": field,
                "value": heat_treatment.get(field),
                "type": "logical",
                "message": "热处理时间为 0，需人工确认",
            }
        )

    if (
        heat_treatment.get("tempering_temperature")
        and not heat_treatment.get("tempering_time")
    ):
        inconsistencies.append(
            {
                "file_path": record.get("file_path"),
                "field": "tempering_time",
                "type": "logical",
                "message": "存在回火温度但缺失回火时间",
            }
        )

    return inconsistencies


def run_quality_checks(
    dataset: Iterable[Dict[str, object]], rules: Dict[str, float]
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    dataset_list = list(dataset)
    missing_rates = calculate_missing_rates(dataset_list)

    anomalies: List[Dict[str, object]] = []
    inconsistencies: List[Dict[str, object]] = []

    for record in dataset_list:
        anomalies.extend(detect_anomalies(record, rules))
        inconsistencies.extend(detect_inconsistent_combinations(record))

    flagged_records = anomalies + inconsistencies

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_records": len(dataset_list),
        "missing_rates": missing_rates,
        "anomalies": anomalies,
        "logical_inconsistencies": inconsistencies,
    }

    return report, flagged_records


def save_report(report: Dict[str, object], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "quality_report.json"
    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    logger.info("质量检查报告已生成: %s", report_path)
    return report_path


def export_flagged_samples(flagged: List[Dict[str, object]], output_dir: Path) -> None:
    if not flagged:
        logger.info("未发现需要人工复核的样本。")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "flagged_samples.json"
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(flagged, fh, indent=2, ensure_ascii=False)

    df = pd.DataFrame(flagged)
    excel_path = output_dir / "flagged_samples.xlsx"
    df.to_excel(excel_path, index=False)
    logger.info("已导出待复核样本: %s, %s", json_path, excel_path)


def generate_review_form(flagged: List[Dict[str, object]], output_path: Path) -> None:
    if not flagged:
        return

    rows_html = "\n".join(
        f"""
        <tr>
            <td>{item.get('file_path')}</td>
            <td>{item.get('field')}</td>
            <td>{item.get('value')}</td>
            <td>{item.get('message', '自动检测异常')}</td>
            <td>
                <select name=\"decision_{index}\">
                    <option value=\"approved\">有效</option>
                    <option value=\"rejected\">无效</option>
                    <option value=\"needs_followup\">需进一步确认</option>
                </select>
            </td>
            <td><input type=\"text\" name=\"notes_{index}\" placeholder=\"备注\" /></td>
        </tr>
        """
        for index, item in enumerate(flagged)
    )

    html_content = f"""
    <!DOCTYPE html>
    <html lang=\"zh\">
    <head>
        <meta charset=\"UTF-8\" />
        <title>钢铁数据质量人工复核</title>
        <style>
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; }}
            th {{ background-color: #f2f2f2; }}
            textarea {{ width: 100%; }}
            .controls {{ margin-top: 16px; }}
        </style>
    </head>
    <body>
        <h1>钢铁数据质量人工复核</h1>
        <p>请审查下表中的异常条目，填写复核结论并点击下载。</p>
        <table>
            <thead>
                <tr>
                    <th>文件</th>
                    <th>字段</th>
                    <th>值</th>
                    <th>问题描述</th>
                    <th>结论</th>
                    <th>备注</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
        <div class="controls">
            <button onclick="downloadResults()">下载复核结果(JSON)</button>
        </div>
        <script>
            function downloadResults() {{
                const rows = Array.from(document.querySelectorAll('tbody tr'));
                const results = rows.map((row, index) => {{
                    const cells = row.querySelectorAll('td');
                    return {{
                        file_path: cells[0].innerText,
                        field: cells[1].innerText,
                        value: cells[2].innerText,
                        message: cells[3].innerText,
                        decision: row.querySelector(`select[name=\"decision_${{index}}\"]`).value,
                        notes: row.querySelector(`input[name=\"notes_${{index}}\"]`).value,
                        reviewed_at: new Date().toISOString(),
                    }};
                }});

                const blob = new Blob([JSON.stringify(results, null, 2)], {{ type: 'application/json' }});
                const url = URL.createObjectURL(blob);
                const link = document.createElement('a');
                link.href = url;
                link.download = 'manual_review_results.json';
                link.click();
                URL.revokeObjectURL(url);
            }}
        </script>
    </body>
    </html>
    """

    output_path.write_text(html_content, encoding="utf-8")
    logger.info("人工复核表单已生成: %s", output_path)


def apply_manual_review_results(
    dataset_path: Path,
    review_results_path: Path,
    output_path: Path | None = None,
) -> Path:
    dataset = load_dataset(dataset_path)
    with review_results_path.open("r", encoding="utf-8") as fh:
        review_results = json.load(fh)

    review_by_file: Dict[str, Dict[str, object]] = {}
    for entry in review_results:
        file_path = entry.get("file_path")
        if not file_path:
            continue
        review_by_file.setdefault(file_path, {"issues": []})
        review_by_file[file_path]["issues"].append(entry)
        review_by_file[file_path]["latest_decision"] = entry.get("decision")
        review_by_file[file_path]["updated_at"] = entry.get("reviewed_at")

    for record in dataset:
        file_path = record.get("file_path")
        if not file_path or file_path not in review_by_file:
            continue
        metadata = record.get("quality_metadata", {}) or {}
        metadata["manual_review"] = review_by_file[file_path]
        record["quality_metadata"] = metadata

    output_path = output_path or dataset_path
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(dataset, fh, indent=2, ensure_ascii=False)

    logger.info("人工复核结果已写回数据集: %s", output_path)
    return output_path


def update_rules_from_reviews(review_results_path: Path, rules_path: Path) -> Path:
    if not review_results_path.exists():
        raise FileNotFoundError(f"未找到复核结果文件: {review_results_path}")

    with review_results_path.open("r", encoding="utf-8") as fh:
        review_results = json.load(fh)

    approved_tempering: List[float] = []
    for entry in review_results:
        if entry.get("decision") != "approved":
            continue
        if entry.get("field") in {"tempering_temperature", "austenitizing_temperature"}:
            try:
                approved_tempering.append(float(entry.get("value")))
            except (TypeError, ValueError):
                continue

    rules = load_quality_rules(rules_path) if rules_path.exists() else dict(DEFAULT_RULES)
    if approved_tempering:
        new_limit = max(approved_tempering) * 1.1
        rules["max_tempering_temperature"] = round(new_limit, 2)

    rules_path.parent.mkdir(parents=True, exist_ok=True)
    with rules_path.open("w", encoding="utf-8") as fh:
        json.dump(rules, fh, indent=2, ensure_ascii=False)

    logger.info("质量规则已根据人工复核更新: %s", rules_path)
    return rules_path


def configure_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "quality_checks.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="钢铁数据质量检查工具")
    parser.add_argument("--dataset", required=True, type=Path, help="钢铁数据 JSON 文件路径")
    parser.add_argument("--output-dir", required=True, type=Path, help="质量报告输出目录")
    parser.add_argument("--rules", type=Path, default=Path("config/quality_rules.json"))
    parser.add_argument(
        "--review-results",
        type=Path,
        help="人工复核结果 JSON 文件路径，若提供则写回数据集",
    )
    parser.add_argument(
        "--update-rules",
        action="store_true",
        help="根据人工复核结果更新质量规则",
    )
    parser.add_argument(
        "--write-updated-dataset",
        type=Path,
        help="写回复核结果后的数据集输出路径，默认覆盖原文件",
    )

    args = parser.parse_args()
    configure_logging(args.output_dir)

    dataset_path: Path = args.dataset
    output_dir: Path = args.output_dir

    dataset = load_dataset(dataset_path)
    rules = load_quality_rules(args.rules)

    report, flagged = run_quality_checks(dataset, rules)
    save_report(report, output_dir)
    export_flagged_samples(flagged, output_dir)

    generate_review_form(flagged, output_dir / "manual_review_form.html")

    if args.review_results:
        apply_manual_review_results(
            dataset_path,
            args.review_results,
            output_path=args.write_updated_dataset,
        )
        if args.update_rules:
            update_rules_from_reviews(args.review_results, args.rules)
if __name__ == "__main__":
    main()

