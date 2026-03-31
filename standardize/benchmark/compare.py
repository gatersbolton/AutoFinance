from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .align import align_benchmark_headers
from .loader import load_workbook_main_sheet


def compare_benchmark_workbook(
    benchmark_path: Path,
    export_workbook_path: Path,
    rules: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    rules = rules or {}
    benchmark = load_workbook_main_sheet(benchmark_path)
    export = load_workbook_main_sheet(export_workbook_path)
    export_rows = {row["mapping_code"]: row for row in export["rows"]}
    export_period_headers = [header for header in export["headers"] if "__" in header]
    alignment = align_benchmark_headers(benchmark["headers"], export_period_headers, rules)
    tolerance = float(rules.get("numeric_tolerance", 0.01))

    cell_rows: List[Dict[str, Any]] = []
    missing_rows: List[Dict[str, Any]] = []
    extra_rows: List[Dict[str, Any]] = []
    value_diff_rows: List[Dict[str, Any]] = []
    subject_gap_counter = Counter()
    period_gap_counter = Counter()

    for row in benchmark["rows"]:
        export_row = export_rows.get(row["mapping_code"], {"values": {}})
        for header, benchmark_value in row["values"].items():
            if header == "科目名称":
                continue
            aligned = alignment.get(header, {"aligned_period_key": "", "reason": "legacy_header_unsupported"})
            aligned_period = aligned["aligned_period_key"]
            compare_key = aligned_period or (header if "__" in header else "")
            auto_value = export_row["values"].get(compare_key)
            status, reason = compare_values(benchmark_value, auto_value, aligned["reason"], tolerance)
            record = {
                "run_id": "",
                "mapping_code": row["mapping_code"],
                "mapping_name": row["mapping_name"],
                "benchmark_header": header,
                "aligned_period_key": aligned_period,
                "benchmark_value": benchmark_value,
                "auto_value": auto_value,
                "status": status,
                "reason": reason,
            }
            cell_rows.append(record)
            if status == "missing_in_auto":
                missing_rows.append(record)
                subject_gap_counter[row["mapping_code"]] += 1
                period_gap_counter[header] += 1
            elif status == "extra_in_auto":
                extra_rows.append(record)
            elif status == "value_diff":
                value_diff_rows.append(record)

    benchmark_keys = {(row["mapping_code"], header) for row in benchmark["rows"] for header in row["values"].keys()}
    for export_row in export["rows"]:
        for header, auto_value in export_row["values"].items():
            if "__" not in header or auto_value in (None, ""):
                continue
            aligned_headers = {legacy_header for legacy_header, info in alignment.items() if info.get("aligned_period_key") == header}
            if not aligned_headers:
                extra_rows.append(
                    {
                        "run_id": "",
                        "mapping_code": export_row["mapping_code"],
                        "mapping_name": export_row["mapping_name"],
                        "benchmark_header": "",
                        "aligned_period_key": header,
                        "benchmark_value": "",
                        "auto_value": auto_value,
                        "status": "extra_in_auto",
                        "reason": "no_benchmark_alignment",
                    }
                )

    matched_cells = sum(1 for row in cell_rows if row["status"] == "match")
    missing_in_auto = sum(1 for row in cell_rows if row["status"] == "missing_in_auto")
    extra_in_auto = len(extra_rows)
    value_diff_cells = sum(1 for row in cell_rows if row["status"] == "value_diff")
    benchmark_filled = sum(1 for row in cell_rows if row["benchmark_value"] not in (None, ""))
    auto_filled = sum(1 for row in cell_rows if row["auto_value"] not in (None, ""))
    amount_rows = [row for row in cell_rows if is_numeric(row["benchmark_value"]) or is_numeric(row["auto_value"])]
    amount_matches = sum(1 for row in amount_rows if row["status"] == "match")
    amount_missing = sum(1 for row in amount_rows if row["status"] == "missing_in_auto")

    summary = {
        "run_id": "",
        "benchmark_workbook": str(benchmark_path),
        "matched_cells": matched_cells,
        "missing_in_auto": missing_in_auto,
        "extra_in_auto": extra_in_auto,
        "value_diff_cells": value_diff_cells,
        "exact_match_ratio": safe_ratio(matched_cells, len(cell_rows)),
        "recall_against_benchmark": safe_ratio(matched_cells, benchmark_filled),
        "precision_against_benchmark": safe_ratio(matched_cells, auto_filled),
        "amount_match_ratio": safe_ratio(amount_matches, len(amount_rows)),
        "amount_missing_ratio": safe_ratio(amount_missing, len(amount_rows)),
    }

    subject_gap_rows = [
        {"run_id": "", "mapping_code": code, "missing_cells": count}
        for code, count in subject_gap_counter.most_common()
    ]
    period_gap_rows = [
        {"run_id": "", "benchmark_header": header, "missing_cells": count}
        for header, count in period_gap_counter.most_common()
    ]
    return {
        "summary": summary,
        "summary_rows": [summary],
        "cell_rows": cell_rows,
        "missing_rows": missing_rows,
        "extra_rows": extra_rows,
        "value_diff_rows": value_diff_rows,
        "subject_gap_rows": subject_gap_rows,
        "period_gap_rows": period_gap_rows,
        "alignment": alignment,
    }


def compare_values(benchmark_value: Any, auto_value: Any, alignment_reason: str, tolerance: float) -> Tuple[str, str]:
    if alignment_reason == "ambiguous_period_alignment":
        return "missing_in_auto", alignment_reason
    if benchmark_value in (None, "") and auto_value in (None, ""):
        return "match", "both_empty"
    if benchmark_value not in (None, "") and auto_value in (None, ""):
        return "missing_in_auto", alignment_reason
    if benchmark_value in (None, "") and auto_value not in (None, ""):
        return "extra_in_auto", alignment_reason
    if is_numeric(benchmark_value) and is_numeric(auto_value):
        if abs(float(benchmark_value) - float(auto_value)) <= tolerance:
            return "match", "numeric_close"
        return "value_diff", "numeric_mismatch"
    if str(benchmark_value).strip() == str(auto_value).strip():
        return "match", "exact_text_match"
    return "value_diff", "text_mismatch"


def is_numeric(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
