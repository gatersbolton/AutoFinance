from __future__ import annotations

from typing import Any, Dict, Sequence


def is_present(value: Any) -> bool:
    return value not in (None, "")


def is_numeric(value: Any) -> bool:
    if not is_present(value):
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def safe_ratio(numerator: float, denominator: float) -> float:
    if float(denominator or 0.0) <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def build_benchmark_metric_summary(
    cell_rows: Sequence[Dict[str, Any]],
    benchmark_workbook: str,
    *,
    extra_in_auto: int = 0,
    missing_in_auto_raw: int | None = None,
    missing_in_auto_true: int | None = None,
    alignment_only_gap_total: int = 0,
    ambiguous_alignment_total: int = 0,
) -> Dict[str, Any]:
    rows = list(cell_rows)
    missing_raw_total = int(missing_in_auto_raw if missing_in_auto_raw is not None else sum(1 for row in rows if row.get("status") == "missing_in_auto"))
    missing_true_total = int(missing_in_auto_true if missing_in_auto_true is not None else missing_raw_total)
    matched_cells_total = sum(1 for row in rows if row.get("status") == "match")
    value_diff_total = sum(1 for row in rows if row.get("status") == "value_diff")
    benchmark_filled_total = sum(1 for row in rows if is_present(row.get("benchmark_value")))
    auto_filled_total = sum(1 for row in rows if is_present(row.get("auto_value")))
    benchmark_true_positive_total = sum(
        1
        for row in rows
        if row.get("status") == "match"
        and is_present(row.get("benchmark_value"))
        and is_present(row.get("auto_value"))
    )
    amount_benchmark_filled_total = sum(1 for row in rows if is_numeric(row.get("benchmark_value")))
    amount_match_total = sum(
        1
        for row in rows
        if row.get("status") == "match"
        and is_numeric(row.get("benchmark_value"))
        and is_numeric(row.get("auto_value"))
    )
    amount_missing_total = sum(
        1
        for row in rows
        if row.get("status") == "missing_in_auto"
        and is_numeric(row.get("benchmark_value"))
    )

    benchmark_precision_ratio = safe_ratio(benchmark_true_positive_total, auto_filled_total)
    benchmark_recall_ratio = safe_ratio(benchmark_true_positive_total, benchmark_filled_total)
    amount_match_ratio = safe_ratio(amount_match_total, amount_benchmark_filled_total)
    amount_missing_ratio = safe_ratio(amount_missing_total, amount_benchmark_filled_total)

    return {
        "run_id": "",
        "benchmark_workbook": benchmark_workbook,
        "matched_cells": matched_cells_total,
        "missing_in_auto": missing_true_total,
        "missing_in_auto_raw": missing_raw_total,
        "missing_in_auto_true": missing_true_total,
        "extra_in_auto": int(extra_in_auto),
        "value_diff_cells": value_diff_total,
        "alignment_only_gap_total": int(alignment_only_gap_total),
        "ambiguous_alignment_total": int(ambiguous_alignment_total),
        "exact_match_ratio": safe_ratio(matched_cells_total, len(rows)),
        "benchmark_true_positive_total": benchmark_true_positive_total,
        "benchmark_filled_total": benchmark_filled_total,
        "auto_filled_total": auto_filled_total,
        "benchmark_precision_ratio": benchmark_precision_ratio,
        "benchmark_recall_ratio": benchmark_recall_ratio,
        "benchmark_precision_pct": round(benchmark_precision_ratio * 100.0, 6),
        "benchmark_recall_pct": round(benchmark_recall_ratio * 100.0, 6),
        "precision_against_benchmark": benchmark_precision_ratio,
        "recall_against_benchmark": benchmark_recall_ratio,
        "amount_benchmark_filled_total": amount_benchmark_filled_total,
        "amount_match_total": amount_match_total,
        "amount_missing_total": amount_missing_total,
        "amount_match_ratio": amount_match_ratio,
        "amount_missing_ratio": amount_missing_ratio,
    }


def build_benchmark_metric_audit(summary: Dict[str, Any], cell_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(cell_rows)
    matched_cells_total = sum(1 for row in rows if row.get("status") == "match")
    benchmark_filled_total = sum(1 for row in rows if is_present(row.get("benchmark_value")))
    auto_filled_total = sum(1 for row in rows if is_present(row.get("auto_value")))
    legacy_amount_rows_total = sum(1 for row in rows if is_numeric(row.get("benchmark_value")) or is_numeric(row.get("auto_value")))
    legacy_amount_match_total = sum(1 for row in rows if row.get("status") == "match" and (is_numeric(row.get("benchmark_value")) or is_numeric(row.get("auto_value"))))
    legacy_amount_missing_total = sum(1 for row in rows if row.get("status") == "missing_in_auto" and (is_numeric(row.get("benchmark_value")) or is_numeric(row.get("auto_value"))))

    ratio_keys = [
        "exact_match_ratio",
        "benchmark_precision_ratio",
        "benchmark_recall_ratio",
        "precision_against_benchmark",
        "recall_against_benchmark",
        "amount_match_ratio",
        "amount_missing_ratio",
    ]
    ratio_range_checks = {
        key: 0.0 <= float(summary.get(key, 0.0) or 0.0) <= 1.0
        for key in ratio_keys
    }

    return {
        "run_id": "",
        "range_check_pass": all(ratio_range_checks.values()),
        "ratio_range_checks": ratio_range_checks,
        "legacy_formula_snapshot": {
            "precision_against_benchmark": safe_ratio(matched_cells_total, auto_filled_total),
            "recall_against_benchmark": safe_ratio(matched_cells_total, benchmark_filled_total),
            "amount_match_ratio": safe_ratio(legacy_amount_match_total, legacy_amount_rows_total),
            "amount_missing_ratio": safe_ratio(legacy_amount_missing_total, legacy_amount_rows_total),
            "matched_cells_total": matched_cells_total,
            "benchmark_filled_total": benchmark_filled_total,
            "auto_filled_total": auto_filled_total,
            "legacy_amount_rows_total": legacy_amount_rows_total,
            "legacy_amount_match_total": legacy_amount_match_total,
            "legacy_amount_missing_total": legacy_amount_missing_total,
        },
        "corrected_formula_snapshot": {
            "benchmark_precision_ratio": float(summary.get("benchmark_precision_ratio", 0.0) or 0.0),
            "benchmark_recall_ratio": float(summary.get("benchmark_recall_ratio", 0.0) or 0.0),
            "amount_match_ratio": float(summary.get("amount_match_ratio", 0.0) or 0.0),
            "amount_missing_ratio": float(summary.get("amount_missing_ratio", 0.0) or 0.0),
            "benchmark_true_positive_total": int(summary.get("benchmark_true_positive_total", 0) or 0),
            "benchmark_filled_total": int(summary.get("benchmark_filled_total", 0) or 0),
            "auto_filled_total": int(summary.get("auto_filled_total", 0) or 0),
            "amount_benchmark_filled_total": int(summary.get("amount_benchmark_filled_total", 0) or 0),
            "amount_match_total": int(summary.get("amount_match_total", 0) or 0),
            "amount_missing_total": int(summary.get("amount_missing_total", 0) or 0),
        },
    }

