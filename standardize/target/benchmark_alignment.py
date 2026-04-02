from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Sequence, Tuple

from ..benchmark.compare import compare_values
from ..benchmark.metrics import build_benchmark_metric_summary
from ..models import FactRecord


ALIGNMENT_REASONS = {
    "legacy_header_unsupported",
    "legacy_role_exact_date_match",
    "legacy_role_unique_match",
    "ambiguous_period_alignment",
    "no_matching_period_for_legacy_role",
    "period_key_not_found",
}


def repair_benchmark_alignment(
    benchmark_payload: Dict[str, Any],
    facts: Sequence[FactRecord],
    rules: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    rules = rules or {}
    tolerance = float(rules.get("numeric_tolerance", 0.01))
    raw_cell_rows = [dict(row) for row in benchmark_payload.get("cell_rows", [])]
    export_period_headers = list(benchmark_payload.get("export_period_headers", []))
    export_rows = benchmark_payload.get("export_rows_map", {}) or {}
    fact_index = build_fact_index(facts)

    repaired_cell_rows: List[Dict[str, Any]] = []
    missing_true_rows: List[Dict[str, Any]] = []
    alignment_only_rows: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []
    subject_gap_counter = Counter()
    period_gap_counter = Counter()

    for row in raw_cell_rows:
        repaired, audit_row = repair_single_row(
            row=row,
            fact_index=fact_index,
            export_period_headers=export_period_headers,
            export_rows=export_rows,
            rules=rules,
            tolerance=tolerance,
        )
        repaired_cell_rows.append(repaired)
        audit_rows.append(audit_row)
        if repaired.get("status") == "missing_in_auto" and audit_row.get("alignment_status") != "ambiguous_alignment":
            missing_true_rows.append(repaired)
            subject_gap_counter[repaired.get("mapping_code", "")] += 1
            period_gap_counter[repaired.get("aligned_period_key", "") or repaired.get("benchmark_header", "")] += 1
        elif audit_row.get("alignment_status") in {"alignment_only_gap", "ambiguous_alignment"}:
            alignment_only_rows.append(repaired)

    raw_summary = dict(benchmark_payload.get("summary", {}))
    missing_raw_total = sum(1 for row in raw_cell_rows if row.get("status") == "missing_in_auto")
    missing_true_total = len(missing_true_rows)
    alignment_only_total = sum(1 for row in audit_rows if row.get("alignment_status") == "alignment_only_gap")
    ambiguous_total = sum(1 for row in audit_rows if row.get("alignment_status") == "ambiguous_alignment")
    summary = {
        **raw_summary,
        **build_benchmark_metric_summary(
            cell_rows=repaired_cell_rows,
            benchmark_workbook=str(raw_summary.get("benchmark_workbook", "")),
            extra_in_auto=sum(1 for row in repaired_cell_rows if row.get("status") == "extra_in_auto"),
            missing_in_auto_raw=missing_raw_total,
            missing_in_auto_true=missing_true_total,
            alignment_only_gap_total=alignment_only_total,
            ambiguous_alignment_total=ambiguous_total,
        ),
    }
    subject_gap_rows = [
        {"run_id": "", "mapping_code": code, "missing_cells": count}
        for code, count in subject_gap_counter.most_common()
    ]
    period_gap_rows = [
        {"run_id": "", "benchmark_header": header, "missing_cells": count}
        for header, count in period_gap_counter.most_common()
    ]
    alignment_summary = {
        "run_id": "",
        "missing_in_auto_raw": missing_raw_total,
        "missing_in_auto_true": missing_true_total,
        "alignment_only_gap_total": alignment_only_total,
        "ambiguous_alignment_total": ambiguous_total,
    }
    repaired_payload = dict(benchmark_payload)
    repaired_payload.update(
        {
            "summary": summary,
            "cell_rows": repaired_cell_rows,
            "missing_rows": missing_true_rows,
            "extra_rows": [row for row in repaired_cell_rows if row.get("status") == "extra_in_auto"],
            "value_diff_rows": [row for row in repaired_cell_rows if row.get("status") == "value_diff"],
            "subject_gap_rows": subject_gap_rows,
            "period_gap_rows": period_gap_rows,
            "alignment_audit_rows": audit_rows,
            "alignment_only_rows": alignment_only_rows,
            "alignment_summary": alignment_summary,
            "benchmark_missing_true_rows": missing_true_rows,
        }
    )
    return repaired_payload


def repair_single_row(
    row: Dict[str, Any],
    fact_index: Dict[str, Any],
    export_period_headers: Sequence[str],
    export_rows: Dict[str, Dict[str, Any]],
    rules: Dict[str, Any],
    tolerance: float,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    repaired = dict(row)
    raw_status = str(row.get("status", ""))
    raw_reason = str(row.get("reason", ""))
    raw_period = str(row.get("aligned_period_key", ""))
    alignment_status = "unchanged"
    repaired_reason = raw_reason
    repaired_period = raw_period
    repaired_auto_value = row.get("auto_value")
    statement_type_hint = fact_index["statement_by_code"].get(row.get("mapping_code", ""), "")

    if raw_status == "missing_in_auto" and raw_reason in ALIGNMENT_REASONS:
        if raw_reason == "legacy_header_unsupported":
            repaired_period, repaired_reason, alignment_status = align_legacy_header(
                row=row,
                statement_type_hint=statement_type_hint,
                fact_index=fact_index,
                export_period_headers=export_period_headers,
                rules=rules,
            )
        elif raw_reason == "ambiguous_period_alignment":
            alignment_status = "ambiguous_alignment"
        elif raw_period:
            repaired_reason = raw_reason
            alignment_status = "true_missing"

        if repaired_period:
            repaired_auto_value = lookup_auto_value(export_rows, row.get("mapping_code", ""), repaired_period)
            repaired_status, repaired_reason = compare_values(
                row.get("benchmark_value"),
                repaired_auto_value,
                repaired_reason,
                tolerance,
            )
            repaired["aligned_period_key"] = repaired_period
            repaired["auto_value"] = repaired_auto_value
            repaired["status"] = repaired_status
            repaired["reason"] = repaired_reason
            if repaired_status == "missing_in_auto":
                alignment_status = "true_missing"
            else:
                alignment_status = "alignment_only_gap"
        elif alignment_status not in {"ambiguous_alignment", "true_missing"}:
            alignment_status = "true_missing"

    audit_row = {
        "run_id": "",
        "mapping_code": row.get("mapping_code", ""),
        "mapping_name": row.get("mapping_name", ""),
        "benchmark_header": row.get("benchmark_header", ""),
        "raw_aligned_period_key": raw_period,
        "repaired_aligned_period_key": repaired.get("aligned_period_key", ""),
        "benchmark_value": row.get("benchmark_value"),
        "raw_auto_value": row.get("auto_value"),
        "repaired_auto_value": repaired.get("auto_value"),
        "raw_status": raw_status,
        "repaired_status": repaired.get("status", ""),
        "raw_reason": raw_reason,
        "repaired_reason": repaired.get("reason", ""),
        "alignment_status": alignment_status,
        "statement_type_hint": statement_type_hint,
    }
    return repaired, audit_row


def align_legacy_header(
    row: Dict[str, Any],
    statement_type_hint: str,
    fact_index: Dict[str, Any],
    export_period_headers: Sequence[str],
    rules: Dict[str, Any],
) -> Tuple[str, str, str]:
    header = str(row.get("benchmark_header", "")).strip()
    if not header:
        return "", "legacy_header_unsupported", "true_missing"
    header_map = rules.get("legacy_role_map", {}) or {}
    amount_headers = set(rules.get("legacy_amount_headers", ["金额"]))
    if header in header_map:
        role = str(header_map.get(header, "")).strip()
        matches = [value for value in export_period_headers if value.endswith(f"__{role}")]
        if len(matches) == 1:
            return matches[0], f"legacy_role_repaired:{header}", "alignment_only_gap"
        if len(matches) > 1:
            return "", "ambiguous_period_alignment", "ambiguous_alignment"
        return "", "no_matching_period_for_legacy_role", "true_missing"
    if header in amount_headers:
        amount_candidates = candidate_amount_periods(
            mapping_code=str(row.get("mapping_code", "")),
            statement_type_hint=statement_type_hint,
            fact_index=fact_index,
            export_period_headers=export_period_headers,
            rules=rules,
            mapping_name=str(row.get("mapping_name", "")),
        )
        if len(amount_candidates) == 1:
            return amount_candidates[0], "legacy_amount_current_period_match", "alignment_only_gap"
        if len(amount_candidates) > 1:
            return "", "ambiguous_alignment", "ambiguous_alignment"
    return "", "legacy_header_unsupported", "true_missing"


def candidate_amount_periods(
    mapping_code: str,
    statement_type_hint: str,
    fact_index: Dict[str, Any],
    export_period_headers: Sequence[str],
    rules: Dict[str, Any],
    mapping_name: str = "",
) -> List[str]:
    allowed_types = set(rules.get("annual_amount_statement_types", ["income_statement", "cash_flow"]))
    inferred_type = statement_type_hint or infer_statement_type_from_name(mapping_name, rules)
    if inferred_type not in allowed_types:
        return []
    per_mapping_candidates = sorted(
        {
            fact.period_key
            for fact in fact_index["facts_by_code"].get(mapping_code, [])
            if fact.period_key.endswith("__本期")
            and "年度" in (fact.report_date_norm or fact.period_key.split("__", 1)[0])
        }
    )
    if len(per_mapping_candidates) == 1:
        return per_mapping_candidates
    candidates = [
        value
        for value in export_period_headers
        if value.endswith("__本期") and "年度" in value.split("__", 1)[0]
    ]
    if len(candidates) == 1:
        return candidates
    return candidates


def infer_statement_type_from_name(mapping_name: str, rules: Dict[str, Any]) -> str:
    name = str(mapping_name or "").strip()
    income_keywords = rules.get(
        "income_statement_keywords",
        ["收入", "成本", "利润", "费用", "税金", "所得税", "营业外"],
    )
    cash_flow_keywords = rules.get(
        "cash_flow_keywords",
        ["现金流", "经营活动", "投资活动", "筹资活动"],
    )
    if any(keyword in name for keyword in income_keywords):
        return "income_statement"
    if any(keyword in name for keyword in cash_flow_keywords):
        return "cash_flow"
    return ""


def build_fact_index(facts: Sequence[FactRecord]) -> Dict[str, Any]:
    facts_by_code: Dict[str, List[FactRecord]] = defaultdict(list)
    statement_by_code: Dict[str, str] = {}
    for fact in facts:
        if not fact.mapping_code:
            continue
        facts_by_code[fact.mapping_code].append(fact)
    for mapping_code, items in facts_by_code.items():
        counts = Counter(fact.statement_type for fact in items if fact.statement_type)
        statement_by_code[mapping_code] = counts.most_common(1)[0][0] if counts else ""
    return {
        "facts_by_code": facts_by_code,
        "statement_by_code": statement_by_code,
    }


def lookup_auto_value(export_rows: Dict[str, Dict[str, Any]], mapping_code: str, period_key: str) -> Any:
    row = export_rows.get(mapping_code, {}) if isinstance(export_rows, dict) else {}
    values = row.get("values", {}) if isinstance(row, dict) else {}
    return values.get(period_key)
