from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence


def build_truly_no_source_backfill(
    benchmark_missing_true_rows: Sequence[Dict[str, Any]],
    investigation_rows: Sequence[Dict[str, Any]],
    benchmark_gap_rows: Sequence[Dict[str, Any]],
    rules: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    rules = rules or {}
    investigation_by_key = {
        (row.get("mapping_code", ""), row.get("aligned_period_key", ""), str(row.get("benchmark_value", ""))): row
        for row in investigation_rows
        if row.get("gap_cause") == "truly_no_source"
    }
    explanation_by_key = {
        (row.get("mapping_code", ""), row.get("aligned_period_key", ""), str(row.get("benchmark_value", ""))): row
        for row in benchmark_gap_rows
    }

    task_rows: List[Dict[str, Any]] = []
    manifest_rows: List[Dict[str, Any]] = []
    target_importance = rules.get("target_importance", {}) or {}
    amount_scale = float(rules.get("amount_scale", 0.000001))
    benchmark_supported_bonus = float(rules.get("benchmark_supported_bonus", 5.0))

    for row in benchmark_missing_true_rows:
        key = (row.get("mapping_code", ""), row.get("aligned_period_key", ""), str(row.get("benchmark_value", "")))
        investigation = investigation_by_key.get(key)
        if not investigation:
            continue
        explanation = explanation_by_key.get(key, {})
        period_key = str(row.get("aligned_period_key", "")).strip()
        report_date_norm, period_role_norm = split_period_key(period_key)
        mapping_code = str(row.get("mapping_code", "")).strip()
        statement_type = infer_statement_type(mapping_code, explanation, rules)
        importance = float(target_importance.get(mapping_code, target_importance.get("default", 1.0)))
        benchmark_support = 1.0 if str(explanation.get("gap_cause", "")).strip() else 0.0
        amount_abs = abs(float(row.get("benchmark_value") or 0.0)) if _is_numeric(row.get("benchmark_value")) else 0.0
        priority_score = round(importance * 10.0 + amount_abs * amount_scale + benchmark_support * benchmark_supported_bonus, 6)
        expected_page_hint = extract_expected_page_hint(str(investigation.get("evidence_refs", "")).strip())
        evidence_status = "missing_source_evidence" if not expected_page_hint else "has_page_hint"
        task_id = f"BACKFILL_{mapping_code}_{period_key or 'unknown'}"
        task_row = {
            "run_id": "",
            "task_id": task_id,
            "mapping_code": mapping_code,
            "mapping_name": row.get("mapping_name", ""),
            "period_key": period_key,
            "report_date_norm": report_date_norm,
            "period_role_norm": period_role_norm,
            "statement_type": statement_type,
            "benchmark_value": row.get("benchmark_value"),
            "target_importance": importance,
            "benchmark_supported": bool(benchmark_support),
            "amount_significance": amount_abs,
            "priority_score": priority_score,
            "expected_page_hint": expected_page_hint,
            "expected_table_hint": str(explanation.get("detail", "")).strip(),
            "evidence_status": evidence_status,
            "suggested_action": "targeted_source_backfill_or_reocr",
        }
        manifest_row = {
            "run_id": "",
            "task_id": task_id,
            "mapping_code": mapping_code,
            "mapping_name": row.get("mapping_name", ""),
            "statement_type": statement_type,
            "period_key": period_key,
            "expected_page_hint": expected_page_hint,
            "expected_table_hint": str(explanation.get("detail", "")).strip(),
            "evidence_status": evidence_status,
            "priority_score": priority_score,
            "meta_json": json.dumps(
                {
                    "benchmark_value": row.get("benchmark_value"),
                    "investigation_evidence_refs": investigation.get("evidence_refs", ""),
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        }
        task_rows.append(task_row)
        manifest_rows.append(manifest_row)

    task_rows.sort(key=lambda item: float(item.get("priority_score", 0.0) or 0.0), reverse=True)
    manifest_rows.sort(key=lambda item: float(item.get("priority_score", 0.0) or 0.0), reverse=True)
    summary = {
        "run_id": "",
        "tasks_total": len(task_rows),
        "missing_evidence_total": sum(1 for row in task_rows if row.get("evidence_status") == "missing_source_evidence"),
    }
    return {
        "task_rows": task_rows,
        "summary": summary,
        "manifest_rows": manifest_rows,
        "manifest_json": {
            "run_id": "",
            "tasks_total": len(manifest_rows),
            "tasks": manifest_rows,
        },
    }


def split_period_key(period_key: str) -> tuple[str, str]:
    if "__" not in period_key:
        return "", ""
    return tuple(period_key.split("__", 1))


def infer_statement_type(mapping_code: str, explanation: Dict[str, Any], rules: Dict[str, Any]) -> str:
    statement_overrides = rules.get("statement_type_by_code", {}) or {}
    if mapping_code in statement_overrides:
        return str(statement_overrides[mapping_code]).strip()
    detail = str(explanation.get("detail", "")).strip()
    if "现金流" in detail:
        return "cash_flow"
    if "利润" in str(explanation.get("mapping_name", "")):
        return "income_statement"
    return "balance_sheet"


def extract_expected_page_hint(evidence_refs: str) -> str:
    refs = [value.strip() for value in evidence_refs.split(";") if value.strip()]
    page_tokens = sorted({part for ref in refs for part in ref.split(":") if part.startswith("p")})
    return ";".join(page_tokens)


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False

