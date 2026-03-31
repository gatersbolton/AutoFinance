from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Tuple

from ..models import FactRecord


def resolve_single_period_annual_roles(
    facts: List[FactRecord],
    rules: Dict[str, Any] | None = None,
    enabled: bool = False,
) -> Tuple[List[FactRecord], List[Dict[str, Any]], Dict[str, Any]]:
    rules = rules or {}
    active_facts = [fact for fact in facts if fact.status != "suppressed"]
    before_unknown = sum(1 for fact in active_facts if (fact.period_role_norm or "") == "unknown")
    before_blocking = sum(1 for fact in active_facts if is_unknown_period_role_blocking(fact))
    if not enabled:
        return facts, [], {
            "enabled": False,
            "unknown_period_role_total_before": before_unknown,
            "unknown_period_role_total_after": before_unknown,
            "unknown_period_role_export_blocking_total_before": before_blocking,
            "unknown_period_role_export_blocking_total_after": before_blocking,
        }

    grouped = defaultdict(list)
    for fact in facts:
        grouped[(fact.doc_id, fact.page_no, fact.logical_subtable_id)].append(fact)

    audit_rows: List[Dict[str, Any]] = []
    changes = 0
    allowed_statement_types = set(rules.get("statement_types", ["income_statement", "cash_flow"]))
    generic_headers = set(rules.get("generic_headers", ["金额", "本期金额", "本年累计数", "本年累计"]))
    inferred_role = str(rules.get("inferred_role", "本期"))
    for _, group_facts in grouped.items():
        value_columns = {fact.column_semantic_key or fact.col_header_raw for fact in group_facts if fact.value_num is not None}
        if len(value_columns) != 1:
            continue
        header_text = " ".join(sorted({(fact.col_header_raw or "").strip() for fact in group_facts if (fact.col_header_raw or "").strip()}))
        numeric_facts = [fact for fact in group_facts if fact.value_num is not None]
        for fact in numeric_facts:
            if fact.statement_type not in allowed_statement_types:
                continue
            if fact.report_date_norm in {"", "unknown_date"}:
                continue
            if not (fact.report_date_norm.endswith("年度") or "月-" in fact.report_date_norm):
                continue
            if (fact.period_role_norm or "") != "unknown":
                continue
            if header_text and header_text not in generic_headers and not any(item in header_text for item in generic_headers):
                continue
            original_period_key = fact.period_key
            fact.period_role_norm = inferred_role
            fact.period_key = f"{fact.report_date_norm}__{fact.period_role_norm}"
            fact.period_role_inference_reason = "single_numeric_annual_main_statement"
            fact.period_role_inference_source = "stage6_annual_period_rules"
            fact.period_reason = fact.period_reason or "single_numeric_annual_main_statement"
            changes += 1
            audit_rows.append(
                {
                    "doc_id": fact.doc_id,
                    "page_no": fact.page_no,
                    "logical_subtable_id": fact.logical_subtable_id,
                    "fact_id": fact.fact_id,
                    "statement_type": fact.statement_type,
                    "original_period_key": original_period_key,
                    "inferred_period_key": fact.period_key,
                    "original_period_role": "unknown",
                    "inferred_period_role": fact.period_role_norm,
                    "inference_reason": fact.period_role_inference_reason,
                    "evidence_source": fact.period_role_inference_source,
                    "header_text": header_text,
                    "run_id": "",
                }
            )

    after_unknown = sum(1 for fact in active_facts if (fact.period_role_norm or "") == "unknown")
    after_blocking = sum(1 for fact in active_facts if is_unknown_period_role_blocking(fact))
    return facts, audit_rows, {
        "enabled": True,
        "changes_total": changes,
        "unknown_period_role_total_before": before_unknown,
        "unknown_period_role_total_after": after_unknown,
        "unknown_period_role_export_blocking_total_before": before_blocking,
        "unknown_period_role_export_blocking_total_after": after_blocking,
    }


def is_unknown_period_role_blocking(fact: FactRecord) -> bool:
    return bool(
        fact.mapping_code
        and fact.value_num is not None
        and fact.report_date_norm
        and fact.report_date_norm != "unknown_date"
        and (fact.period_role_norm or "") == "unknown"
    )
