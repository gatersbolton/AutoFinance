from __future__ import annotations

from typing import Dict, List, Sequence

from ..models import FactRecord


def apply_period_overrides(facts: List[FactRecord], entries: Sequence[Dict[str, object]]) -> List[FactRecord]:
    by_fact_id = {str(entry.get("fact_id", "")).strip(): entry for entry in entries if str(entry.get("fact_id", "")).strip()}
    by_source_ref = {str(entry.get("source_cell_ref", "")).strip(): entry for entry in entries if str(entry.get("source_cell_ref", "")).strip()}

    for fact in facts:
        entry = by_fact_id.get(fact.fact_id) or by_source_ref.get(fact.source_cell_ref)
        if entry is None:
            continue
        period_key = str(entry.get("period_key", "")).strip()
        report_date_norm = str(entry.get("report_date_norm", "")).strip()
        period_role_norm = str(entry.get("period_role_norm", "")).strip()
        if period_key:
            fact.period_key = period_key
            if "__" in period_key:
                report_date_norm, period_role_norm = period_key.split("__", 1)
        if report_date_norm:
            fact.report_date_norm = report_date_norm
        if period_role_norm:
            fact.period_role_norm = period_role_norm
        fact.period_source_level = "manual_override"
        fact.period_reason = str(entry.get("note", "manual_period_override")).strip() or "manual_period_override"
        fact.override_source = "manual_override"
    return facts
