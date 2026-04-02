from __future__ import annotations

from typing import Any, Dict

from ..models import FactRecord


DEFAULT_ALLOWED_STATUSES = {
    "observed",
    "repaired",
    "accepted",
    "accepted_with_rule_support",
    "accepted_with_validation_support",
    "derived_resolved",
}


def classify_export_blocker(fact: FactRecord, export_rules: Dict[str, Any] | None = None) -> str:
    export_rules = export_rules or {}
    blocked_statement_types = set(export_rules.get("blocked_statement_types", ["unknown"]))
    blocked_period_roles = set(export_rules.get("blocked_period_roles", ["unknown"]))
    blocked_report_dates = set(export_rules.get("blocked_report_dates", ["unknown_date"]))
    blocked_target_scopes = set(export_rules.get("blocked_target_scopes", ["note_detail", "note_aggregation", "non_target_noise"]))
    allowed_statuses = set(export_rules.get("allowed_statuses", sorted(DEFAULT_ALLOWED_STATUSES)))

    if not fact.mapping_code:
        return "unmapped"
    if fact.status == "suppressed":
        return fact.suppression_reason or "suppressed"
    if fact.mapping_review_required:
        return "mapping_review_required"
    if fact.value_num is None:
        return "non_numeric_or_blank"
    if fact.statement_type in blocked_statement_types:
        return f"statement_type_blocked:{fact.statement_type}"
    if fact.target_scope and fact.target_scope in blocked_target_scopes:
        return f"target_scope_blocked:{fact.target_scope}"
    if not fact.report_date_norm or fact.report_date_norm in blocked_report_dates:
        return "unknown_date"
    if not fact.period_role_norm or fact.period_role_norm in blocked_period_roles:
        return "unknown_period_role"
    if fact.period_key.startswith("unknown_date__") or fact.period_key.endswith("__unknown"):
        return "unresolved_period_key"
    if fact.status not in allowed_statuses:
        return f"status_blocked:{fact.status}"
    if fact.conflict_decision in {"review_required", "unresolved"}:
        return f"conflict_{fact.conflict_decision}"
    if fact.duplicate_group_id and fact.kept_fact_id and fact.kept_fact_id != fact.fact_id:
        return "dropped_by_dedupe"
    if fact.source_kind == "derived_formula" and fact.unplaced_reason:
        return fact.unplaced_reason
    return ""


def is_exportable_fact(fact: FactRecord, export_rules: Dict[str, Any] | None = None) -> bool:
    return not classify_export_blocker(fact, export_rules)
