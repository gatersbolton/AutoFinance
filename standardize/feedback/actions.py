from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple


SUPPORTED_ACTIONS = {
    "accept_mapping_alias",
    "set_mapping_override",
    "set_conflict_winner",
    "set_period_override",
    "suppress_false_positive",
    "mark_not_financial_fact",
    "request_reocr",
    "accept_relation_review_only",
    "ignore",
    "defer",
}


ACTION_GUIDE = [
    {
        "action_type": "accept_mapping_alias",
        "action_value_format": "leave blank or canonical code",
        "effect": "Create reusable global alias override for the current label.",
    },
    {
        "action_type": "set_mapping_override",
        "action_value_format": "canonical code",
        "effect": "Set a local mapping override for this fact or source cell.",
    },
    {
        "action_type": "set_conflict_winner",
        "action_value_format": "fact_id or provider:<provider>",
        "effect": "Pick the winning provider result for a specific conflict.",
    },
    {
        "action_type": "set_period_override",
        "action_value_format": "period_key like 2022-12-31__期末数",
        "effect": "Override the normalized period for the targeted fact.",
    },
    {
        "action_type": "suppress_false_positive",
        "action_value_format": "leave blank",
        "effect": "Suppress the fact from export/review because it is an OCR false positive.",
    },
    {
        "action_type": "mark_not_financial_fact",
        "action_value_format": "leave blank",
        "effect": "Suppress the fact as non-financial explanatory content.",
    },
    {
        "action_type": "request_reocr",
        "action_value_format": "task_id or leave blank",
        "effect": "Confirm a targeted re-OCR request without changing facts directly.",
    },
    {
        "action_type": "accept_relation_review_only",
        "action_value_format": "leave blank",
        "effect": "Close a relation-based review item while keeping it out of the main export.",
    },
    {
        "action_type": "ignore",
        "action_value_format": "leave blank",
        "effect": "Close the review item without changing facts.",
    },
    {
        "action_type": "defer",
        "action_value_format": "leave blank",
        "effect": "Record a deferral and keep the item in future review backlogs.",
    },
]


def action_value_or_fallback(row: Dict[str, Any], field_name: str) -> str:
    primary = str(row.get("action_value", "")).strip()
    if primary:
        return primary
    return str(row.get(field_name, "")).strip()


def validate_action_row(row: Dict[str, Any], valid_review_ids: Iterable[str]) -> Tuple[bool, str]:
    review_id = str(row.get("review_id", "")).strip()
    action_type = str(row.get("action_type", "")).strip()
    if not action_type:
        return False, "blank_action"
    if action_type not in SUPPORTED_ACTIONS:
        return False, "unsupported_action_type"
    if review_id not in set(valid_review_ids):
        return False, "review_id_not_found"
    if action_type in {"accept_mapping_alias", "set_mapping_override"}:
        if not (str(row.get("candidate_mapping_code", "")).strip() or str(row.get("action_value", "")).strip()):
            return False, "mapping_code_missing"
    if action_type == "set_conflict_winner":
        if not (str(row.get("action_value", "")).strip() or str(row.get("candidate_conflict_fact_id", "")).strip()):
            return False, "conflict_winner_missing"
    if action_type == "set_period_override":
        if not (str(row.get("action_value", "")).strip() or str(row.get("candidate_period_override", "")).strip()):
            return False, "period_override_missing"
    if action_type == "request_reocr":
        if not (str(row.get("action_value", "")).strip() or str(row.get("suggested_reocr_task_id", "")).strip()):
            return False, "reocr_task_missing"
    return True, ""
