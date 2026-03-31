from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from ..overrides.storage import append_override_entry
from ..stable_ids import action_id_parts, stable_id
from .actions import action_value_or_fallback, validate_action_row


def apply_review_actions(
    action_rows: Sequence[Dict[str, str]],
    valid_review_ids: Iterable[str],
    config_dir: Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    valid_review_ids = set(valid_review_ids)
    applied: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []
    touched_files = set()

    for row in action_rows:
        if not str(row.get("action_type", "")).strip():
            continue
        is_valid, reason = validate_action_row(row, valid_review_ids)
        action_id = stable_id(
            "ACT_",
            action_id_parts(
                str(row.get("review_id", "")).strip(),
                str(row.get("action_type", "")).strip(),
                str(row.get("action_value", "")).strip(),
            ),
        )
        if not is_valid:
            rejected.append(
                {
                    "action_id": action_id,
                    "review_id": row.get("review_id", ""),
                    "action_type": row.get("action_type", ""),
                    "action_value": row.get("action_value", ""),
                    "reject_reason": reason,
                }
            )
            continue

        action_type = str(row.get("action_type", "")).strip()
        timestamp = datetime.now(timezone.utc).isoformat()
        override_entry, override_key, old_state, new_state, apply_message = build_override_entry(row, action_type, action_id)
        if override_key:
            touched_path = append_override_entry(config_dir, override_key, override_entry)
            touched_files.add(str(touched_path))
        applied_row = {
            "action_id": action_id,
            "review_id": row.get("review_id", ""),
            "action_type": action_type,
            "action_value": row.get("action_value", ""),
            "target_id": override_entry.get("target_id", ""),
            "target_scope": override_entry.get("target_scope", ""),
            "config_file_touched": override_entry.get("config_file_touched", ""),
            "apply_timestamp": timestamp,
            "apply_message": apply_message,
        }
        applied.append(applied_row)
        audit_rows.append(
            {
                "action_id": action_id,
                "review_id": row.get("review_id", ""),
                "action_type": action_type,
                "target_id": override_entry.get("target_id", ""),
                "target_scope": override_entry.get("target_scope", ""),
                "old_state": old_state,
                "new_state": new_state,
                "config_file_touched": override_entry.get("config_file_touched", ""),
                "apply_timestamp": timestamp,
                "apply_message": apply_message,
            }
        )

    summary = {
        "applied_total": len(applied),
        "rejected_total": len(rejected),
        "touched_files": sorted(touched_files),
        "action_type_breakdown": count_by_key(applied, "action_type"),
    }
    return applied, rejected, audit_rows, summary


def build_override_entry(row: Dict[str, str], action_type: str, action_id: str) -> Tuple[Dict[str, Any], str, str, str, str]:
    base = {
        "override_id": action_id,
        "review_id": str(row.get("review_id", "")).strip(),
        "fact_id": str(row.get("candidate_conflict_fact_id", "")).strip() if action_type == "set_conflict_winner" else "",
        "source_cell_ref": str(row.get("source_cell_ref", "")).strip(),
        "row_label_std": str(row.get("row_label_std", "")).strip(),
        "row_label_raw": str(row.get("row_label_raw", "")).strip(),
        "note": str(row.get("reviewer_note", "")).strip(),
        "reviewer_name": str(row.get("reviewer_name", "")).strip(),
        "action_type": action_type,
        "enabled": True,
    }

    if action_type == "accept_mapping_alias":
        canonical_code = action_value_or_fallback(row, "candidate_mapping_code")
        canonical_name = str(row.get("candidate_mapping_name", "")).strip()
        base.update(
            {
                "canonical_code": canonical_code,
                "canonical_name": canonical_name,
                "alias": str(row.get("row_label_std", "")).strip() or str(row.get("row_label_raw", "")).strip(),
                "alias_type": "exact_alias",
                "target_id": canonical_code,
                "target_scope": "global_label_alias",
                "config_file_touched": "manual_overrides/mapping_overrides.yml",
                "mapping_method": "manual_alias",
            }
        )
        return base, "mapping", "", f"{canonical_code}:{canonical_name}", "mapping_alias_override_applied"

    if action_type == "set_mapping_override":
        canonical_code = action_value_or_fallback(row, "candidate_mapping_code")
        canonical_name = str(row.get("candidate_mapping_name", "")).strip()
        base.update(
            {
                "canonical_code": canonical_code,
                "canonical_name": canonical_name,
                "fact_id": str(row.get("candidate_conflict_fact_id", "")).strip() or str(row.get("fact_id", "")).strip(),
                "target_id": str(row.get("fact_id", "")).strip() or str(row.get("review_id", "")).strip(),
                "target_scope": "local_fact_mapping",
                "config_file_touched": "manual_overrides/mapping_overrides.yml",
                "mapping_method": "manual_override",
            }
        )
        return base, "mapping", "", f"{canonical_code}:{canonical_name}", "local_mapping_override_applied"

    if action_type == "set_conflict_winner":
        action_value = action_value_or_fallback(row, "candidate_conflict_fact_id")
        if action_value.startswith("provider:"):
            base["winner_provider"] = action_value.split(":", 1)[1]
        else:
            base["winner_fact_id"] = action_value
        base.update(
            {
                "conflict_id": str(row.get("related_conflict_ids", row.get("candidate_conflict_id", ""))).split(",")[0].strip() or str(row.get("review_id", "")).strip(),
                "target_id": str(row.get("related_conflict_ids", row.get("candidate_conflict_id", ""))).split(",")[0].strip() or str(row.get("review_id", "")).strip(),
                "target_scope": "conflict",
                "config_file_touched": "manual_overrides/conflict_overrides.yml",
            }
        )
        return base, "conflict", "", action_value, "conflict_winner_override_applied"

    if action_type == "set_period_override":
        period_key = action_value_or_fallback(row, "candidate_period_override")
        report_date_norm, period_role_norm = split_period_key(period_key)
        base.update(
            {
                "period_key": period_key,
                "report_date_norm": report_date_norm,
                "period_role_norm": period_role_norm,
                "target_id": str(row.get("fact_id", "")).strip() or str(row.get("review_id", "")).strip(),
                "target_scope": "fact_period",
                "config_file_touched": "manual_overrides/period_overrides.yml",
            }
        )
        return base, "period", "", period_key, "period_override_applied"

    if action_type in {"suppress_false_positive", "mark_not_financial_fact"}:
        base.update(
            {
                "fact_id": str(row.get("fact_id", "")).strip(),
                "target_id": str(row.get("fact_id", "")).strip() or str(row.get("review_id", "")).strip(),
                "target_scope": "fact_suppression",
                "config_file_touched": "manual_overrides/suppression_overrides.yml",
            }
        )
        return base, "suppression", "", action_type, "suppression_override_applied"

    if action_type in {"request_reocr", "accept_relation_review_only", "ignore"}:
        base.update(
            {
                "task_id": action_value_or_fallback(row, "suggested_reocr_task_id"),
                "target_id": str(row.get("review_id", "")).strip(),
                "target_scope": "review_item",
                "config_file_touched": "manual_overrides/placement_overrides.yml",
            }
        )
        return base, "placement", "", action_type, "placement_review_override_applied"

    if action_type == "defer":
        base.update({"target_id": str(row.get("review_id", "")).strip(), "target_scope": "review_item", "config_file_touched": ""})
        return base, "", "", "deferred", "deferred_no_override_written"

    return base, "", "", "", "no_op"


def split_period_key(period_key: str) -> Tuple[str, str]:
    if "__" not in period_key:
        return "", ""
    return tuple(period_key.split("__", 1))


def count_by_key(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, int]:
    counter: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, "")).strip()
        counter[value] = counter.get(value, 0) + 1
    return counter
