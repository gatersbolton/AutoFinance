from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Sequence

from ..normalize.text import normalize_label_for_matching


def score_shadow_candidate(
    candidate: Dict[str, Any],
    *,
    existing_alias_bindings: Dict[str, str],
    existing_formula_rules: set[str],
    rules: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    rules = rules or {}
    payload = dict(candidate)
    promotion_kind = str(payload.get("promotion_kind", "")).strip()
    payload.setdefault("safe_to_auto_apply", False)
    payload.setdefault("target_gap_closing_potential", 0)
    payload.setdefault("benchmark_support", 0)
    payload.setdefault("amount_gain", 0.0)
    payload.setdefault("evidence_count", 0)
    payload.setdefault("selection_reason", "")
    payload.setdefault("selection_status", "rejected")

    if promotion_kind == "alias":
        return _score_alias_candidate(payload, existing_alias_bindings=existing_alias_bindings, rules=rules)
    if promotion_kind == "formula":
        return _score_formula_candidate(payload, existing_formula_rules=existing_formula_rules, rules=rules)
    if promotion_kind == "placement":
        return _score_closure_candidate(payload, rules=rules, expected_kind="placement")
    if promotion_kind == "period":
        return _score_closure_candidate(payload, rules=rules, expected_kind="period")
    payload["selection_reason"] = "unsupported_promotion_kind"
    return payload


def sort_shadow_candidates(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = list(rows)
    ordered.sort(
        key=lambda row: (
            not bool(row.get("safe_to_auto_apply")),
            -int(row.get("target_gap_closing_potential", 0) or 0),
            -int(row.get("benchmark_support", 0) or 0),
            -float(row.get("amount_gain", 0.0) or 0.0),
            -int(row.get("evidence_count", 0) or 0),
            str(row.get("promotion_kind", "")),
            str(row.get("mapping_code", "")),
            str(row.get("period_key", "")),
            str(row.get("promotion_id", "")),
        )
    )
    return ordered


def _score_alias_candidate(
    row: Dict[str, Any],
    *,
    existing_alias_bindings: Dict[str, str],
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    safe_methods = set(rules.get("alias_safe_methods", ["exact_normalized_match", "alias_table"]))
    min_benchmark_support = int(rules.get("alias_min_benchmark_support", 1))
    min_evidence = int(rules.get("alias_min_evidence_count", 2))
    min_score = float(rules.get("alias_min_average_score", 0.75))
    alias_text = str(row.get("alias", "")).strip()
    mapping_code = str(row.get("mapping_code", "")).strip()
    normalized_alias = normalize_label_for_matching(alias_text)
    existing_binding = existing_alias_bindings.get(normalized_alias, "")
    if existing_binding and existing_binding != mapping_code:
        row["selection_reason"] = "alias_conflicts_with_existing_pack"
        return row
    if existing_binding == mapping_code:
        row["selection_reason"] = "alias_already_present"
        return row
    if bool(row.get("aggregate_ambiguous")) or bool(row.get("split_ambiguous")):
        row["selection_reason"] = "aggregate_or_split_ambiguous"
        return row
    if int(row.get("conflicting_target_count", 0) or 0) > 1:
        row["selection_reason"] = "multiple_competing_targets"
        return row
    if bool(row.get("review_only_alias_type")):
        row["selection_reason"] = "review_only_alias"
        return row
    method = str(row.get("candidate_method", "")).strip()
    benchmark_support = int(row.get("benchmark_support", 0) or 0)
    evidence_count = int(row.get("evidence_count", 0) or 0)
    average_score = float(row.get("average_candidate_score", 0.0) or 0.0)
    safe = (
        (method in safe_methods or average_score >= min_score)
        and (benchmark_support >= min_benchmark_support or evidence_count >= min_evidence)
        and bool(alias_text)
        and bool(mapping_code)
    )
    row["safe_to_auto_apply"] = safe
    row["selection_status"] = "selected" if safe else "rejected"
    row["selection_reason"] = "safe_alias_candidate" if safe else "alias_strength_below_threshold"
    return row


def _score_formula_candidate(
    row: Dict[str, Any],
    *,
    existing_formula_rules: set[str],
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    payload = parse_payload_json(row.get("payload_json", ""))
    rule_id = str(payload.get("rule_id", row.get("rule_id", ""))).strip()
    if not rule_id:
        row["selection_reason"] = "missing_formula_rule_id"
        return row
    if rule_id in existing_formula_rules:
        row["selection_reason"] = "formula_rule_already_present"
        return row
    if str(payload.get("safety_level", "deterministic")).strip() != "deterministic":
        row["selection_reason"] = "formula_not_deterministic"
        return row
    if int(row.get("conflicts_introduced", 0) or 0) > 0:
        row["selection_reason"] = "formula_conflicts_present"
        return row
    if not bool(row.get("children_resolved", False)):
        row["selection_reason"] = "formula_children_not_resolved"
        return row
    row["safe_to_auto_apply"] = True
    row["selection_status"] = "selected"
    row["selection_reason"] = "safe_formula_candidate"
    return row


def _score_closure_candidate(row: Dict[str, Any], *, rules: Dict[str, Any], expected_kind: str) -> Dict[str, Any]:
    if str(row.get("promotion_kind", "")).strip() != expected_kind:
        row["selection_reason"] = "unexpected_closure_kind"
        return row
    if bool(row.get("ambiguous")):
        row["selection_reason"] = "ambiguous_closure_candidate"
        return row
    if not bool(row.get("safe_to_auto_close", False)):
        row["selection_reason"] = "not_safe_to_auto_close"
        return row
    row["safe_to_auto_apply"] = True
    row["selection_status"] = "selected"
    row["selection_reason"] = f"safe_{expected_kind}_candidate"
    return row


def parse_payload_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_existing_alias_bindings(rows: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    bindings: Dict[str, str] = {}
    for row in rows:
        alias = normalize_label_for_matching(row.get("alias", ""))
        mapping_target = str(
            row.get("canonical_code", "")
            or row.get("mapping_code", "")
            or row.get("canonical_name", "")
        ).strip()
        if alias and mapping_target:
            bindings[alias] = mapping_target
    return bindings
