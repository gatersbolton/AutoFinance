from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import yaml

from .batch_apply import materialize_shadow_promotion_batch, parse_payload_json
from .scorer import build_existing_alias_bindings, score_shadow_candidate, sort_shadow_candidates


def load_shadow_inputs_from_dir(run_dir: Path) -> Dict[str, Any]:
    return {
        "alias_candidates": read_csv(run_dir / "alias_acceptance_candidates.csv"),
        "formula_candidates": read_csv(run_dir / "candidate_formula_placements.csv"),
        "benchmark_gap_rows": read_csv(run_dir / "benchmark_gap_explanations.csv"),
        "benchmark_missing_true_rows": read_csv(run_dir / "benchmark_missing_true.csv"),
        "export_target_scope_rows": read_csv(run_dir / "export_target_scope.csv"),
        "unmapped_value_bearing_rows": read_csv(run_dir / "unmapped_value_bearing.csv"),
        "source_backed_closure_rows": read_csv(run_dir / "source_backed_gap_closure.csv"),
    }


def build_shadow_promotion_plan(
    *,
    baseline_run_id: str,
    inputs: Dict[str, Any],
    config_dir: Path,
    rules: Dict[str, Any] | None = None,
    max_auto_promotions: int = 10,
    auto_apply_safe_promotions: bool = False,
) -> Dict[str, Any]:
    rules = rules or {}
    candidate_rows = build_shadow_candidate_rows(
        baseline_run_id=baseline_run_id,
        inputs=inputs,
        rules=rules,
    )
    existing_alias_bindings = gather_existing_alias_bindings(config_dir)
    existing_formula_rules = gather_existing_formula_rule_ids(config_dir)

    scored_rows = [
        score_shadow_candidate(
            row,
            existing_alias_bindings=existing_alias_bindings,
            existing_formula_rules=existing_formula_rules,
            rules=rules,
        )
        for row in candidate_rows
    ]
    ordered_rows = sort_shadow_candidates(scored_rows)
    selected_rows = [
        {**row, "selected_rank": index}
        for index, row in enumerate(
            [row for row in ordered_rows if row.get("safe_to_auto_apply")][: max(0, int(max_auto_promotions or 0))],
            start=1,
        )
    ]
    selected_ids = {row.get("promotion_id", "") for row in selected_rows}
    audit_rows = [
        {
            **row,
            "selected_for_shadow_batch": row.get("promotion_id", "") in selected_ids,
        }
        for row in ordered_rows
    ]
    batch_payload = materialize_shadow_promotion_batch(selected_rows, config_dir)
    summary = {
        "run_id": "",
        "baseline_run_id": baseline_run_id,
        "candidate_total": len(candidate_rows),
        "selected_total": len(selected_rows),
        "applied_total": len(selected_rows) if auto_apply_safe_promotions else 0,
        "selected_alias_total": batch_payload.get("selected_alias_total", 0),
        "selected_formula_total": batch_payload.get("selected_formula_total", 0),
        "selected_placement_total": batch_payload.get("selected_placement_total", 0),
        "selected_period_total": batch_payload.get("selected_period_total", 0),
        "max_auto_promotions": int(max_auto_promotions or 0),
        "shadow_alias_pack_path": str(batch_payload.get("shadow_alias_path", "")),
        "shadow_formula_pack_path": str(batch_payload.get("shadow_formula_path", "")),
        "auto_apply_safe_promotions": bool(auto_apply_safe_promotions),
        "selection_reason_breakdown": count_by_key(audit_rows, "selection_reason"),
    }
    return {
        "selected_rows": selected_rows,
        "audit_rows": audit_rows,
        "summary": summary,
        "runtime_actions": batch_payload.get("runtime_actions", {}),
    }


def build_shadow_candidate_rows(
    *,
    baseline_run_id: str,
    inputs: Dict[str, Any],
    rules: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    alias_types_by_gap: Dict[str, str] = {}
    closure_rows = inputs.get("source_backed_closure_rows", []) or []
    for closure in closure_rows:
        payload = parse_payload_json(closure.get("payload_json", ""))
        promotion_kind = str(payload.get("promotion_kind", "")).strip()
        if promotion_kind == "alias":
            alias_value = str(payload.get("alias", "")).strip()
            rows.append(
                {
                    "promotion_id": payload.get("promotion_id", ""),
                    "promotion_kind": "alias",
                    "source_type": "source_backed_gap_closure",
                    "gap_id": closure.get("gap_id", ""),
                    "mapping_code": closure.get("mapping_code", ""),
                    "mapping_name": closure.get("mapping_name", ""),
                    "period_key": closure.get("aligned_period_key", ""),
                    "alias": alias_value,
                    "alias_type": str(rules.get("default_shadow_alias_type", "exact_alias")).strip() or "exact_alias",
                    "statement_types": list(payload.get("statement_types", [])),
                    "candidate_method": "source_backed_gap_closure",
                    "average_candidate_score": 1.0,
                    "conflicting_target_count": 1,
                    "aggregate_ambiguous": False,
                    "split_ambiguous": False,
                    "review_only_alias_type": False,
                    "benchmark_support": 1,
                    "amount_gain": _as_float(closure.get("benchmark_value")),
                    "evidence_count": evidence_count(closure.get("source_fact_ids", "")),
                    "target_gap_closing_potential": 1,
                    "safe_to_auto_close": parse_bool(closure.get("safe_to_auto_close", False)),
                    "selection_reason": "",
                    "selection_status": "pending",
                    "source_run_id": baseline_run_id,
                    "note": f"shadow_promotion:{closure.get('gap_id', '')}",
                }
            )
            alias_types_by_gap[str(closure.get("gap_id", "")).strip()] = "alias"
            continue
        if promotion_kind == "placement":
            rows.append(
                {
                    "promotion_id": payload.get("promotion_id", ""),
                    "promotion_kind": "placement",
                    "source_type": "source_backed_gap_closure",
                    "gap_id": closure.get("gap_id", ""),
                    "mapping_code": closure.get("mapping_code", ""),
                    "mapping_name": closure.get("mapping_name", ""),
                    "period_key": payload.get("period_key", closure.get("aligned_period_key", "")),
                    "fact_id": payload.get("fact_id", ""),
                    "statement_type": payload.get("statement_type", ""),
                    "benchmark_support": 1,
                    "amount_gain": _as_float(closure.get("benchmark_value")),
                    "evidence_count": evidence_count(closure.get("source_fact_ids", "")),
                    "target_gap_closing_potential": 1,
                    "safe_to_auto_close": parse_bool(closure.get("safe_to_auto_close", False)),
                    "ambiguous": False,
                    "source_run_id": baseline_run_id,
                }
            )
            continue
        if promotion_kind == "period":
            rows.append(
                {
                    "promotion_id": payload.get("promotion_id", ""),
                    "promotion_kind": "period",
                    "source_type": "source_backed_gap_closure",
                    "gap_id": closure.get("gap_id", ""),
                    "mapping_code": closure.get("mapping_code", ""),
                    "mapping_name": closure.get("mapping_name", ""),
                    "period_key": payload.get("aligned_period_key", ""),
                    "benchmark_header": payload.get("benchmark_header", ""),
                    "aligned_period_key": payload.get("aligned_period_key", ""),
                    "benchmark_support": 1,
                    "amount_gain": _as_float(closure.get("benchmark_value")),
                    "evidence_count": evidence_count(closure.get("source_fact_ids", "")),
                    "target_gap_closing_potential": 1,
                    "safe_to_auto_close": parse_bool(closure.get("safe_to_auto_close", False)),
                    "ambiguous": False,
                    "source_run_id": baseline_run_id,
                }
            )

    for row in inputs.get("alias_candidates", []) or []:
        if not parse_bool(row.get("safe_to_auto_accept", False)):
            continue
        rows.append(
            {
                "promotion_id": f"SAFE_ALIAS_{row.get('canonical_code', '')}_{normalize_token(row.get('candidate_alias', ''))}",
                "promotion_kind": "alias",
                "source_type": "alias_acceptance_candidates",
                "gap_id": "",
                "mapping_code": row.get("canonical_code", ""),
                "mapping_name": row.get("canonical_name", ""),
                "period_key": "",
                "alias": row.get("candidate_alias", ""),
                "alias_type": str(rules.get("default_shadow_alias_type", "exact_alias")).strip() or "exact_alias",
                "statement_types": [row.get("statement_type", "")] if row.get("statement_type") else [],
                "candidate_method": row.get("candidate_method", ""),
                "average_candidate_score": _as_float(row.get("average_candidate_score")),
                "conflicting_target_count": int(row.get("conflicting_target_count", 0) or 0),
                "aggregate_ambiguous": False,
                "split_ambiguous": False,
                "review_only_alias_type": False,
                "benchmark_support": int(row.get("benchmark_support", 0) or 0),
                "amount_gain": _as_float(row.get("amount_coverage_gain")),
                "evidence_count": int(row.get("evidence_count", 0) or 0),
                "target_gap_closing_potential": 0,
                "safe_to_auto_close": True,
                "source_run_id": baseline_run_id,
                "note": "shadow_safe_alias_acceptance",
            }
        )

    formula_rules = build_formula_rule_lookup(inputs.get("formula_candidates", []) or [])
    for rule_id, values in formula_rules.items():
        rows.append(
            {
                "promotion_id": f"SAFE_FORMULA_{rule_id}",
                "promotion_kind": "formula",
                "source_type": "candidate_formula_placements",
                "gap_id": "",
                "mapping_code": values.get("mapping_code", ""),
                "mapping_name": values.get("mapping_name", ""),
                "period_key": values.get("period_key", ""),
                "rule_id": rule_id,
                "payload_json": values.get("payload_json", ""),
                "children_resolved": bool(values.get("children_resolved", False)),
                "conflicts_introduced": int(values.get("conflicts_introduced", 0)),
                "benchmark_support": 0,
                "amount_gain": float(values.get("amount_gain", 0.0) or 0.0),
                "evidence_count": int(values.get("evidence_count", 0) or 0),
                "target_gap_closing_potential": 0,
                "source_run_id": baseline_run_id,
            }
        )

    return rows


def build_formula_rule_lookup(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        rule_id = str(row.get("rule_id", "")).strip()
        if not rule_id:
            continue
        bucket = grouped.setdefault(
            rule_id,
            {
                "mapping_code": row.get("mapping_code", ""),
                "mapping_name": row.get("mapping_name", ""),
                "period_key": row.get("period_key", ""),
                "payload_json": row.get("formula_payload_json", ""),
                "amount_gain": 0.0,
                "evidence_count": 0,
                "children_resolved": True,
                "conflicts_introduced": 0,
            },
        )
        bucket["amount_gain"] += _as_float(row.get("value_num"))
        bucket["evidence_count"] += 1
        bucket["children_resolved"] = bucket["children_resolved"] and parse_bool(row.get("exportable", False))
    return grouped


def gather_existing_alias_bindings(config_dir: Path) -> Dict[str, str]:
    rows: List[Dict[str, Any]] = []
    for filename in ("subject_aliases.yml", "curated_alias_pack.yml", "curated_shadow_alias_pack.yml"):
        path = config_dir / filename
        if not path.exists():
            continue
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        aliases = payload.get("aliases", [])
        if isinstance(aliases, list):
            rows.extend(item for item in aliases if isinstance(item, dict))
        elif isinstance(aliases, dict):
            for canonical_name, alias_values in aliases.items():
                values = alias_values if isinstance(alias_values, list) else [alias_values]
                for alias in values:
                    rows.append({"alias": alias, "mapping_code": "", "canonical_code": "", "canonical_name": canonical_name})
    return build_existing_alias_bindings(rows)


def gather_existing_formula_rule_ids(config_dir: Path) -> set[str]:
    rule_ids: set[str] = set()
    for filename in ("formula_rules.yml", "formula_pack_rules.yml", "curated_formula_pack.yml", "curated_shadow_formula_pack.yml"):
        path = config_dir / filename
        if not path.exists():
            continue
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for rule in payload.get("rules", []) or []:
            if isinstance(rule, dict) and str(rule.get("rule_id", "")).strip():
                rule_ids.add(str(rule.get("rule_id", "")).strip())
    return rule_ids


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def count_by_key(rows: Iterable[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, "")).strip()
        counts[value] = counts.get(value, 0) + 1
    return counts


def evidence_count(value: Any) -> int:
    return len([token for token in str(value or "").split(";") if token.strip()])


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def normalize_token(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isalnum())[:24] or "alias"

