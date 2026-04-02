from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import yaml


def apply_promotions(
    action_rows: Sequence[Dict[str, Any]],
    config_dir: Path,
    promotion_rules: Dict[str, Any] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    promotion_rules = promotion_rules or {}
    supported = set(promotion_rules.get("supported_actions", []) or promotion_rules.get("supported_action_types", []))
    curated_alias_path = config_dir / "curated_alias_pack.yml"
    curated_formula_path = config_dir / "curated_formula_pack.yml"
    target_scope_rules_path = config_dir / "target_scope_rules.yml"

    alias_payload = load_yaml(curated_alias_path)
    formula_payload = load_yaml(curated_formula_path)
    target_scope_payload = load_yaml(target_scope_rules_path)
    alias_entries = list(alias_payload.get("aliases", []))
    formula_entries = list(formula_payload.get("rules", []))
    target_entries = list(target_scope_payload.get("promoted_rules", []))

    applied: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []
    touched_files = set()
    promoted_aliases: List[Dict[str, Any]] = []
    promoted_formulas: List[Dict[str, Any]] = []

    for index, row in enumerate(action_rows, start=1):
        action_type = str(row.get("action_type", "")).strip()
        if not action_type:
            continue
        action_id = f"PROMACT_{index:04d}"
        if supported and action_type not in supported:
            rejected.append({"action_id": action_id, "promotion_id": row.get("promotion_id", ""), "action_type": action_type, "reject_reason": "unsupported_action"})
            continue
        if action_type in {"reject", "defer"}:
            rejected.append({"action_id": action_id, "promotion_id": row.get("promotion_id", ""), "action_type": action_type, "reject_reason": action_type})
            continue
        timestamp = datetime.now(timezone.utc).isoformat()
        if action_type in {"promote_alias", "promote_legacy_alias"}:
            alias_text = str(row.get("action_value", "") or row.get("candidate_alias", "")).strip()
            canonical_code = str(row.get("canonical_code", "")).strip()
            canonical_name = str(row.get("canonical_name", "")).strip()
            if not alias_text or not canonical_code or not canonical_name:
                rejected.append({"action_id": action_id, "promotion_id": row.get("promotion_id", ""), "action_type": action_type, "reject_reason": "missing_alias_fields"})
                continue
            alias_type = "legacy_alias" if action_type == "promote_legacy_alias" else "exact_alias"
            entry = {
                "canonical_code": canonical_code,
                "canonical_name": canonical_name,
                "alias": alias_text,
                "alias_type": alias_type,
                "enabled": True,
                "note": f"promoted:{row.get('promotion_id', '')}",
            }
            if not any(existing.get("canonical_code") == canonical_code and existing.get("alias") == alias_text for existing in alias_entries):
                alias_entries.append(entry)
            touched_files.add(str(curated_alias_path.name))
            applied_row = {
                "action_id": action_id,
                "promotion_id": row.get("promotion_id", ""),
                "action_type": action_type,
                "target_id": canonical_code,
                "target_scope": "curated_alias_pack",
                "config_file_touched": curated_alias_path.name,
                "apply_timestamp": timestamp,
                "apply_message": "alias_promoted",
            }
            applied.append(applied_row)
            promoted_aliases.append({"run_id": "", **entry})
            audit_rows.append({**applied_row, "old_state": "", "new_state": json.dumps(entry, ensure_ascii=False, sort_keys=True)})
            continue
        if action_type == "promote_formula_rule":
            payload = parse_json_field(row.get("action_value", "")) or parse_json_field(row.get("formula_payload_json", ""))
            if not payload or not payload.get("target_code"):
                rejected.append({"action_id": action_id, "promotion_id": row.get("promotion_id", ""), "action_type": action_type, "reject_reason": "missing_formula_payload"})
                continue
            if not any(existing.get("rule_id") == payload.get("rule_id") for existing in formula_entries):
                formula_entries.append(payload)
            touched_files.add(str(curated_formula_path.name))
            applied_row = {
                "action_id": action_id,
                "promotion_id": row.get("promotion_id", ""),
                "action_type": action_type,
                "target_id": payload.get("rule_id", ""),
                "target_scope": "curated_formula_pack",
                "config_file_touched": curated_formula_path.name,
                "apply_timestamp": timestamp,
                "apply_message": "formula_promoted",
            }
            applied.append(applied_row)
            promoted_formulas.append({"run_id": "", **payload})
            audit_rows.append({**applied_row, "old_state": "", "new_state": json.dumps(payload, ensure_ascii=False, sort_keys=True)})
            continue
        if action_type == "promote_target_rule":
            payload = parse_json_field(row.get("action_value", "")) or parse_json_field(row.get("target_scope_rule_json", ""))
            if not payload:
                rejected.append({"action_id": action_id, "promotion_id": row.get("promotion_id", ""), "action_type": action_type, "reject_reason": "missing_target_scope_payload"})
                continue
            if payload not in target_entries:
                target_entries.append(payload)
            touched_files.add(str(target_scope_rules_path.name))
            applied_row = {
                "action_id": action_id,
                "promotion_id": row.get("promotion_id", ""),
                "action_type": action_type,
                "target_id": payload.get("mapping_code", ""),
                "target_scope": "target_scope_rules",
                "config_file_touched": target_scope_rules_path.name,
                "apply_timestamp": timestamp,
                "apply_message": "target_rule_promoted",
            }
            applied.append(applied_row)
            audit_rows.append({**applied_row, "old_state": "", "new_state": json.dumps(payload, ensure_ascii=False, sort_keys=True)})
            continue
        rejected.append({"action_id": action_id, "promotion_id": row.get("promotion_id", ""), "action_type": action_type, "reject_reason": "unsupported_action"})

    if touched_files:
        alias_payload["aliases"] = alias_entries
        formula_payload["rules"] = formula_entries
        target_scope_payload["promoted_rules"] = target_entries
        write_yaml(curated_alias_path, alias_payload)
        write_yaml(curated_formula_path, formula_payload)
        write_yaml(target_scope_rules_path, target_scope_payload)

    summary = {
        "run_id": "",
        "applied_total": len(applied),
        "rejected_total": len(rejected),
        "promoted_alias_total": len(promoted_aliases),
        "promoted_formula_total": len(promoted_formulas),
        "touched_files": sorted(touched_files),
        "promoted_aliases": promoted_aliases,
        "promoted_formulas": promoted_formulas,
    }
    return applied, rejected, audit_rows, summary


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}


def write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def parse_json_field(value: Any) -> Dict[str, Any]:
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
