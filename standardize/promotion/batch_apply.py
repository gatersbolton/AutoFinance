from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

import yaml


def materialize_shadow_promotion_batch(
    selected_rows: Sequence[Dict[str, Any]],
    config_dir: Path,
) -> Dict[str, Any]:
    alias_rows = [row for row in selected_rows if str(row.get("promotion_kind", "")).strip() == "alias"]
    formula_rows = [row for row in selected_rows if str(row.get("promotion_kind", "")).strip() == "formula"]
    placement_rows = [row for row in selected_rows if str(row.get("promotion_kind", "")).strip() == "placement"]
    period_rows = [row for row in selected_rows if str(row.get("promotion_kind", "")).strip() == "period"]

    shadow_alias_path = config_dir / "curated_shadow_alias_pack.yml"
    shadow_formula_path = config_dir / "curated_shadow_formula_pack.yml"

    alias_payload = {
        "aliases": [
            {
                "canonical_code": row.get("mapping_code", ""),
                "canonical_name": row.get("mapping_name", ""),
                "alias": row.get("alias", ""),
                "alias_type": row.get("alias_type", "exact_alias"),
                "statement_types": list(row.get("statement_types", [])),
                "enabled": True,
                "note": row.get("note", ""),
                "source_run_id": row.get("source_run_id", ""),
                "promotion_id": row.get("promotion_id", ""),
                "gap_id": row.get("gap_id", ""),
            }
            for row in alias_rows
        ]
    }
    formula_payload = {
        "rules": [
            {
                **parse_payload_json(row.get("payload_json", "")),
                "enabled": True,
                "source_run_id": row.get("source_run_id", ""),
                "promotion_id": row.get("promotion_id", ""),
                "gap_id": row.get("gap_id", ""),
            }
            for row in formula_rows
        ]
    }
    write_yaml(shadow_alias_path, alias_payload)
    write_yaml(shadow_formula_path, formula_payload)

    runtime_actions = {
        "placement_preferences": [
            {
                "fact_id": row.get("fact_id", ""),
                "mapping_code": row.get("mapping_code", ""),
                "period_key": row.get("period_key", ""),
                "statement_type": row.get("statement_type", ""),
                "promotion_id": row.get("promotion_id", ""),
                "gap_id": row.get("gap_id", ""),
            }
            for row in placement_rows
        ],
        "period_overrides": [
            {
                "mapping_code": row.get("mapping_code", ""),
                "mapping_name": row.get("mapping_name", ""),
                "benchmark_header": row.get("benchmark_header", ""),
                "aligned_period_key": row.get("aligned_period_key", ""),
                "promotion_id": row.get("promotion_id", ""),
                "gap_id": row.get("gap_id", ""),
            }
            for row in period_rows
        ],
    }
    return {
        "shadow_alias_path": shadow_alias_path,
        "shadow_formula_path": shadow_formula_path,
        "runtime_actions": runtime_actions,
        "selected_alias_total": len(alias_rows),
        "selected_formula_total": len(formula_rows),
        "selected_placement_total": len(placement_rows),
        "selected_period_total": len(period_rows),
    }


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


def write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

