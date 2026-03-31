from __future__ import annotations

import json
import csv
from pathlib import Path
from typing import Any, Dict, List, Sequence

from openpyxl import load_workbook


def run_full_run_contract(
    output_dir: Path,
    workbook_path: Path,
    run_id: str,
    feature_flags: Dict[str, Any],
    export_stats: Dict[str, Any],
    required_helper_sheets: Sequence[str],
) -> Dict[str, Any]:
    workbook = load_workbook(workbook_path)
    actual_sheets = list(workbook.sheetnames)
    checks: List[Dict[str, Any]] = []
    produced = {path.name for path in output_dir.iterdir() if path.is_file()}

    add_check(
        checks,
        "helper_sheets_present",
        all(sheet in actual_sheets for sheet in required_helper_sheets),
        {"required_helper_sheets": list(required_helper_sheets), "actual_sheets": actual_sheets},
    )
    if feature_flags.get("emit_benchmark_report"):
        required = {
            "benchmark_summary.json",
            "benchmark_summary.csv",
            "benchmark_missing_in_auto.csv",
            "benchmark_value_diff.csv",
            "benchmark_gap_explanations.csv",
            "benchmark_gap_summary.json",
        }
        add_check(checks, "benchmark_outputs_present", required.issubset(produced), {"required": sorted(required), "produced": sorted(produced)})
    if feature_flags.get("enable_derived_facts"):
        required = {
            "derived_facts.csv",
            "derived_formula_audit.csv",
            "derived_formula_summary.json",
            "derived_conflicts.csv",
        }
        add_check(checks, "derived_outputs_present", required.issubset(produced), {"required": sorted(required), "produced": sorted(produced)})
    if feature_flags.get("emit_run_manifest"):
        required = {"run_manifest.json", "artifact_manifest.csv"}
        add_check(checks, "manifest_outputs_present", required.issubset(produced), {"required": sorted(required), "produced": sorted(produced)})
        manifest = read_json(output_dir / "run_manifest.json")
        add_check(
            checks,
            "manifest_run_id_matches",
            str(manifest.get("run_id", "")) == run_id,
            {"expected_run_id": run_id, "manifest_run_id": manifest.get("run_id", "")},
        )
        artifact_rows = read_csv_rows(output_dir / "artifact_manifest.csv")
        artifact_run_ids = sorted({row.get("run_id", "") for row in artifact_rows if row.get("run_id")})
        add_check(
            checks,
            "artifact_manifest_run_id_matches",
            artifact_run_ids == [run_id] if artifact_run_ids else False,
            {"expected_run_id": run_id, "artifact_run_ids": artifact_run_ids},
        )
    add_check(
        checks,
        "export_source_facts_is_deduped",
        str(export_stats.get("source_facts", "")) == "facts_deduped",
        {"source_facts": export_stats.get("source_facts", "")},
    )
    return {
        "run_id": run_id,
        "feature_flags": feature_flags,
        "required_artifacts": build_required_artifacts(feature_flags),
        "produced_artifacts": sorted(produced),
        "workbook_helper_sheets": actual_sheets,
        "checks_total": len(checks),
        "contract_fail_total": sum(1 for item in checks if item["status"] == "fail"),
        "checks": checks,
    }


def build_required_artifacts(feature_flags: Dict[str, Any]) -> List[str]:
    required = ["run_summary.json", "summary.json", "会计报表_填充结果.xlsx"]
    if feature_flags.get("emit_benchmark_report"):
        required.extend(["benchmark_summary.json", "benchmark_gap_summary.json"])
    if feature_flags.get("enable_derived_facts"):
        required.extend(["derived_formula_summary.json", "derived_facts.csv"])
    if feature_flags.get("emit_run_manifest"):
        required.extend(["run_manifest.json", "artifact_manifest.csv"])
    return required


def add_check(checks: List[Dict[str, Any]], name: str, ok: bool, meta: Dict[str, Any]) -> None:
    checks.append({"check_name": name, "status": "pass" if ok else "fail", "meta": meta})


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))
