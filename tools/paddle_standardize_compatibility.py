from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project_paths import DEFAULT_TEMPLATE_PATH, PADDLE_STANDARDIZE_CONTROL_ROOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run standardize compatibility validation for Paddle pilot outputs.")
    parser.add_argument("--registry", default="benchmarks/registry.yml")
    parser.add_argument("--doc-id", default="D01")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--template", default=str(DEFAULT_TEMPLATE_PATH))
    return parser.parse_args()


def load_registry(path: Path) -> Dict[str, Dict[str, Any]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = payload.get("entries", []) or []
    by_doc: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        doc_id = str(entry.get("doc_id", "")).strip()
        if doc_id:
            resolved = dict(entry)
            resolved["_input_dir"] = (path.parent / entry["input_dir"]).resolve()
            resolved["_source_image_dir"] = (path.parent / entry["source_image_dir"]).resolve()
            by_doc[doc_id] = resolved
    return by_doc


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    return max(len(rows) - 1, 0)


def collect_missing_fields(input_dir: Path) -> List[str]:
    provider_dir = input_dir / "paddle_table_local"
    if not provider_dir.exists():
        return []
    missing: set[str] = set()
    for raw_path in provider_dir.rglob("raw/page_*.json"):
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
        for field in payload.get("missing_fields", []):
            if field:
                missing.add(str(field))
    return sorted(missing)


def main() -> int:
    args = parse_args()
    repo_root = REPO_ROOT
    registry_path = (repo_root / args.registry).resolve()
    registry_by_doc = load_registry(registry_path)
    entry = registry_by_doc[args.doc_id]

    input_dir = Path(entry["_input_dir"])
    source_image_dir = Path(entry["_source_image_dir"])
    output_dir = (PADDLE_STANDARDIZE_CONTROL_ROOT / args.run_id).resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "-m",
        "standardize.cli",
        "--input-dir",
        str(input_dir),
        "--template",
        str(Path(args.template).resolve()),
        "--output-dir",
        str(output_dir),
        "--output-run-subdir",
        "none",
        "--source-image-dir",
        str(source_image_dir),
        "--provider-priority",
        "paddle_table_local",
        "--enable-period-normalization",
        "--enable-dedupe",
        "--enable-validation",
        "--enable-label-canonicalization",
        "--enable-derived-facts",
        "--enable-main-statement-specialization",
        "--enable-single-period-role-inference",
        "--enable-integrity-check",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(repo_root),
    )

    run_summary_path = output_dir / "run_summary.json"
    run_summary = json.loads(run_summary_path.read_text(encoding="utf-8")) if run_summary_path.exists() else {}
    compatibility_summary = {
        "run_id": args.run_id,
        "doc_id": args.doc_id,
        "standardize_exit_code": completed.returncode,
        "output_dir": str(output_dir),
        "cells_csv_exists": (output_dir / "cells.csv").exists(),
        "facts_csv_exists": (output_dir / "facts.csv").exists(),
        "issues_csv_exists": (output_dir / "issues.csv").exists(),
        "run_summary_exists": run_summary_path.exists(),
        "cells_total": count_csv_rows(output_dir / "cells.csv"),
        "facts_total": count_csv_rows(output_dir / "facts.csv"),
        "issues_total": count_csv_rows(output_dir / "issues.csv"),
        "missing_fields_to_adapt": collect_missing_fields(input_dir),
        "standardize_consumable": completed.returncode == 0 and (output_dir / "cells.csv").exists() and (output_dir / "facts.csv").exists(),
        "notes": [],
        "command": command,
        "stderr_tail": "\n".join((completed.stderr or "").splitlines()[-20:]),
        "stdout_tail": "\n".join((completed.stdout or "").splitlines()[-20:]),
        "run_summary": run_summary,
    }
    if not compatibility_summary["standardize_consumable"]:
        compatibility_summary["notes"].append("standardize_did_not_complete_cleanly")
    if compatibility_summary["cells_total"] <= 0:
        compatibility_summary["notes"].append("no_cells_emitted")
    if compatibility_summary["facts_total"] <= 0:
        compatibility_summary["notes"].append("no_facts_emitted")

    write_json(output_dir / "paddle_standardize_compatibility.json", compatibility_summary)
    return 0 if completed.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
