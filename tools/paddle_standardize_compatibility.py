from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project_paths import DEFAULT_TEMPLATE_PATH, PADDLE_STANDARDIZE_EVAL_CONTROL_ROOT, REGISTRY_PATH
from tools.paddle_eval_support import (
    aggregate_compatibility_summaries,
    execute_standardize_compatibility,
    load_paddle_pilot_registry,
    load_registry,
    write_csv,
    write_json,
)


DEFAULT_PADDLE_PILOT_REGISTRY = Path("benchmarks/paddle_pilot_registry.yml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run expanded standardize compatibility validation for Paddle evaluation outputs."
    )
    parser.add_argument("--registry", default=str(REGISTRY_PATH))
    parser.add_argument("--sample-registry", default=str(DEFAULT_PADDLE_PILOT_REGISTRY))
    parser.add_argument("--doc-id", action="append", default=[])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--template", default=str(DEFAULT_TEMPLATE_PATH))
    return parser.parse_args()


def resolve_doc_ids(args: argparse.Namespace, registry_by_doc: Dict[str, Dict[str, Any]]) -> List[str]:
    if args.doc_id:
        return sorted({str(item).strip() for item in args.doc_id if str(item).strip()})
    sample_registry_path = (REPO_ROOT / args.sample_registry).resolve()
    samples = load_paddle_pilot_registry(
        sample_registry_path,
        main_registry_path=(REPO_ROOT / args.registry).resolve(),
        registry_by_doc=registry_by_doc,
    )
    return sorted({sample["doc_id"] for sample in samples})


def main() -> int:
    args = parse_args()
    registry_path = (REPO_ROOT / args.registry).resolve()
    registry_by_doc = load_registry(registry_path)
    template_path = Path(args.template).resolve()
    output_root = (PADDLE_STANDARDIZE_EVAL_CONTROL_ROOT / args.run_id).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    doc_ids = resolve_doc_ids(args, registry_by_doc)
    sample_registry_path = (REPO_ROOT / args.sample_registry).resolve()
    sample_rows = load_paddle_pilot_registry(
        sample_registry_path,
        main_registry_path=registry_path,
        registry_by_doc=registry_by_doc,
    )
    roles_by_doc: Dict[str, List[str]] = {}
    for row in sample_rows:
        roles_by_doc.setdefault(row["doc_id"], []).append(str(row["page_role"]))
    doc_summaries: List[Dict[str, Any]] = []
    for doc_id in doc_ids:
        entry = registry_by_doc[doc_id]
        summary = execute_standardize_compatibility(
            input_dir=Path(entry["_input_dir"]),
            source_image_dir=Path(entry["_source_image_dir"]),
            output_dir=output_root / doc_id,
            template_path=template_path,
            doc_id=doc_id,
            scope_name=doc_id,
            sampled_page_roles=sorted(set(roles_by_doc.get(doc_id, []))),
        )
        doc_summaries.append(summary)

    compatibility_summary = aggregate_compatibility_summaries(args.run_id, doc_summaries)
    compatibility_summary["provider_name"] = "paddle_table_local"

    write_json(output_root / "paddle_standardize_compatibility.json", compatibility_summary)
    write_csv(
        output_root / "paddle_standardize_compatibility_by_doc.csv",
        [
            {
                "doc_id": summary["doc_id"],
                "scope_name": summary["scope_name"],
                "sampled_page_roles": ";".join(summary.get("sampled_page_roles", [])),
                "provider_priority": summary["provider_priority"],
                "standardize_exit_code": summary["standardize_exit_code"],
                "cells_total": summary["cells_total"],
                "facts_total": summary["facts_total"],
                "issues_total": summary["issues_total"],
                "standardize_consumable": summary["standardize_consumable"],
                "zero_fact_output": summary.get("zero_fact_output", False),
                "weak_output": summary.get("weak_output", False),
                "missing_fields_to_adapt": ";".join(summary["missing_fields_to_adapt"]),
                "notes": ";".join(summary["notes"]),
                "output_dir": summary["output_dir"],
            }
            for summary in doc_summaries
        ],
        [
            "doc_id",
            "scope_name",
            "sampled_page_roles",
            "provider_priority",
            "standardize_exit_code",
            "cells_total",
            "facts_total",
            "issues_total",
            "standardize_consumable",
            "zero_fact_output",
            "weak_output",
            "missing_fields_to_adapt",
            "notes",
            "output_dir",
        ],
    )
    return 0 if compatibility_summary["standardize_compatible"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
