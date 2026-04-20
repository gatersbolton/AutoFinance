from __future__ import annotations

import argparse
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import OCR
from project_paths import (
    DEFAULT_TEMPLATE_PATH,
    PADDLE_PROVIDER_EVAL_ROOT,
    PADDLE_STANDARDIZE_EVAL_CONTROL_ROOT,
    REGISTRY_PATH,
)
from tools.paddle_eval_support import (
    aggregate_compatibility_summaries,
    build_compare_rows,
    build_conservative_evidence_thresholds,
    build_failure_analysis_rows,
    build_parity_by_role_rows,
    build_parity_summary,
    build_quality_gate,
    build_role_summaries,
    build_route_recommendation,
    build_zero_fact_rows,
    clean_provider_doc_output,
    collect_paddle_result_pages,
    execute_standardize_compatibility,
    extract_page_quality_metrics,
    generate_run_id,
    load_paddle_pilot_registry,
    load_provider_result,
    materialize_paddle_input_subset,
    percentile,
    resolve_pdf_path,
    write_csv,
    write_json,
)


DEFAULT_PADDLE_PILOT_REGISTRY = Path("benchmarks/paddle_pilot_registry.yml")

PAGE_FIELDNAMES = [
    "doc_id",
    "page_no",
    "page_role",
    "layout_detection",
    "selected_device",
    "runtime_seconds",
    "tables_detected",
    "cloud_tables_detected_if_available",
    "tables_detected_delta",
    "tables_deficit_ratio",
    "fewer_tables_than_cloud_reference",
    "xlsx_emitted",
    "html_emitted",
    "raw_json_emitted",
    "bbox_coverage",
    "standardize_consumable",
    "cells_total_if_available",
    "facts_total_if_available",
    "issues_total_if_available",
    "mapped_facts_ratio_if_available",
    "review_total_if_available",
    "validation_fail_total_if_available",
    "zero_fact_page",
    "notes",
]

COMPARE_FIELDNAMES = [
    "doc_id",
    "page_no",
    "page_role",
    "provider",
    "runtime_seconds",
    "paddle_tables_detected",
    "cloud_tables_detected_if_available",
    "tables_detected_delta",
    "tables_deficit_ratio",
    "paddle_xlsx_emitted",
    "paddle_html_emitted",
    "paddle_raw_json_emitted",
    "cloud_reference_provider",
    "cloud_reference_tables_detected",
    "aliyun_tables_detected",
    "tencent_tables_detected",
    "aliyun_xlsx_emitted",
    "tencent_xlsx_emitted",
    "cloud_reference_present",
    "fewer_tables_than_cloud_reference",
    "coarse_structural_parity",
    "notes",
]

PARITY_BY_ROLE_FIELDNAMES = [
    "page_role",
    "pages_total",
    "docs_total",
    "zero_fact_pages_total",
    "pages_with_fewer_tables_than_cloud_reference",
    "standardize_consumable_ratio",
    "average_runtime_seconds",
    "average_tables_detected",
    "average_cloud_tables_detected",
    "average_tables_detected_delta",
    "average_tables_deficit_ratio",
    "average_cells_total",
    "average_facts_total",
    "average_mapped_facts_ratio",
    "average_review_total",
    "average_validation_fail_total",
    "recommendation_candidate",
    "notes",
]

FAILURE_FIELDNAMES = [
    "doc_id",
    "page_no",
    "page_role",
    "zero_fact_page",
    "standardize_consumable",
    "fewer_tables_than_cloud_reference",
    "tables_detected_delta",
    "tables_deficit_ratio",
    "mapped_facts_ratio_if_available",
    "review_total_if_available",
    "validation_fail_total_if_available",
    "failure_reasons",
    "notes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Stage 8.2 Paddle role-aware parity benchmark and conservative fallback gate."
    )
    parser.add_argument("--registry", default=str(DEFAULT_PADDLE_PILOT_REGISTRY))
    parser.add_argument("--main-registry", default=str(REGISTRY_PATH))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--template", default=str(DEFAULT_TEMPLATE_PATH))
    parser.add_argument("--paddle-device", default="auto", choices=("auto", "gpu", "cpu"))
    parser.add_argument("--paddle-runtime-python", default="")
    parser.add_argument("--paddle-skip-if-no-gpu", action="store_true")
    parser.add_argument("--cloud-control-doc-id", default="D01")
    return parser.parse_args()


def collect_page_contexts(samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    contexts: List[Dict[str, Any]] = []
    for sample in samples:
        entry = sample["_registry_doc"]
        ocr_output_root = Path(entry["_input_dir"])
        source_image_dir = Path(entry["_source_image_dir"])
        pdf_path = resolve_pdf_path(source_image_dir)
        provider_doc_dir = ocr_output_root / OCR.PADDLE_PROVIDER_NAME / pdf_path.stem
        provider_result = load_provider_result(ocr_output_root, pdf_path)
        page_entry = next(item for item in provider_result["pages"] if int(item["page_number"]) == sample["page_no"])
        contexts.append(
            {
                "sample": sample,
                "page_entry": dict(page_entry),
                "provider_doc_dir": provider_doc_dir,
                "ocr_output_root": ocr_output_root,
                "source_image_dir": source_image_dir,
                "page_row": {
                    "doc_id": sample["doc_id"],
                    "page_no": sample["page_no"],
                    "page_role": sample["page_role"],
                    "layout_detection": sample["layout_detection"],
                    "selected_device": page_entry.get("selected_device", ""),
                    "runtime_seconds": page_entry.get("runtime_seconds", 0.0),
                    "tables_detected": page_entry.get("table_count", 0),
                    "cloud_tables_detected_if_available": 0,
                    "tables_detected_delta": 0,
                    "tables_deficit_ratio": 0.0,
                    "fewer_tables_than_cloud_reference": "no",
                    "xlsx_emitted": any(
                        str(item).lower().endswith(".xlsx") for item in page_entry.get("artifact_files", [])
                    ),
                    "html_emitted": any(
                        str(item).lower().endswith(".html") for item in page_entry.get("artifact_files", [])
                    ),
                    "raw_json_emitted": bool(page_entry.get("raw_file")),
                    "bbox_coverage": page_entry.get("bbox_coverage", 0.0),
                    "standardize_consumable": "yes"
                    if any(str(item).lower().endswith(".xlsx") for item in page_entry.get("artifact_files", []))
                    else "no",
                    "cells_total_if_available": "",
                    "facts_total_if_available": "",
                    "issues_total_if_available": "",
                    "mapped_facts_ratio_if_available": "",
                    "review_total_if_available": "",
                    "validation_fail_total_if_available": "",
                    "zero_fact_page": "no",
                    "cloud_reference_present": False,
                    "notes": str(sample.get("notes", "") or ""),
                },
            }
        )
    return contexts


def run_page_micro_eval(page_context: Dict[str, Any], template_path: Path) -> Dict[str, Any]:
    sample = page_context["sample"]
    with tempfile.TemporaryDirectory(prefix="autofinance_paddle_page_eval_") as tmpdir:
        tmpdir_path = Path(tmpdir)
        isolated_input_dir = tmpdir_path / "input"
        isolated_output_dir = tmpdir_path / "output"
        materialize_paddle_input_subset(
            source_provider_doc_dir=Path(page_context["provider_doc_dir"]),
            page_entries=[dict(page_context["page_entry"])],
            target_input_dir=isolated_input_dir,
        )
        summary = execute_standardize_compatibility(
            input_dir=isolated_input_dir,
            source_image_dir=Path(page_context["source_image_dir"]),
            output_dir=isolated_output_dir,
            template_path=template_path,
            provider_priority=OCR.PADDLE_PROVIDER_NAME,
            doc_id=str(sample["doc_id"]),
            scope_name=f"{sample['doc_id']}_page_{int(sample['page_no']):04d}",
            sampled_page_roles=[sample["page_role"]],
            remove_existing_output=False,
        )
    return summary


def run_cloud_control_check(
    *,
    registry_samples: List[Dict[str, Any]],
    doc_id: str,
    template_path: Path,
    control_root: Path,
) -> Dict[str, Any]:
    target_sample = next((item for item in registry_samples if item["doc_id"] == doc_id), None)
    if target_sample is None:
        target_sample = registry_samples[0]
    entry = target_sample["_registry_doc"]
    summary = execute_standardize_compatibility(
        input_dir=Path(entry["_input_dir"]),
        source_image_dir=Path(entry["_source_image_dir"]),
        output_dir=control_root / f"cloud_control_{target_sample['doc_id']}",
        template_path=template_path,
        provider_priority="aliyun_table,tencent_table_v3",
        doc_id=target_sample["doc_id"],
        scope_name=f"cloud_control_{target_sample['doc_id']}",
        sampled_page_roles=[target_sample["page_role"]],
        provider_name_for_missing_fields="aliyun_table",
    )
    summary["cloud_non_regression_pass"] = bool(
        summary.get("standardize_consumable", False)
        and int(summary.get("cells_total", 0) or 0) > 0
        and int(summary.get("facts_total", 0) or 0) > 0
    )
    return summary


def merge_compare_metrics(page_row: Dict[str, Any], compare_row: Dict[str, Any]) -> None:
    page_row["cloud_reference_present"] = compare_row.get("cloud_reference_present", False)
    page_row["cloud_tables_detected_if_available"] = compare_row.get("cloud_tables_detected_if_available", 0)
    page_row["tables_detected_delta"] = compare_row.get("tables_detected_delta", 0)
    page_row["tables_deficit_ratio"] = compare_row.get("tables_deficit_ratio", 0.0)
    page_row["fewer_tables_than_cloud_reference"] = compare_row.get("fewer_tables_than_cloud_reference", "no")


def write_empty_eval_outputs(
    *,
    experiment_root: Path,
    run_id: str,
    sample_pages: List[Dict[str, Any]],
    environment_summary: Dict[str, Any],
) -> int:
    evidence_thresholds = build_conservative_evidence_thresholds()
    empty_role_summaries: List[Dict[str, Any]] = []
    eval_summary = {
        "run_id": run_id,
        "status": "skipped",
        "sample_pages_total": len(sample_pages),
        "docs_total": len({item["doc_id"] for item in sample_pages}),
        "page_roles": sorted({item["page_role"] for item in sample_pages}),
        "provider_name": OCR.PADDLE_PROVIDER_NAME,
        "runtime_seconds_total": 0.0,
        "runtime_seconds_average": 0.0,
        "runtime_seconds_max": 0.0,
        "selected_device": environment_summary.get("selected_device", ""),
        "gpu_available": environment_summary.get("gpu_available", False),
        "notes": [environment_summary.get("skip_reason_if_any", "provider_not_ready")],
        "parity_summary": {},
    }
    contract_summary = {
        "provider_name": OCR.PADDLE_PROVIDER_NAME,
        "pages_processed": 0,
        "tables_emitted": 0,
        "raw_json_present": False,
        "xlsx_present": False,
        "html_present": False,
        "bbox_coverage": 0.0,
        "contract_pass": False,
        "missing_fields": [],
    }
    compatibility_summary = {
        "run_id": run_id,
        "docs_total": 0,
        "docs_consumable_total": 0,
        "docs_failed_total": 0,
        "zero_fact_docs_total": 0,
        "weak_output_docs_total": 0,
        "standardize_compatible": False,
        "missing_fields_to_adapt": [],
        "doc_ids": [],
        "notes": ["provider_not_ready"],
        "provider_name": OCR.PADDLE_PROVIDER_NAME,
    }
    cloud_control_summary = {"cloud_non_regression_pass": False, "notes": ["provider_not_ready"]}
    quality_gate = build_quality_gate(
        environment_summary=environment_summary,
        contract_summary=contract_summary,
        eval_summary=eval_summary,
        role_summaries=empty_role_summaries,
        compatibility_summary=compatibility_summary,
        cloud_control_summary=cloud_control_summary,
        evidence_thresholds=evidence_thresholds,
    )
    route_recommendation = build_route_recommendation(quality_gate, empty_role_summaries)
    write_json(experiment_root / "paddle_eval_summary.json", eval_summary)
    write_json(experiment_root / "paddle_eval_runtime.json", {})
    write_json(experiment_root / "paddle_provider_contract_summary.json", contract_summary)
    write_json(experiment_root / "paddle_parity_summary.json", {})
    write_json(experiment_root / "paddle_quality_gate.json", quality_gate)
    write_json(experiment_root / "paddle_route_recommendation.json", route_recommendation)
    write_json(
        experiment_root / "paddle_role_summary.json",
        {
            "run_id": run_id,
            "evidence_thresholds": evidence_thresholds,
            "roles": empty_role_summaries,
            "fallback_candidate_roles": [],
        },
    )
    write_csv(experiment_root / "paddle_eval_pages.csv", [], PAGE_FIELDNAMES)
    write_csv(experiment_root / "paddle_vs_cloud_compare.csv", [], COMPARE_FIELDNAMES)
    write_csv(experiment_root / "paddle_parity_by_role.csv", [], PARITY_BY_ROLE_FIELDNAMES)
    write_csv(experiment_root / "paddle_zero_fact_pages.csv", [], PAGE_FIELDNAMES)
    write_csv(experiment_root / "paddle_failure_analysis.csv", [], FAILURE_FIELDNAMES)
    write_csv(experiment_root / "paddle_role_compare.csv", [], PARITY_BY_ROLE_FIELDNAMES)
    return 1


def main() -> int:
    args = parse_args()
    template_path = Path(args.template).resolve()
    registry_path = (REPO_ROOT / args.registry).resolve()
    main_registry_path = (REPO_ROOT / args.main_registry).resolve()
    sample_pages = load_paddle_pilot_registry(registry_path, main_registry_path=main_registry_path)
    run_id = args.run_id or generate_run_id("PADDLE_EVAL")
    experiment_root = PADDLE_PROVIDER_EVAL_ROOT / run_id
    control_root = PADDLE_STANDARDIZE_EVAL_CONTROL_ROOT / run_id
    experiment_root.mkdir(parents=True, exist_ok=True)
    control_root.mkdir(parents=True, exist_ok=True)

    paddle_options = OCR.PaddleProviderOptions(
        device=args.paddle_device,
        layout_detection="auto",
        skip_if_no_gpu=args.paddle_skip_if_no_gpu,
        runtime_python=args.paddle_runtime_python,
        pipeline=OCR.DEFAULT_PADDLE_PIPELINE,
    )
    runtime_python = OCR.resolve_paddle_runtime_python(paddle_options.runtime_python)
    environment_summary = OCR.probe_paddle_environment(runtime_python, paddle_options)
    write_json(experiment_root / "paddle_environment_summary.json", environment_summary)
    if not environment_summary.get("provider_ready", False):
        return write_empty_eval_outputs(
            experiment_root=experiment_root,
            run_id=run_id,
            sample_pages=sample_pages,
            environment_summary=environment_summary,
        )

    provider = OCR.PaddleLocalOCRProvider(paddle_options)
    samples_by_doc: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for sample in sample_pages:
        samples_by_doc[sample["doc_id"]].append(sample)

    for doc_id, samples in samples_by_doc.items():
        entry = samples[0]["_registry_doc"]
        source_image_dir = Path(entry["_source_image_dir"])
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(source_image_dir)
        clean_provider_doc_output(ocr_output_root, pdf_path)
        page_numbers = [int(item["page_no"]) for item in samples]
        page_options_by_number = {
            int(item["page_no"]): {"layout_detection": item["layout_detection"]}
            for item in samples
        }
        rendered_pages = OCR.render_pdf_pages(pdf_path, page_numbers=page_numbers)
        OCR.process_pdf_with_provider(
            pdf_path=pdf_path,
            rendered_pages=rendered_pages,
            provider=provider,
            output_root=ocr_output_root,
            page_options_by_number=page_options_by_number,
        )

    page_contexts = collect_page_contexts(sample_pages)
    compare_rows = build_compare_rows(sample_pages, [context["page_row"] for context in page_contexts])
    compare_by_key = {(row["doc_id"], row["page_no"]): row for row in compare_rows}

    for page_context in page_contexts:
        page_row = page_context["page_row"]
        compare_row = compare_by_key[(page_row["doc_id"], page_row["page_no"])]
        merge_compare_metrics(page_row, compare_row)
        micro_summary = run_page_micro_eval(page_context, template_path)
        page_row.update(extract_page_quality_metrics(micro_summary))
        page_row["standardize_consumable"] = "yes" if micro_summary.get("standardize_consumable", False) else "no"
        page_row["zero_fact_page"] = "yes" if int(page_row.get("facts_total_if_available", 0) or 0) <= 0 else "no"
        notes: List[str] = []
        if page_row.get("notes"):
            notes.append(str(page_row["notes"]))
        notes.extend(compare_row["notes"].split(";") if compare_row.get("notes") else [])
        notes.extend(micro_summary.get("notes", []))
        page_row["notes"] = ";".join(item for item in notes if item)

    page_rows = sorted(
        [context["page_row"] for context in page_contexts],
        key=lambda item: (str(item["doc_id"]), int(item["page_no"])),
    )
    evidence_thresholds = build_conservative_evidence_thresholds()
    role_summaries = build_role_summaries(page_rows, evidence_thresholds=evidence_thresholds)
    parity_summary = build_parity_summary(page_rows, role_summaries)
    parity_by_role_rows = build_parity_by_role_rows(role_summaries)
    zero_fact_rows = build_zero_fact_rows(page_rows)
    failure_rows = build_failure_analysis_rows(page_rows)

    contract_summary = OCR.build_paddle_provider_contract_summary(collect_paddle_result_pages(sample_pages))
    runtime_values = [float(row["runtime_seconds"] or 0.0) for row in page_rows]
    runtime_summary = {
        "run_id": run_id,
        "pages_total": len(page_rows),
        "runtime_seconds_total": round(sum(runtime_values), 6),
        "runtime_seconds_average": round(sum(runtime_values) / len(runtime_values), 6) if runtime_values else 0.0,
        "runtime_seconds_max": round(max(runtime_values), 6) if runtime_values else 0.0,
        "runtime_seconds_p95": round(percentile(runtime_values, 0.95), 6),
        "selected_device": environment_summary.get("selected_device", ""),
        "gpu_available": environment_summary.get("gpu_available", False),
    }

    compatibility_by_doc: List[Dict[str, Any]] = []
    for doc_id, samples in samples_by_doc.items():
        entry = samples[0]["_registry_doc"]
        sampled_roles = sorted({str(sample["page_role"]) for sample in samples})
        doc_summary = execute_standardize_compatibility(
            input_dir=Path(entry["_input_dir"]),
            source_image_dir=Path(entry["_source_image_dir"]),
            output_dir=control_root / doc_id,
            template_path=template_path,
            provider_priority=OCR.PADDLE_PROVIDER_NAME,
            doc_id=doc_id,
            scope_name=doc_id,
            sampled_page_roles=sampled_roles,
        )
        compatibility_by_doc.append(doc_summary)
    compatibility_summary = aggregate_compatibility_summaries(run_id, compatibility_by_doc)
    compatibility_summary["provider_name"] = OCR.PADDLE_PROVIDER_NAME

    cloud_control_summary = run_cloud_control_check(
        registry_samples=sample_pages,
        doc_id=args.cloud_control_doc_id,
        template_path=template_path,
        control_root=control_root,
    )

    eval_summary = {
        "run_id": run_id,
        "status": "completed",
        "sample_pages_total": len(page_rows),
        "docs_total": len({row["doc_id"] for row in page_rows}),
        "page_roles": sorted({row["page_role"] for row in page_rows}),
        "provider_name": OCR.PADDLE_PROVIDER_NAME,
        "runtime_seconds_total": runtime_summary["runtime_seconds_total"],
        "runtime_seconds_average": runtime_summary["runtime_seconds_average"],
        "runtime_seconds_max": runtime_summary["runtime_seconds_max"],
        "selected_device": environment_summary.get("selected_device", ""),
        "gpu_available": environment_summary.get("gpu_available", False),
        "provider_contract_pass": contract_summary["contract_pass"],
        "standardize_compatible": compatibility_summary["standardize_compatible"],
        "cloud_non_regression_pass": cloud_control_summary["cloud_non_regression_pass"],
        "parity_summary": parity_summary,
        "notes": [],
    }
    quality_gate = build_quality_gate(
        environment_summary=environment_summary,
        contract_summary=contract_summary,
        eval_summary=eval_summary,
        role_summaries=role_summaries,
        compatibility_summary=compatibility_summary,
        cloud_control_summary=cloud_control_summary,
        evidence_thresholds=evidence_thresholds,
    )
    route_recommendation = build_route_recommendation(quality_gate, role_summaries)
    eval_summary["quality_gate_status"] = quality_gate["quality_gate_status"]

    write_json(experiment_root / "paddle_eval_summary.json", eval_summary)
    write_json(experiment_root / "paddle_eval_runtime.json", runtime_summary)
    write_json(experiment_root / "paddle_provider_contract_summary.json", contract_summary)
    write_json(experiment_root / "paddle_parity_summary.json", parity_summary)
    write_json(experiment_root / "paddle_quality_gate.json", quality_gate)
    write_json(experiment_root / "paddle_route_recommendation.json", route_recommendation)
    write_json(
        experiment_root / "paddle_role_summary.json",
        {
            "run_id": run_id,
            "evidence_thresholds": evidence_thresholds,
            "roles": role_summaries,
            "fallback_candidate_roles": quality_gate["fallback_candidate_roles"],
        },
    )
    write_csv(experiment_root / "paddle_eval_pages.csv", page_rows, PAGE_FIELDNAMES)
    write_csv(experiment_root / "paddle_vs_cloud_compare.csv", compare_rows, COMPARE_FIELDNAMES)
    write_csv(experiment_root / "paddle_parity_by_role.csv", parity_by_role_rows, PARITY_BY_ROLE_FIELDNAMES)
    write_csv(experiment_root / "paddle_zero_fact_pages.csv", zero_fact_rows, PAGE_FIELDNAMES)
    write_csv(experiment_root / "paddle_failure_analysis.csv", failure_rows, FAILURE_FIELDNAMES)
    write_csv(experiment_root / "paddle_role_compare.csv", parity_by_role_rows, PARITY_BY_ROLE_FIELDNAMES)

    write_json(control_root / "paddle_standardize_compatibility.json", compatibility_summary)
    write_csv(
        control_root / "paddle_standardize_compatibility_by_doc.csv",
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
            for summary in compatibility_by_doc
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
    write_json(control_root / "cloud_control_summary.json", cloud_control_summary)
    return 0 if quality_gate["quality_gate_status"] != "not_ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
