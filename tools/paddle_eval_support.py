from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import yaml

import OCR
from project_paths import DEFAULT_TEMPLATE_PATH, REGISTRY_PATH


REPO_ROOT = Path(__file__).resolve().parent.parent


def generate_run_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    return max(len(rows) - 1, 0)


def average_numeric(values: Iterable[Any]) -> Optional[float]:
    numeric_values = [float(value) for value in values if value not in (None, "")]
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)


def percentile(values: Sequence[Any], quantile: float) -> float:
    numeric_values = sorted(float(value) for value in values if value not in (None, ""))
    if not numeric_values:
        return 0.0
    if len(numeric_values) == 1:
        return numeric_values[0]
    index = max(0.0, min(quantile, 1.0)) * (len(numeric_values) - 1)
    lower = int(index)
    upper = min(lower + 1, len(numeric_values) - 1)
    fraction = index - lower
    return numeric_values[lower] + (numeric_values[upper] - numeric_values[lower]) * fraction


def normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def load_registry(path: Path) -> Dict[str, Dict[str, Any]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = payload.get("entries", []) or []
    by_doc: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        doc_id = str(entry.get("doc_id", "")).strip()
        if not doc_id:
            continue
        resolved = dict(entry)
        resolved["_input_dir"] = (path.parent / entry["input_dir"]).resolve()
        resolved["_source_image_dir"] = (path.parent / entry["source_image_dir"]).resolve()
        by_doc[doc_id] = resolved
    return by_doc


def load_paddle_pilot_registry(
    path: Path,
    *,
    main_registry_path: Path = REGISTRY_PATH,
    include_disabled: bool = False,
    registry_by_doc: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    registry_by_doc = registry_by_doc or load_registry(main_registry_path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = payload.get("entries", []) or []
    resolved_entries: List[Dict[str, Any]] = []
    for raw_entry in entries:
        enabled = normalize_bool(raw_entry.get("enabled", True))
        if not enabled and not include_disabled:
            continue
        doc_id = str(raw_entry.get("doc_id", "")).strip()
        if not doc_id:
            raise ValueError("paddle pilot registry entry is missing doc_id")
        if doc_id not in registry_by_doc:
            raise KeyError(f"paddle pilot registry doc_id is not present in the main registry: {doc_id}")
        if "page_no" not in raw_entry:
            raise ValueError(f"paddle pilot registry entry is missing page_no for doc_id={doc_id}")
        main_entry = registry_by_doc[doc_id]
        raw_layout_detection = raw_entry.get("layout_detection", "auto")
        if isinstance(raw_layout_detection, bool):
            layout_detection = "on" if raw_layout_detection else "off"
        else:
            layout_detection = str(raw_layout_detection or "auto").strip().lower()
        resolved = dict(raw_entry)
        resolved["doc_id"] = doc_id
        resolved["page_no"] = int(raw_entry["page_no"])
        resolved["enabled"] = enabled
        resolved["input_source"] = str(raw_entry.get("input_source", "corpus_registry") or "corpus_registry")
        resolved["page_role"] = str(raw_entry.get("page_role", "unclassified") or "unclassified").strip()
        resolved["layout_detection"] = layout_detection if layout_detection in {"auto", "on", "off"} else "auto"
        resolved["_input_dir"] = Path(main_entry["_input_dir"])
        resolved["_source_image_dir"] = Path(main_entry["_source_image_dir"])
        resolved["_registry_doc"] = dict(main_entry)
        resolved_entries.append(resolved)
    return sorted(resolved_entries, key=lambda item: (item["doc_id"], int(item["page_no"])))


def resolve_pdf_path(source_image_dir: Path) -> Path:
    pdfs = sorted(source_image_dir.glob("*.pdf"))
    if not pdfs:
        raise FileNotFoundError(f"No PDF files found in {source_image_dir}")
    return pdfs[0]


def clean_provider_doc_output(
    ocr_output_root: Path,
    pdf_path: Path,
    *,
    provider_name: str = OCR.PADDLE_PROVIDER_NAME,
) -> None:
    target = ocr_output_root / provider_name / pdf_path.stem
    if target.exists():
        shutil.rmtree(target)


def load_provider_result(
    ocr_output_root: Path,
    pdf_path: Path,
    *,
    provider_name: str = OCR.PADDLE_PROVIDER_NAME,
) -> Dict[str, Any]:
    result_path = ocr_output_root / provider_name / pdf_path.stem / "result.json"
    return json.loads(result_path.read_text(encoding="utf-8"))


def collect_paddle_result_pages(samples: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    for sample in samples:
        entry = sample["_registry_doc"]
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(Path(entry["_source_image_dir"]))
        provider_result = load_provider_result(ocr_output_root, pdf_path)
        page_entry = next(item for item in provider_result["pages"] if int(item["page_number"]) == sample["page_no"])
        pages.append(dict(page_entry))
    return pages


def infer_table_count_from_page(provider: str, provider_doc_dir: Path, page: Dict[str, Any]) -> int:
    raw_rel = str(page.get("raw_file", "") or "").strip()
    if not raw_rel:
        return 0
    raw_path = provider_doc_dir / raw_rel
    if not raw_path.exists():
        return 0
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    if provider == "aliyun_table":
        page_data = OCR.extract_aliyun_data(payload)
        return len(page_data.get("prism_tablesInfo", []))
    if provider == "tencent_table_v3":
        body = payload.get("Response", payload)
        return len(body.get("TableDetections", []))
    if provider == OCR.PADDLE_PROVIDER_NAME:
        return len(payload.get("tables", []))
    return 0


def summarize_existing_provider_page(
    ocr_output_root: Path,
    provider: str,
    pdf_path: Path,
    page_no: int,
) -> Dict[str, Any]:
    result_path = ocr_output_root / provider / pdf_path.stem / "result.json"
    if not result_path.exists():
        return {
            "tables_detected": 0,
            "xlsx_emitted": False,
            "html_emitted": False,
            "raw_json_emitted": False,
        }
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    page = next((item for item in payload.get("pages", []) if int(item.get("page_number", 0)) == page_no), {})
    artifacts = [str(item) for item in page.get("artifact_files", [])]
    tables_detected = infer_table_count_from_page(provider, ocr_output_root / provider / pdf_path.stem, page)
    return {
        "tables_detected": tables_detected,
        "xlsx_emitted": any(item.lower().endswith(".xlsx") for item in artifacts),
        "html_emitted": any(item.lower().endswith(".html") for item in artifacts),
        "raw_json_emitted": bool(page.get("raw_file")),
    }


def build_compare_rows(samples: Sequence[Dict[str, Any]], paddle_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    paddle_by_key = {(row["doc_id"], row["page_no"]): row for row in paddle_rows}
    rows: List[Dict[str, Any]] = []
    for sample in samples:
        entry = sample["_registry_doc"]
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(Path(entry["_source_image_dir"]))
        paddle_row = paddle_by_key[(sample["doc_id"], sample["page_no"])]
        aliyun = summarize_existing_provider_page(ocr_output_root, "aliyun_table", pdf_path, sample["page_no"])
        tencent = summarize_existing_provider_page(ocr_output_root, "tencent_table_v3", pdf_path, sample["page_no"])
        cloud_reference_provider = "tencent_table_v3"
        cloud_reference_tables = int(tencent["tables_detected"])
        if int(aliyun["tables_detected"]) > cloud_reference_tables:
            cloud_reference_provider = "aliyun_table"
            cloud_reference_tables = int(aliyun["tables_detected"])
        cloud_reference_present = bool(
            cloud_reference_tables > 0
            or aliyun["xlsx_emitted"]
            or tencent["xlsx_emitted"]
            or aliyun["raw_json_emitted"]
            or tencent["raw_json_emitted"]
        )
        coarse_structural_parity = (
            "pass"
            if paddle_row["xlsx_emitted"]
            and paddle_row["html_emitted"]
            and paddle_row["raw_json_emitted"]
            and (int(paddle_row["tables_detected"]) > 0 or not cloud_reference_present)
            else "gap"
        )
        notes: List[str] = []
        if cloud_reference_present and int(paddle_row["tables_detected"]) == 0:
            notes.append("no_tables_detected_vs_cloud_reference")
        if cloud_reference_tables > 0 and int(paddle_row["tables_detected"]) < cloud_reference_tables:
            notes.append("paddle_detected_fewer_tables_than_cloud_reference")
        tables_detected_delta = int(paddle_row["tables_detected"] or 0) - int(cloud_reference_tables or 0)
        tables_deficit_ratio = 0.0
        if cloud_reference_tables > 0 and tables_detected_delta < 0:
            tables_deficit_ratio = round(abs(tables_detected_delta) / cloud_reference_tables, 6)
        rows.append(
            {
                "doc_id": sample["doc_id"],
                "page_no": sample["page_no"],
                "page_role": sample["page_role"],
                "provider": OCR.PADDLE_PROVIDER_NAME,
                "runtime_seconds": paddle_row["runtime_seconds"],
                "paddle_tables_detected": paddle_row["tables_detected"],
                "cloud_tables_detected_if_available": cloud_reference_tables,
                "tables_detected_delta": tables_detected_delta,
                "tables_deficit_ratio": tables_deficit_ratio,
                "paddle_xlsx_emitted": paddle_row["xlsx_emitted"],
                "paddle_html_emitted": paddle_row["html_emitted"],
                "paddle_raw_json_emitted": paddle_row["raw_json_emitted"],
                "cloud_reference_provider": cloud_reference_provider,
                "cloud_reference_tables_detected": cloud_reference_tables,
                "aliyun_tables_detected": aliyun["tables_detected"],
                "tencent_tables_detected": tencent["tables_detected"],
                "aliyun_xlsx_emitted": aliyun["xlsx_emitted"],
                "tencent_xlsx_emitted": tencent["xlsx_emitted"],
                "cloud_reference_present": cloud_reference_present,
                "fewer_tables_than_cloud_reference": "yes"
                if cloud_reference_tables > 0 and int(paddle_row["tables_detected"] or 0) < cloud_reference_tables
                else "no",
                "coarse_structural_parity": coarse_structural_parity,
                "notes": ";".join(notes),
            }
        )
    return rows


def build_standardize_command(
    *,
    input_dir: Path,
    source_image_dir: Path,
    output_dir: Path,
    template_path: Path = DEFAULT_TEMPLATE_PATH,
    provider_priority: str = OCR.PADDLE_PROVIDER_NAME,
) -> List[str]:
    return [
        sys.executable,
        "-m",
        "standardize.cli",
        "--input-dir",
        str(input_dir),
        "--template",
        str(Path(template_path).resolve()),
        "--output-dir",
        str(output_dir),
        "--output-run-subdir",
        "none",
        "--source-image-dir",
        str(source_image_dir),
        "--provider-priority",
        provider_priority,
        "--enable-period-normalization",
        "--enable-dedupe",
        "--enable-validation",
        "--enable-label-canonicalization",
        "--enable-derived-facts",
        "--enable-main-statement-specialization",
        "--enable-single-period-role-inference",
        "--enable-integrity-check",
    ]


def collect_missing_fields(input_dir: Path, provider_name: str = OCR.PADDLE_PROVIDER_NAME) -> List[str]:
    provider_dir = input_dir / provider_name
    if not provider_dir.exists():
        return []
    missing: set[str] = set()
    for raw_path in provider_dir.rglob("raw/page_*.json"):
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
        for field in payload.get("missing_fields", []):
            if field:
                missing.add(str(field))
    return sorted(missing)


def execute_standardize_compatibility(
    *,
    input_dir: Path,
    source_image_dir: Path,
    output_dir: Path,
    template_path: Path = DEFAULT_TEMPLATE_PATH,
    provider_priority: str = OCR.PADDLE_PROVIDER_NAME,
    doc_id: str = "",
    scope_name: str = "",
    sampled_page_roles: Optional[Sequence[str]] = None,
    remove_existing_output: bool = True,
    provider_name_for_missing_fields: str = OCR.PADDLE_PROVIDER_NAME,
) -> Dict[str, Any]:
    if remove_existing_output and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    command = build_standardize_command(
        input_dir=input_dir,
        source_image_dir=source_image_dir,
        output_dir=output_dir,
        template_path=template_path,
        provider_priority=provider_priority,
    )
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(REPO_ROOT),
    )

    run_summary_path = output_dir / "run_summary.json"
    run_summary = json.loads(run_summary_path.read_text(encoding="utf-8")) if run_summary_path.exists() else {}
    summary = {
        "doc_id": doc_id,
        "scope_name": scope_name or doc_id,
        "sampled_page_roles": sorted({str(item) for item in (sampled_page_roles or []) if str(item)}),
        "provider_priority": provider_priority,
        "standardize_exit_code": completed.returncode,
        "output_dir": str(output_dir),
        "cells_csv_exists": (output_dir / "cells.csv").exists(),
        "facts_csv_exists": (output_dir / "facts.csv").exists(),
        "issues_csv_exists": (output_dir / "issues.csv").exists(),
        "run_summary_exists": run_summary_path.exists(),
        "cells_total": count_csv_rows(output_dir / "cells.csv"),
        "facts_total": count_csv_rows(output_dir / "facts.csv"),
        "issues_total": count_csv_rows(output_dir / "issues.csv"),
        "missing_fields_to_adapt": collect_missing_fields(input_dir, provider_name=provider_name_for_missing_fields),
        "standardize_consumable": completed.returncode == 0
        and (output_dir / "cells.csv").exists()
        and (output_dir / "facts.csv").exists(),
        "notes": [],
        "command": command,
        "stderr_tail": "\n".join((completed.stderr or "").splitlines()[-20:]),
        "stdout_tail": "\n".join((completed.stdout or "").splitlines()[-20:]),
        "run_summary": run_summary,
    }
    summary["zero_fact_output"] = summary["facts_total"] <= 0
    summary["weak_output"] = bool(summary["zero_fact_output"]) or (
        summary["cells_total"] > 0 and float(run_summary.get("mapped_facts_ratio", 0.0) or 0.0) < 0.10
    )
    if not summary["standardize_consumable"]:
        summary["notes"].append("standardize_did_not_complete_cleanly")
    if summary["cells_total"] <= 0:
        summary["notes"].append("no_cells_emitted")
    if summary["facts_total"] <= 0:
        summary["notes"].append("no_facts_emitted")
    if summary["weak_output"] and not summary["zero_fact_output"]:
        summary["notes"].append("weak_output")
    return summary


def aggregate_compatibility_summaries(run_id: str, doc_summaries: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    docs = list(doc_summaries)
    missing_fields = sorted(
        {
            field
            for summary in docs
            for field in summary.get("missing_fields_to_adapt", [])
            if field
        }
    )
    notes: List[str] = []
    docs_failed = [summary["doc_id"] for summary in docs if not summary.get("standardize_consumable", False)]
    zero_fact_docs = [summary["doc_id"] for summary in docs if bool(summary.get("zero_fact_output", False))]
    weak_output_docs = [summary["doc_id"] for summary in docs if bool(summary.get("weak_output", False))]
    if docs_failed:
        notes.append(f"docs_failed={','.join(sorted(set(str(item) for item in docs_failed if item)))}")
    if zero_fact_docs:
        notes.append(f"zero_fact_docs={','.join(sorted(set(str(item) for item in zero_fact_docs if item)))}")
    if weak_output_docs:
        notes.append(f"weak_output_docs={','.join(sorted(set(str(item) for item in weak_output_docs if item)))}")
    if missing_fields:
        notes.append(f"missing_fields={','.join(missing_fields)}")
    return {
        "run_id": run_id,
        "docs_total": len(docs),
        "docs_consumable_total": sum(1 for summary in docs if summary.get("standardize_consumable", False)),
        "docs_failed_total": sum(1 for summary in docs if not summary.get("standardize_consumable", False)),
        "zero_fact_docs_total": sum(1 for summary in docs if bool(summary.get("zero_fact_output", False))),
        "weak_output_docs_total": sum(1 for summary in docs if bool(summary.get("weak_output", False))),
        "standardize_compatible": bool(docs) and all(
            summary.get("standardize_consumable", False) for summary in docs
        ),
        "missing_fields_to_adapt": missing_fields,
        "doc_ids": [summary.get("doc_id", "") for summary in docs],
        "notes": notes,
    }


def materialize_paddle_input_subset(
    *,
    source_provider_doc_dir: Path,
    page_entries: Sequence[Dict[str, Any]],
    target_input_dir: Path,
    provider_name: str = OCR.PADDLE_PROVIDER_NAME,
) -> Path:
    target_doc_dir = target_input_dir / provider_name / source_provider_doc_dir.name
    target_doc_dir.mkdir(parents=True, exist_ok=True)

    pages_payload: List[Dict[str, Any]] = []
    for page_entry in page_entries:
        page_copy = dict(page_entry)
        raw_rel = str(page_copy.get("raw_file", "") or "").strip()
        if raw_rel:
            source_raw = source_provider_doc_dir / raw_rel
            target_raw = target_doc_dir / raw_rel
            target_raw.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_raw, target_raw)
        artifact_files = [str(item) for item in page_copy.get("artifact_files", [])]
        for artifact_rel in artifact_files:
            source_artifact = source_provider_doc_dir / artifact_rel
            target_artifact = target_doc_dir / artifact_rel
            target_artifact.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_artifact, target_artifact)
        pages_payload.append(page_copy)

    result_payload = {
        "provider": provider_name,
        "page_count": len(pages_payload),
        "pages": sorted(pages_payload, key=lambda item: int(item.get("page_number", 0))),
    }
    (target_doc_dir / "result.json").write_text(
        json.dumps(result_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return target_doc_dir


def extract_page_quality_metrics(summary: Dict[str, Any]) -> Dict[str, Any]:
    run_summary = summary.get("run_summary", {}) or {}
    return {
        "cells_total_if_available": summary.get("cells_total"),
        "facts_total_if_available": summary.get("facts_total"),
        "issues_total_if_available": summary.get("issues_total"),
        "mapped_facts_ratio_if_available": run_summary.get("mapped_facts_ratio"),
        "review_total_if_available": run_summary.get("review_total"),
        "validation_fail_total_if_available": run_summary.get("validation_fail_total"),
    }


def build_conservative_evidence_thresholds() -> Dict[str, Any]:
    return {
        "minimum_docs_per_role": 2,
        "minimum_pages_per_role": 3,
        "maximum_zero_fact_pages": 0,
        "maximum_pages_with_fewer_tables_than_cloud_reference": 0,
        "maximum_average_tables_deficit_ratio": 0.35,
        "minimum_standardize_consumable_ratio": 1.0,
        "minimum_average_mapped_facts_ratio": 0.35,
        "maximum_average_review_total": 80.0,
        "maximum_average_validation_fail_total": 4.0,
    }


def evaluate_role_thresholds(
    role_summary: Dict[str, Any],
    evidence_thresholds: Dict[str, Any],
) -> Dict[str, Any]:
    thresholds_met = {
        "minimum_docs_per_role": int(role_summary.get("docs_total", 0) or 0)
        >= int(evidence_thresholds["minimum_docs_per_role"]),
        "minimum_pages_per_role": int(role_summary.get("pages_total", 0) or 0)
        >= int(evidence_thresholds["minimum_pages_per_role"]),
        "maximum_zero_fact_pages": int(role_summary.get("zero_fact_pages_total", 0) or 0)
        <= int(evidence_thresholds["maximum_zero_fact_pages"]),
        "maximum_pages_with_fewer_tables_than_cloud_reference": int(
            role_summary.get("pages_with_fewer_tables_than_cloud_reference", 0) or 0
        )
        <= int(evidence_thresholds["maximum_pages_with_fewer_tables_than_cloud_reference"]),
        "maximum_average_tables_deficit_ratio": float(role_summary.get("average_tables_deficit_ratio", 0.0) or 0.0)
        <= float(evidence_thresholds["maximum_average_tables_deficit_ratio"]),
        "minimum_standardize_consumable_ratio": float(role_summary.get("standardize_consumable_ratio", 0.0) or 0.0)
        >= float(evidence_thresholds["minimum_standardize_consumable_ratio"]),
        "minimum_average_mapped_facts_ratio": float(role_summary.get("average_mapped_facts_ratio", 0.0) or 0.0)
        >= float(evidence_thresholds["minimum_average_mapped_facts_ratio"]),
        "maximum_average_review_total": float(role_summary.get("average_review_total", 0.0) or 0.0)
        <= float(evidence_thresholds["maximum_average_review_total"]),
        "maximum_average_validation_fail_total": float(
            role_summary.get("average_validation_fail_total", 0.0) or 0.0
        )
        <= float(evidence_thresholds["maximum_average_validation_fail_total"]),
    }
    if int(role_summary.get("pages_total", 0) or 0) <= 0:
        recommendation_candidate = "not_ready"
    elif all(thresholds_met.values()):
        recommendation_candidate = "fallback_candidate"
    else:
        recommendation_candidate = "pilot_only"
    return {
        "thresholds_met": thresholds_met,
        "recommendation_candidate": recommendation_candidate,
    }


def build_role_summaries(
    page_rows: Sequence[Dict[str, Any]],
    *,
    evidence_thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    evidence_thresholds = dict(evidence_thresholds or build_conservative_evidence_thresholds())
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in page_rows:
        grouped.setdefault(str(row.get("page_role", "unclassified")), []).append(row)

    summaries: List[Dict[str, Any]] = []
    for page_role, rows in sorted(grouped.items()):
        average_runtime_seconds = average_numeric(row.get("runtime_seconds") for row in rows) or 0.0
        average_bbox_coverage = average_numeric(row.get("bbox_coverage") for row in rows) or 0.0
        average_tables_detected = average_numeric(row.get("tables_detected") for row in rows) or 0.0
        average_cloud_tables_detected = average_numeric(row.get("cloud_tables_detected_if_available") for row in rows) or 0.0
        average_tables_detected_delta = average_numeric(row.get("tables_detected_delta") for row in rows) or 0.0
        average_tables_deficit_ratio = average_numeric(row.get("tables_deficit_ratio") for row in rows) or 0.0
        average_cells_total = average_numeric(row.get("cells_total_if_available") for row in rows) or 0.0
        average_facts_total = average_numeric(row.get("facts_total_if_available") for row in rows) or 0.0
        average_mapped_facts_ratio = average_numeric(row.get("mapped_facts_ratio_if_available") for row in rows) or 0.0
        average_review_total = average_numeric(row.get("review_total_if_available") for row in rows) or 0.0
        average_validation_fail_total = average_numeric(row.get("validation_fail_total_if_available") for row in rows) or 0.0
        summary = {
            "page_role": page_role,
            "pages_total": len(rows),
            "docs_total": len({str(row.get("doc_id", "")) for row in rows}),
            "zero_fact_pages_total": sum(1 for row in rows if str(row.get("zero_fact_page", "")).strip().lower() == "yes"),
            "pages_with_fewer_tables_than_cloud_reference": sum(
                1
                for row in rows
                if str(row.get("fewer_tables_than_cloud_reference", "")).strip().lower() == "yes"
            ),
            "standardize_consumable_ratio": round(
                sum(1 for row in rows if str(row.get("standardize_consumable", "")).strip().lower() == "yes") / len(rows),
                6,
            ),
            "average_runtime_seconds": round(average_runtime_seconds, 6),
            "average_bbox_coverage": round(average_bbox_coverage, 6),
            "average_tables_detected": round(average_tables_detected, 6),
            "average_cloud_tables_detected": round(average_cloud_tables_detected, 6),
            "average_tables_detected_delta": round(average_tables_detected_delta, 6),
            "average_tables_deficit_ratio": round(average_tables_deficit_ratio, 6),
            "average_cells_total": round(average_cells_total, 6),
            "average_facts_total": round(average_facts_total, 6),
            "average_mapped_facts_ratio": round(average_mapped_facts_ratio, 6),
            "average_review_total": round(average_review_total, 6),
            "average_validation_fail_total": round(average_validation_fail_total, 6),
            "notes": [],
        }
        if len(rows) == 1:
            summary["notes"].append("single_page_evidence")
        if "note" in page_role.lower():
            summary["notes"].append("high_variance_note_role")
        if "noise" in page_role.lower():
            summary["notes"].append("noise_sensitive_sample")
        evaluation = evaluate_role_thresholds(summary, evidence_thresholds)
        summary["thresholds_met"] = evaluation["thresholds_met"]
        summary["recommendation_candidate"] = evaluation["recommendation_candidate"]
        summary["recommendation"] = evaluation["recommendation_candidate"]
        summaries.append(summary)
    return summaries


def build_parity_summary(
    page_rows: Sequence[Dict[str, Any]],
    role_summaries: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    rows = list(page_rows)
    return {
        "pages_total": len(rows),
        "docs_total": len({str(row.get("doc_id", "")) for row in rows}),
        "roles_total": len(role_summaries),
        "zero_fact_pages_total": sum(1 for row in rows if str(row.get("zero_fact_page", "")).strip().lower() == "yes"),
        "pages_with_fewer_tables_than_cloud_reference_total": sum(
            1 for row in rows if str(row.get("fewer_tables_than_cloud_reference", "")).strip().lower() == "yes"
        ),
        "standardize_consumable_ratio": round(
            sum(1 for row in rows if str(row.get("standardize_consumable", "")).strip().lower() == "yes") / len(rows),
            6,
        )
        if rows
        else 0.0,
        "average_runtime_seconds": round(average_numeric(row.get("runtime_seconds") for row in rows) or 0.0, 6),
        "average_tables_detected": round(average_numeric(row.get("tables_detected") for row in rows) or 0.0, 6),
        "average_cloud_tables_detected": round(
            average_numeric(row.get("cloud_tables_detected_if_available") for row in rows) or 0.0,
            6,
        ),
        "average_tables_detected_delta": round(
            average_numeric(row.get("tables_detected_delta") for row in rows) or 0.0,
            6,
        ),
        "average_tables_deficit_ratio": round(
            average_numeric(row.get("tables_deficit_ratio") for row in rows) or 0.0,
            6,
        ),
        "average_cells_total": round(average_numeric(row.get("cells_total_if_available") for row in rows) or 0.0, 6),
        "average_facts_total": round(average_numeric(row.get("facts_total_if_available") for row in rows) or 0.0, 6),
        "average_mapped_facts_ratio": round(
            average_numeric(row.get("mapped_facts_ratio_if_available") for row in rows) or 0.0,
            6,
        ),
        "average_review_total": round(average_numeric(row.get("review_total_if_available") for row in rows) or 0.0, 6),
        "average_validation_fail_total": round(
            average_numeric(row.get("validation_fail_total_if_available") for row in rows) or 0.0,
            6,
        ),
        "roles_with_candidate_recommendation": [
            summary["page_role"]
            for summary in role_summaries
            if summary.get("recommendation_candidate") == "fallback_candidate"
        ],
    }


def build_parity_by_role_rows(role_summaries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for summary in role_summaries:
        rows.append(
            {
                "page_role": summary["page_role"],
                "pages_total": summary["pages_total"],
                "docs_total": summary["docs_total"],
                "zero_fact_pages_total": summary["zero_fact_pages_total"],
                "pages_with_fewer_tables_than_cloud_reference": summary[
                    "pages_with_fewer_tables_than_cloud_reference"
                ],
                "standardize_consumable_ratio": summary["standardize_consumable_ratio"],
                "average_runtime_seconds": summary["average_runtime_seconds"],
                "average_tables_detected": summary["average_tables_detected"],
                "average_cloud_tables_detected": summary["average_cloud_tables_detected"],
                "average_tables_detected_delta": summary["average_tables_detected_delta"],
                "average_tables_deficit_ratio": summary["average_tables_deficit_ratio"],
                "average_cells_total": summary["average_cells_total"],
                "average_facts_total": summary["average_facts_total"],
                "average_mapped_facts_ratio": summary["average_mapped_facts_ratio"],
                "average_review_total": summary["average_review_total"],
                "average_validation_fail_total": summary["average_validation_fail_total"],
                "recommendation_candidate": summary["recommendation_candidate"],
                "notes": ";".join(summary["notes"]),
            }
        )
    return rows


def build_zero_fact_rows(page_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in page_rows
        if str(row.get("zero_fact_page", "")).strip().lower() == "yes"
    ]


def build_failure_analysis_rows(page_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in page_rows:
        failure_reasons: List[str] = []
        if str(row.get("zero_fact_page", "")).strip().lower() == "yes":
            failure_reasons.append("zero_fact_page")
        if str(row.get("standardize_consumable", "")).strip().lower() != "yes":
            failure_reasons.append("standardize_not_consumable")
        if str(row.get("fewer_tables_than_cloud_reference", "")).strip().lower() == "yes":
            failure_reasons.append("fewer_tables_than_cloud_reference")
        if float(row.get("tables_deficit_ratio", 0.0) or 0.0) > 0.35:
            failure_reasons.append("high_tables_deficit_ratio")
        if float(row.get("mapped_facts_ratio_if_available", 0.0) or 0.0) < 0.35:
            failure_reasons.append("low_mapped_facts_ratio")
        if float(row.get("review_total_if_available", 0.0) or 0.0) > 80.0:
            failure_reasons.append("high_review_total")
        if float(row.get("validation_fail_total_if_available", 0.0) or 0.0) > 4.0:
            failure_reasons.append("high_validation_fail_total")
        rows.append(
            {
                "doc_id": row.get("doc_id", ""),
                "page_no": row.get("page_no", ""),
                "page_role": row.get("page_role", ""),
                "zero_fact_page": row.get("zero_fact_page", "no"),
                "standardize_consumable": row.get("standardize_consumable", ""),
                "fewer_tables_than_cloud_reference": row.get("fewer_tables_than_cloud_reference", "no"),
                "tables_detected_delta": row.get("tables_detected_delta", 0),
                "tables_deficit_ratio": row.get("tables_deficit_ratio", 0.0),
                "mapped_facts_ratio_if_available": row.get("mapped_facts_ratio_if_available", ""),
                "review_total_if_available": row.get("review_total_if_available", ""),
                "validation_fail_total_if_available": row.get("validation_fail_total_if_available", ""),
                "failure_reasons": ";".join(failure_reasons),
                "notes": row.get("notes", ""),
            }
        )
    return rows


def build_quality_gate(
    *,
    environment_summary: Dict[str, Any],
    contract_summary: Dict[str, Any],
    eval_summary: Dict[str, Any],
    role_summaries: Sequence[Dict[str, Any]],
    compatibility_summary: Dict[str, Any],
    cloud_control_summary: Dict[str, Any],
    evidence_thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    evidence_thresholds = dict(evidence_thresholds or build_conservative_evidence_thresholds())
    fallback_roles = [
        summary["page_role"]
        for summary in role_summaries
        if summary.get("recommendation_candidate") == "fallback_candidate"
    ]
    runtime_average = float(eval_summary.get("runtime_seconds_average", 0.0) or 0.0)
    runtime_max = float(eval_summary.get("runtime_seconds_max", 0.0) or 0.0)
    runtime_reasonable = runtime_average <= 15.0 and runtime_max <= 30.0
    evidence_breadth_ok = (
        int(eval_summary.get("sample_pages_total", 0) or 0) >= 8
        and int(eval_summary.get("docs_total", 0) or 0) >= 5
        and len(role_summaries) >= 4
    )
    environment_ready = bool(environment_summary.get("provider_ready", False))
    provider_contract_pass = bool(contract_summary.get("contract_pass", False))
    standardize_compatible = bool(compatibility_summary.get("standardize_compatible", False))
    cloud_non_regression_pass = bool(cloud_control_summary.get("cloud_non_regression_pass", False))
    parity_summary = eval_summary.get("parity_summary", {}) or {}
    parity_reasonable = (
        int(parity_summary.get("zero_fact_pages_total", 0) or 0) <= 1
        and float(parity_summary.get("average_tables_deficit_ratio", 0.0) or 0.0) <= 0.75
    )
    thresholds_met_by_role = {
        summary["page_role"]: dict(summary.get("thresholds_met", {}))
        for summary in role_summaries
    }

    notes: List[str] = []
    if not environment_ready:
        notes.append("environment_not_ready")
    if not provider_contract_pass:
        notes.append("provider_contract_failed")
    if not runtime_reasonable:
        notes.append("runtime_not_reasonable")
    if not standardize_compatible:
        notes.append("standardize_compatibility_failed")
    if not cloud_non_regression_pass:
        notes.append("cloud_non_regression_failed")
    if not evidence_breadth_ok:
        notes.append("evidence_breadth_is_still_narrow")
    if not parity_reasonable:
        notes.append("overall_parity_is_not_yet_reasonable")
    if not environment_ready or not provider_contract_pass or not standardize_compatible or not cloud_non_regression_pass:
        quality_gate_status = "not_ready"
    elif fallback_roles and runtime_reasonable and evidence_breadth_ok and parity_reasonable:
        quality_gate_status = "fallback_candidate_for_specific_roles"
    else:
        quality_gate_status = "pilot_only"

    return {
        "quality_gate_status": quality_gate_status,
        "environment_ready": environment_ready,
        "provider_contract_pass": provider_contract_pass,
        "runtime_reasonable": runtime_reasonable,
        "standardize_compatible": standardize_compatible,
        "cloud_non_regression_pass": cloud_non_regression_pass,
        "evidence_breadth_ok": evidence_breadth_ok,
        "parity_reasonable": parity_reasonable,
        "evidence_thresholds": evidence_thresholds,
        "thresholds_met_by_role": thresholds_met_by_role,
        "fallback_candidate_roles": fallback_roles,
        "notes": notes,
    }


def build_route_recommendation(
    quality_gate: Dict[str, Any],
    role_summaries: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    per_role = {summary["page_role"]: summary["recommendation_candidate"] for summary in role_summaries}
    fallback_roles = [role for role, recommendation in per_role.items() if recommendation == "fallback_candidate"]
    notes = [
        "default production provider priority remains unchanged",
        "cloud-first routing remains unchanged",
    ]
    if fallback_roles:
        notes.append(f"explicit paddle fallback can be considered only for roles: {', '.join(sorted(fallback_roles))}")
    else:
        notes.append("no page role cleared for explicit paddle fallback")
    return {
        "global_recommendation": quality_gate["quality_gate_status"],
        "per_role_recommendation": per_role,
        "evidence_thresholds": quality_gate.get("evidence_thresholds", {}),
        "thresholds_met_by_role": quality_gate.get("thresholds_met_by_role", {}),
        "roles_remaining_cloud_only": sorted(role for role, recommendation in per_role.items() if recommendation != "fallback_candidate"),
        "default_priority_preserved": True,
        "cloud_first_preserved": True,
        "notes": notes,
    }
