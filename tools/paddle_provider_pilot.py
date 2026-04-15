from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import OCR
from project_paths import PADDLE_PROVIDER_PILOT_ROOT


DEFAULT_SAMPLE = [
    {"doc_id": "D01", "page_no": 4, "page_role": "main_statement", "layout_detection": "off"},
    {"doc_id": "D01", "page_no": 15, "page_role": "note_multi_table", "layout_detection": "on"},
    {"doc_id": "D02", "page_no": 4, "page_role": "cross_doc_main_statement", "layout_detection": "off"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Stage 8 Paddle local provider pilot.")
    parser.add_argument("--registry", default="benchmarks/registry.yml")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--paddle-device", default="auto", choices=("auto", "gpu", "cpu"))
    parser.add_argument("--paddle-runtime-python", default="")
    parser.add_argument("--paddle-skip-if-no-gpu", action="store_true")
    return parser.parse_args()


def generate_run_id() -> str:
    return f"PADDLE_PILOT_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


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


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def resolve_pdf_path(source_image_dir: Path) -> Path:
    pdfs = sorted(source_image_dir.glob("*.pdf"))
    if not pdfs:
        raise FileNotFoundError(f"No PDF files found in {source_image_dir}")
    return pdfs[0]


def clean_provider_doc_output(ocr_output_root: Path, pdf_path: Path) -> None:
    target = ocr_output_root / OCR.PADDLE_PROVIDER_NAME / pdf_path.stem
    if target.exists():
        shutil.rmtree(target)


def load_provider_result(ocr_output_root: Path, pdf_path: Path) -> Dict[str, Any]:
    result_path = ocr_output_root / OCR.PADDLE_PROVIDER_NAME / pdf_path.stem / "result.json"
    return json.loads(result_path.read_text(encoding="utf-8"))


def collect_paddle_result_pages(
    registry_by_doc: Dict[str, Dict[str, Any]],
    sample_pages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    for sample in sample_pages:
        entry = registry_by_doc[sample["doc_id"]]
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(Path(entry["_source_image_dir"]))
        provider_result = load_provider_result(ocr_output_root, pdf_path)
        page_entry = next(item for item in provider_result["pages"] if int(item["page_number"]) == sample["page_no"])
        pages.append(dict(page_entry))
    return pages


def summarize_existing_provider_page(ocr_output_root: Path, provider: str, pdf_path: Path, page_no: int) -> Dict[str, Any]:
    result_path = ocr_output_root / provider / pdf_path.stem / "result.json"
    if not result_path.exists():
        return {"tables_detected": 0, "xlsx_emitted": False, "html_emitted": False, "raw_json_emitted": False}
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


def collect_paddle_page_rows(
    registry_by_doc: Dict[str, Dict[str, Any]],
    sample_pages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for sample in sample_pages:
        entry = registry_by_doc[sample["doc_id"]]
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(Path(entry["_source_image_dir"]))
        provider_result = load_provider_result(ocr_output_root, pdf_path)
        page_entry = next(item for item in provider_result["pages"] if int(item["page_number"]) == sample["page_no"])
        rows.append(
            {
                "doc_id": sample["doc_id"],
                "page_no": sample["page_no"],
                "provider": OCR.PADDLE_PROVIDER_NAME,
                "runtime_seconds": page_entry.get("runtime_seconds", 0.0),
                "tables_detected": page_entry.get("table_count", 0),
                "xlsx_emitted": any(str(item).lower().endswith(".xlsx") for item in page_entry.get("artifact_files", [])),
                "html_emitted": any(str(item).lower().endswith(".html") for item in page_entry.get("artifact_files", [])),
                "raw_json_emitted": bool(page_entry.get("raw_file")),
                "standardize_consumable": "yes" if any(str(item).lower().endswith(".xlsx") for item in page_entry.get("artifact_files", [])) else "no",
                "notes": ";".join(page_entry.get("missing_fields", []) or []),
                "layout_detection": sample["layout_detection"],
                "page_role": sample["page_role"],
                "selected_device": page_entry.get("selected_device", ""),
            }
        )
    return rows


def build_compare_rows(
    registry_by_doc: Dict[str, Dict[str, Any]],
    sample_pages: List[Dict[str, Any]],
    paddle_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    compare_rows: List[Dict[str, Any]] = []
    paddle_by_key = {(row["doc_id"], row["page_no"]): row for row in paddle_rows}
    for sample in sample_pages:
        entry = registry_by_doc[sample["doc_id"]]
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(Path(entry["_source_image_dir"]))
        paddle_row = paddle_by_key[(sample["doc_id"], sample["page_no"])]
        aliyun = summarize_existing_provider_page(ocr_output_root, "aliyun_table", pdf_path, sample["page_no"])
        tencent = summarize_existing_provider_page(ocr_output_root, "tencent_table_v3", pdf_path, sample["page_no"])
        compare_rows.append(
            {
                "doc_id": sample["doc_id"],
                "page_no": sample["page_no"],
                "provider": OCR.PADDLE_PROVIDER_NAME,
                "runtime_seconds": paddle_row["runtime_seconds"],
                "tables_detected": paddle_row["tables_detected"],
                "xlsx_emitted": paddle_row["xlsx_emitted"],
                "html_emitted": paddle_row["html_emitted"],
                "raw_json_emitted": paddle_row["raw_json_emitted"],
                "standardize_consumable": paddle_row["standardize_consumable"],
                "aliyun_tables_detected": aliyun["tables_detected"],
                "tencent_tables_detected": tencent["tables_detected"],
                "aliyun_xlsx_emitted": aliyun["xlsx_emitted"],
                "tencent_xlsx_emitted": tencent["xlsx_emitted"],
                "notes": paddle_row["notes"],
            }
        )
    return compare_rows


def main() -> int:
    args = parse_args()
    repo_root = REPO_ROOT
    registry_path = (repo_root / args.registry).resolve()
    registry_by_doc = load_registry(registry_path)
    run_id = args.run_id or generate_run_id()
    experiment_root = PADDLE_PROVIDER_PILOT_ROOT / run_id
    experiment_root.mkdir(parents=True, exist_ok=True)

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
        write_json(
            experiment_root / "paddle_pilot_summary.json",
            {
                "run_id": run_id,
                "status": "skipped",
                "sample_pages_total": len(DEFAULT_SAMPLE),
                "skip_reason": environment_summary.get("skip_reason_if_any", ""),
            },
        )
        return 1

    provider = OCR.PaddleLocalOCRProvider(paddle_options)
    sample_pages = [dict(item) for item in DEFAULT_SAMPLE]
    sample_by_doc: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for sample in sample_pages:
        sample_by_doc[sample["doc_id"]].append(sample)

    for doc_id, samples in sample_by_doc.items():
        entry = registry_by_doc[doc_id]
        source_image_dir = Path(entry["_source_image_dir"])
        ocr_output_root = Path(entry["_input_dir"])
        pdf_path = resolve_pdf_path(source_image_dir)
        clean_provider_doc_output(ocr_output_root, pdf_path)
        page_numbers = [int(item["page_no"]) for item in samples]
        page_options = {
            int(item["page_no"]): {"layout_detection": item["layout_detection"]}
            for item in samples
        }
        rendered_pages = OCR.render_pdf_pages(pdf_path, page_numbers=page_numbers)
        OCR.process_pdf_with_provider(
            pdf_path=pdf_path,
            rendered_pages=rendered_pages,
            provider=provider,
            output_root=ocr_output_root,
            page_options_by_number=page_options,
        )

    paddle_rows = collect_paddle_page_rows(registry_by_doc, sample_pages)
    compare_rows = build_compare_rows(registry_by_doc, sample_pages, paddle_rows)
    contract_summary = OCR.build_paddle_provider_contract_summary(
        collect_paddle_result_pages(registry_by_doc, sample_pages)
    )
    runtime_summary = {
        "run_id": run_id,
        "pages_total": len(paddle_rows),
        "runtime_seconds_total": round(sum(float(row["runtime_seconds"] or 0.0) for row in paddle_rows), 6),
        "runtime_seconds_average": round(
            sum(float(row["runtime_seconds"] or 0.0) for row in paddle_rows) / len(paddle_rows),
            6,
        )
        if paddle_rows
        else 0.0,
        "selected_device": environment_summary.get("selected_device", ""),
        "gpu_available": environment_summary.get("gpu_available", False),
    }
    pilot_summary = {
        "run_id": run_id,
        "status": "completed",
        "sample_pages_total": len(sample_pages),
        "doc_ids": sorted({item["doc_id"] for item in sample_pages}),
        "page_roles": sorted({item["page_role"] for item in sample_pages}),
        "provider_name": OCR.PADDLE_PROVIDER_NAME,
    }

    write_csv(
        experiment_root / "paddle_pilot_pages.csv",
        paddle_rows,
        [
            "doc_id",
            "page_no",
            "provider",
            "runtime_seconds",
            "tables_detected",
            "xlsx_emitted",
            "html_emitted",
            "raw_json_emitted",
            "standardize_consumable",
            "notes",
            "layout_detection",
            "page_role",
            "selected_device",
        ],
    )
    write_csv(
        experiment_root / "paddle_vs_cloud_compare.csv",
        compare_rows,
        [
            "doc_id",
            "page_no",
            "provider",
            "runtime_seconds",
            "tables_detected",
            "xlsx_emitted",
            "html_emitted",
            "raw_json_emitted",
            "standardize_consumable",
            "aliyun_tables_detected",
            "tencent_tables_detected",
            "aliyun_xlsx_emitted",
            "tencent_xlsx_emitted",
            "notes",
        ],
    )
    write_json(experiment_root / "paddle_runtime_summary.json", runtime_summary)
    write_json(experiment_root / "paddle_provider_contract_summary.json", contract_summary)
    write_json(experiment_root / "paddle_pilot_summary.json", pilot_summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
