from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List


os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker process for AutoFinance Paddle table OCR.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    env_parser = subparsers.add_parser("env-summary", help="Probe Paddle/PaddleX runtime availability.")
    env_parser.add_argument("--requested-device", default="auto", choices=("auto", "gpu", "cpu"))
    env_parser.add_argument("--skip-if-no-gpu", action="store_true")
    env_parser.add_argument("--output-json", required=True)

    recognize_parser = subparsers.add_parser("recognize-image", help="Run Paddle table recognition for one rendered page image.")
    recognize_parser.add_argument("--image-path", required=True)
    recognize_parser.add_argument("--page-number", required=True, type=int)
    recognize_parser.add_argument("--requested-device", default="auto", choices=("auto", "gpu", "cpu"))
    recognize_parser.add_argument("--layout-detection", required=True, choices=("on", "off"))
    recognize_parser.add_argument("--pipeline", default="table_recognition")
    recognize_parser.add_argument("--artifact-dir", required=True)
    recognize_parser.add_argument("--output-json", required=True)
    return parser.parse_args()


def compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def import_runtime() -> tuple[Any, Any, Any]:
    try:
        import paddle  # type: ignore
        import paddlex  # type: ignore
        from paddlex.utils.cache import CACHE_DIR  # type: ignore
    except Exception as exc:  # pragma: no cover - exercised by integration path
        raise RuntimeError(
            "Paddle runtime is unavailable in the selected Python environment. "
            "Install paddle + paddlex, or point --paddle-runtime-python at a ready environment."
        ) from exc
    return paddle, paddlex, CACHE_DIR


def build_vram_summary(paddle_module: Any) -> Dict[str, Any] | str:
    try:
        if not paddle_module.device.is_compiled_with_cuda():
            return ""
        props = paddle_module.device.cuda.get_device_properties()
        total_memory_mb = None
        if hasattr(props, "total_memory"):
            total_memory_raw = float(props.total_memory)
            if total_memory_raw > 1024 * 1024 * 1024:
                total_memory_mb = int(round(total_memory_raw / (1024 * 1024)))
            else:
                total_memory_mb = int(round(total_memory_raw))
        return {
            "name": getattr(props, "name", ""),
            "major": getattr(props, "major", None),
            "minor": getattr(props, "minor", None),
            "multi_processor_count": getattr(props, "multi_processor_count", None),
            "total_memory_mb": total_memory_mb,
        }
    except Exception:
        return ""


def resolve_device(paddle_module: Any, requested_device: str) -> tuple[bool, str]:
    gpu_available = bool(paddle_module.device.is_compiled_with_cuda())
    if requested_device == "gpu":
        return gpu_available, "gpu:0" if gpu_available else "cpu"
    if requested_device == "cpu":
        return gpu_available, "cpu"
    return gpu_available, "gpu:0" if gpu_available else "cpu"


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def build_environment_summary(requested_device: str, skip_if_no_gpu: bool) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "gpu_available": False,
        "selected_device": "cpu",
        "vram_info_if_available": "",
        "model_cache_path": "",
        "provider_enabled": True,
        "provider_ready": False,
        "skip_reason_if_any": "",
        "runtime_python": sys.executable,
        "paddle_version": "",
        "paddlex_version": "",
        "compiled_with_cuda": False,
    }
    try:
        paddle_module, paddlex_module, cache_dir = import_runtime()
    except Exception as exc:
        summary["skip_reason_if_any"] = str(exc)
        return summary

    gpu_available, selected_device = resolve_device(paddle_module, requested_device)
    summary.update(
        {
            "gpu_available": gpu_available,
            "selected_device": selected_device,
            "vram_info_if_available": build_vram_summary(paddle_module),
            "model_cache_path": str(cache_dir),
            "paddle_version": getattr(paddle_module, "__version__", ""),
            "paddlex_version": getattr(paddlex_module, "__version__", ""),
            "compiled_with_cuda": bool(paddle_module.device.is_compiled_with_cuda()),
        }
    )
    if skip_if_no_gpu and selected_device != "gpu:0":
        summary["skip_reason_if_any"] = "GPU requested for Paddle provider but no Paddle GPU runtime is available."
        summary["provider_ready"] = False
        return summary
    summary["provider_ready"] = True
    return summary


def normalize_text_block(rec_texts: List[str], rec_scores: List[Any], rec_boxes: List[Any], dt_polys: List[Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for index, text in enumerate(rec_texts):
        block: Dict[str, Any] = {"text": text}
        if index < len(rec_scores):
            block["confidence"] = rec_scores[index]
        if index < len(rec_boxes):
            block["bounding_box"] = rec_boxes[index]
        if index < len(dt_polys):
            block["polygon"] = dt_polys[index]
        blocks.append(block)
    return blocks


def compute_bbox(cell_box_list: List[List[int]]) -> List[int] | None:
    if not cell_box_list:
        return None
    xs1 = [int(item[0]) for item in cell_box_list if len(item) >= 4]
    ys1 = [int(item[1]) for item in cell_box_list if len(item) >= 4]
    xs2 = [int(item[2]) for item in cell_box_list if len(item) >= 4]
    ys2 = [int(item[3]) for item in cell_box_list if len(item) >= 4]
    if not xs1 or not ys1 or not xs2 or not ys2:
        return None
    return [min(xs1), min(ys1), max(xs2), max(ys2)]


def recognize_image(
    *,
    image_path: Path,
    page_number: int,
    requested_device: str,
    layout_detection: str,
    pipeline_name: str,
    artifact_dir: Path,
) -> Dict[str, Any]:
    env_summary = build_environment_summary(requested_device, skip_if_no_gpu=False)
    if not env_summary.get("provider_ready", False):
        raise RuntimeError(env_summary.get("skip_reason_if_any") or "Paddle provider is not ready.")

    paddle_module, paddlex_module, _ = import_runtime()
    selected_device = str(env_summary["selected_device"])
    pipeline = paddlex_module.create_pipeline(pipeline_name, device=selected_device)
    started_at = time.perf_counter()
    result = next(
        pipeline.predict(
            str(image_path),
            use_layout_detection=layout_detection == "on",
        )
    )
    runtime_seconds = round(time.perf_counter() - started_at, 6)
    payload = result.json["res"]
    overall_ocr = payload.get("overall_ocr_res", {}) or {}
    rec_texts = [str(item) for item in overall_ocr.get("rec_texts", [])]
    rec_scores = list(overall_ocr.get("rec_scores", []))
    rec_boxes = list(overall_ocr.get("rec_boxes", []))
    dt_polys = list(overall_ocr.get("dt_polys", []))
    page_text = "\n".join(text.strip() for text in rec_texts if str(text).strip())

    artifact_dir.mkdir(parents=True, exist_ok=True)
    tables: List[Dict[str, Any]] = []
    artifact_files: List[str] = []
    missing_fields: List[str] = []

    for table_index, table_result in enumerate(result["table_res_list"], start=1):
        table_payload = table_result.json["res"]
        cell_box_list = list(table_payload.get("cell_box_list", []))
        table_id = str(table_index)
        xlsx_basename = f"page_{page_number:04d}_table_{table_index:02d}.xlsx"
        html_basename = f"page_{page_number:04d}_table_{table_index:02d}.html"
        table_result.save_to_xlsx((artifact_dir / xlsx_basename).as_posix())
        (artifact_dir / html_basename).write_text(str(table_payload.get("pred_html", "")), encoding="utf-8")
        artifact_files.extend([xlsx_basename, html_basename])

        table_bbox = compute_bbox(cell_box_list)
        table_missing_fields: List[str] = []
        if not cell_box_list:
            table_missing_fields.append("cell_box_list")
        if not table_bbox:
            table_missing_fields.append("bbox")
        if not table_payload.get("pred_html"):
            table_missing_fields.append("pred_html")
        tables.append(
            {
                "table_id": table_id,
                "table_index": table_index,
                "table_region_id": table_result.get("table_region_id"),
                "bbox": table_bbox,
                "cell_box_count": len(cell_box_list),
                "cell_box_list": cell_box_list,
                "pred_html": table_payload.get("pred_html", ""),
                "table_ocr_pred": table_payload.get("table_ocr_pred", {}),
                "neighbor_texts": table_result.get("neighbor_texts", []),
                "xlsx_filename": xlsx_basename,
                "html_filename": html_basename,
                "missing_fields": table_missing_fields,
            }
        )
        missing_fields.extend(table_missing_fields)

    if not tables:
        missing_fields.append("tables")

    raw_payload: Dict[str, Any] = {
        "provider_name": "paddle_table_local",
        "pipeline_name": pipeline_name,
        "page_number": page_number,
        "input_path": str(image_path),
        "runtime_seconds": runtime_seconds,
        "device_requested": requested_device,
        "selected_device": selected_device,
        "layout_detection_enabled": layout_detection == "on",
        "gpu_available": env_summary.get("gpu_available", False),
        "vram_info_if_available": env_summary.get("vram_info_if_available", ""),
        "page_text": page_text,
        "blocks": normalize_text_block(rec_texts, rec_scores, rec_boxes, dt_polys),
        "overall_ocr_res": overall_ocr,
        "layout_det_res": payload.get("layout_det_res", {}),
        "tables": tables,
        "artifact_filenames": artifact_files,
        "missing_fields": sorted(set(field for field in missing_fields if field)),
        "contract_version": "stage8_paddle_local_v1",
        "worker_runtime": {
            "runtime_python": sys.executable,
            "paddle_version": getattr(paddle_module, "__version__", ""),
            "paddlex_version": getattr(paddlex_module, "__version__", ""),
        },
    }
    return raw_payload


def main() -> int:
    args = parse_args()
    output_json = Path(args.output_json)
    if args.command == "env-summary":
        payload = build_environment_summary(args.requested_device, args.skip_if_no_gpu)
        write_json(output_json, payload)
        return 0

    try:
        payload = recognize_image(
            image_path=Path(args.image_path),
            page_number=int(args.page_number),
            requested_device=args.requested_device,
            layout_detection=args.layout_detection,
            pipeline_name=args.pipeline,
            artifact_dir=Path(args.artifact_dir),
        )
    except Exception as exc:
        write_json(
            output_json,
            {
                "provider_name": "paddle_table_local",
                "page_number": int(args.page_number),
                "error": str(exc),
            },
        )
        return 1

    write_json(output_json, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
