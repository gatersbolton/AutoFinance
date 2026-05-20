from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from fastapi import HTTPException

from project_paths import RAW_METRICS_GENERATED_ROOT, REPO_ROOT, STANDARD_METRICS_GENERATED_ROOT, STANDARD_TERMS_PATH
from raw_extract.cli import run_raw_extraction
from raw_extract.loader import infer_doc_id as infer_raw_doc_id
from standard_map.decisions import append_mapping_decision_file, apply_mapping_decision_to_output
from standard_map.loader import infer_doc_id as infer_standard_doc_id
from standard_map.loader import validate_input_path as validate_raw_metrics_input_path
from standard_map.mapper import mapping_run_to_web_summary, run_standard_mapping
from standard_map.store import LocalMappingStore

from .combined_downloads import COMBINED_WORKBOOK_DOWNLOAD_NAME, build_combined_metrics_workbook
from .config import WebAppSettings
from .models import JOB_MODE_UPLOAD, JobRecord


RAW_REVIEW_ACTIONS = {"approve", "skip", "edit", "next_table"}
MAPPING_REVIEW_ACTIONS = {
    "reject",
    "accept_once",
    "accept_and_remember",
    "approve_mapping",
    "skip_mapping",
    "change_mapping",
}
_SOURCE_BLOCK_CACHE: dict[Path, list[dict[str, Any]]] = {}
_SOURCE_TABLE_CELL_CACHE: dict[Path, list[dict[str, Any]]] = {}
_SOURCE_PAGE_METADATA_CACHE: dict[Path, dict[str, Any]] = {}


def job_root(job: JobRecord) -> Path:
    return Path(job.output_dir).resolve().parent


def simple_flow_dir(job: JobRecord) -> Path:
    return job_root(job) / "simple_flow"


def raw_review_dir(job: JobRecord) -> Path:
    return job_root(job) / "raw_review"


def mapping_review_dir(job: JobRecord) -> Path:
    return job_root(job) / "mapping_review"


def raw_step_summary_path(job: JobRecord) -> Path:
    return simple_flow_dir(job) / "raw_metrics_step_summary.json"


def standard_step_summary_path(job: JobRecord) -> Path:
    return simple_flow_dir(job) / "standard_metrics_step_summary.json"


def combined_downloads_dir(job: JobRecord) -> Path:
    return Path(job.result_dir).resolve() / "downloads"


def combined_workbook_path(job: JobRecord) -> Path:
    return combined_downloads_dir(job) / COMBINED_WORKBOOK_DOWNLOAD_NAME


def combined_download_summary_path(job: JobRecord) -> Path:
    return job_root(job) / "combined_download_summary.json"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def load_simple_flow_state(job: JobRecord) -> dict[str, Any]:
    raw_summary = load_json(raw_step_summary_path(job))
    standard_summary = load_json(standard_step_summary_path(job))
    combined_summary = load_json(combined_download_summary_path(job))
    raw_metrics_csv = _portable_summary_file(raw_summary.get("raw_metrics_csv", ""), RAW_METRICS_GENERATED_ROOT / job.job_id, "raw_metrics.csv")
    if raw_metrics_csv is None and standard_summary.get("input_path"):
        raw_metrics_csv = _portable_summary_file(standard_summary.get("input_path", ""), RAW_METRICS_GENERATED_ROOT / job.job_id, "raw_metrics.csv")
    raw_metrics_xlsx = _portable_summary_file(raw_summary.get("raw_metrics_xlsx", ""), RAW_METRICS_GENERATED_ROOT / job.job_id, "raw_metrics.xlsx")
    standardized_metrics_csv = _portable_summary_file(
        standard_summary.get("standardized_metrics_csv", ""),
        STANDARD_METRICS_GENERATED_ROOT / job.job_id,
        "standardized_metrics.csv",
    )
    standardized_metrics_xlsx = _portable_summary_file(
        standard_summary.get("standardized_metrics_xlsx", ""),
        STANDARD_METRICS_GENERATED_ROOT / job.job_id,
        "standardized_metrics.xlsx",
    )
    combined_workbook = _portable_summary_file(
        combined_summary.get("workbook_path", ""),
        combined_downloads_dir(job),
        COMBINED_WORKBOOK_DOWNLOAD_NAME,
    )
    return {
        "raw_summary": raw_summary,
        "standard_summary": standard_summary,
        "combined_summary": combined_summary,
        "raw_metrics_csv": str(raw_metrics_csv) if raw_metrics_csv else str(raw_summary.get("raw_metrics_csv", "") or ""),
        "raw_metrics_xlsx": str(raw_metrics_xlsx) if raw_metrics_xlsx else str(raw_summary.get("raw_metrics_xlsx", "") or ""),
        "standardized_metrics_csv": str(standardized_metrics_csv) if standardized_metrics_csv else str(standard_summary.get("standardized_metrics_csv", "") or ""),
        "standardized_metrics_xlsx": str(standardized_metrics_xlsx) if standardized_metrics_xlsx else str(standard_summary.get("standardized_metrics_xlsx", "") or ""),
        "combined_metrics_xlsx": str(combined_workbook) if combined_workbook else str(combined_summary.get("workbook_path", "") or ""),
        "combined_download_summary": str(combined_download_summary_path(job)) if combined_download_summary_path(job).exists() else "",
        "raw_ready": bool(raw_metrics_csv and raw_metrics_csv.exists()),
        "standard_ready": bool(standardized_metrics_csv and standardized_metrics_csv.exists()),
        "combined_ready": bool(combined_workbook and combined_workbook.exists()),
    }


def _portable_summary_file(raw_path: object, fallback_root: Path, filename: str) -> Path | None:
    raw_value = str(raw_path or "").strip()
    if raw_value:
        candidate = Path(raw_value)
        if not candidate.is_absolute():
            candidate = REPO_ROOT / candidate
        candidate = candidate.resolve()
        if candidate.exists():
            return candidate
    if fallback_root.exists():
        matches = sorted(fallback_root.rglob(filename), key=lambda path: path.stat().st_mtime if path.exists() else 0)
        if matches:
            return matches[-1].resolve()
    return None


def run_raw_metrics_step(settings: WebAppSettings, job: JobRecord) -> dict[str, Any]:
    input_dir = Path(job.ocr_output_dir).resolve() if job.mode == JOB_MODE_UPLOAD and job.ocr_output_dir else Path(job.input_path).resolve()
    if not input_dir.exists():
        raise ValueError(f"OCR 输出目录不存在: {input_dir}")
    source_image_dir = Path(job.source_image_dir).resolve() if job.source_image_dir and Path(job.source_image_dir).exists() else None
    doc_id = infer_raw_doc_id(input_dir, "")
    output_base = RAW_METRICS_GENERATED_ROOT / doc_id
    args = argparse.Namespace(
        input_dir=str(input_dir),
        output_dir=str(output_base),
        source_image_dir=str(source_image_dir or ""),
        provider_priority=job.provider_priority,
        doc_id=doc_id,
        company_name="",
        default_fill_date="",
        include_ratios=True,
        include_blank=False,
        debug=False,
    )
    result = run_raw_extraction(args=args, cli_args=["--input-dir", str(input_dir), "--output-dir", str(output_base)])
    payload = {
        "pass": bool(result.summary.get("pass")),
        "run_id": result.run_id,
        "doc_id": doc_id,
        "input_dir": str(input_dir),
        "output_dir": result.output_dir,
        "raw_metrics_csv": str(Path(result.output_dir) / "raw_metrics.csv"),
        "raw_metrics_xlsx": str(Path(result.output_dir) / "raw_metrics.xlsx"),
        "raw_metrics_detailed_csv": str(Path(result.output_dir) / "raw_metrics_detailed.csv"),
        "accepted_metrics_total": result.summary.get("accepted_metrics_total", 0),
        "output_files": list(result.output_files),
        "no_ocr_api_called": True,
    }
    write_json(raw_step_summary_path(job), payload)
    refresh_combined_metrics_workbook(settings, job)
    return payload


def resolve_raw_metrics_for_step2(raw_metrics_path: str, job: JobRecord) -> Path:
    explicit = str(raw_metrics_path or "").strip()
    if explicit:
        path = Path(explicit)
        if not path.is_absolute():
            path = REPO_ROOT / path
        path = path.resolve()
        validate_raw_metrics_input_path(path)
        if not path.exists():
            raise ValueError(f"原始数据表不存在: {path}")
        return path
    state = load_simple_flow_state(job)
    raw_csv = str(state.get("raw_metrics_csv", "") or "")
    if not raw_csv:
        raise ValueError("请先生成原始数据表，或提供 raw_metrics.csv 路径。")
    path = Path(raw_csv).resolve()
    validate_raw_metrics_input_path(path)
    if not path.exists():
        raise ValueError(f"原始数据表不存在: {path}")
    return path


def run_standard_metrics_step(settings: WebAppSettings, job: JobRecord, *, raw_metrics_path: str = "") -> dict[str, Any]:
    input_path = resolve_raw_metrics_for_step2(raw_metrics_path, job)
    doc_id = infer_standard_doc_id(input_path, "")
    output_base = STANDARD_METRICS_GENERATED_ROOT / (doc_id or "default")
    args = argparse.Namespace(
        input=str(input_path),
        output_dir=str(output_base),
        mapping_registry=str(STANDARD_TERMS_PATH),
        mapping_store_path=str(settings.mapping_store_path),
        doc_id=doc_id,
        company_name="",
        enable_llm_mapping=True,
        disable_llm_mapping=False,
        llm_model="deepseek-v4-flash",
        llm_env_file=str(settings.deepseek_env_path),
        llm_mock=None,
        disable_llm_cache=False,
        debug=False,
    )
    result = run_standard_mapping(args=args, cli_args=["--input", str(input_path), "--output-dir", str(output_base)])
    payload = mapping_run_to_web_summary(result)
    payload["doc_id"] = doc_id
    write_json(standard_step_summary_path(job), payload)
    refresh_combined_metrics_workbook(settings, job)
    return payload


def refresh_combined_metrics_workbook(settings: WebAppSettings, job: JobRecord) -> dict[str, Any]:
    state = load_simple_flow_state(job)
    raw_csv = _existing_state_file(state.get("raw_metrics_csv", ""))
    standard_csv = _existing_state_file(state.get("standardized_metrics_csv", ""))
    mapping_review_csv = standard_csv.parent / "mapping_review_items.csv" if standard_csv else None
    raw_summary = state.get("raw_summary", {}) if isinstance(state.get("raw_summary"), dict) else {}
    standard_summary = state.get("standard_summary", {}) if isinstance(state.get("standard_summary"), dict) else {}
    doc_id = str(raw_summary.get("doc_id") or standard_summary.get("doc_id") or job.job_id)
    summary = build_combined_metrics_workbook(
        raw_csv,
        standard_csv,
        mapping_review_csv if mapping_review_csv and mapping_review_csv.exists() else None,
        combined_workbook_path(job),
        metadata={
            "doc_id": doc_id,
            "job_id": job.job_id,
            "summary_path": combined_download_summary_path(job),
            "path_hygiene_roots": [settings.runtime_root, STANDARD_METRICS_GENERATED_ROOT],
        },
    )
    write_json(settings.runtime_root / "combined_download_summary.json", summary)
    return summary


def _existing_state_file(value: object) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    return path if path.exists() and path.is_file() else None


def load_raw_review_items(job: JobRecord, *, apply_actions: bool = True) -> list[dict[str, Any]]:
    state = load_simple_flow_state(job)
    raw_csv = Path(str(state.get("raw_metrics_csv", "") or ""))
    if not raw_csv.exists():
        return []
    rows = read_csv_rows(raw_csv)
    detailed_rows = read_csv_rows(raw_csv.parent / "raw_metrics_detailed.csv")
    items: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        detailed = detailed_rows[index - 1] if index - 1 < len(detailed_rows) else {}
        raw_metric_id = str(detailed.get("source_cell_ref", "") or f"raw_{index:06d}")
        source_bbox_json = detailed.get("bbox_json", "") or _infer_bbox_json_from_source_blocks(detailed, row)
        if _is_coarse_bbox_json(source_bbox_json):
            source_bbox_json = _resolve_source_table_cell_bbox_json(detailed) or _resolve_peer_review_bbox_json(
                detailed_rows,
                detailed,
                str(row.get("指标名", "") or detailed.get("row_label_clean", "") or detailed.get("metric_name", "")),
            )
        if not source_bbox_json:
            source_bbox_json = _resolve_source_table_cell_bbox_json(detailed)
        items.append(
            {
                "review_item_id": f"rawrev_{index:06d}",
                "raw_metric_id": raw_metric_id,
                "填表日期": row.get("填表日期", ""),
                "当前条目日期": row.get("当前条目日期", ""),
                "公司名": row.get("公司名", ""),
                "指标名": row.get("指标名", ""),
                "指标数值": row.get("指标数值", ""),
                "source_page_no": detailed.get("page_no", ""),
                "source_bbox_json": source_bbox_json,
                "source_pdf_path": detailed.get("evidence_path", ""),
                "source_file": detailed.get("source_file", ""),
                "provider": detailed.get("provider", ""),
                "doc_id": detailed.get("doc_id", ""),
                "table_id": detailed.get("table_id", ""),
                "logical_subtable_id": detailed.get("logical_subtable_id", ""),
                "row_index": detailed.get("row_index", ""),
                "col_index": detailed.get("col_index", ""),
                "row_label_clean": detailed.get("row_label_clean", ""),
                "row_label_raw": detailed.get("row_label_raw", ""),
                "header_path": detailed.get("header_path", ""),
                "period_role_raw": detailed.get("period_role_raw", ""),
                "value_raw": detailed.get("value_raw", ""),
                "value_type": detailed.get("value_type", ""),
                "confidence": detailed.get("confidence", ""),
            }
        )
    return _apply_raw_review_table_edits(job, items) if apply_actions else items


def _apply_raw_review_table_edits(job: JobRecord, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    item_by_review_id = {str(item.get("review_item_id", "")): item for item in items}
    item_by_raw_id = {str(item.get("raw_metric_id", "")): item for item in items}
    for action in _load_raw_review_actions(job):
        if str(action.get("action", "")) not in {"edit", "next_table"}:
            continue
        edits = action.get("edits")
        if not isinstance(edits, dict):
            continue
        table_edits = edits.get("table_edits")
        if not isinstance(table_edits, list):
            continue
        for edit in table_edits:
            if not isinstance(edit, dict):
                continue
            target = item_by_review_id.get(str(edit.get("review_item_id", "")))
            if target is None:
                target = item_by_raw_id.get(str(edit.get("raw_metric_id", "")))
            if target is None:
                continue
            if "value" in edit:
                value = str(edit.get("value", "") or "")
                target["指标数值"] = value
                target["value_raw"] = value
            metric_name = str(edit.get("metric_name", "") or "").strip()
            if metric_name:
                target["指标名"] = metric_name
                target["row_label_clean"] = metric_name
                target["row_label_raw"] = metric_name
    return items


def _load_raw_review_actions(job: JobRecord) -> list[dict[str, Any]]:
    path = raw_review_dir(job) / "raw_review_actions.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return [action for action in payload if isinstance(action, dict)] if isinstance(payload, list) else []


def _infer_bbox_json_from_source_blocks(detailed: dict[str, Any], row: dict[str, Any]) -> str:
    source_file = str(detailed.get("source_file", "") or "").strip()
    target_value = str(detailed.get("value_raw", "") or row.get("指标数值", "") or "").strip()
    if not source_file or not target_value:
        return ""
    blocks = _load_source_blocks(source_file)
    if not blocks:
        return ""
    value_key = _normalize_ocr_text(target_value)
    if not value_key:
        return ""
    value_candidates = [
        (block, bbox)
        for block in blocks
        if (bbox := _block_bbox(block))
        and _normalize_ocr_text(str(block.get("text", ""))) == value_key
    ]
    if not value_candidates:
        value_candidates = [
            (block, bbox)
            for block in blocks
            if (bbox := _block_bbox(block))
            and value_key in _normalize_ocr_text(str(block.get("text", "")))
        ]
    if not value_candidates:
        return ""

    label_key = _normalize_ocr_label(str(detailed.get("row_label_clean", "") or detailed.get("row_label_raw", "") or row.get("指标名", "")))
    label_candidates = [
        bbox
        for block in blocks
        if (bbox := _block_bbox(block))
        and label_key
        and _label_matches(_normalize_ocr_label(str(block.get("text", ""))), label_key)
    ]
    col_index = _safe_int(detailed.get("col_index"), default=0)
    chosen = min(
        (bbox for _, bbox in value_candidates),
        key=lambda bbox: _bbox_choice_score(bbox, label_candidates, col_index),
    )
    return _bbox_to_json(chosen, y_offset=_inferred_bbox_y_offset(chosen))


def _load_source_blocks(source_file: str) -> list[dict[str, Any]]:
    path = Path(source_file)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    cached = _SOURCE_BLOCK_CACHE.get(path)
    if cached is not None:
        return cached
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _SOURCE_BLOCK_CACHE[path] = []
        return []
    blocks = payload.get("blocks", []) if isinstance(payload, dict) else []
    if not blocks and isinstance(payload, dict):
        for page in payload.get("pages", []) or []:
            if isinstance(page, dict):
                blocks.extend(page.get("blocks", []) or [])
    result = [block for block in blocks if isinstance(block, dict)]
    _SOURCE_BLOCK_CACHE[path] = result
    return result


def _block_bbox(block: dict[str, Any]) -> tuple[float, float, float, float] | None:
    raw_bbox = block.get("bounding_box") or block.get("bbox")
    if isinstance(raw_bbox, list) and len(raw_bbox) >= 4 and all(_is_number(value) for value in raw_bbox[:4]):
        left, top, right, bottom = (float(value) for value in raw_bbox[:4])
        return min(left, right), min(top, bottom), max(left, right), max(top, bottom)
    polygon = block.get("polygon") or block.get("points")
    if isinstance(polygon, list) and polygon:
        xs: list[float] = []
        ys: list[float] = []
        for point in polygon:
            if isinstance(point, dict):
                x_value = point.get("x", point.get("X"))
                y_value = point.get("y", point.get("Y"))
            elif isinstance(point, list) and len(point) >= 2:
                x_value, y_value = point[0], point[1]
            else:
                continue
            if _is_number(x_value) and _is_number(y_value):
                xs.append(float(x_value))
                ys.append(float(y_value))
        if xs and ys:
            return min(xs), min(ys), max(xs), max(ys)
    return None


def _is_number(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _normalize_ocr_text(value: str) -> str:
    return (
        value.replace(",", "")
        .replace("，", "")
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
        .strip()
    )


def _normalize_ocr_label(value: str) -> str:
    return (
        _normalize_ocr_text(value)
        .replace(":", "")
        .replace("：", "")
        .replace(";", "")
        .replace("；", "")
    )


def _label_matches(candidate: str, target: str) -> bool:
    if not candidate or not target:
        return False
    if candidate == target or candidate in target:
        return True
    if len(candidate) >= 6 and len(target) >= 6:
        common_prefix = 0
        for left, right in zip(candidate, target):
            if left != right:
                break
            common_prefix += 1
        shorter = min(len(candidate), len(target))
        return common_prefix >= max(6, shorter - 1)
    return False


def _bbox_choice_score(
    bbox: tuple[float, float, float, float],
    label_candidates: list[tuple[float, float, float, float]],
    col_index: int,
) -> tuple[float, float, float]:
    left, top, right, bottom = bbox
    center_x = (left + right) / 2
    center_y = (top + bottom) / 2
    if label_candidates:
        vertical_score = min(abs(center_y - ((label[1] + label[3]) / 2)) for label in label_candidates)
    else:
        vertical_score = 0
    column_penalty = 0.0
    if col_index in {0, 1, 2, 3} and center_x > 720:
        column_penalty = 600.0
    elif col_index >= 4 and center_x < 720:
        column_penalty = 600.0
    row_band_score = int(vertical_score // 12)
    column_order_score = -center_x if col_index in {3, 7} else center_x
    return column_penalty + row_band_score, column_order_score, vertical_score


def _inferred_bbox_y_offset(bbox: tuple[float, float, float, float]) -> float:
    # Paddle table pilot artifacts on the upper half of these report pages are
    # about one text-line above the PDF raster used by the browser preview.
    return 20.0 if bbox[1] < 720 else 0.0


def _bbox_to_json(bbox: tuple[float, float, float, float], *, y_offset: float = 0.0) -> str:
    left, top, right, bottom = bbox
    top = max(0.0, top + y_offset)
    bottom = max(top + 1.0, bottom + y_offset)
    return json.dumps(
        [
            {"x": round(left, 2), "y": round(top, 2)},
            {"x": round(right, 2), "y": round(top, 2)},
            {"x": round(right, 2), "y": round(bottom, 2)},
            {"x": round(left, 2), "y": round(bottom, 2)},
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _is_coarse_bbox_json(bbox_json: str) -> bool:
    bbox = _bbox_from_json(bbox_json)
    if bbox is None:
        return False
    return _is_coarse_bbox(bbox)


def _is_coarse_bbox(bbox: tuple[float, float, float, float]) -> bool:
    left, top, right, bottom = bbox
    return (right - left) >= 700 and (bottom - top) >= 300


def _bbox_from_json(bbox_json: str) -> tuple[float, float, float, float] | None:
    if not bbox_json:
        return None
    try:
        points = json.loads(bbox_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(points, list) or not points:
        return None
    xs: list[float] = []
    ys: list[float] = []
    for point in points:
        if not isinstance(point, dict):
            continue
        x_value = point.get("x", point.get("X"))
        y_value = point.get("y", point.get("Y"))
        if _is_number(x_value) and _is_number(y_value):
            xs.append(float(x_value))
            ys.append(float(y_value))
    if not xs or not ys:
        return None
    return min(xs), min(ys), max(xs), max(ys)


def build_raw_review_sheet(job: JobRecord, selected_item_id: str = "") -> dict[str, Any]:
    items = load_raw_review_items(job)
    if not items:
        return {"items": [], "selected_item": None, "groups": [], "columns": [], "rows": []}
    selected = find_review_item(items, selected_item_id) if selected_item_id else None
    if selected is None:
        selected = items[0]

    def group_key(item: dict[str, Any]) -> tuple[str, str, str, str]:
        return (
            str(item.get("source_pdf_path", "")),
            str(item.get("source_page_no", "")),
            str(item.get("table_id", "")),
            str(item.get("logical_subtable_id", "")),
        )

    selected_key = group_key(selected)
    groups: list[dict[str, Any]] = []
    seen: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        key = group_key(item)
        group = seen.get(key)
        if group is None:
            group = {
                "key": "|".join(key),
                "page_no": key[1],
                "first_item_id": item.get("review_item_id", ""),
                "count": 0,
                "selected": key == selected_key,
            }
            seen[key] = group
            groups.append(group)
        group["count"] = int(group["count"]) + 1
        if key == selected_key:
            group["selected"] = True
    for index, group in enumerate(groups, start=1):
        group["label"] = f"第{group.get('page_no') or '?'}页 表格{index}"
    selected_group_index = next((index for index, group in enumerate(groups) if group.get("selected")), -1)
    next_group = groups[selected_group_index + 1] if 0 <= selected_group_index < len(groups) - 1 else None

    group_items = [item for item in items if group_key(item) == selected_key]
    columns_by_index: dict[int, dict[str, Any]] = {}
    for item in group_items:
        col_index = _safe_int(item.get("col_index"), default=0)
        header = str(item.get("header_path") or item.get("period_role_raw") or item.get("当前条目日期") or f"列{col_index}")
        columns_by_index[col_index] = {"col_index": col_index, "label": header}
    columns = [columns_by_index[key] for key in sorted(columns_by_index)]

    rows_by_index: dict[int, dict[str, Any]] = {}
    for item in group_items:
        row_index = _safe_int(item.get("row_index"), default=0)
        col_index = _safe_int(item.get("col_index"), default=0)
        row = rows_by_index.setdefault(
            row_index,
            {
                "row_index": row_index,
                "label": str(item.get("row_label_clean") or item.get("row_label_raw") or item.get("指标名") or f"第{row_index}行"),
                "cells": {},
            },
        )
        row["cells"][col_index] = item

    return {
        "items": items,
        "selected_item": selected,
        "groups": groups,
        "next_item_id": next_group.get("first_item_id", "") if next_group else "",
        "has_next_group": next_group is not None,
        "columns": columns,
        "rows": [rows_by_index[key] for key in sorted(rows_by_index)],
    }


def _safe_int(value: Any, *, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def load_mapping_review_items(job: JobRecord) -> list[dict[str, Any]]:
    state = load_simple_flow_state(job)
    standard_csv = Path(str(state.get("standardized_metrics_csv", "") or ""))
    if not standard_csv.exists():
        return []
    review_csv = standard_csv.parent / "mapping_review_items.csv"
    if not review_csv.exists():
        return []
    return _enrich_mapping_review_items(job, read_csv_rows(review_csv))


def build_mapping_review_sheet(job: JobRecord, selected_item_id: str = "") -> dict[str, Any]:
    items = load_mapping_review_items(job)
    if not items:
        return {"items": [], "selected_item": None}
    selected = find_review_item(items, selected_item_id) if selected_item_id else None
    if selected is None:
        selected = items[0]
    for item in items:
        item["selected"] = str(item.get("review_item_id", "")) == str(selected.get("review_item_id", ""))
    return {"items": items, "selected_item": selected}


def _enrich_mapping_review_items(job: JobRecord, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    detailed_lookup = _load_raw_detailed_lookup(job)
    detailed_rows = _unique_detailed_rows(detailed_lookup.values())
    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        raw_metric_id = str(item.get("raw_metric_id", "") or "")
        detailed = detailed_lookup.get(raw_metric_id, {})
        original_name = str(item.get("原始指标名") or item.get("original_metric_name") or detailed.get("row_label_clean") or detailed.get("metric_name") or "")
        mapping_method = str(item.get("mapping_method") or item.get("映射方法") or "")
        mapping_status = str(item.get("mapping_status", "") or "")
        is_unmapped_without_suggestion = mapping_status == "unmapped" and mapping_method == "none"
        current_code = "" if is_unmapped_without_suggestion else str(item.get("candidate_code") or item.get("标准指标编码") or "")
        current_name = "" if is_unmapped_without_suggestion else str(item.get("candidate_name") or item.get("标准指标名称") or "")
        is_ai_suggestion = mapping_method == "llm_suggested" or bool(item.get("ai_suggestion_code"))
        mapping_status_code = "llm_suggested" if is_ai_suggestion else mapping_status
        mapping_confidence = "" if is_unmapped_without_suggestion else str(item.get("mapping_confidence") or item.get("candidate_score") or item.get("映射置信度") or "")
        system_candidate_label = "" if is_unmapped_without_suggestion else _mapping_label(item.get("system_candidate_code", ""), item.get("system_candidate_name", ""))
        show_decision_actions, decision_note = _mapping_decision_action_state(item, mapping_status=mapping_status, mapping_method=mapping_method)
        source_page_no = str(item.get("source_page_no") or detailed.get("page_no") or "")
        source_pdf_path = str(item.get("source_pdf_path") or detailed.get("evidence_path") or "")
        value_bbox_json = str(item.get("source_bbox_json") or detailed.get("bbox_json") or "")
        term_bbox_json = _resolve_mapping_term_bbox_json(item, detailed, original_name) or ""
        if _is_coarse_bbox_json(term_bbox_json):
            term_bbox_json = ""
        if not term_bbox_json:
            term_bbox_json = _resolve_peer_review_bbox_json(detailed_rows, detailed, original_name)
        if _is_coarse_bbox_json(value_bbox_json):
            value_bbox_json = _resolve_source_table_cell_bbox_json(detailed)
        if not value_bbox_json:
            value_bbox_json = _resolve_source_table_cell_bbox_json(detailed)
        display_bbox_json = term_bbox_json or value_bbox_json
        item.update(
            {
                "original_metric_name": original_name,
                "current_code": current_code,
                "current_name": current_name,
                "current_mapping_label": f"{current_code} {current_name}".strip(),
                "mapping_status_code": mapping_status_code,
                "mapping_status_label": _mapping_status_label(mapping_status, mapping_method=mapping_method, has_ai_suggestion=is_ai_suggestion),
                "mapping_confidence": mapping_confidence,
                "relation_type": str(item.get("relation_type") or item.get("口径关系") or ""),
                "relation_note": str(item.get("issue_reason") or item.get("口径说明") or ""),
                "system_candidate_label": system_candidate_label,
                "ai_suggestion_label": _mapping_label(item.get("ai_suggestion_code", ""), item.get("ai_suggestion_name", "")),
                "ai_confidence_display": _format_percent(item.get("ai_confidence", "")),
                "ai_reason": str(item.get("ai_reason") or ""),
                "ai_relation_type": str(item.get("ai_relation_type") or ""),
                "ai_validation_status": str(item.get("ai_validation_status") or ""),
                "show_mapping_decision_actions": show_decision_actions,
                "mapping_decision_note": decision_note,
                "source_page_no": source_page_no,
                "source_pdf_path": source_pdf_path,
                "source_file": str(detailed.get("source_file") or item.get("source_file") or ""),
                "source_term_bbox_json": display_bbox_json,
                "source_value_bbox_json": value_bbox_json,
                "bbox_state": "has-bbox" if display_bbox_json else "missing-bbox",
            }
        )
        enriched.append(item)
    return enriched


def _mapping_label(code: Any, name: Any) -> str:
    return f"{str(code or '').strip()} {str(name or '').strip()}".strip()


def _format_percent(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if number <= 1:
        number *= 100
    return f"{number:.0f}%" if abs(number - round(number)) < 0.01 else f"{number:.1f}%"


def _mapping_decision_action_state(item: dict[str, Any], *, mapping_status: str, mapping_method: str) -> tuple[bool, str]:
    decision = str(item.get("mapping_decision", "") or "").strip()
    normalized_method = str(mapping_method or "").strip()
    normalized_status = str(mapping_status or "").strip()
    if decision == "accept_once" or normalized_method == "manual_once":
        return False, "已本次采用"
    if decision == "accept_and_remember" or normalized_method == "manual_saved":
        return False, "已采用并记住"
    if normalized_method == "exact":
        return False, "精确匹配，无需决策"
    if normalized_method == "legacy_alias":
        return False, "旧术语匹配，无需决策"
    if normalized_status == "unmapped" and normalized_method == "none":
        return False, "未找到可采纳映射"
    return True, ""


def _load_raw_detailed_lookup(job: JobRecord) -> dict[str, dict[str, Any]]:
    state = load_simple_flow_state(job)
    raw_csv = Path(str(state.get("raw_metrics_csv", "") or ""))
    detailed_rows = read_csv_rows(raw_csv.parent / "raw_metrics_detailed.csv") if raw_csv.exists() else []
    lookup: dict[str, dict[str, Any]] = {}
    for row in detailed_rows:
        for key in (row.get("source_cell_ref", ""), row.get("raw_metric_id", "")):
            key_text = str(key or "").strip()
            if key_text:
                lookup[key_text] = row
    return lookup


def _unique_detailed_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in rows:
        identity = id(row)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(row)
    return unique


def _mapping_status_label(status: str, *, mapping_method: str = "", has_ai_suggestion: bool = False) -> str:
    if mapping_method == "llm_suggested" or has_ai_suggestion:
        return "AI建议"
    return {
        "mapped": "已映射",
        "review_required": "建议校对",
        "unmapped": "未映射",
        "changed": "已修改",
        "skipped": "已跳过",
        "approved": "已通过",
        "llm_suggested": "AI建议",
    }.get(status, status or "未记录")


def _resolve_mapping_term_bbox_json(item: dict[str, Any], detailed: dict[str, Any], original_name: str) -> str:
    source_file = str(detailed.get("source_file", "") or item.get("source_file", "") or "").strip()
    if not source_file or not original_name:
        return ""
    cells = _load_source_table_cells(source_file)
    if not cells:
        return ""
    row_index = _safe_int(detailed.get("row_index") or _row_index_from_source_ref(str(item.get("raw_metric_id", ""))), default=-1)
    table_id = str(detailed.get("table_id") or _table_id_from_source_ref(str(item.get("raw_metric_id", ""))) or "")
    col_index = _safe_int(detailed.get("col_index"), default=-1)
    label_key = _normalize_ocr_label(original_name)
    row_cells = [
        cell
        for cell in cells
        if _cell_row_matches(cell, row_index)
        and (not table_id or str(cell.get("tableId", cell.get("table_id", ""))) == table_id)
    ]
    exact_candidates = [
        cell
        for cell in row_cells
        if label_key and _label_matches(_normalize_ocr_label(str(cell.get("word", cell.get("text", "")))), label_key)
    ]
    if exact_candidates:
        chosen = min(exact_candidates, key=lambda cell: _label_cell_score(cell, col_index))
        return _cell_bbox_to_json(chosen, source_file=source_file)

    heuristic_col = 4 if col_index >= 6 else 0
    heuristic_candidates = [
        cell
        for cell in row_cells
        if _safe_int(cell.get("xsc", cell.get("col_start")), default=-1) == heuristic_col
        and str(cell.get("word", cell.get("text", ""))).strip()
    ]
    if heuristic_candidates:
        return _cell_bbox_to_json(heuristic_candidates[0], source_file=source_file)
    return ""


def _resolve_source_table_cell_bbox_json(detailed: dict[str, Any]) -> str:
    source_file = str(detailed.get("source_file", "") or "").strip()
    if not source_file:
        return ""
    row_index = _safe_int(detailed.get("row_index"), default=-1)
    col_index = _safe_int(detailed.get("col_index"), default=-1)
    if row_index < 0 or col_index < 0:
        return ""
    table_id = str(detailed.get("table_id", "") or "")
    cells = _load_source_table_cells(source_file)
    candidates = [
        cell
        for cell in cells
        if _cell_row_matches(cell, row_index)
        and _cell_col_matches(cell, col_index)
        and (not table_id or str(cell.get("tableId", cell.get("table_id", ""))) == table_id)
    ]
    if not candidates:
        return ""
    chosen = min(candidates, key=_cell_span_score)
    bbox_json = _cell_bbox_to_json(chosen, source_file=source_file)
    return "" if _is_coarse_bbox_json(bbox_json) else bbox_json


def _resolve_peer_review_bbox_json(
    detailed_rows: Iterable[dict[str, Any]],
    detailed: dict[str, Any],
    original_name: str,
) -> str:
    label_bbox = _resolve_peer_term_bbox_json(detailed_rows, detailed, original_name)
    if label_bbox:
        return label_bbox

    page_no = str(detailed.get("page_no", "") or "")
    col_index = str(detailed.get("col_index", "") or "")
    label_key = _normalize_ocr_label(original_name)
    for candidate in detailed_rows:
        if str(candidate.get("page_no", "") or "") != page_no:
            continue
        if str(candidate.get("col_index", "") or "") != col_index:
            continue
        candidate_label = str(candidate.get("row_label_clean", "") or candidate.get("metric_name", "") or "")
        if label_key and not _label_matches(_normalize_ocr_label(candidate_label), label_key):
            continue
        bbox_json = str(candidate.get("bbox_json", "") or "")
        if bbox_json and not _is_coarse_bbox_json(bbox_json):
            return bbox_json
    return ""


def _resolve_peer_term_bbox_json(
    detailed_rows: Iterable[dict[str, Any]],
    detailed: dict[str, Any],
    original_name: str,
) -> str:
    page_no = str(detailed.get("page_no", "") or "")
    if not page_no or not original_name:
        return ""
    row_index = _safe_int(detailed.get("row_index"), default=-1)
    col_index = _safe_int(detailed.get("col_index"), default=-1)
    label_key = _normalize_ocr_label(original_name)
    source_files: list[str] = []
    seen_sources: set[str] = set()
    for candidate in detailed_rows:
        if str(candidate.get("page_no", "") or "") != page_no:
            continue
        source_file = str(candidate.get("source_file", "") or "").strip()
        if not source_file or source_file in seen_sources:
            continue
        seen_sources.add(source_file)
        source_files.append(source_file)

    def source_priority(source_file: str) -> tuple[int, str]:
        lowered = source_file.lower()
        if "aliyun_table" in lowered:
            return (0, source_file)
        if "tencent_table_v3" in lowered:
            return (2, source_file)
        return (1, source_file)

    for source_file in sorted(source_files, key=source_priority):
        cells = _load_source_table_cells(source_file)
        if not cells:
            continue
        row_cells = [cell for cell in cells if _cell_row_matches(cell, row_index)]
        exact_candidates = [
            cell
            for cell in row_cells
            if label_key and _label_matches(_normalize_ocr_label(str(cell.get("word", cell.get("text", "")))), label_key)
        ]
        if not exact_candidates:
            continue
        chosen = min(exact_candidates, key=lambda cell: _label_cell_score(cell, col_index))
        bbox_json = _cell_bbox_to_json(chosen, source_file=source_file)
        if bbox_json and not _is_coarse_bbox_json(bbox_json):
            return bbox_json
    return ""


def _load_source_table_cells(source_file: str) -> list[dict[str, Any]]:
    path = Path(source_file)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    cached = _SOURCE_TABLE_CELL_CACHE.get(path)
    if cached is not None:
        return cached
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _SOURCE_TABLE_CELL_CACHE[path] = []
        return []
    data = payload.get("Data", payload) if isinstance(payload, dict) else {}
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            data = {}
    tables = data.get("prism_tablesInfo", []) if isinstance(data, dict) else []
    cells: list[dict[str, Any]] = []
    for table in tables:
        if not isinstance(table, dict):
            continue
        table_id = str(table.get("tableId", table.get("table_id", "")))
        for cell in table.get("cellInfos", []) or []:
            if isinstance(cell, dict):
                normalized = dict(cell)
                normalized.setdefault("tableId", table_id)
                cells.append(normalized)
    for table_index, table in enumerate(payload.get("TableDetections", []) if isinstance(payload, dict) else [], start=1):
        if not isinstance(table, dict):
            continue
        table_id = str(table_index)
        table_bbox = table.get("TableCoordPoint") or table.get("Polygon") or table.get("CoordPoint")
        table_cells: list[dict[str, Any]] = []
        for cell in table.get("Cells", []) or []:
            if not isinstance(cell, dict):
                continue
            row_tl = _safe_int(cell.get("RowTl"), default=-1)
            col_tl = _safe_int(cell.get("ColTl"), default=-1)
            if row_tl < 0 or col_tl < 0:
                continue
            row_start, row_end = _normalize_exclusive_grid_range(cell.get("RowTl"), cell.get("RowBr"))
            col_start, col_end = _normalize_exclusive_grid_range(cell.get("ColTl"), cell.get("ColBr"))
            normalized = dict(cell)
            normalized["tableId"] = table_id
            normalized["ysc"] = row_start
            normalized["yec"] = row_end
            normalized["xsc"] = col_start
            normalized["xec"] = col_end
            normalized["word"] = str(cell.get("Text", "") or "")
            normalized["text"] = str(cell.get("Text", "") or "")
            normalized["polygon"] = cell.get("Polygon") or cell.get("CoordPoint")
            normalized["table_bbox"] = table_bbox
            table_cells.append(normalized)
        if not table_cells:
            continue
        grid_row_count = max(_safe_int(cell.get("yec"), default=0) for cell in table_cells) + 1
        grid_col_count = max(_safe_int(cell.get("xec"), default=0) for cell in table_cells) + 1
        for cell in table_cells:
            cell["grid_row_count"] = grid_row_count
            cell["grid_col_count"] = grid_col_count
        cells.extend(table_cells)
    _SOURCE_TABLE_CELL_CACHE[path] = cells
    return cells


def _row_index_from_source_ref(source_ref: str) -> str:
    parts = str(source_ref or "").split(":")
    if len(parts) < 5:
        return ""
    row_part = parts[4].split("-")[0]
    return row_part.strip()


def _table_id_from_source_ref(source_ref: str) -> str:
    parts = str(source_ref or "").split(":")
    return parts[3].strip() if len(parts) >= 4 else ""


def _cell_row_matches(cell: dict[str, Any], row_index: int) -> bool:
    if row_index < 0:
        return False
    row_start = _safe_int(cell.get("ysc", cell.get("row_start")), default=-1)
    row_end = _safe_int(cell.get("yec", cell.get("row_end")), default=row_start)
    return row_start <= row_index <= row_end


def _cell_col_matches(cell: dict[str, Any], col_index: int) -> bool:
    if col_index < 0:
        return False
    col_start = _safe_int(cell.get("xsc", cell.get("col_start")), default=-1)
    col_end = _safe_int(cell.get("xec", cell.get("col_end")), default=col_start)
    return col_start <= col_index <= col_end


def _cell_span_score(cell: dict[str, Any]) -> tuple[int, int]:
    row_start = _safe_int(cell.get("ysc", cell.get("row_start")), default=0)
    row_end = _safe_int(cell.get("yec", cell.get("row_end")), default=row_start)
    col_start = _safe_int(cell.get("xsc", cell.get("col_start")), default=0)
    col_end = _safe_int(cell.get("xec", cell.get("col_end")), default=col_start)
    return (row_end - row_start) + (col_end - col_start), col_start


def _label_cell_score(cell: dict[str, Any], value_col_index: int) -> tuple[int, int]:
    col = _safe_int(cell.get("xsc", cell.get("col_start")), default=0)
    preferred_col = 4 if value_col_index >= 6 else 0
    return abs(col - preferred_col), col


def _cell_bbox_to_json(cell: dict[str, Any], *, source_file: str = "") -> str:
    bbox = _cell_bbox(cell)
    table_bbox = _bbox_from_raw_points(cell.get("table_bbox"))
    if bbox is None or _is_coarse_bbox(bbox) or (table_bbox is not None and _same_bbox(bbox, table_bbox)):
        bbox = _infer_cell_bbox_from_table_grid(cell)
    bbox_json = _bbox_to_json(bbox) if bbox else ""
    return bbox_json


def _cell_bbox(cell: dict[str, Any]) -> tuple[float, float, float, float] | None:
    raw_bbox = cell.get("pos") or cell.get("polygon") or cell.get("points") or cell.get("bbox")
    return _bbox_from_raw_points(raw_bbox)


def _bbox_from_raw_points(raw_bbox: Any) -> tuple[float, float, float, float] | None:
    if not isinstance(raw_bbox, list) or not raw_bbox:
        return None
    if len(raw_bbox) >= 4 and all(_is_number(value) for value in raw_bbox[:4]):
        left, top, right, bottom = (float(value) for value in raw_bbox[:4])
        return min(left, right), min(top, bottom), max(left, right), max(top, bottom)
    xs: list[float] = []
    ys: list[float] = []
    for point in raw_bbox:
        if isinstance(point, dict):
            x_value = point.get("x", point.get("X"))
            y_value = point.get("y", point.get("Y"))
        elif isinstance(point, list) and len(point) >= 2:
            x_value, y_value = point[0], point[1]
        else:
            continue
        if _is_number(x_value) and _is_number(y_value):
            xs.append(float(x_value))
            ys.append(float(y_value))
    if not xs or not ys:
        return None
    return min(xs), min(ys), max(xs), max(ys)


def _same_bbox(left_bbox: tuple[float, float, float, float] | None, right_bbox: tuple[float, float, float, float] | None) -> bool:
    if left_bbox is None or right_bbox is None:
        return False
    return all(abs(left - right) <= 0.01 for left, right in zip(left_bbox, right_bbox))


def _infer_cell_bbox_from_table_grid(cell: dict[str, Any]) -> tuple[float, float, float, float] | None:
    table_bbox = _bbox_from_raw_points(cell.get("table_bbox"))
    if table_bbox is None:
        return None
    row_start = _safe_int(cell.get("ysc", cell.get("row_start")), default=-1)
    row_end = _safe_int(cell.get("yec", cell.get("row_end")), default=row_start)
    col_start = _safe_int(cell.get("xsc", cell.get("col_start")), default=-1)
    col_end = _safe_int(cell.get("xec", cell.get("col_end")), default=col_start)
    row_count = _safe_int(cell.get("grid_row_count"), default=0)
    col_count = _safe_int(cell.get("grid_col_count"), default=0)
    if row_start < 0 or col_start < 0 or row_count <= 0 or col_count <= 0:
        return None
    left, top, right, bottom = table_bbox
    col_width = (right - left) / col_count
    row_height = (bottom - top) / row_count
    return (
        left + (col_start * col_width),
        top + (row_start * row_height),
        left + ((col_end + 1) * col_width),
        top + ((row_end + 1) * row_height),
    )


def _normalize_exclusive_grid_range(start_raw: Any, end_raw: Any) -> tuple[int, int]:
    start = _safe_int(start_raw, default=0)
    end_exclusive = _safe_int(end_raw, default=start + 1)
    normalized_start = max(0, start)
    normalized_end = max(normalized_start, end_exclusive - 1)
    return normalized_start, normalized_end


def source_preview_rotation_degrees(item: dict[str, Any]) -> int:
    source_file = str(item.get("source_file", "") or "").strip()
    if not source_file:
        return 0
    angle = _safe_int(_load_source_page_metadata(source_file).get("angle"), default=0) % 360
    if angle == 90:
        return -90
    if angle == 270:
        return 90
    if angle == 180:
        return 180
    return 0


def _load_source_page_metadata(source_file: str) -> dict[str, Any]:
    path = Path(source_file)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    cached = _SOURCE_PAGE_METADATA_CACHE.get(path)
    if cached is not None:
        return cached
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _SOURCE_PAGE_METADATA_CACHE[path] = {}
        return {}
    top_level_angle = payload.get("Angle") if isinstance(payload, dict) else None
    data = payload.get("Data", payload) if isinstance(payload, dict) else {}
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            data = {}
    if not isinstance(data, dict):
        data = {}
    angle = data.get("angle", 0)
    if not angle and top_level_angle is not None:
        angle = _normalize_tencent_source_angle(top_level_angle)
    metadata = {
        "angle": angle,
        "width": data.get("width", data.get("Width", 0)),
        "height": data.get("height", data.get("Height", 0)),
        "orgWidth": data.get("orgWidth", data.get("originalWidth", 0)),
        "orgHeight": data.get("orgHeight", data.get("originalHeight", 0)),
    }
    _SOURCE_PAGE_METADATA_CACHE[path] = metadata
    return metadata


def _normalize_tencent_source_angle(value: Any) -> int:
    angle = _safe_int(value, default=0)
    if abs(angle) >= 100:
        angle = int(round(angle / 100))
    angle %= 360
    return (-angle) % 360


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def find_review_item(items: list[dict[str, Any]], item_id: str) -> dict[str, Any] | None:
    for item in items:
        if str(item.get("review_item_id", "")) == item_id:
            return item
    return None


def save_raw_review_action(job: JobRecord, *, item: dict[str, Any], action: str, edits: dict[str, Any], reviewer_note: str = "") -> dict[str, Any]:
    if action not in RAW_REVIEW_ACTIONS:
        raise ValueError(f"不支持的原始数据校对动作: {action}")
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "review_item_id": item.get("review_item_id", ""),
        "raw_metric_id": item.get("raw_metric_id", ""),
        "action": action,
        "reviewer_note": reviewer_note,
        "edits": edits,
    }
    _append_action(raw_review_dir(job), "raw_review_actions", payload)
    return payload


def save_mapping_review_action(
    job: JobRecord,
    *,
    item: dict[str, Any],
    action: str,
    selected_code: str = "",
    selected_name: str = "",
    reviewer_note: str = "",
    mapping_store_path: str | Path | None = None,
    decided_by: str = "web",
    persist_decision: bool = True,
) -> dict[str, Any]:
    if action not in MAPPING_REVIEW_ACTIONS:
        raise ValueError(f"不支持的术语映射校对动作: {action}")
    decision = _mapping_action_to_decision(action)
    previous_code = str(item.get("candidate_code") or item.get("current_code") or item.get("标准指标编码") or "")
    previous_name = str(item.get("candidate_name") or item.get("current_name") or item.get("标准指标名称") or "")
    if decision in {"accept_once", "accept_and_remember"} and not selected_code:
        selected_code = previous_code
        selected_name = selected_name or previous_name
    if selected_code and not selected_name:
        selected_name = previous_name
    raw_metric_name = str(item.get("原始指标名", "") or item.get("original_metric_name", ""))
    confidence = _safe_float(item.get("mapping_confidence") or item.get("candidate_score") or item.get("映射置信度"))
    relation_type = str(item.get("relation_type") or item.get("口径关系") or "")
    if decision in {"accept_once", "accept_and_remember"} and not relation_type:
        relation_type = "exact_alias"
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "review_item_id": item.get("review_item_id", ""),
        "raw_metric_id": item.get("raw_metric_id", ""),
        "original_metric_name": raw_metric_name,
        "previous_code": previous_code,
        "previous_name": previous_name,
        "action": action,
        "decision": decision,
        "selected_code": selected_code,
        "selected_name": selected_name,
        "relation_type": relation_type,
        "confidence": confidence if confidence is not None else "",
        "reviewer_note": reviewer_note,
    }
    _append_action(mapping_review_dir(job), "mapping_review_actions", payload)
    if not persist_decision:
        return payload
    store = LocalMappingStore(mapping_store_path)
    decision_payload = store.record_decision(
        job_id=job.job_id,
        doc_id=job.job_id,
        raw_metric_id=str(item.get("raw_metric_id", "") or ""),
        raw_metric_name=raw_metric_name,
        suggested_code=previous_code,
        suggested_name=previous_name,
        decision=decision,
        final_code=selected_code if decision != "reject" else "",
        final_name=selected_name if decision != "reject" else "",
        relation_type=relation_type,
        confidence=confidence,
        decided_by=decided_by,
        note=reviewer_note,
    )
    append_mapping_decision_file(mapping_review_dir(job), decision_payload)
    state = load_simple_flow_state(job)
    standard_csv = Path(str(state.get("standardized_metrics_csv", "") or ""))
    if standard_csv.exists():
        output_dir = standard_csv.parent
        apply_mapping_decision_to_output(output_dir, decision_payload)
        append_mapping_decision_file(output_dir, decision_payload)
        store.write_snapshot(output_dir / "mapping_store_snapshot.yml")
    store.export_aliases(Path(mapping_store_path).resolve().parent / "local_aliases_export.yml" if mapping_store_path else None)
    store.export_decision_audit(Path(mapping_store_path).resolve().parent / "mapping_decisions_audit.csv" if mapping_store_path else None)
    return payload


def _mapping_action_to_decision(action: str) -> str:
    return {
        "approve_mapping": "accept_once",
        "change_mapping": "accept_once",
        "skip_mapping": "reject",
    }.get(action, action)


def _safe_float(value: Any) -> float | None:
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _append_action(target_dir: Path, basename: str, payload: dict[str, Any]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / f"{basename}.json"
    csv_path = target_dir / f"{basename}.csv"
    actions = []
    if json_path.exists():
        try:
            parsed = json.loads(json_path.read_text(encoding="utf-8"))
            actions = parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            actions = []
    actions.append(payload)
    write_json(json_path, actions)

    fieldnames = sorted({key for action in actions for key in action.keys()})
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for action in actions:
            writer.writerow({field: _serialize_action_value(action.get(field, "")) for field in fieldnames})


def _serialize_action_value(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "" if value is None else str(value)


def resolve_safe_source_file(settings: WebAppSettings, job: JobRecord, raw_path: str) -> Path:
    if not raw_path:
        raise HTTPException(status_code=404, detail="未记录原始文件路径。")
    path = Path(str(raw_path))
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    allowed_roots = [
        settings.corpus_root.resolve(),
        settings.uploads_root.resolve(),
        settings.jobs_root.resolve(),
    ]
    if job.source_image_dir:
        allowed_roots.append(Path(job.source_image_dir).resolve())
    if not any(_is_within(path, root) for root in allowed_roots):
        raise HTTPException(status_code=400, detail="原始文件路径不在允许目录内。")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="原始文件不存在。")
    return path


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False
