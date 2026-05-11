from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .models import JobRecord
from .simple_flow import (
    find_review_item,
    job_root,
    load_mapping_review_items,
    load_raw_review_items,
    load_simple_flow_state,
    read_csv_rows,
    save_mapping_review_action,
    save_raw_review_action,
    write_json,
)


UNIFIED_REVIEW_ACTION_HEADERS = [
    "item_id",
    "raw_metric_id",
    "original_metric_name",
    "edit_type",
    "previous_value",
    "new_value",
    "previous_code",
    "previous_name",
    "new_code",
    "new_name",
    "source_page_no",
    "source_bbox_json",
    "reviewer_name",
    "created_at",
]

STATUS_LABELS_ZH = {
    "exact": "精确匹配",
    "alias": "别名匹配",
    "legacy_alias": "旧术语匹配",
    "mapped": "已映射",
    "manual": "已映射",
    "candidate": "建议校对",
    "relation_review": "建议校对",
    "review_required": "建议校对",
    "unmapped": "未映射",
    "changed": "术语已修改",
    "value_changed": "数值已修改",
    "term_changed": "术语已修改",
    "skipped": "已跳过",
    "none": "未映射",
}

PROVIDER_LABELS_ZH = {
    "aliyun": "阿里云",
    "aliyun_table": "阿里云",
    "aliyun_text": "阿里云",
    "tencent": "腾讯",
    "tencent_table_v3": "腾讯",
    "tencent_text": "腾讯",
    "paddle": "Paddle",
    "paddle_table_local": "Paddle",
}

_METRIC_NUMBER_RE = re.compile(r"^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))$")


def unified_review_dir(job: JobRecord) -> Path:
    return job_root(job) / "unified_review"


def unified_review_results_dir(job: JobRecord) -> Path:
    return Path(job.result_dir).resolve() / "unified_review"


def format_metric_number(value: object, *, value_type: str = "") -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    if "%" in text or str(value_type or "").strip().lower() in {"ratio", "percentage", "percent"}:
        return text
    normalized = text.replace(",", "").replace("，", "")
    match = _METRIC_NUMBER_RE.match(normalized)
    if not match:
        return text
    sign, int_part, frac_part, leading_frac = match.groups()
    if leading_frac is not None:
        int_part = "0"
        frac_part = leading_frac
    int_part = int_part or "0"
    grouped = f"{int(int_part):,}"
    if frac_part is None:
        return f"{sign}{grouped}"
    return f"{sign}{grouped}.{frac_part}"


def parse_metric_number_input(value: object, *, value_type: str = "") -> dict[str, Any]:
    text = str(value if value is not None else "").strip()
    if not text:
        return {"valid": False, "value": "", "reason": "empty"}
    if "%" in text or str(value_type or "").strip().lower() in {"ratio", "percentage", "percent"}:
        return {"valid": False, "value": text, "reason": "ratio_or_percent_requires_explicit_handling"}
    normalized = text.replace(",", "").replace("，", "").replace(" ", "").replace("\t", "")
    match = _METRIC_NUMBER_RE.match(normalized)
    if not match:
        return {"valid": False, "value": text, "reason": "invalid_number"}
    sign, int_part, frac_part, leading_frac = match.groups()
    if leading_frac is not None:
        return {"valid": True, "value": f"{sign}0.{leading_frac}", "reason": ""}
    int_part = str(int(int_part or "0"))
    if frac_part is None:
        return {"valid": True, "value": f"{sign}{int_part}", "reason": ""}
    return {"valid": True, "value": f"{sign}{int_part}.{frac_part}", "reason": ""}


def build_unified_review_sheet(job: JobRecord, selected_item_id: str = "") -> dict[str, Any]:
    items = load_unified_review_items(job)
    if not items:
        return {
            "items": [],
            "selected_item": None,
            "confidence_available_total": 0,
            "confidence_missing_total": 0,
            "message": "请先提取原始数据。",
        }
    selected = find_review_item(items, selected_item_id) if selected_item_id else None
    if selected is None:
        selected = items[0]
    selected_id = str(selected.get("review_item_id", ""))
    for item in items:
        item["selected"] = str(item.get("review_item_id", "")) == selected_id
    return {
        "items": items,
        "selected_item": selected,
        "confidence_available_total": sum(1 for item in items if item.get("confidence_available")),
        "confidence_missing_total": sum(1 for item in items if not item.get("confidence_available")),
        "message": "",
    }


def load_unified_review_items(job: JobRecord) -> list[dict[str, Any]]:
    raw_items = load_raw_review_items(job, apply_actions=False)
    if not raw_items:
        return []
    mapping_lookup = _mapping_lookup(job)
    standardized_lookup = _standardized_lookup(job)
    saved_actions = _load_unified_review_actions(job)
    value_overrides, mapping_overrides = _latest_unified_overrides(saved_actions)

    items: list[dict[str, Any]] = []
    last_section_key = ""
    for index, raw in enumerate(raw_items, start=1):
        raw_metric_id = str(raw.get("raw_metric_id", "") or "")
        mapping = mapping_lookup.get(raw_metric_id) or standardized_lookup.get(raw_metric_id) or {}
        original_value = str(raw.get("指标数值", "") or "")
        value_type = str(raw.get("value_type", "") or "")
        value_override = value_overrides.get(raw_metric_id, {})
        current_value = str(value_override.get("new_value", original_value) or "")
        parsed_current = parse_metric_number_input(current_value, value_type=value_type)
        current_value_normalized = str(parsed_current.get("value") if parsed_current.get("valid") else current_value)

        original_code = str(mapping.get("current_code") or mapping.get("标准指标编码") or mapping.get("candidate_code") or "")
        original_name = str(mapping.get("current_name") or mapping.get("标准指标名称") or mapping.get("candidate_name") or "")
        mapping_override = mapping_overrides.get(raw_metric_id, {})
        current_code = str(mapping_override.get("new_code", original_code) or "")
        current_name = str(mapping_override.get("new_name", original_name) or "")
        mapping_changed = bool(mapping_override) and (current_code != original_code or current_name != original_name)
        value_changed = bool(value_override) and str(current_value_normalized) != str(parse_metric_number_input(original_value, value_type=value_type).get("value", original_value))

        mapping_status = str(mapping.get("mapping_status") or mapping.get("映射状态") or ("mapped" if current_code else "unmapped"))
        mapping_method = str(mapping.get("mapping_method") or mapping.get("映射方法") or "")
        base_status_code, base_status_label = _status_label(mapping_status, mapping_method)
        status_badges = _status_badges(
            base_status_code=base_status_code,
            base_status_label=base_status_label,
            value_changed=value_changed,
            mapping_changed=mapping_changed,
        )

        provider = str(raw.get("provider") or mapping.get("provider") or "")
        confidence_raw = str(raw.get("confidence", "") or mapping.get("confidence", "") or "").strip()
        confidence_display = _format_confidence(provider, confidence_raw)
        source_term_bbox_json = str(mapping.get("source_term_bbox_json") or "")
        source_value_bbox_json = str(mapping.get("source_value_bbox_json") or raw.get("source_bbox_json") or mapping.get("source_bbox_json") or "")
        section_key = "|".join(
            [
                str(raw.get("source_page_no", "") or mapping.get("source_page_no", "")),
                str(raw.get("table_id", "")),
                str(raw.get("logical_subtable_id", "")),
            ]
        )
        section_label = _section_label(raw, index)
        starts_section = section_key != last_section_key
        last_section_key = section_key

        item = {
            "review_item_id": f"unirev_{index:06d}",
            "raw_review_item_id": raw.get("review_item_id", ""),
            "mapping_review_item_id": mapping.get("review_item_id", ""),
            "raw_metric_id": raw_metric_id,
            "original_metric_name": str(raw.get("指标名") or raw.get("row_label_clean") or mapping.get("original_metric_name") or ""),
            "item_date": raw.get("当前条目日期", ""),
            "fill_date": raw.get("填表日期", ""),
            "company_name": raw.get("公司名", ""),
            "original_value": original_value,
            "original_value_normalized": str(parse_metric_number_input(original_value, value_type=value_type).get("value", original_value)),
            "current_value": current_value_normalized,
            "display_value": format_metric_number(current_value_normalized, value_type=value_type),
            "value_type": value_type,
            "original_code": original_code,
            "original_name": original_name,
            "current_code": current_code,
            "current_name": current_name,
            "current_mapping_label": f"{current_code} {current_name}".strip(),
            "original_mapping_label": f"{original_code} {original_name}".strip(),
            "mapping_status": status_badges[0]["code"],
            "mapping_status_label": status_badges[0]["label"],
            "base_status": base_status_code,
            "base_status_label": base_status_label,
            "status_badges": status_badges,
            "value_changed": value_changed,
            "mapping_changed": mapping_changed,
            "source_page_no": raw.get("source_page_no") or mapping.get("source_page_no") or "",
            "source_pdf_path": raw.get("source_pdf_path") or mapping.get("source_pdf_path") or "",
            "source_file": raw.get("source_file") or mapping.get("source_file") or "",
            "provider": provider,
            "source_term_bbox_json": source_term_bbox_json,
            "source_value_bbox_json": source_value_bbox_json,
            "source_bbox_json": source_term_bbox_json or source_value_bbox_json,
            "bbox_state": "has-bbox" if (source_term_bbox_json or source_value_bbox_json) else "missing-bbox",
            "confidence": confidence_raw,
            "confidence_display": confidence_display or "未记录",
            "confidence_available": bool(confidence_display),
            "section_key": section_key,
            "section_label": section_label,
            "starts_section": starts_section,
        }
        items.append(item)
    return items


def save_unified_review_actions(job: JobRecord, edits: Iterable[dict[str, Any]], *, reviewer_name: str = "") -> dict[str, Any]:
    items = load_unified_review_items(job)
    by_item_id = {str(item.get("review_item_id", "")): item for item in items}
    by_raw_id = {str(item.get("raw_metric_id", "")): item for item in items}
    created_at = datetime.now(timezone.utc).isoformat()
    new_actions: list[dict[str, Any]] = []
    for edit in edits:
        if not isinstance(edit, dict):
            continue
        item = by_item_id.get(str(edit.get("item_id", "") or ""))
        if item is None:
            item = by_raw_id.get(str(edit.get("raw_metric_id", "") or ""))
        if item is None:
            raise ValueError("校对项不存在。")
        edit_type = str(edit.get("edit_type", "") or "").strip()
        if edit_type not in {"value_change", "mapping_change", "reset_value", "reset_mapping"}:
            raise ValueError(f"不支持的统一校对动作: {edit_type}")
        action = _build_action_row(item, edit, edit_type=edit_type, reviewer_name=reviewer_name, created_at=created_at)
        if edit_type in {"value_change", "reset_value"}:
            parsed = parse_metric_number_input(action["new_value"], value_type=str(item.get("value_type", "")))
            if not parsed.get("valid"):
                raise ValueError("数值格式有误，请输入普通数字或带千分位分隔符的数字。")
            action["new_value"] = str(parsed["value"])
        new_actions.append(action)

    target_dir = unified_review_dir(job)
    existing_actions = _load_unified_review_actions(job)
    all_actions = [*existing_actions, *new_actions]
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / "unified_review_actions.json"
    csv_path = target_dir / "unified_review_actions.csv"
    summary_path = target_dir / "unified_review_summary.json"
    write_json(json_path, all_actions)
    _write_actions_csv(csv_path, all_actions)

    _write_compatibility_actions(job, new_actions)

    summary = {
        "pass": True,
        "job_id": job.job_id,
        "actions_total": len(all_actions),
        "new_actions_total": len(new_actions),
        "value_changes_total": sum(1 for action in new_actions if action.get("edit_type") in {"value_change", "reset_value"}),
        "mapping_changes_total": sum(1 for action in new_actions if action.get("edit_type") in {"mapping_change", "reset_mapping"}),
        "confidence_available_total": sum(1 for item in items if item.get("confidence_available")),
        "confidence_missing_total": sum(1 for item in items if not item.get("confidence_available")),
        "updated_at": created_at,
        "output_files": [str(csv_path), str(json_path), str(summary_path)],
    }
    write_json(summary_path, summary)
    result_summary_path = unified_review_results_dir(job) / "unified_review_summary.json"
    summary["output_files"].append(str(result_summary_path))
    write_json(summary_path, summary)
    write_json(result_summary_path, summary)
    return summary


def _build_action_row(
    item: dict[str, Any],
    edit: dict[str, Any],
    *,
    edit_type: str,
    reviewer_name: str,
    created_at: str,
) -> dict[str, Any]:
    is_mapping = edit_type in {"mapping_change", "reset_mapping"}
    return {
        "item_id": item.get("review_item_id", ""),
        "raw_metric_id": item.get("raw_metric_id", ""),
        "original_metric_name": item.get("original_metric_name", ""),
        "edit_type": edit_type,
        "previous_value": str(edit.get("previous_value", item.get("current_value", "")) or ""),
        "new_value": str(edit.get("new_value", item.get("original_value_normalized", "")) or ""),
        "previous_code": str(edit.get("previous_code", item.get("current_code", "")) or ""),
        "previous_name": str(edit.get("previous_name", item.get("current_name", "")) or ""),
        "new_code": str(edit.get("new_code", item.get("original_code" if edit_type == "reset_mapping" else "current_code", "")) or ""),
        "new_name": str(edit.get("new_name", item.get("original_name" if edit_type == "reset_mapping" else "current_name", "")) or ""),
        "source_page_no": item.get("source_page_no", ""),
        "source_bbox_json": item.get("source_term_bbox_json" if is_mapping else "source_value_bbox_json", "") or item.get("source_bbox_json", ""),
        "reviewer_name": reviewer_name,
        "created_at": created_at,
    }


def _mapping_lookup(job: JobRecord) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for item in load_mapping_review_items(job):
        raw_metric_id = str(item.get("raw_metric_id", "") or "")
        if raw_metric_id:
            lookup[raw_metric_id] = item
    return lookup


def _standardized_lookup(job: JobRecord) -> dict[str, dict[str, Any]]:
    state = load_simple_flow_state(job)
    standard_csv = Path(str(state.get("standardized_metrics_csv", "") or ""))
    if not standard_csv.exists():
        return {}
    rows = read_csv_rows(standard_csv.parent / "standardized_metrics_detailed.csv")
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        raw_metric_id = str(row.get("raw_metric_id", "") or "")
        if not raw_metric_id:
            continue
        lookup[raw_metric_id] = {
            "review_item_id": "",
            "raw_metric_id": raw_metric_id,
            "current_code": row.get("标准指标编码", ""),
            "current_name": row.get("标准指标名称", ""),
            "mapping_status": row.get("映射状态", ""),
            "mapping_method": row.get("映射方法", ""),
            "source_page_no": row.get("source_page_no", ""),
            "source_bbox_json": row.get("source_bbox_json", ""),
            "source_value_bbox_json": row.get("source_bbox_json", ""),
            "source_pdf_path": row.get("source_pdf_path", ""),
            "source_file": row.get("source_file", ""),
            "provider": row.get("provider", ""),
        }
    return lookup


def _latest_unified_overrides(actions: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    value_overrides: dict[str, dict[str, Any]] = {}
    mapping_overrides: dict[str, dict[str, Any]] = {}
    for action in actions:
        raw_metric_id = str(action.get("raw_metric_id", "") or "")
        if not raw_metric_id:
            continue
        edit_type = str(action.get("edit_type", "") or "")
        if edit_type in {"value_change", "reset_value"}:
            value_overrides[raw_metric_id] = action
        elif edit_type in {"mapping_change", "reset_mapping"}:
            mapping_overrides[raw_metric_id] = action
    return value_overrides, mapping_overrides


def _load_unified_review_actions(job: JobRecord) -> list[dict[str, Any]]:
    path = unified_review_dir(job) / "unified_review_actions.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def _write_actions_csv(path: Path, actions: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=UNIFIED_REVIEW_ACTION_HEADERS)
        writer.writeheader()
        for action in actions:
            writer.writerow({field: _serialize(action.get(field, "")) for field in UNIFIED_REVIEW_ACTION_HEADERS})


def _write_compatibility_actions(job: JobRecord, actions: list[dict[str, Any]]) -> None:
    if not actions:
        return
    raw_items = load_raw_review_items(job, apply_actions=False)
    mapping_items = load_mapping_review_items(job)
    raw_by_metric_id = {str(item.get("raw_metric_id", "")): item for item in raw_items}
    mapping_by_metric_id = {str(item.get("raw_metric_id", "")): item for item in mapping_items}
    for action in actions:
        raw_metric_id = str(action.get("raw_metric_id", "") or "")
        edit_type = str(action.get("edit_type", "") or "")
        if edit_type in {"value_change", "reset_value"}:
            raw_item = raw_by_metric_id.get(raw_metric_id)
            if raw_item is None:
                continue
            save_raw_review_action(
                job,
                item=raw_item,
                action="edit",
                edits={
                    "table_edits": [
                        {
                            "review_item_id": raw_item.get("review_item_id", ""),
                            "raw_metric_id": raw_metric_id,
                            "metric_name": action.get("original_metric_name", ""),
                            "value": action.get("new_value", ""),
                        }
                    ]
                },
                reviewer_note="unified_review",
            )
        elif edit_type in {"mapping_change", "reset_mapping"}:
            mapping_item = mapping_by_metric_id.get(raw_metric_id)
            if mapping_item is None:
                continue
            save_mapping_review_action(
                job,
                item=mapping_item,
                action="change_mapping",
                selected_code=str(action.get("new_code", "") or ""),
                selected_name=str(action.get("new_name", "") or ""),
                reviewer_note="unified_review",
            )


def _status_label(status: str, method: str) -> tuple[str, str]:
    normalized_method = str(method or "").strip()
    normalized_status = str(status or "").strip()
    if normalized_status == "mapped" and normalized_method in STATUS_LABELS_ZH:
        return normalized_method, STATUS_LABELS_ZH[normalized_method]
    return normalized_status or "unmapped", STATUS_LABELS_ZH.get(normalized_status, normalized_status or "未映射")


def _status_badges(
    *,
    base_status_code: str,
    base_status_label: str,
    value_changed: bool,
    mapping_changed: bool,
) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    if value_changed:
        badges.append({"code": "value_changed", "label": STATUS_LABELS_ZH["value_changed"]})
    if mapping_changed:
        badges.append({"code": "term_changed", "label": STATUS_LABELS_ZH["term_changed"]})
    if badges:
        return badges
    return [{"code": base_status_code, "label": base_status_label}]


def _section_label(raw: dict[str, Any], index: int) -> str:
    page = str(raw.get("source_page_no", "") or "?")
    table = str(raw.get("table_id", "") or "")
    subtable = str(raw.get("logical_subtable_id", "") or "")
    if table or subtable:
        suffix = f" 表格{table or index}"
        if subtable and subtable != table:
            suffix = f"{suffix} / {subtable}"
        return f"第{page}页{suffix}"
    return f"第{page}页"


def _format_confidence(provider: str, raw_value: str) -> str:
    if not raw_value:
        return ""
    try:
        value = float(raw_value)
    except ValueError:
        return ""
    if value <= 1:
        value *= 100
    label = PROVIDER_LABELS_ZH.get(str(provider or "").strip(), str(provider or "").strip())
    value_text = f"{value:.0f}%" if abs(value - round(value)) < 0.01 else f"{value:.1f}%"
    return f"{label} {value_text}".strip()


def _serialize(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "" if value is None else str(value)
