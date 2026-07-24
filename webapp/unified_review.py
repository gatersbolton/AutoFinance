from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

from .models import JobRecord
from .simple_flow import (
    _is_coarse_bbox_json,
    find_review_item,
    job_root,
    load_mapping_review_items,
    load_raw_review_items,
    load_simple_flow_state,
    read_csv_rows,
    save_mapping_review_action,
    save_raw_review_action,
    _source_group_label,
    write_json,
)
from .review_quality import display_period_role, has_temporal_key


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
    "llm_suggested": "AI建议",
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

PERIOD_ROLE_LABELS_ZH = {
    "beginning": "期初数",
    "ending": "期末数",
    "previous_ending": "上期期末",
    "current_point": "当前时点",
    "current_period": "本期",
    "current_year": "本年",
    "previous_period": "上期",
    "previous_year": "上年",
    "amount": "金额",
    "explicit_date": "明确日期",
}

STATEMENT_TYPE_LABELS_ZH = {
    "balance_sheet": "资产负债表",
    "income_statement": "利润表",
    "cash_flow": "现金流量表",
    "changes_in_equity": "所有者权益变动表",
    "equity_statement": "所有者权益变动表",
    "note": "附注",
    "unknown": "未识别报表",
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
    try:
        number = _canonical_metric_decimal(normalized)
    except InvalidOperation:
        return text
    return f"{number:,.2f}"


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
    try:
        number = _canonical_metric_decimal(normalized)
    except InvalidOperation:
        return {"valid": False, "value": text, "reason": "invalid_number"}
    frac_text = match.group(3) if match.group(3) is not None else match.group(4)
    extra_precision = bool(frac_text and len(frac_text) > 2 and any(char != "0" for char in frac_text[2:]))
    return {
        "valid": True,
        "value": f"{number:.2f}",
        "reason": "",
        "precision_adjusted": extra_precision,
    }


def _canonical_metric_decimal(normalized: str) -> Decimal:
    return Decimal(normalized).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def build_unified_review_sheet(job: JobRecord, selected_item_id: str = "") -> dict[str, Any]:
    items = load_unified_review_items(job)
    if not items:
        return {
            "items": [],
            "selected_item": None,
            "confidence_available_total": 0,
            "confidence_missing_total": 0,
            "text_confidence_available_total": 0,
            "value_confidence_available_total": 0,
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
        "text_confidence_available_total": sum(1 for item in items if item.get("text_confidence_available")),
        "value_confidence_available_total": sum(1 for item in items if item.get("value_confidence_available")),
        "message": "",
    }


def load_unified_review_items(job: JobRecord) -> list[dict[str, Any]]:
    raw_items = load_raw_review_items(job, apply_actions=False)
    if not raw_items:
        return []
    state = load_simple_flow_state(job)
    standard_csv = Path(str(state.get("standardized_metrics_csv", "") or ""))
    mapping_override_not_before = standard_csv.stat().st_mtime if standard_csv.exists() else None
    mapping_lookup = _mapping_lookup(job)
    standardized_lookup = _standardized_lookup(job)
    saved_actions = _load_unified_review_actions(job)
    value_overrides, mapping_overrides = _latest_unified_overrides(saved_actions, mapping_not_before_timestamp=mapping_override_not_before)

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

        mapping_method = str(mapping.get("mapping_method") or mapping.get("映射方法") or "")
        candidate_original_code = str(mapping.get("current_code") or mapping.get("标准指标编码") or mapping.get("candidate_code") or "")
        candidate_original_name = str(mapping.get("current_name") or mapping.get("标准指标名称") or mapping.get("candidate_name") or "")
        mapping_status = str(mapping.get("mapping_status") or mapping.get("映射状态") or ("mapped" if candidate_original_code else "unmapped"))
        is_unmapped_without_suggestion = mapping_status == "unmapped" and mapping_method == "none"
        original_code = "" if is_unmapped_without_suggestion else candidate_original_code
        original_name = "" if is_unmapped_without_suggestion else candidate_original_name
        mapping_override = mapping_overrides.get(raw_metric_id, {})
        current_code = str(mapping_override.get("new_code", original_code) or "")
        current_name = str(mapping_override.get("new_name", original_name) or "")
        mapping_changed = bool(mapping_override) and (current_code != original_code or current_name != original_name)
        value_changed = bool(value_override) and str(current_value_normalized) != str(parse_metric_number_input(original_value, value_type=value_type).get("value", original_value))
        temporal_review_required = bool(raw.get("temporal_review_required")) or not has_temporal_key(
            raw.get("当前条目日期", ""),
            raw.get("period_role_norm", "") or raw.get("期间类型", ""),
            raw.get("period_role_raw", ""),
        )

        base_status_code, base_status_label = _status_label(mapping_status, mapping_method)
        display_status_code = "review_required" if temporal_review_required else base_status_code
        display_status_label = "日期待校对" if temporal_review_required else base_status_label
        status_badges = _status_badges(
            base_status_code=display_status_code,
            base_status_label=display_status_label,
            value_changed=value_changed,
            mapping_changed=mapping_changed,
            temporal_review_required=temporal_review_required,
        )

        provider = str(raw.get("provider") or mapping.get("provider") or "")
        text_confidence_raw = str(raw.get("text_confidence", "") or "").strip()
        value_confidence_raw = str(raw.get("value_confidence", "") or raw.get("confidence", "") or mapping.get("confidence", "") or "").strip()
        confidence_raw = value_confidence_raw
        text_confidence_score = _confidence_score(text_confidence_raw)
        value_confidence_score = _confidence_score(value_confidence_raw)
        text_confidence_display = _format_ocr_confidence("文字", text_confidence_raw)
        value_confidence_display = _format_ocr_confidence("数字", value_confidence_raw)
        confidence_display = value_confidence_display
        source_term_bbox_json = _safe_cell_bbox_json(mapping.get("source_term_bbox_json") or "")
        source_value_bbox_json = _safe_cell_bbox_json(
            mapping.get("source_value_bbox_json") or raw.get("source_bbox_json") or mapping.get("source_bbox_json") or ""
        )
        section_key = "|".join(
            [
                str(raw.get("source_pdf_path", "") or mapping.get("source_pdf_path", "")),
                str(raw.get("source_file", "") or mapping.get("source_file", "")),
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
            "table_date": raw.get("填表日期", "") or raw.get("当前条目日期", ""),
            "period_role": _display_period_role(raw.get("period_role_norm", ""), raw.get("period_role_raw", ""))
            or _display_period_role(raw.get("期间类型", ""), ""),
            "_period_role_norm": raw.get("period_role_norm", ""),
            "_period_role_raw": raw.get("period_role_raw", "") or raw.get("期间类型", ""),
            "header_path": raw.get("header_path", ""),
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
            "base_status": display_status_code,
            "base_status_label": display_status_label,
            "mapping_base_status": base_status_code,
            "mapping_base_status_label": base_status_label,
            "status_badges": status_badges,
            "temporal_review_required": temporal_review_required,
            "value_changed": value_changed,
            "mapping_changed": mapping_changed,
            "source_page_no": raw.get("source_page_no") or mapping.get("source_page_no") or "",
            "source_pdf_path": raw.get("source_pdf_path") or mapping.get("source_pdf_path") or "",
            "source_file": raw.get("source_file") or mapping.get("source_file") or "",
            "provider": provider,
            "statement_type": raw.get("statement_type", "") or mapping.get("statement_type", ""),
            "statement_name_raw": raw.get("statement_name_raw", "") or mapping.get("statement_name_raw", ""),
            "source_term_bbox_json": source_term_bbox_json,
            "source_value_bbox_json": source_value_bbox_json,
            "source_bbox_json": source_term_bbox_json or source_value_bbox_json,
            "bbox_state": "has-bbox" if (source_term_bbox_json or source_value_bbox_json) else "missing-bbox",
            "confidence": confidence_raw,
            "confidence_display": confidence_display or "未记录",
            "confidence_available": bool(text_confidence_display or value_confidence_display),
            "text_confidence": text_confidence_raw,
            "value_confidence": value_confidence_raw,
            "text_confidence_score": "" if text_confidence_score is None else f"{text_confidence_score:.6f}",
            "value_confidence_score": "" if value_confidence_score is None else f"{value_confidence_score:.6f}",
            "text_confidence_display": text_confidence_display or "文字 未记录",
            "value_confidence_display": value_confidence_display or "数字 未记录",
            "text_confidence_available": bool(text_confidence_display),
            "value_confidence_available": bool(value_confidence_display),
            "text_confidence_level": _confidence_level(text_confidence_score),
            "value_confidence_level": _confidence_level(value_confidence_score),
            "mapping_confidence_display": "" if is_unmapped_without_suggestion else _format_mapping_confidence(mapping.get("mapping_confidence") or mapping.get("candidate_score") or mapping.get("映射置信度")),
            "ai_suggestion_label": str(mapping.get("ai_suggestion_label") or ""),
            "ai_confidence_display": str(mapping.get("ai_confidence_display") or ""),
            "ai_reason": str(mapping.get("ai_reason") or ""),
            "ai_relation_type": str(mapping.get("ai_relation_type") or ""),
            "ai_validation_status": str(mapping.get("ai_validation_status") or ""),
            "show_mapping_decision_actions": bool(mapping.get("show_mapping_decision_actions", True)),
            "mapping_decision_note": str(mapping.get("mapping_decision_note") or ""),
            "section_key": section_key,
            "section_label": section_label,
            "starts_section": starts_section,
        }
        items.append(item)
    return _merge_period_review_items(items)


def _merge_period_review_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped_items: list[dict[str, Any]] = []
    buckets: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for item in items:
        slot = _period_value_slot(item)
        key = _period_group_key(item)
        candidates = buckets.setdefault(key, [])
        group = next((candidate for candidate in candidates if not candidate.get(_slot_field(slot), {}).get("available")), None)
        if group is None:
            group = _new_period_group(item)
            candidates.append(group)
            grouped_items.append(group)
        _add_period_item_to_group(group, item, slot)

    merged: list[dict[str, Any]] = []
    last_section_key = ""
    for group in grouped_items:
        finalized = _finalize_period_group(group)
        section_key = str(finalized.get("section_key", "") or "")
        finalized["starts_section"] = section_key != last_section_key
        last_section_key = section_key
        merged.append(finalized)
    return merged


def _period_group_key(item: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(item.get("section_key", "") or ""),
        str(item.get("company_name", "") or ""),
        str(item.get("fill_date", "") or item.get("table_date", "") or ""),
        str(item.get("statement_type", "") or ""),
        str(item.get("statement_name_raw", "") or ""),
        str(item.get("original_metric_name", "") or ""),
    )


def _period_value_slot(item: dict[str, Any]) -> str:
    role_norm = str(item.get("_period_role_norm", "") or "").strip().lower()
    role_label = str(item.get("period_role", "") or "")
    role_raw = str(item.get("_period_role_raw", "") or "")
    role_text = f"{role_norm} {role_label} {role_raw}".lower()
    if role_norm in {"ending", "current_point"} or "期末" in role_text or "年末" in role_text or "期末" in role_label:
        return "second"
    if (
        role_norm in {"beginning", "previous_ending"}
        or "期初" in role_text
        or "年初" in role_text
        or "上期期末" in role_text
        or "期初" in role_label
    ):
        return "first"
    if role_norm in {"current_period", "current_year"} or any(keyword in role_text for keyword in ("本期", "本年", "本年累计")):
        return "first"
    if role_norm in {"previous_period", "previous_year"} or any(keyword in role_text for keyword in ("上期", "上年", "上年累计")):
        return "second"
    return "first"


def _new_period_group(item: dict[str, Any]) -> dict[str, Any]:
    group = dict(item)
    group["_leaf_items"] = []
    group["raw_metric_ids"] = []
    group["raw_metric_ids_joined"] = ""
    group["beginning_value"] = _empty_period_value("first")
    group["ending_value"] = _empty_period_value("second")
    group["value_items"] = []
    group["period_values"] = [group["beginning_value"], group["ending_value"]]
    group["period_columns"] = _period_columns(group["beginning_value"], group["ending_value"])
    group["period_role"] = ""
    return group


def _add_period_item_to_group(group: dict[str, Any], item: dict[str, Any], slot: str) -> None:
    value_item = _period_value_payload(item, slot, group_review_item_id=str(group.get("review_item_id", "") or ""))
    field = _slot_field(slot)
    group[field] = value_item
    group["_leaf_items"].append(item)
    raw_metric_id = str(item.get("raw_metric_id", "") or "")
    if raw_metric_id:
        group["raw_metric_ids"].append(raw_metric_id)


def _period_value_payload(item: dict[str, Any], slot: str, *, group_review_item_id: str) -> dict[str, Any]:
    payload = dict(item)
    payload.update(
        {
            "available": True,
            "slot": slot,
            "slot_label": _period_value_label(item, slot),
            "group_review_item_id": group_review_item_id,
            "cell_changed": bool(item.get("value_changed")),
        }
    )
    return payload


def _empty_period_value(slot: str) -> dict[str, Any]:
    return {
        "available": False,
        "slot": slot,
        "slot_label": _generic_period_slot_label(slot),
        "raw_metric_id": "",
        "original_value": "",
        "original_value_normalized": "",
        "current_value": "",
        "display_value": "",
        "value_type": "",
        "source_page_no": "",
        "source_value_bbox_json": "",
        "value_confidence_available": False,
        "value_confidence_display": "",
        "value_confidence_level": "missing",
        "cell_changed": False,
    }


def _slot_field(slot: str) -> str:
    return "ending_value" if slot == "second" else "beginning_value"


def _period_columns(first_value: dict[str, Any], second_value: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {"slot": "first", "label": _period_column_label(first_value, "first")},
        {"slot": "second", "label": _period_column_label(second_value, "second")},
    ]


def _period_column_label(value: dict[str, Any], slot: str) -> str:
    if isinstance(value, dict) and value.get("available"):
        return str(value.get("slot_label") or _default_period_slot_label(slot))
    return _generic_period_slot_label(slot)


def _generic_period_slot_label(slot: str) -> str:
    return "期间数值二" if slot == "second" else "期间数值一"


def _default_period_slot_label(slot: str) -> str:
    return "期末数" if slot == "second" else "期初数"


def _period_value_label(item: dict[str, Any], slot: str) -> str:
    role_norm = str(item.get("_period_role_norm", "") or "").strip().lower()
    role_label = str(item.get("period_role", "") or "").strip()
    if role_norm in {"beginning", "previous_ending"} or role_label in {"期初数", "上期期末"}:
        return "期初数"
    if role_norm in {"ending", "current_point"} or role_label in {"期末数", "当前时点"}:
        return "期末数"
    header_label = _period_header_label(item.get("header_path", ""))
    if header_label:
        return header_label
    raw_label = _period_header_label(item.get("_period_role_raw", ""))
    if raw_label:
        return raw_label
    if role_label:
        return role_label
    return _default_period_slot_label(slot)


def _period_header_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = [part.strip() for part in re.split(r"[/|>＞]+", text) if part.strip()]
    label = parts[-1] if parts else text
    if len(label) > 14:
        return ""
    if any(keyword in label for keyword in ("期初", "年初", "期末", "年末", "本期", "本年", "上期", "上年", "累计")):
        return label
    return ""


def _finalize_period_group(group: dict[str, Any]) -> dict[str, Any]:
    leaf_items = [item for item in group.get("_leaf_items", []) if isinstance(item, dict)]
    value_items = [
        value
        for value in (group.get("beginning_value"), group.get("ending_value"))
        if isinstance(value, dict) and value.get("available")
    ]
    base = _select_group_base_item(leaf_items) or group
    merged = dict(group)
    merged.update(
        {
            "review_item_id": str(group.get("review_item_id", "") or base.get("review_item_id", "")),
            "raw_metric_id": str(base.get("raw_metric_id", "") or ""),
            "raw_review_item_id": base.get("raw_review_item_id", ""),
            "mapping_review_item_id": base.get("mapping_review_item_id", ""),
            "raw_metric_ids": list(dict.fromkeys(str(raw_id) for raw_id in group.get("raw_metric_ids", []) if str(raw_id))),
            "table_date": group.get("fill_date") or group.get("table_date") or group.get("item_date") or "",
            "item_date": group.get("fill_date") or group.get("table_date") or group.get("item_date") or "",
            "period_role": "",
            "value_items": value_items,
            "leaf_items": leaf_items,
            "value_changed": any(bool(item.get("value_changed")) for item in leaf_items),
            "mapping_changed": any(bool(item.get("mapping_changed")) for item in leaf_items),
        }
    )
    merged["raw_metric_ids_joined"] = ",".join(merged["raw_metric_ids"])
    first_value = value_items[0] if value_items else _empty_period_value("first")
    merged["original_value"] = first_value.get("original_value", "")
    merged["original_value_normalized"] = first_value.get("original_value_normalized", "")
    merged["current_value"] = first_value.get("current_value", "")
    merged["display_value"] = first_value.get("display_value", "")
    merged["value_type"] = first_value.get("value_type", "")
    merged["source_value_bbox_json"] = _first_non_empty(value.get("source_value_bbox_json", "") for value in value_items)
    merged["source_bbox_json"] = merged.get("source_term_bbox_json") or merged.get("source_value_bbox_json") or ""
    merged["bbox_state"] = "has-bbox" if merged.get("source_bbox_json") else "missing-bbox"
    merged["value_confidence_score"] = _min_score_text(value.get("value_confidence_score", "") for value in value_items)
    merged["value_confidence_available"] = any(bool(value.get("value_confidence_available")) for value in value_items)
    merged["confidence_available"] = bool(merged.get("text_confidence_available")) or merged["value_confidence_available"]
    merged["status_badges"] = _status_badges(
        base_status_code=str(merged.get("base_status", "") or "unmapped"),
        base_status_label=str(merged.get("base_status_label", "") or "未映射"),
        value_changed=bool(merged.get("value_changed")),
        mapping_changed=bool(merged.get("mapping_changed")),
        temporal_review_required=any(bool(item.get("temporal_review_required")) for item in leaf_items),
    )
    merged["period_values"] = [merged.get("beginning_value") or _empty_period_value("first"), merged.get("ending_value") or _empty_period_value("second")]
    merged["period_columns"] = _period_columns(merged["period_values"][0], merged["period_values"][1])
    merged["mapping_status"] = merged["status_badges"][0]["code"]
    merged["mapping_status_label"] = merged["status_badges"][0]["label"]
    return merged


def _select_group_base_item(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not items:
        return None
    return next((item for item in items if str(item.get("current_code", "") or "")), items[0])


def _first_non_empty(values: Iterable[Any]) -> str:
    for value in values:
        text = str(value or "")
        if text:
            return text
    return ""


def _min_score_text(values: Iterable[Any]) -> str:
    scores: list[float] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            scores.append(float(text))
        except ValueError:
            continue
    return f"{min(scores):.6f}" if scores else ""


def save_unified_review_actions(job: JobRecord, edits: Iterable[dict[str, Any]], *, reviewer_name: str = "") -> dict[str, Any]:
    items = load_unified_review_items(job)
    by_item_id = {str(item.get("review_item_id", "")): item for item in items}
    by_raw_id: dict[str, dict[str, Any]] = {}
    for item in items:
        for raw_metric_id in item.get("raw_metric_ids", []) or [item.get("raw_metric_id", "")]:
            raw_metric_id_text = str(raw_metric_id or "")
            if raw_metric_id_text:
                by_raw_id[raw_metric_id_text] = item
    created_at = datetime.now(timezone.utc).isoformat()
    new_actions: list[dict[str, Any]] = []
    precision_warnings_total = 0
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
        for target_item in _action_target_items(item, edit, edit_type):
            action = _build_action_row(target_item, edit, edit_type=edit_type, reviewer_name=reviewer_name, created_at=created_at)
            if edit_type in {"value_change", "reset_value"}:
                parsed = parse_metric_number_input(action["new_value"], value_type=str(target_item.get("value_type", "")))
                if not parsed.get("valid"):
                    raise ValueError("数值格式有误，请输入普通数字或带千分位分隔符的数字。")
                action["new_value"] = str(parsed["value"])
                if parsed.get("precision_adjusted"):
                    precision_warnings_total += 1
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
        "precision_warnings_total": precision_warnings_total,
        "confidence_available_total": sum(1 for item in items if item.get("confidence_available")),
        "confidence_missing_total": sum(1 for item in items if not item.get("confidence_available")),
        "text_confidence_available_total": sum(1 for item in items if item.get("text_confidence_available")),
        "value_confidence_available_total": sum(1 for item in items if item.get("value_confidence_available")),
        "updated_at": created_at,
        "output_files": [str(csv_path), str(json_path), str(summary_path)],
    }
    write_json(summary_path, summary)
    result_summary_path = unified_review_results_dir(job) / "unified_review_summary.json"
    summary["output_files"].append(str(result_summary_path))
    write_json(summary_path, summary)
    write_json(result_summary_path, summary)
    return summary


def _action_target_items(item: dict[str, Any], edit: dict[str, Any], edit_type: str) -> list[dict[str, Any]]:
    if edit_type in {"value_change", "reset_value"}:
        return [_value_item_for_edit(item, edit)]
    raw_metric_ids = _edit_raw_metric_ids(edit)
    leaf_items = [candidate for candidate in item.get("leaf_items", []) if isinstance(candidate, dict)]
    if raw_metric_ids:
        selected = [candidate for candidate in leaf_items if str(candidate.get("raw_metric_id", "") or "") in raw_metric_ids]
        return selected or [item]
    return leaf_items or [item]


def _value_item_for_edit(item: dict[str, Any], edit: dict[str, Any]) -> dict[str, Any]:
    raw_metric_id = str(edit.get("raw_metric_id", "") or "")
    for value_item in item.get("value_items", []):
        if isinstance(value_item, dict) and raw_metric_id and str(value_item.get("raw_metric_id", "") or "") == raw_metric_id:
            return value_item
    slot = str(edit.get("value_slot", "") or edit.get("period_slot", "") or "")
    if slot:
        for value_item in item.get("value_items", []):
            if isinstance(value_item, dict) and str(value_item.get("slot", "") or "") == slot:
                return value_item
    for value_item in item.get("value_items", []):
        if isinstance(value_item, dict) and value_item.get("available"):
            return value_item
    return item


def _edit_raw_metric_ids(edit: dict[str, Any]) -> set[str]:
    raw_metric_ids = edit.get("raw_metric_ids", [])
    if isinstance(raw_metric_ids, str):
        values = [value.strip() for value in raw_metric_ids.split(",")]
    elif isinstance(raw_metric_ids, list):
        values = [str(value).strip() for value in raw_metric_ids]
    else:
        values = []
    raw_metric_id = str(edit.get("raw_metric_id", "") or "").strip()
    if raw_metric_id:
        values.append(raw_metric_id)
    return {value for value in values if value}


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
        "item_id": edit.get("item_id") or item.get("group_review_item_id") or item.get("review_item_id", ""),
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
            "statement_type": row.get("statement_type", ""),
            "statement_name_raw": row.get("statement_name_raw", ""),
        }
    return lookup


def _latest_unified_overrides(
    actions: list[dict[str, Any]], *, mapping_not_before_timestamp: float | None = None
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
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
            if mapping_not_before_timestamp is not None:
                action_timestamp = _action_created_timestamp(action)
                if action_timestamp is not None and action_timestamp < mapping_not_before_timestamp:
                    continue
            mapping_overrides[raw_metric_id] = action
    return value_overrides, mapping_overrides


def _action_created_timestamp(action: dict[str, Any]) -> float | None:
    created_at = str(action.get("created_at", "") or "").strip()
    if not created_at:
        return None
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


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
                persist_decision=False,
            )


def _status_label(status: str, method: str) -> tuple[str, str]:
    normalized_method = str(method or "").strip()
    normalized_status = str(status or "").strip()
    if normalized_method == "llm_suggested":
        return "llm_suggested", STATUS_LABELS_ZH["llm_suggested"]
    if normalized_status == "mapped" and normalized_method in STATUS_LABELS_ZH:
        return normalized_method, STATUS_LABELS_ZH[normalized_method]
    return normalized_status or "unmapped", STATUS_LABELS_ZH.get(normalized_status, normalized_status or "未映射")


def _status_badges(
    *,
    base_status_code: str,
    base_status_label: str,
    value_changed: bool,
    mapping_changed: bool,
    temporal_review_required: bool = False,
) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    if value_changed:
        badges.append({"code": "value_changed", "label": STATUS_LABELS_ZH["value_changed"]})
    if mapping_changed:
        badges.append({"code": "term_changed", "label": STATUS_LABELS_ZH["term_changed"]})
    if temporal_review_required:
        badges.append({"code": "review_required", "label": "日期待校对"})
    if badges:
        return badges
    return [{"code": base_status_code, "label": base_status_label}]


def _section_label(raw: dict[str, Any], index: int) -> str:
    return _source_group_label(raw, index)


def _display_period_role(period_role_norm: Any, period_role_raw: Any = "") -> str:
    return display_period_role(period_role_norm, period_role_raw)


def _safe_cell_bbox_json(value: Any) -> str:
    bbox_json = str(value or "")
    return "" if _is_coarse_bbox_json(bbox_json) else bbox_json


def _statement_label(raw: dict[str, Any]) -> str:
    raw_name = str(raw.get("statement_name_raw", "") or "").strip()
    if raw_name:
        return raw_name
    statement_type = str(raw.get("statement_type", "") or "").strip()
    return STATEMENT_TYPE_LABELS_ZH.get(statement_type, statement_type)


def _table_source_label(raw: dict[str, Any], index: int) -> str:
    table = str(raw.get("table_id", "") or "")
    subtable = str(raw.get("logical_subtable_id", "") or "")
    if not table and not subtable:
        return f"来源表 {index}"
    label = f"来源表 {table or index}"
    sub_index = _subtable_index(subtable)
    if sub_index:
        return f"{label} · 拆分区 {sub_index}"
    if subtable and subtable != table:
        return f"{label} · {subtable}"
    return label


def _subtable_index(value: str) -> str:
    match = re.search(r"sub(\d+)$", str(value or ""))
    return match.group(1) if match else ""


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


def _confidence_score(raw_value: object) -> float | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value > 1:
        value /= 100
    return max(0.0, min(1.0, value))


def _format_ocr_confidence(label: str, raw_value: object) -> str:
    score = _confidence_score(raw_value)
    if score is None:
        return ""
    value = score * 100
    value_text = f"{value:.0f}%" if abs(value - round(value)) < 0.01 else f"{value:.1f}%"
    return f"OCR识别{label} 置信度{value_text}"


def _confidence_level(score: float | None) -> str:
    if score is None:
        return "missing"
    if score < 0.8:
        return "danger"
    if score < 0.9:
        return "warning"
    return "ok"


def _format_mapping_confidence(raw_value: object) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    try:
        value = float(text)
    except ValueError:
        return ""
    if value <= 1:
        value *= 100
    value_text = f"{value:.0f}%" if abs(value - round(value)) < 0.01 else f"{value:.1f}%"
    return f"词语映射 置信度{value_text}"


def _serialize(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "" if value is None else str(value)
