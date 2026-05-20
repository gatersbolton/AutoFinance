from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .export import append_rows, write_dict_csv, write_json, write_xlsx
from .models import (
    CANDIDATE_OUTPUT_COLUMNS,
    DECISION_VALUES,
    DETAILED_OUTPUT_COLUMNS,
    ISSUE_OUTPUT_COLUMNS,
    REVIEW_ITEM_COLUMNS,
    STANDARD_OUTPUT_COLUMNS,
)
from .store import DECISION_FIELDNAMES


def apply_mapping_decision_to_output(output_dir: str | Path, decision: dict[str, Any]) -> list[str]:
    output_dir = Path(output_dir).resolve()
    if not output_dir.exists():
        return []
    raw_metric_id = str(decision.get("raw_metric_id", "") or "")
    raw_metric_name = str(decision.get("raw_metric_name", "") or "")
    decision_value = str(decision.get("decision", "") or "")
    if decision_value not in DECISION_VALUES:
        raise ValueError(f"Unsupported mapping decision: {decision_value}")

    touched: list[str] = []
    main_path = output_dir / "standardized_metrics.csv"
    detailed_path = output_dir / "standardized_metrics_detailed.csv"
    review_path = output_dir / "mapping_review_items.csv"

    if main_path.exists():
        rows, fieldnames = _read_csv_with_fieldnames(main_path)
        rows = [_apply_to_standardized_row(row, decision, raw_metric_id=raw_metric_id, raw_metric_name=raw_metric_name) for row in rows]
        write_dict_csv(main_path, rows, _merged_fieldnames(fieldnames, STANDARD_OUTPUT_COLUMNS))
        touched.append(str(main_path))

    if detailed_path.exists():
        rows, fieldnames = _read_csv_with_fieldnames(detailed_path)
        rows = [_apply_to_standardized_row(row, decision, raw_metric_id=raw_metric_id, raw_metric_name=raw_metric_name) for row in rows]
        write_dict_csv(detailed_path, rows, _merged_fieldnames(fieldnames, DETAILED_OUTPUT_COLUMNS))
        touched.append(str(detailed_path))

    if review_path.exists():
        rows, fieldnames = _read_csv_with_fieldnames(review_path)
        rows = [_apply_to_review_row(row, decision, raw_metric_id=raw_metric_id, raw_metric_name=raw_metric_name) for row in rows]
        write_dict_csv(review_path, rows, _merged_fieldnames(fieldnames, REVIEW_ITEM_COLUMNS))
        touched.append(str(review_path))

    _refresh_mapping_workbook(output_dir)
    if (output_dir / "standardized_metrics.xlsx").exists():
        touched.append(str(output_dir / "standardized_metrics.xlsx"))
    return touched


def write_mapping_decision_files(target_dir: str | Path, decisions: list[dict[str, Any]]) -> dict[str, Any]:
    target_dir = Path(target_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    csv_path = target_dir / "mapping_decisions.csv"
    json_path = target_dir / "mapping_decisions.json"
    summary_path = target_dir / "mapping_decision_summary.json"

    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DECISION_FIELDNAMES)
        writer.writeheader()
        for row in decisions:
            writer.writerow({field: "" if row.get(field) is None else row.get(field, "") for field in DECISION_FIELDNAMES})
    write_json(json_path, {"decisions": decisions})
    summary = {
        "pass": True,
        "decisions_total": len(decisions),
        "rejected_total": sum(1 for row in decisions if row.get("decision") == "reject"),
        "accept_once_total": sum(1 for row in decisions if row.get("decision") == "accept_once"),
        "accept_and_remember_total": sum(1 for row in decisions if row.get("decision") == "accept_and_remember"),
        "output_files": [str(csv_path), str(json_path), str(summary_path)],
    }
    write_json(summary_path, summary)
    return summary


def append_mapping_decision_file(target_dir: str | Path, decision: dict[str, Any]) -> dict[str, Any]:
    target_dir = Path(target_dir).resolve()
    existing = _read_existing_decisions(target_dir / "mapping_decisions.json")
    existing.append(decision)
    return write_mapping_decision_files(target_dir, existing)


def _apply_to_standardized_row(
    row: dict[str, Any],
    decision: dict[str, Any],
    *,
    raw_metric_id: str,
    raw_metric_name: str,
) -> dict[str, Any]:
    if not _row_matches(row, raw_metric_id=raw_metric_id, raw_metric_name=raw_metric_name):
        return row
    row = dict(row)
    decision_value = str(decision.get("decision", "") or "")
    if decision_value == "reject":
        row["标准指标编码"] = ""
        row["标准指标名称"] = ""
        row["映射方法"] = "none"
        row["映射状态"] = "unmapped"
        row["映射置信度"] = ""
        row["口径关系"] = ""
        row["口径说明"] = "人工不采纳当前映射建议。"
        row["是否需要人工校对"] = "是"
        row["issue_reason"] = "human_rejected_mapping"
        return row

    final_code = str(decision.get("final_code") or decision.get("suggested_code") or "")
    final_name = str(decision.get("final_name") or decision.get("suggested_name") or "")
    row["标准指标编码"] = final_code
    row["标准指标名称"] = final_name
    row["映射方法"] = "manual_saved" if decision_value == "accept_and_remember" else "manual_once"
    row["映射状态"] = "mapped"
    row["映射置信度"] = _confidence_text(decision.get("confidence"), default="1.0")
    row["口径关系"] = str(decision.get("relation_type", "") or "exact_alias")
    row["口径说明"] = "人工采用并记住。" if decision_value == "accept_and_remember" else "人工仅本次采用。"
    row["是否需要人工校对"] = "否"
    row["issue_reason"] = ""
    return row


def _apply_to_review_row(
    row: dict[str, Any],
    decision: dict[str, Any],
    *,
    raw_metric_id: str,
    raw_metric_name: str,
) -> dict[str, Any]:
    if not _row_matches(row, raw_metric_id=raw_metric_id, raw_metric_name=raw_metric_name):
        return row
    row = dict(row)
    decision_value = str(decision.get("decision", "") or "")
    row["mapping_decision"] = decision_value
    row["relation_type"] = str(decision.get("relation_type", "") or row.get("relation_type", "") or "")
    row["mapping_confidence"] = _confidence_text(decision.get("confidence"), default=str(row.get("mapping_confidence", "") or ""))
    if decision_value == "reject":
        row["mapping_status"] = "rejected"
        row["issue_reason"] = "人工不采纳当前映射建议。"
        row["action_default"] = "reject"
    else:
        row["mapping_status"] = "mapped"
        row["candidate_code"] = str(decision.get("final_code") or decision.get("suggested_code") or row.get("candidate_code") or "")
        row["candidate_name"] = str(decision.get("final_name") or decision.get("suggested_name") or row.get("candidate_name") or "")
        row["issue_reason"] = "人工采用并记住。" if decision_value == "accept_and_remember" else "人工仅本次采用。"
        row["action_default"] = decision_value
    row["action_options"] = ["reject", "accept_once", "accept_and_remember"]
    return row


def _refresh_mapping_workbook(output_dir: Path) -> None:
    main_rows, _ = _read_csv_with_fieldnames(output_dir / "standardized_metrics.csv")
    detailed_rows, _ = _read_csv_with_fieldnames(output_dir / "standardized_metrics_detailed.csv")
    candidate_rows, _ = _read_csv_with_fieldnames(output_dir / "mapping_candidates.csv")
    issue_rows, _ = _read_csv_with_fieldnames(output_dir / "mapping_issues.csv")
    review_rows, _ = _read_csv_with_fieldnames(output_dir / "mapping_review_items.csv")
    if not main_rows and not (output_dir / "standardized_metrics.csv").exists():
        return
    write_xlsx(output_dir / "standardized_metrics.xlsx", main_rows, detailed_rows, candidate_rows, issue_rows, review_rows)


def _row_matches(row: dict[str, Any], *, raw_metric_id: str, raw_metric_name: str) -> bool:
    if raw_metric_id and str(row.get("raw_metric_id", "") or row.get("raw_metric_id", "")) == raw_metric_id:
        return True
    if raw_metric_id and str(row.get("raw_metric_id", "")) == raw_metric_id:
        return True
    return bool(raw_metric_name and str(row.get("原始指标名", "") or row.get("original_metric_name", "")) == raw_metric_name)


def _read_csv_with_fieldnames(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


def _read_existing_decisions(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if isinstance(payload, dict) and isinstance(payload.get("decisions"), list):
        return [dict(row) for row in payload["decisions"] if isinstance(row, dict)]
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    return []


def _merged_fieldnames(current: list[str], preferred: list[str]) -> list[str]:
    merged = list(preferred)
    for field in current:
        if field not in merged:
            merged.append(field)
    return merged


def _confidence_text(value: Any, *, default: str = "") -> str:
    if value in ("", None):
        return default
    try:
        return str(float(value))
    except (TypeError, ValueError):
        return str(value)
