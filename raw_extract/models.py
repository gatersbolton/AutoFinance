from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields, is_dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional


MAIN_OUTPUT_COLUMNS = [
    "填表日期",
    "当前条目日期",
    "期间类型",
    "公司名",
    "指标名",
    "指标数值",
]


DETAILED_OUTPUT_FIELDS = [
    "fill_date",
    "item_date",
    "company_name",
    "metric_name",
    "metric_value",
    "value_raw",
    "value_type",
    "unit_raw",
    "unit_multiplier",
    "statement_type",
    "statement_name_raw",
    "provider",
    "doc_id",
    "source_file",
    "page_no",
    "table_id",
    "logical_subtable_id",
    "row_index",
    "col_index",
    "row_label_raw",
    "row_label_clean",
    "row_context_path",
    "header_path",
    "period_role_raw",
    "period_role_norm",
    "period_start_date",
    "period_end_date",
    "date_resolution_method",
    "company_resolution_method",
    "source_cell_ref",
    "bbox_json",
    "text_confidence",
    "value_confidence",
    "confidence",
    "evidence_path",
    "issue_flags",
]


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


def display_period_role(period_role_norm: Any, period_role_raw: Any = "") -> str:
    norm = str(period_role_norm or "").strip()
    raw = str(period_role_raw or "").strip()
    if norm and norm != "unknown":
        label = PERIOD_ROLE_LABELS_ZH.get(norm)
        if label:
            return label
        if _is_displayable_period_role_raw(norm):
            return norm
        return ""
    if _is_displayable_period_role_raw(raw):
        return raw
    return ""


def _is_displayable_period_role_raw(value: Any) -> bool:
    text = str(value or "").strip()
    if not text or text == "unknown":
        return False
    if "/" in text:
        return False
    compact = re.sub(r"\s+", "", text)
    if re.fullmatch(r"[-+]?[\d,，]+(?:\.\d+)?%?", compact):
        return False
    if len(compact) > 16:
        return False
    return any(
        keyword in compact
        for keyword in ("期初", "年初", "期末", "年末", "当前时点", "本期", "上期", "本年", "上年", "本月", "累计")
    )


def compact_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def serialize_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, dict, tuple)):
        return compact_json(value)
    return value


def dataclass_row(instance: Any) -> Dict[str, Any]:
    if not is_dataclass(instance):
        raise TypeError(f"Expected dataclass instance, got {type(instance)!r}")
    return {item.name: serialize_value(getattr(instance, item.name)) for item in fields(instance)}


@dataclass
class CompanyResolution:
    doc_id: str
    company_name: str
    method: str
    source_text: str = ""
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    issue_flags: List[str] = field(default_factory=list)


@dataclass
class FillDateResolution:
    doc_id: str
    provider: str
    page_no: int
    table_id: str
    logical_subtable_id: str
    fill_date: str
    method: str
    source_text: str = ""
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    issue_flags: List[str] = field(default_factory=list)


@dataclass
class ItemDateResolution:
    item_date: str
    period_start_date: str
    period_end_date: str
    period_role_raw: str
    period_role_norm: str
    method: str
    issue_flags: List[str] = field(default_factory=list)


@dataclass
class NumberParseResult:
    value: Optional[Decimal]
    value_type: str
    normalized_text: str
    issue_flags: List[str] = field(default_factory=list)
    suspicious_reason: str = ""


@dataclass
class RawMetricCandidate:
    candidate_id: str
    fill_date: str
    item_date: str
    company_name: str
    metric_name: str
    metric_value: Optional[Decimal]
    value_raw: str
    value_type: str
    unit_raw: str
    provider: str
    doc_id: str
    source_file: str
    page_no: int
    table_id: str
    logical_subtable_id: str
    row_index: int
    col_index: int
    row_label_raw: str
    row_label_clean: str
    row_context_path: str
    header_path: str
    period_role_raw: str
    period_role_norm: str
    period_start_date: str
    period_end_date: str
    date_resolution_method: str
    company_resolution_method: str
    source_cell_ref: str
    bbox_json: str
    text_confidence: Optional[float]
    value_confidence: Optional[float]
    confidence: Optional[float]
    statement_type: str = ""
    statement_name_raw: str = ""
    evidence_path: str = ""
    issue_flags: List[str] = field(default_factory=list)
    unit_multiplier: Decimal = Decimal("1")
    provider_rank: int = 9999
    duplicate_key: str = ""
    accepted: bool = False
    selection_status: str = "candidate"

    def main_row(self) -> Dict[str, Any]:
        return {
            "填表日期": self.fill_date,
            "当前条目日期": self.item_date,
            "期间类型": display_period_role(self.period_role_norm, self.period_role_raw),
            "公司名": self.company_name,
            "指标名": self.metric_name,
            "指标数值": self.metric_value if self.metric_value is not None else "",
        }

    def detailed_row(self) -> Dict[str, Any]:
        row = {field_name: serialize_value(getattr(self, field_name)) for field_name in DETAILED_OUTPUT_FIELDS}
        return row

    def candidate_row(self) -> Dict[str, Any]:
        row = {
            "candidate_id": self.candidate_id,
            "accepted": self.accepted,
            "selection_status": self.selection_status,
            "provider_rank": self.provider_rank,
            "duplicate_key": self.duplicate_key,
        }
        row.update(self.detailed_row())
        return row


@dataclass
class RawMetricIssue:
    issue_id: str
    issue_type: str
    severity: str
    message: str
    doc_id: str = ""
    provider: str = ""
    source_file: str = ""
    page_no: int = 0
    table_id: str = ""
    logical_subtable_id: str = ""
    source_cell_ref: str = ""
    metric_name: str = ""
    fill_date: str = ""
    item_date: str = ""
    value_raw: str = ""
    candidate_id: str = ""
    meta_json: str = ""


@dataclass
class RawExtractionResult:
    run_id: str
    output_dir: str
    candidates: List[RawMetricCandidate]
    accepted: List[RawMetricCandidate]
    issues: List[RawMetricIssue]
    date_audits: List[FillDateResolution]
    company_audits: List[CompanyResolution]
    summary: Dict[str, Any]
    output_files: List[str] = field(default_factory=list)
