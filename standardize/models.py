from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from typing import Any, Dict, List, Optional


def compact_json(value: Any) -> str:
    """Serialize arbitrary values into compact, deterministic JSON."""

    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def serialize_value(value: Any) -> Any:
    """Serialize nested structures for flat CSV export."""

    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return compact_json(value)
    return value


def dataclass_row(instance: Any) -> Dict[str, Any]:
    """Convert a dataclass instance into a flat CSV-friendly dictionary."""

    if not is_dataclass(instance):
        raise TypeError(f"Expected dataclass instance, got {type(instance)!r}")

    row: Dict[str, Any] = {}
    for item in fields(instance):
        row[item.name] = serialize_value(getattr(instance, item.name))
    return row


@dataclass
class DiscoveredSource:
    doc_id: str
    page_no: int
    provider: str
    provider_family: str
    provider_dir: str
    raw_file: Optional[str] = None
    artifact_file: Optional[str] = None
    result_json_file: Optional[str] = None
    result_page_meta: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass
class ProviderCell:
    table_id: str
    row_start: int
    row_end: int
    col_start: int
    col_end: int
    text: str
    bbox: Optional[List[Dict[str, Any]]] = None
    confidence: Optional[float] = None
    cell_type: str = "body"
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderPage:
    doc_id: str
    page_no: int
    provider: str
    source_file: str
    source_kind: str
    page_text: str
    tables: Dict[str, List[ProviderCell]]
    context_lines: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StatementMeta:
    statement_type: str
    statement_name_raw: str
    report_date_raw: str
    report_date_norm: str
    unit_raw: str
    unit_multiplier: float


@dataclass
class LogicalSubtable:
    doc_id: str
    page_no: int
    provider: str
    source_file: str
    table_id: str
    logical_subtable_id: str
    table_semantic_key: str
    start_col: int
    end_col: int
    max_row: int
    header_row_count: int
    statement_meta: StatementMeta
    grid: List[List["CellRecord"]]
    header_paths: Dict[int, List[str]]
    row_label_col: int
    line_no_col: Optional[int]
    value_cols: List[int]
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CellRecord:
    doc_id: str
    page_no: int
    provider: str
    source_file: str
    table_id: str
    logical_subtable_id: str
    row_start: int
    row_end: int
    col_start: int
    col_end: int
    bbox_json: str
    text_raw: str
    text_clean: str
    ocr_conf: Optional[float]
    is_empty: bool
    is_header: bool
    is_suspicious: bool
    suspicious_reason: str
    repair_status: str
    meta_json: str


@dataclass
class FactRecord:
    doc_id: str
    page_no: int
    provider: str
    statement_type: str
    statement_name_raw: str
    logical_subtable_id: str
    table_semantic_key: str
    row_label_raw: str
    row_label_std: str
    col_header_raw: str
    col_header_path: List[str]
    column_semantic_key: str
    period_role_raw: str
    report_date_raw: str
    period_key: str
    value_raw: str
    value_num: Optional[float]
    value_type: str
    unit_raw: str
    unit_multiplier: float
    source_cell_ref: str
    status: str
    mapping_code: str
    mapping_name: str
    mapping_method: str
    mapping_confidence: Optional[float]
    issue_flags: List[str]


@dataclass
class IssueRecord:
    doc_id: str
    page_no: int
    provider: str
    source_file: str
    table_id: str
    logical_subtable_id: str
    source_cell_ref: str
    issue_type: str
    severity: str
    message: str
    text_raw: str
    text_clean: str
    status: str
    meta_json: str


@dataclass
class ConflictRecord:
    doc_id: str
    page_no: int
    logical_subtable_id: str
    table_semantic_key: str
    statement_type: str
    row_label_std: str
    column_semantic_key: str
    period_key: str
    provider_values_json: str
    decision: str
    accepted_provider: str
    reason: str
    meta_json: str


@dataclass
class MappingReviewRecord:
    doc_id: str
    page_no: int
    provider: str
    statement_type: str
    row_label_raw: str
    row_label_std: str
    best_code: str
    best_name: str
    candidate_codes_json: str
    reason: str
    source_cell_ref: str
    status: str


@dataclass
class TemplateSubject:
    code: str
    canonical_name: str
    row_index: int
    sheet_name: str
    source_value: str
