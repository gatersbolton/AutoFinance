from __future__ import annotations

import json
from dataclasses import dataclass, field, fields, is_dataclass
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
    report_date_candidates_json: str = ""
    statement_group_key: str = ""
    source_level: str = ""
    reason: str = ""
    statement_type_source: str = ""
    statement_type_score: float = 0.0
    statement_type_reason: str = ""


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
    row_label_norm: str
    row_label_canonical_candidate: str
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
    fact_id: str = ""
    report_date_norm: str = ""
    period_role_norm: str = ""
    period_source_level: str = ""
    period_reason: str = ""
    duplicate_group_id: str = ""
    kept_fact_id: str = ""
    comparison_status: str = ""
    comparison_reason: str = ""
    source_kind: str = ""
    statement_group_key: str = ""
    source_row_start: int = 0
    source_row_end: int = 0
    source_col_start: int = 0
    source_col_end: int = 0
    mapping_relation_type: str = ""
    mapping_review_required: bool = False
    conflict_id: str = ""
    conflict_decision: str = ""
    unplaced_reason: str = ""
    review_id: str = ""
    suppression_reason: str = ""
    override_source: str = ""
    parent_review_id: str = ""
    parent_task_id: str = ""
    normalization_rule_ids: List[str] = field(default_factory=list)
    statement_type_source: str = ""
    statement_type_score: float = 0.0
    statement_type_reason: str = ""
    period_role_inference_reason: str = ""
    period_role_inference_source: str = ""
    target_scope: str = ""
    target_scope_reason: str = ""
    benchmark_alignment_status: str = ""
    benchmark_alignment_reason: str = ""


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
    conflict_id: str = ""
    compared_pair_count: int = 0
    providers: str = ""
    candidate_values_json: str = ""
    magnitude_ratio: Optional[float] = None
    validation_delta: str = ""
    needs_review: bool = False
    accepted_fact_id: str = ""


@dataclass
class DuplicateRecord:
    duplicate_group_id: str
    doc_id: str
    statement_type: str
    statement_group_key: str
    period_key: str
    canonical_key: str
    kept_fact_id: str
    dropped_fact_id: str
    kept_provider: str
    dropped_provider: str
    kept_source_cell_ref: str
    dropped_source_cell_ref: str
    dedupe_reason: str
    decision: str
    meta_json: str


@dataclass
class ProviderComparisonRecord:
    doc_id: str
    page_no: int
    providers_present: str
    aligned_groups: int
    compared_pairs: int
    equal_pairs: int
    conflict_pairs: int
    uncomparable_groups: int
    reason: str
    meta_json: str


@dataclass
class ValidationResultRecord:
    validation_id: str
    doc_id: str
    statement_type: str
    period_key: str
    rule_name: str
    rule_type: str
    lhs_value: Optional[float]
    rhs_value: Optional[float]
    diff_value: Optional[float]
    tolerance: Optional[float]
    status: str
    evidence_fact_refs: List[str]
    message: str
    meta_json: str


@dataclass
class PageSelectionRecord:
    doc_id: str
    page_no: int
    source_file: str
    table_likelihood_score: float
    numeric_density_score: float
    line_density_score: float
    keyword_score: float
    is_candidate_table_page: bool
    selection_reason: str
    meta_json: str


@dataclass
class SecondaryOCRCandidateRecord:
    doc_id: str
    page_no: int
    providers_present: str
    provider_comparison_coverage: float
    trigger_score: float
    trigger_reasons: List[str]
    recommend_secondary_ocr: bool
    reason: str
    meta_json: str


@dataclass
class RunSummaryRecord:
    docs_total: int
    pages_total: int
    pages_with_tables: int
    pages_skipped_as_non_table: int
    tables_total: int
    cells_total: int
    facts_raw_total: int
    facts_deduped_total: int
    mapped_facts_total: int
    mapped_facts_ratio: float
    unknown_date_total: int
    unknown_date_ratio: float
    suspicious_cells_total: int
    repaired_facts_total: int
    review_facts_total: int
    duplicates_total: int
    duplicate_groups_total: int
    provider_compared_pairs: int
    provider_equal_pairs: int
    provider_conflict_pairs: int
    validation_total: int
    validation_pass_total: int
    validation_fail_total: int


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
class MappingCandidateRecord:
    doc_id: str
    page_no: int
    provider: str
    statement_type: str
    row_label_raw: str
    row_label_std: str
    normalized_label: str
    candidate_code: str
    candidate_name: str
    candidate_rank: int
    candidate_score: float
    candidate_method: str
    relation_type: str
    review_required: bool
    source_cell_ref: str
    meta_json: str


@dataclass
class UnmappedLabelSummaryRecord:
    row_label_std: str
    normalized_label: str
    occurrences: int
    numeric_occurrences: int
    amount_abs_total: float
    example_source_cell_ref: str
    top_candidate_code: str
    top_candidate_name: str
    top_candidate_score: float
    top_candidate_method: str
    meta_json: str


@dataclass
class TemplateSubject:
    code: str
    canonical_name: str
    row_index: int
    sheet_name: str
    source_value: str


@dataclass
class AliasRecord:
    canonical_code: str
    canonical_name: str
    alias: str
    alias_type: str
    enabled: bool
    statement_types: List[str] = field(default_factory=list)
    note: str = ""


@dataclass
class RelationRecord:
    canonical_code: str
    canonical_name: str
    relation_type: str
    related_codes: List[str]
    related_names: List[str]
    enabled: bool
    review_required: bool
    note: str = ""


@dataclass
class ConflictDecisionAuditRecord:
    conflict_id: str
    doc_id: str
    page_no: int
    statement_type: str
    period_key: str
    providers: str
    compared_pair_count: int
    candidate_values_json: str
    magnitude_ratio: Optional[float]
    decision: str
    decision_reason: str
    accepted_fact_id: str
    needs_review: bool
    validation_delta: str
    meta_json: str


@dataclass
class ValidationImpactRecord:
    conflict_id: str
    candidate_provider: str
    candidate_fact_id: str
    doc_id: str
    statement_type: str
    period_key: str
    fail_count: int
    review_count: int
    impacted_rules_json: str
    delta_score: float
    meta_json: str


@dataclass
class ReviewQueueRecord:
    review_id: str
    priority_score: float
    reason_codes: List[str]
    doc_id: str
    page_no: int
    statement_type: str
    row_label_raw: str
    row_label_std: str
    period_key: str
    value_raw: str
    value_num: Optional[float]
    provider: str
    source_file: str
    bbox: str
    related_fact_ids: List[str]
    related_conflict_ids: List[str]
    related_validation_ids: List[str]
    mapping_candidates: str
    evidence_cell_path: str
    evidence_row_path: str
    evidence_table_path: str
    meta_json: str


@dataclass
class ReOCRTaskRecord:
    task_id: str
    granularity: str
    doc_id: str
    page_no: int
    table_id: str
    logical_subtable_id: str
    bbox: str
    reason_codes: List[str]
    suggested_provider: str
    priority_score: float
    expected_benefit: str
    source_review_id: str
    meta_json: str


@dataclass
class ArtifactIntegrityRecord:
    check_id: str
    check_name: str
    severity: str
    status: str
    message: str
    meta_json: str
