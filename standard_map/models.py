from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from typing import Any


STANDARD_OUTPUT_COLUMNS = [
    "填表日期",
    "当前条目日期",
    "期间类型",
    "公司名",
    "原始指标名",
    "标准指标编码",
    "标准指标名称",
    "指标数值",
    "映射方法",
    "映射状态",
    "映射置信度",
    "口径关系",
    "口径说明",
    "是否需要人工校对",
]

DETAILED_OUTPUT_COLUMNS = [
    *STANDARD_OUTPUT_COLUMNS,
    "raw_metric_id",
    "source_page_no",
    "source_bbox_json",
    "source_pdf_path",
    "source_file",
    "provider",
    "doc_id",
    "source_cell_ref",
    "mapping_candidates_json",
    "llm_suggestion_json",
    "issue_reason",
]

CANDIDATE_OUTPUT_COLUMNS = [
    "raw_metric_id",
    "原始指标名",
    "candidate_rank",
    "candidate_code",
    "candidate_name",
    "candidate_score",
    "candidate_method",
    "relation_type",
    "review_required",
    "issue_reason",
]

ISSUE_OUTPUT_COLUMNS = [
    "issue_id",
    "raw_metric_id",
    "severity",
    "issue_type",
    "issue_reason",
    "填表日期",
    "当前条目日期",
    "期间类型",
    "公司名",
    "原始指标名",
    "指标数值",
]

REVIEW_ITEM_COLUMNS = [
    "review_item_id",
    "raw_metric_id",
    "填表日期",
    "当前条目日期",
    "期间类型",
    "公司名",
    "原始指标名",
    "指标数值",
    "candidate_code",
    "candidate_name",
    "candidate_score",
    "mapping_status",
    "mapping_method",
    "mapping_confidence",
    "relation_type",
    "issue_reason",
    "source_page_no",
    "source_bbox_json",
    "source_pdf_path",
    "system_candidate_code",
    "system_candidate_name",
    "system_candidate_score",
    "ai_suggestion_code",
    "ai_suggestion_name",
    "ai_confidence",
    "ai_reason",
    "ai_relation_type",
    "ai_review_required",
    "ai_validation_status",
    "suggestion_id",
    "suggestion_source",
    "mapping_decision",
    "action_default",
    "action_options",
]

LLM_SUGGESTION_COLUMNS = [
    "suggestion_id",
    "cache_key",
    "raw_metric_name",
    "context_json",
    "candidate_codes_json",
    "candidate_code",
    "candidate_name",
    "relation_type",
    "confidence",
    "review_required",
    "reason",
    "model_name",
    "prompt_hash",
    "response_json",
    "validation_status",
    "created_at",
    "from_cache",
]

LLM_SUGGESTION_AUDIT_COLUMNS = [
    *LLM_SUGGESTION_COLUMNS,
    "decision",
    "standard_code_valid",
    "candidate_rank",
    "issue_reason",
]

MAPPING_STATUSES = {"mapped", "review_required", "unmapped", "skipped"}
MAPPING_METHODS = {
    "exact",
    "alias",
    "local_alias",
    "legacy_alias",
    "candidate",
    "relation_review",
    "manual",
    "manual_once",
    "manual_saved",
    "llm_suggested",
    "none",
}
REVIEW_ACTION_OPTIONS = ["reject", "accept_once", "accept_and_remember"]
DECISION_VALUES = {"reject", "accept_once", "accept_and_remember"}
SAFE_DECISION_RELATION_TYPES = {"same_as", "exact_alias", "legacy_alias"}
REVIEW_RELATION_TYPES = {"broader_than", "narrower_than", "aggregate", "split", "formula", "ambiguous"}


def compact_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def serialize_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (list, dict, tuple)):
        return compact_json(value)
    return value


def dataclass_row(instance: Any) -> dict[str, Any]:
    if not is_dataclass(instance):
        raise TypeError(f"Expected dataclass instance, got {type(instance)!r}")
    return {item.name: serialize_value(getattr(instance, item.name)) for item in fields(instance)}


@dataclass(frozen=True)
class StandardTerm:
    code: str
    name: str
    category: str = ""
    aliases: tuple[str, ...] = ()
    statement_scope: str = ""
    metric_type: str = ""
    period_type: str = ""
    legacy_aliases: tuple[str, ...] = ()
    description: str = ""
    enabled: bool = True
    notes: str = ""


@dataclass(frozen=True)
class AliasEntry:
    term: StandardTerm
    alias: str
    alias_type: str
    safe_auto_map: bool = True
    source: str = "base"
    note: str = ""


@dataclass(frozen=True)
class TermRelation:
    relation_type: str
    relation_id: str = ""
    canonical_code: str = ""
    canonical_name: str = ""
    raw_names: tuple[str, ...] = ()
    related_names: tuple[str, ...] = ()
    candidate_codes: tuple[str, ...] = ()
    formula_json: str = ""
    auto_apply: bool = False
    review_required: bool = True
    enabled: bool = True
    note: str = ""


@dataclass
class StandardRegistry:
    terms: list[StandardTerm]
    aliases: list[AliasEntry]
    relations: list[TermRelation]
    registry_path: str
    aliases_path: str
    relations_path: str
    term_by_code: dict[str, StandardTerm] = field(default_factory=dict)
    normalized_term_lookup: dict[str, list[StandardTerm]] = field(default_factory=dict)
    normalized_alias_lookup: dict[str, list[AliasEntry]] = field(default_factory=dict)
    normalized_legacy_alias_lookup: dict[str, list[AliasEntry]] = field(default_factory=dict)


@dataclass
class RawMetricRow:
    row_number: int
    review_item_id: str
    raw_metric_id: str
    fill_date: Any
    item_date: Any
    company_name: str
    metric_name: str
    metric_value: Any
    period_role: Any = ""
    raw_row: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingCandidate:
    raw_metric_id: str
    raw_metric_name: str
    candidate_rank: int
    candidate_code: str
    candidate_name: str
    candidate_score: float
    candidate_method: str
    relation_type: str = ""
    review_required: bool = False
    issue_reason: str = ""

    def as_output_row(self) -> dict[str, Any]:
        return {
            "raw_metric_id": self.raw_metric_id,
            "原始指标名": self.raw_metric_name,
            "candidate_rank": self.candidate_rank,
            "candidate_code": self.candidate_code,
            "candidate_name": self.candidate_name,
            "candidate_score": self.candidate_score,
            "candidate_method": self.candidate_method,
            "relation_type": self.relation_type,
            "review_required": self.review_required,
            "issue_reason": self.issue_reason,
        }


@dataclass
class MappingResult:
    raw: RawMetricRow
    standard_code: str
    standard_name: str
    mapping_method: str
    mapping_status: str
    confidence: float | None = None
    relation_type: str = ""
    notes: str = ""
    review_required: bool = False
    issue_reason: str = ""
    candidates: list[MappingCandidate] = field(default_factory=list)
    llm_suggestion: dict[str, Any] | None = None

    def main_row(self) -> dict[str, Any]:
        return {
            "填表日期": self.raw.fill_date,
            "当前条目日期": self.raw.item_date,
            "期间类型": self.raw.period_role,
            "公司名": self.raw.company_name,
            "原始指标名": self.raw.metric_name,
            "标准指标编码": self.standard_code,
            "标准指标名称": self.standard_name,
            "指标数值": self.raw.metric_value,
            "映射方法": self.mapping_method,
            "映射状态": self.mapping_status,
            "映射置信度": self.confidence if self.confidence is not None else "",
            "口径关系": self.relation_type,
            "口径说明": self.notes,
            "是否需要人工校对": self.review_required,
        }

    def detailed_row(self) -> dict[str, Any]:
        row = self.main_row()
        candidate_rows = [candidate.as_output_row() for candidate in self.candidates]
        row.update(
            {
                "raw_metric_id": self.raw.raw_metric_id,
                "source_page_no": self.raw.provenance.get("source_page_no", ""),
                "source_bbox_json": self.raw.provenance.get("source_bbox_json", ""),
                "source_pdf_path": self.raw.provenance.get("source_pdf_path", ""),
                "source_file": self.raw.provenance.get("source_file", ""),
                "provider": self.raw.provenance.get("provider", ""),
                "doc_id": self.raw.provenance.get("doc_id", ""),
                "source_cell_ref": self.raw.provenance.get("source_cell_ref", ""),
                "mapping_candidates_json": compact_json(candidate_rows),
                "llm_suggestion_json": compact_json(self.llm_suggestion or {}),
                "issue_reason": self.issue_reason,
            }
        )
        return row

    def issue_row(self, issue_index: int) -> dict[str, Any] | None:
        if self.mapping_status == "mapped" and not self.review_required:
            return None
        issue_type = self.mapping_status
        if self.mapping_status == "review_required" and self.issue_reason:
            issue_type = self.issue_reason
        return {
            "issue_id": f"MAP_ISSUE_{issue_index:06d}",
            "raw_metric_id": self.raw.raw_metric_id,
            "severity": "warning" if self.mapping_status != "unmapped" else "error",
            "issue_type": issue_type,
            "issue_reason": self.issue_reason or self.notes,
            "填表日期": self.raw.fill_date,
            "当前条目日期": self.raw.item_date,
            "期间类型": self.raw.period_role,
            "公司名": self.raw.company_name,
            "原始指标名": self.raw.metric_name,
            "指标数值": self.raw.metric_value,
        }

    def review_item_row(self) -> dict[str, Any]:
        top = self.candidates[0] if self.candidates else None
        system_top = next((candidate for candidate in self.candidates if candidate.candidate_method != "llm_suggested"), None)
        display_top = top
        display_system_top = system_top
        if self.mapping_status == "unmapped" and self.mapping_method == "none":
            display_top = None
            display_system_top = None
        llm = self.llm_suggestion or {}
        if self.mapping_status == "mapped":
            action_default = "accept_once"
        elif display_top and display_top.candidate_code:
            action_default = "accept_once"
        else:
            action_default = "accept_once"
        if self.mapping_status == "skipped":
            action_default = "reject"
        return {
            "review_item_id": self.raw.review_item_id,
            "raw_metric_id": self.raw.raw_metric_id,
            "填表日期": self.raw.fill_date,
            "当前条目日期": self.raw.item_date,
            "期间类型": self.raw.period_role,
            "公司名": self.raw.company_name,
            "原始指标名": self.raw.metric_name,
            "指标数值": self.raw.metric_value,
            "candidate_code": display_top.candidate_code if display_top else self.standard_code,
            "candidate_name": display_top.candidate_name if display_top else self.standard_name,
            "candidate_score": display_top.candidate_score if display_top else "",
            "mapping_status": self.mapping_status,
            "mapping_method": self.mapping_method,
            "mapping_confidence": self.confidence if self.confidence is not None else (display_top.candidate_score if display_top else ""),
            "relation_type": self.relation_type or (display_top.relation_type if display_top else ""),
            "issue_reason": self.issue_reason or self.notes,
            "source_page_no": self.raw.provenance.get("source_page_no", ""),
            "source_bbox_json": self.raw.provenance.get("source_bbox_json", ""),
            "source_pdf_path": self.raw.provenance.get("source_pdf_path", ""),
            "system_candidate_code": display_system_top.candidate_code if display_system_top else "",
            "system_candidate_name": display_system_top.candidate_name if display_system_top else "",
            "system_candidate_score": display_system_top.candidate_score if display_system_top else "",
            "ai_suggestion_code": llm.get("candidate_code", ""),
            "ai_suggestion_name": llm.get("candidate_name", ""),
            "ai_confidence": llm.get("confidence", ""),
            "ai_reason": llm.get("reason", ""),
            "ai_relation_type": llm.get("relation_type", ""),
            "ai_review_required": llm.get("review_required", ""),
            "ai_validation_status": llm.get("validation_status", ""),
            "suggestion_id": llm.get("suggestion_id", ""),
            "suggestion_source": "AI建议" if llm else "",
            "mapping_decision": "",
            "action_default": action_default,
            "action_options": REVIEW_ACTION_OPTIONS,
        }


@dataclass
class StandardMappingRun:
    run_id: str
    input_path: str
    output_dir: str
    doc_id: str
    rows: list[MappingResult]
    summary: dict[str, Any]
    manifest: dict[str, Any]
    output_files: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
