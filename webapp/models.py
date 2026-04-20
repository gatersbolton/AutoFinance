from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping


JOB_STATUS_CREATED = "created"
JOB_STATUS_QUEUED = "queued"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_SUCCEEDED_WITH_WARNINGS = "succeeded_with_warnings"
JOB_STATUS_NEEDS_REVIEW = "needs_review"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"

JOB_MODE_EXISTING = "existing_ocr_outputs"
JOB_MODE_UPLOAD = "upload_pdf"

ACTIVE_JOB_STATUSES = {JOB_STATUS_QUEUED, JOB_STATUS_RUNNING}
WARNING_JOB_STATUSES = {JOB_STATUS_SUCCEEDED_WITH_WARNINGS, JOB_STATUS_NEEDS_REVIEW}
SUCCESS_LIKE_JOB_STATUSES = {JOB_STATUS_SUCCEEDED, *WARNING_JOB_STATUSES}


@dataclass(slots=True)
class JobRecord:
    job_id: str
    display_name: str
    mode: str
    provider_mode: str
    input_path: str
    source_image_dir: str
    upload_dir: str
    ocr_output_dir: str
    template_path: str
    output_dir: str
    result_dir: str
    log_dir: str
    provider_priority: str
    status: str
    current_stage: str
    progress_summary: str
    created_at: str
    updated_at: str
    started_at: str
    finished_at: str
    error_message: str
    raw_error_message: str
    user_friendly_error: str
    recommended_action: str
    run_id: str
    command_executed: str
    exit_code: int | None
    timeout_seconds: int

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> "JobRecord":
        return cls(
            job_id=str(row["job_id"]),
            display_name=str(row["display_name"]),
            mode=str(row["mode"]),
            provider_mode=str(row["provider_mode"]),
            input_path=str(row["input_path"]),
            source_image_dir=str(row["source_image_dir"]),
            upload_dir=str(row["upload_dir"]),
            ocr_output_dir=str(row["ocr_output_dir"]),
            template_path=str(row["template_path"]),
            output_dir=str(row["output_dir"]),
            result_dir=str(row["result_dir"]),
            log_dir=str(row["log_dir"]),
            provider_priority=str(row["provider_priority"]),
            status=str(row["status"]),
            current_stage=str(row["current_stage"]),
            progress_summary=str(row["progress_summary"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            started_at=str(row["started_at"] or ""),
            finished_at=str(row["finished_at"] or ""),
            error_message=str(row["error_message"] or ""),
            raw_error_message=str(row["raw_error_message"] or ""),
            user_friendly_error=str(row["user_friendly_error"] or ""),
            recommended_action=str(row["recommended_action"] or ""),
            run_id=str(row["run_id"] or ""),
            command_executed=str(row["command_executed"] or ""),
            exit_code=int(row["exit_code"]) if row["exit_code"] is not None else None,
            timeout_seconds=int(row["timeout_seconds"]),
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class OutputArtifact:
    slug: str
    label: str
    path: str
    relative_path: str
    exists: bool
    size_bytes: int
    download_name: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class SystemStatusRecord:
    app_name: str
    app_version: str
    environment: str
    python_version: str
    template_path: str
    template_exists: bool
    runtime_directories: dict[str, bool]
    available_provider_modes: list[str]
    redis_configured: bool
    ocr_credentials: dict[str, object]
    local_worker_enabled: bool
    auth_enabled: bool
    auth_required: bool
    worker_mode: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


REVIEW_STATUS_UNRESOLVED = "unresolved"
REVIEW_STATUS_RESOLVED = "resolved"
REVIEW_STATUS_IGNORED = "ignored"
REVIEW_STATUS_DEFERRED = "deferred"
REVIEW_STATUS_REOCR_REQUESTED = "reocr_requested"

REVIEW_ITEM_SOURCE_TYPES = {
    "review_queue",
    "issue",
    "validation",
    "conflict",
    "unplaced_fact",
    "mapping_candidate",
}

REVIEW_ACTION_TYPES = {
    "ignore",
    "defer",
    "mark_not_financial_fact",
    "request_reocr",
    "accept_mapping_candidate",
    "set_mapping_override",
    "set_conflict_winner",
    "suppress_false_positive",
}


@dataclass(slots=True)
class ReviewSourceArtifact:
    slug: str
    label: str
    path: str
    relative_path: str
    exists: bool
    row_count: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class ReviewActionRecord:
    job_id: str
    review_item_id: str
    action_type: str
    action_value: str
    reviewer_note: str
    reviewer_name: str
    review_status: str
    source_type: str
    source_ref: str
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> "ReviewActionRecord":
        return cls(
            job_id=str(row["job_id"]),
            review_item_id=str(row["review_item_id"]),
            action_type=str(row["action_type"] or ""),
            action_value=str(row["action_value"] or ""),
            reviewer_note=str(row["reviewer_note"] or ""),
            reviewer_name=str(row["reviewer_name"] or ""),
            review_status=str(row["review_status"] or ""),
            source_type=str(row["source_type"] or ""),
            source_ref=str(row["source_ref"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class ReviewItemRecord:
    review_item_id: str
    review_id: str
    source_file: str
    source_type: str
    priority_score: float
    reason_code: str
    reason_codes: list[str] = field(default_factory=list)
    reason_label_zh: str = ""
    doc_id: str = ""
    page_no: int | None = None
    statement_type: str = ""
    row_label_raw: str = ""
    row_label_std: str = ""
    mapping_code: str = ""
    mapping_name: str = ""
    period_key: str = ""
    value_raw: str = ""
    value_num: float | None = None
    provider: str = ""
    source_cell_ref: str = ""
    evidence_path: str = ""
    evidence_label: str = ""
    current_status: str = REVIEW_STATUS_UNRESOLVED
    action_type: str = ""
    action_value: str = ""
    reviewer_note: str = ""
    reviewer_name: str = ""
    source_ref: str = ""
    related_conflict_ids: list[str] = field(default_factory=list)
    related_validation_ids: list[str] = field(default_factory=list)
    related_fact_ids: list[str] = field(default_factory=list)
    candidate_conflict_fact_id: str = ""
    candidate_period_override: str = ""
    suggested_reocr_task_id: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)
