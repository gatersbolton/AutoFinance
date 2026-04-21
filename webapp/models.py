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

OPERATION_STATUS_CREATED = "created"
OPERATION_STATUS_QUEUED = "queued"
OPERATION_STATUS_RUNNING = "running"
OPERATION_STATUS_SUCCEEDED = "succeeded"
OPERATION_STATUS_FAILED = "failed"
OPERATION_STATUS_CANCELLED = "cancelled"

ACTIVE_OPERATION_STATUSES = {
    OPERATION_STATUS_CREATED,
    OPERATION_STATUS_QUEUED,
    OPERATION_STATUS_RUNNING,
}
TERMINAL_OPERATION_STATUSES = {
    OPERATION_STATUS_SUCCEEDED,
    OPERATION_STATUS_FAILED,
    OPERATION_STATUS_CANCELLED,
}

OPERATION_TYPE_APPLY_REVIEW_ACTIONS = "apply_review_actions"
OPERATION_TYPE_APPLY_AND_RERUN = "apply_and_rerun"
OPERATION_TYPE_RERUN_ONLY = "rerun_only"

REVIEW_OPERATION_TYPES = {
    OPERATION_TYPE_APPLY_REVIEW_ACTIONS,
    OPERATION_TYPE_APPLY_AND_RERUN,
    OPERATION_TYPE_RERUN_ONLY,
}


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
    queue_backend: str
    operation_timeout_seconds: int

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

REVIEW_COMPATIBILITY_BACKEND_READY = "backend_ready"
REVIEW_COMPATIBILITY_PARTIAL = "partial"
REVIEW_COMPATIBILITY_SUGGESTION_ONLY = "suggestion_only"
REVIEW_COMPATIBILITY_UNSUPPORTED = "unsupported"

REVIEW_COMPATIBILITY_STATUSES = {
    REVIEW_COMPATIBILITY_BACKEND_READY,
    REVIEW_COMPATIBILITY_PARTIAL,
    REVIEW_COMPATIBILITY_SUGGESTION_ONLY,
    REVIEW_COMPATIBILITY_UNSUPPORTED,
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
    backend_review_id: str = ""
    apply_target_type: str = ""
    apply_compatibility_status: str = ""
    apply_incompatibility_reason: str = ""
    priority_bucket: str = ""
    evidence_available: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class ReviewOperationRecord:
    operation_id: str
    job_id: str
    operation_type: str
    queue_backend: str
    status: str
    created_at: str
    updated_at: str
    started_at: str
    finished_at: str
    duration_seconds: float
    progress_stage: str
    progress_message_zh: str
    error_message: str
    user_friendly_error_zh: str
    log_paths: list[str] = field(default_factory=list)
    result_paths: list[str] = field(default_factory=list)
    operation_dir: str = ""
    summary_path: str = ""
    timeline_path: str = ""
    retry_of_operation_id: str = ""
    retry_count: int = 0
    cancel_requested: bool = False
    cancel_acknowledged: bool = False
    queue_job_id: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> "ReviewOperationRecord":
        import json

        def _load_json_list(value: object) -> list[str]:
            raw = str(value or "").strip()
            if not raw:
                return []
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return []
            if not isinstance(parsed, list):
                return []
            return [str(item).strip() for item in parsed if str(item).strip()]

        def _load_json_object(value: object) -> dict[str, Any]:
            raw = str(value or "").strip()
            if not raw:
                return {}
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}

        return cls(
            operation_id=str(row["operation_id"]),
            job_id=str(row["job_id"]),
            operation_type=str(row["operation_type"]),
            queue_backend=str(row["queue_backend"]),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            started_at=str(row["started_at"] or ""),
            finished_at=str(row["finished_at"] or ""),
            duration_seconds=float(row["duration_seconds"] or 0.0),
            progress_stage=str(row["progress_stage"] or ""),
            progress_message_zh=str(row["progress_message_zh"] or ""),
            error_message=str(row["error_message"] or ""),
            user_friendly_error_zh=str(row["user_friendly_error_zh"] or ""),
            log_paths=_load_json_list(row["log_paths_json"]),
            result_paths=_load_json_list(row["result_paths_json"]),
            operation_dir=str(row["operation_dir"] or ""),
            summary_path=str(row["summary_path"] or ""),
            timeline_path=str(row["timeline_path"] or ""),
            retry_of_operation_id=str(row["retry_of_operation_id"] or ""),
            retry_count=int(row["retry_count"] or 0),
            cancel_requested=bool(int(row["cancel_requested"] or 0)),
            cancel_acknowledged=bool(int(row["cancel_acknowledged"] or 0)),
            queue_job_id=str(row["queue_job_id"] or ""),
            extra=_load_json_object(row["extra_json"]),
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def as_db_dict(self) -> dict[str, object]:
        import json

        payload = asdict(self)
        payload["log_paths_json"] = json.dumps(self.log_paths, ensure_ascii=False, separators=(",", ":"))
        payload["result_paths_json"] = json.dumps(self.result_paths, ensure_ascii=False, separators=(",", ":"))
        payload["extra_json"] = json.dumps(self.extra, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        payload["cancel_requested"] = 1 if self.cancel_requested else 0
        payload["cancel_acknowledged"] = 1 if self.cancel_acknowledged else 0
        payload.pop("log_paths", None)
        payload.pop("result_paths", None)
        payload.pop("extra", None)
        return payload
