from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


STATUS_NOT_STARTED = "not_started"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

VALID_STEP_STATUSES = {
    STATUS_NOT_STARTED,
    STATUS_RUNNING,
    STATUS_COMPLETED,
    STATUS_FAILED,
}

OCR_STATUS_LABELS_ZH = {
    STATUS_NOT_STARTED: "未识别",
    STATUS_RUNNING: "正在识别",
    STATUS_COMPLETED: "已识别",
    STATUS_FAILED: "识别失败",
}

RAW_METRICS_STATUS_LABELS_ZH = {
    STATUS_NOT_STARTED: "未提取",
    STATUS_RUNNING: "正在提取",
    STATUS_COMPLETED: "已提取原始数据",
    STATUS_FAILED: "提取失败",
}

STANDARD_METRICS_STATUS_LABELS_ZH = {
    STATUS_NOT_STARTED: "未生成",
    STATUS_RUNNING: "正在生成",
    STATUS_COMPLETED: "已生成标准化数据",
    STATUS_FAILED: "生成失败",
}


@dataclass(slots=True)
class DocumentRecord:
    doc_id: str
    display_name: str
    original_filename: str
    pdf_path: str
    input_dir: str
    ocr_output_dir: str
    latest_job_id: str
    ocr_status: str
    raw_metrics_status: str
    standard_metrics_status: str
    created_at: str
    updated_at: str
    metadata_path: str
    deleted_at: str = ""
    error_message: str = ""

    @classmethod
    def from_metadata(cls, payload: dict[str, Any], *, metadata_path: str) -> "DocumentRecord":
        return cls(
            doc_id=str(payload.get("doc_id", "")),
            display_name=str(payload.get("display_name", "") or payload.get("original_filename", "")),
            original_filename=str(payload.get("original_filename", "")),
            pdf_path=str(payload.get("pdf_path", "")),
            input_dir=str(payload.get("input_dir", "")),
            ocr_output_dir=str(payload.get("ocr_output_dir", "")),
            latest_job_id=str(payload.get("latest_job_id", "")),
            ocr_status=_valid_status(payload.get("ocr_status")),
            raw_metrics_status=_valid_status(payload.get("raw_metrics_status")),
            standard_metrics_status=_valid_status(payload.get("standard_metrics_status")),
            created_at=str(payload.get("created_at", "")),
            updated_at=str(payload.get("updated_at", "")),
            deleted_at=str(payload.get("deleted_at", "")),
            metadata_path=metadata_path,
            error_message=str(payload.get("error_message", "")),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def has_ocr(self) -> bool:
        return self.ocr_status == STATUS_COMPLETED

    @property
    def document_status_label_zh(self) -> str:
        if STATUS_FAILED in {self.ocr_status, self.raw_metrics_status, self.standard_metrics_status}:
            return "处理失败"
        if self.standard_metrics_status == STATUS_COMPLETED:
            return "已生成标准化数据"
        if self.raw_metrics_status == STATUS_COMPLETED:
            return "已提取原始数据"
        if self.ocr_status == STATUS_COMPLETED:
            return "已识别，可继续处理"
        if self.ocr_status == STATUS_RUNNING:
            return "正在识别"
        return "未识别"

    @property
    def ocr_status_label_zh(self) -> str:
        return OCR_STATUS_LABELS_ZH.get(self.ocr_status, self.ocr_status)

    @property
    def raw_metrics_status_label_zh(self) -> str:
        return RAW_METRICS_STATUS_LABELS_ZH.get(self.raw_metrics_status, self.raw_metrics_status)

    @property
    def standard_metrics_status_label_zh(self) -> str:
        return STANDARD_METRICS_STATUS_LABELS_ZH.get(self.standard_metrics_status, self.standard_metrics_status)

    @property
    def updated_at_label_zh(self) -> str:
        raw_value = self.updated_at or self.created_at
        if not raw_value:
            return ""
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return raw_value[:16].replace("T", " ")
        local_dt = parsed.astimezone() if parsed.tzinfo else parsed
        now = datetime.now(local_dt.tzinfo) if local_dt.tzinfo else datetime.now()
        return local_dt.strftime("%m-%d %H:%M") if local_dt.year == now.year else local_dt.strftime("%Y-%m-%d %H:%M")


def _valid_status(value: object) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in VALID_STEP_STATUSES else STATUS_NOT_STARTED
