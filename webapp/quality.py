from __future__ import annotations

import json
from pathlib import Path

from .models import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_CREATED,
    JOB_STATUS_FAILED,
    JOB_STATUS_NEEDS_REVIEW,
    JOB_STATUS_QUEUED,
    JOB_STATUS_RUNNING,
    JOB_STATUS_SUCCEEDED,
    JOB_STATUS_SUCCEEDED_WITH_WARNINGS,
    SUCCESS_LIKE_JOB_STATUSES,
    WARNING_JOB_STATUSES,
    JobRecord,
)


FILLED_WORKBOOK_NAME = "会计报表_填充结果.xlsx"


def load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def describe_job_status(status: str) -> str:
    mapping = {
        JOB_STATUS_CREATED: "已创建",
        JOB_STATUS_QUEUED: "排队中",
        JOB_STATUS_RUNNING: "处理中",
        JOB_STATUS_SUCCEEDED: "已完成",
        JOB_STATUS_SUCCEEDED_WITH_WARNINGS: "完成但有警告",
        JOB_STATUS_NEEDS_REVIEW: "完成但建议复核",
        JOB_STATUS_FAILED: "处理失败",
        JOB_STATUS_CANCELLED: "已取消",
    }
    return mapping.get(status, status)


def status_tone(status: str) -> str:
    if status == JOB_STATUS_SUCCEEDED:
        return "success"
    if status in WARNING_JOB_STATUSES:
        return "warning"
    if status == JOB_STATUS_FAILED:
        return "danger"
    return "neutral"


def translate_job_issue(
    raw_error_message: str,
    *,
    exit_code: int | None,
    workbook_generated: bool = False,
    run_summary_present: bool = False,
    artifact_integrity_fail_total: int = 0,
    full_run_contract_fail_total: int = 0,
    review_total: int = 0,
    validation_fail_total: int = 0,
) -> tuple[str, str]:
    text = (raw_error_message or "").strip()
    lower = text.lower()

    if "template workbook does not exist" in lower or ("模板" in text and "不存在" in text):
        return (
            "模板文件不存在，无法启动处理。",
            "请检查 WEBAPP_TEMPLATE_PATH 或 data/templates/会计报表.xlsx 是否存在。",
        )
    if "input directory does not exist" in lower or "ocr 输出目录不存在" in text:
        return (
            "OCR 输出目录不存在，任务无法开始。",
            "请确认选择的是有效的 OCR 输出目录，并且目录位于 data/corpus/ 或 data/generated/web/ 下。",
        )
    if "source image directory does not exist" in lower:
        return (
            "源图片目录不存在，标准化命令无法继续。",
            "请检查原始 PDF 所在目录是否仍然可用，或重新选择一份包含 input/ 的样本。",
        )
    if "云 ocr 凭据" in text.lower() or "credentials" in lower:
        return (
            "当前环境缺少云 OCR 凭据，无法处理上传文件。",
            "请在部署环境中配置云 OCR 凭据，或改用已有 OCR 输出目录创建任务。",
        )
    if "ocr 子进程执行失败" in text.lower() or ("ocr" in lower and exit_code not in (None, 0)):
        return (
            "OCR 阶段执行失败。",
            "请先下载 OCR 日志，核对云 OCR 密钥、PDF 文件和网络连通性后再重试。",
        )
    if "标准化子进程执行失败" in text or "standardize" in lower and exit_code not in (None, 0):
        return (
            "标准化命令执行失败。",
            "请先下载日志，核对 OCR 输出目录、模板文件和标准化配置是否正确。",
        )
    if exit_code not in (None, 0):
        return (
            "后台命令执行失败。",
            "请下载日志并联系管理员排查执行环境。",
        )
    if not workbook_generated:
        return (
            "处理已结束，但没有生成会计报表。",
            "请下载日志并检查输入 OCR 结果是否完整，必要时重新运行任务。",
        )
    if review_total > 0 or validation_fail_total > 0:
        return (
            "结果已生成，但存在需要人工复核的问题。",
            "请下载人工复核表与问题清单，先处理需要复核的项目后再交付。",
        )
    if artifact_integrity_fail_total > 0 or full_run_contract_fail_total > 0:
        return (
            "结果已生成，但存在完整性检查警告。",
            "请优先查看质量摘要和日志，确认完整性警告是否影响交付。",
        )
    if run_summary_present:
        return (
            "处理已完成。",
            "可以先下载会计报表，再按需查看质量摘要和日志。",
        )
    return (
        "任务状态已更新。",
        "请查看任务详情中的日志和输出文件。",
    )


def classify_final_job_status(
    *,
    exit_code: int | None,
    workbook_generated: bool,
    run_summary_present: bool,
    artifact_integrity_fail_total: int,
    artifact_integrity_review_total: int,
    full_run_contract_fail_total: int,
    review_total: int,
    validation_fail_total: int,
) -> str:
    if exit_code not in (None, 0):
        return JOB_STATUS_FAILED
    if not workbook_generated or not run_summary_present:
        return JOB_STATUS_FAILED
    if review_total > 0 or validation_fail_total > 0:
        return JOB_STATUS_NEEDS_REVIEW
    if artifact_integrity_fail_total > 0 or full_run_contract_fail_total > 0 or artifact_integrity_review_total > 0:
        return JOB_STATUS_SUCCEEDED_WITH_WARNINGS
    return JOB_STATUS_SUCCEEDED


def build_job_quality_summary(
    job: JobRecord,
    *,
    command_exit_code: int | None,
    raw_error_message: str = "",
    user_friendly_error: str = "",
    recommended_action: str = "",
) -> dict[str, object]:
    output_dir = Path(job.output_dir)
    workbook_generated = (output_dir / FILLED_WORKBOOK_NAME).exists()
    run_summary = load_json(output_dir / "run_summary.json")
    artifact_integrity = load_json(output_dir / "artifact_integrity.json")
    full_run_contract = load_json(output_dir / "full_run_contract_summary.json")
    review_summary = load_json(output_dir / "review_summary.json")
    validation_summary = load_json(output_dir / "validation_summary.json")

    run_summary_present = bool(run_summary)
    artifact_integrity_fail_total = int(
        artifact_integrity.get("integrity_fail_total", run_summary.get("integrity_fail_total", 0)) or 0
    )
    artifact_integrity_review_total = int(artifact_integrity.get("integrity_review_total", 0) or 0)
    full_run_contract_fail_total = int(full_run_contract.get("contract_fail_total", 0) or 0)
    review_total = int(review_summary.get("review_total", run_summary.get("review_total", 0)) or 0)
    validation_fail_total = int(validation_summary.get("validation_fail_total", run_summary.get("validation_fail_total", 0)) or 0)

    inferred_raw_error = raw_error_message
    if not inferred_raw_error and any(
        value > 0
        for value in (
            artifact_integrity_fail_total,
            artifact_integrity_review_total,
            full_run_contract_fail_total,
            review_total,
            validation_fail_total,
        )
    ):
        inferred_raw_error = (
            f"artifact_integrity_fail_total={artifact_integrity_fail_total}; "
            f"artifact_integrity_review_total={artifact_integrity_review_total}; "
            f"full_run_contract_fail_total={full_run_contract_fail_total}; "
            f"review_total={review_total}; validation_fail_total={validation_fail_total}"
        )

    final_job_status = classify_final_job_status(
        exit_code=command_exit_code,
        workbook_generated=workbook_generated,
        run_summary_present=run_summary_present,
        artifact_integrity_fail_total=artifact_integrity_fail_total,
        artifact_integrity_review_total=artifact_integrity_review_total,
        full_run_contract_fail_total=full_run_contract_fail_total,
        review_total=review_total,
        validation_fail_total=validation_fail_total,
    )

    if not user_friendly_error or not recommended_action:
        translated_error, translated_action = translate_job_issue(
            inferred_raw_error,
            exit_code=command_exit_code,
            workbook_generated=workbook_generated,
            run_summary_present=run_summary_present,
            artifact_integrity_fail_total=artifact_integrity_fail_total,
            full_run_contract_fail_total=full_run_contract_fail_total,
            review_total=review_total,
            validation_fail_total=validation_fail_total,
        )
        user_friendly_error = user_friendly_error or translated_error
        recommended_action = recommended_action or translated_action

    return {
        "job_id": job.job_id,
        "command_exit_code": command_exit_code,
        "workbook_generated": workbook_generated,
        "run_summary_present": run_summary_present,
        "artifact_integrity_fail_total": artifact_integrity_fail_total,
        "artifact_integrity_review_total": artifact_integrity_review_total,
        "full_run_contract_fail_total": full_run_contract_fail_total,
        "review_total": review_total,
        "validation_fail_total": validation_fail_total,
        "raw_error_message": inferred_raw_error,
        "user_friendly_error": user_friendly_error,
        "recommended_user_action": recommended_action,
        "final_job_status": final_job_status,
        "status_label": describe_job_status(final_job_status),
        "status_tone": status_tone(final_job_status),
    }


def summarize_for_operator(job: JobRecord, quality_summary: dict[str, object]) -> dict[str, object]:
    final_status = str(quality_summary.get("final_job_status", job.status))
    workbook_generated = bool(quality_summary.get("workbook_generated", False))
    review_total = int(quality_summary.get("review_total", 0) or 0)
    validation_fail_total = int(quality_summary.get("validation_fail_total", 0) or 0)
    integrity_fail_total = int(quality_summary.get("artifact_integrity_fail_total", 0) or 0)
    contract_fail_total = int(quality_summary.get("full_run_contract_fail_total", 0) or 0)
    needs_review = final_status == JOB_STATUS_NEEDS_REVIEW or review_total > 0 or validation_fail_total > 0

    if final_status == JOB_STATUS_SUCCEEDED:
        headline = "已完成"
        summary_text = "会计报表已生成，当前没有检测到需要优先处理的警告信号。"
    elif final_status in WARNING_JOB_STATUSES:
        headline = "已生成结果，但建议复核"
        summary_text = "处理已完成并生成结果，但质量检查提示仍需人工确认。"
    elif final_status == JOB_STATUS_FAILED:
        headline = "处理失败"
        summary_text = "后台命令没有顺利完成，当前结果不建议直接交付。"
    else:
        headline = describe_job_status(final_status)
        summary_text = job.progress_summary

    return {
        "headline": headline,
        "status_label": describe_job_status(final_status),
        "status_tone": status_tone(final_status),
        "summary_text": summary_text,
        "workbook_generated_label": "是" if workbook_generated else "否",
        "needs_review_label": "是" if needs_review else "否",
        "next_step": str(quality_summary.get("recommended_user_action", "") or "请查看任务日志与输出文件。"),
        "user_friendly_error": str(quality_summary.get("user_friendly_error", "") or "无"),
        "review_total": review_total,
        "validation_fail_total": validation_fail_total,
        "artifact_integrity_fail_total": integrity_fail_total,
        "full_run_contract_fail_total": contract_fail_total,
        "is_success_like": final_status in SUCCESS_LIKE_JOB_STATUSES,
    }
