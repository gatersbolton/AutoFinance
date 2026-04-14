from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import yaml
from project_paths import DEFAULT_BATCH_OUTPUT_ROOT, REGISTRY_PATH, REPO_ROOT, repo_relative

from . import cli
from .curation.backlog import granularity_rank, normalize_bbox
from .stable_ids import stable_id


LOGGER = logging.getLogger(__name__)
DEFAULT_REGISTRY_PATH = REGISTRY_PATH
CORE_SUMMARY_FILES = [
    "run_summary.json",
    "run_manifest.json",
    "metadata_contract_summary.json",
    "pages_skipped_metric_audit.json",
    "reocr_dedupe_audit.json",
    "source_backed_gap_closure_summary.json",
]
REQUIRED_BATCH_OUTPUT_FILES = [
    "hardening_summary.json",
    "benchmark_registry_resolution.json",
    "batch_scope_summary.json",
    "batch_scope_by_doc.csv",
    "batch_completion_summary.json",
    "batch_run_matrix.csv",
    "batch_orchestrator_audit.json",
    "batch_supervisor_audit.json",
    "doc_lifecycle_manifest.csv",
    "metadata_contract_summary.json",
    "pages_skipped_metric_audit.json",
    "reocr_dedupe_pass2_audit.json",
    "source_backed_gap_closure_summary.json",
]
DEFAULT_DOC_TIMEOUT_SECONDS = 900
DOC_RESULT_FIELDNAMES = [
    "batch_run_id",
    "job_id",
    "doc_id",
    "company",
    "output_root",
    "run_id",
    "run_status",
    "lifecycle_state",
    "failure_kind",
    "entered_processing_scope",
    "run_dir",
    "exit_code",
    "started_at",
    "finished_at",
    "duration_seconds",
    "error_message",
    "required_core_outputs",
    "missing_required_outputs",
    "missing_required_output_count",
    "benchmark_scope_status",
    "target_scope_status",
    "metadata_contract_pass",
    "pages_skipped_pass",
    "full_run_contract_pass",
    "benchmark_missing_true_total",
    "benchmark_missing_true_total_raw",
    "target_missing_total",
    "target_missing_total_raw",
    "review_total",
    "pages_total",
    "pages_with_tables",
    "pages_skipped_as_non_table",
    "source_backed_gap_total_before",
    "source_backed_gap_total_after",
    "safe_to_apply_total",
    "applied_total",
    "closed_total",
]
DOC_LIFECYCLE_FIELDNAMES = [
    "batch_run_id",
    "job_id",
    "doc_id",
    "company",
    "command",
    "child_pid",
    "started_at",
    "finished_at",
    "duration_seconds",
    "exit_code",
    "timeout_hit",
    "stdout_path",
    "stderr_path",
    "lifecycle_state",
    "run_status",
    "run_id",
    "cleanup_performed",
    "cleaned_up_pids",
    "orphan_process_detected",
    "error_message",
]


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "y", "on"}


def get_doc_timeout_seconds() -> int:
    raw_value = str(os.environ.get("AUTOFINANCE_BATCH_DOC_TIMEOUT_SECONDS", "") or "").strip()
    if not raw_value:
        return DEFAULT_DOC_TIMEOUT_SECONDS
    try:
        value = int(raw_value)
    except ValueError:
        return DEFAULT_DOC_TIMEOUT_SECONDS
    return max(value, 1)


def resolve_registry_path(registry_path: Path, value: str) -> Path:
    candidate = Path(str(value or "").strip())
    if candidate.is_absolute():
        return candidate.resolve()
    return (registry_path.parent / candidate).resolve()


def load_benchmark_registry(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = raw.get("entries", []) if isinstance(raw, dict) else raw
    registry: Dict[str, Dict[str, Any]] = {}
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        doc_id = str(entry.get("doc_id", "")).strip()
        if not doc_id:
            continue
        benchmark_path_value = str(entry.get("benchmark_path", "") or "").strip()
        input_dir_value = str(entry.get("input_dir", "") or "").strip()
        source_image_dir_value = str(entry.get("source_image_dir", "") or "").strip()
        benchmark_path = resolve_registry_path(path, benchmark_path_value) if benchmark_path_value else None
        input_dir = resolve_registry_path(path, input_dir_value) if input_dir_value else None
        source_image_dir = resolve_registry_path(path, source_image_dir_value) if source_image_dir_value else None
        registry[doc_id] = {
            "doc_id": doc_id,
            "job_id": str(entry.get("job_id", doc_id) or doc_id).strip(),
            "company": str(entry.get("company", "") or "").strip(),
            "input_dir": str(input_dir) if input_dir else "",
            "source_image_dir": str(source_image_dir) if source_image_dir else "",
            "benchmark_path": str(benchmark_path) if benchmark_path else "",
            "benchmark_enabled": parse_bool(entry.get("benchmark_enabled", True), default=True),
            "target_gap_enabled": parse_bool(entry.get("target_gap_enabled", True), default=True),
            "batch_enabled": parse_bool(entry.get("batch_enabled", True), default=True),
            "control_candidate": parse_bool(entry.get("control_candidate", False), default=False),
            "notes": str(entry.get("notes", "") or "").strip(),
            "benchmark_path_exists": bool(benchmark_path and benchmark_path.exists()),
            "input_dir_exists": bool(input_dir and input_dir.exists()),
            "source_image_dir_exists": bool(source_image_dir and source_image_dir.exists()),
        }
    return registry


def resolve_benchmark_entry(
    *,
    doc_id: str,
    registry: Dict[str, Dict[str, Any]],
    job_id: str = "",
) -> Dict[str, Any]:
    entry = registry.get(doc_id)
    if not entry:
        return {
            "doc_id": doc_id,
            "job_id": job_id or doc_id,
            "company": "",
            "registry_found": False,
            "batch_enabled": False,
            "benchmark_enabled": False,
            "target_gap_enabled": False,
            "input_dir": "",
            "source_image_dir": "",
            "benchmark_path": "",
            "benchmark_path_exists": False,
            "benchmark_scope_status": "skipped_no_registry",
            "target_scope_status": "skipped_no_benchmark",
            "resolution_reason": "no_registry_entry",
            "notes": "",
        }

    benchmark_enabled = bool(entry.get("benchmark_enabled", False))
    target_gap_enabled = bool(entry.get("target_gap_enabled", False))
    benchmark_path = str(entry.get("benchmark_path", "") or "").strip()
    benchmark_path_exists = bool(entry.get("benchmark_path_exists", False))
    benchmark_scope_status = "candidate_enabled"
    target_scope_status = "candidate_enabled"
    resolution_reason = "registry_entry_loaded"
    if not benchmark_enabled:
        benchmark_scope_status = "disabled"
        target_scope_status = "disabled" if not target_gap_enabled else "skipped_no_benchmark"
        resolution_reason = "benchmark_disabled_in_registry"
    elif not benchmark_path:
        benchmark_scope_status = "disabled"
        target_scope_status = "skipped_no_benchmark"
        resolution_reason = "benchmark_path_blank"
    elif not benchmark_path_exists:
        benchmark_scope_status = "disabled"
        target_scope_status = "skipped_no_benchmark"
        resolution_reason = "benchmark_path_missing"
    elif not target_gap_enabled:
        target_scope_status = "disabled"
        resolution_reason = "target_gap_disabled_in_registry"

    return {
        "doc_id": doc_id,
        "job_id": job_id or str(entry.get("job_id", doc_id) or doc_id).strip(),
        "company": str(entry.get("company", "") or "").strip(),
        "registry_found": True,
        "batch_enabled": bool(entry.get("batch_enabled", True)),
        "benchmark_enabled": benchmark_enabled,
        "target_gap_enabled": target_gap_enabled,
        "input_dir": str(entry.get("input_dir", "") or "").strip(),
        "source_image_dir": str(entry.get("source_image_dir", "") or "").strip(),
        "benchmark_path": benchmark_path,
        "benchmark_path_exists": benchmark_path_exists,
        "benchmark_scope_status": benchmark_scope_status,
        "target_scope_status": target_scope_status,
        "resolution_reason": resolution_reason,
        "notes": str(entry.get("notes", "") or "").strip(),
    }


def is_alignment_eligible(
    benchmark_summary: Dict[str, Any] | None,
    alignment_summary: Dict[str, Any] | None,
) -> bool:
    benchmark_summary = benchmark_summary or {}
    alignment_summary = alignment_summary or {}
    benchmark_workbook = str(benchmark_summary.get("benchmark_workbook", "") or "").strip()
    benchmark_filled_total = int(benchmark_summary.get("benchmark_filled_total", 0) or 0)
    ambiguous_alignment_total = int(alignment_summary.get("ambiguous_alignment_total", 0) or 0)
    return bool(benchmark_workbook) and benchmark_filled_total > 0 and ambiguous_alignment_total == 0


def finalize_scope_status(
    resolution: Dict[str, Any],
    benchmark_summary: Dict[str, Any] | None,
    alignment_summary: Dict[str, Any] | None,
) -> Dict[str, Any]:
    row = dict(resolution)
    benchmark_status = str(row.get("benchmark_scope_status", "") or "").strip()
    target_status = str(row.get("target_scope_status", "") or "").strip()
    if benchmark_status in {"skipped_no_registry", "disabled"}:
        row["benchmark_scope_status"] = benchmark_status or "disabled"
        row["target_scope_status"] = target_status or ("disabled" if not row.get("target_gap_enabled", False) else "skipped_no_benchmark")
        row["alignment_eligible"] = False
        row["counted_in_batch_benchmark_total"] = False
        row["counted_in_batch_target_total"] = False
        return row

    alignment_eligible = is_alignment_eligible(benchmark_summary, alignment_summary)
    if alignment_eligible:
        row["benchmark_scope_status"] = "enabled"
        row["target_scope_status"] = "enabled" if row.get("target_gap_enabled", False) else "disabled"
        row["alignment_eligible"] = True
        row["counted_in_batch_benchmark_total"] = True
        row["counted_in_batch_target_total"] = row["target_scope_status"] == "enabled"
        return row

    row["benchmark_scope_status"] = "skipped_alignment_ineligible"
    row["target_scope_status"] = "skipped_ineligible" if row.get("target_gap_enabled", False) else "disabled"
    row["alignment_eligible"] = False
    row["counted_in_batch_benchmark_total"] = False
    row["counted_in_batch_target_total"] = False
    row["resolution_reason"] = "alignment_ineligible_after_compare"
    return row


def finalize_incomplete_scope_status(resolution: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(resolution)
    benchmark_status = str(row.get("benchmark_scope_status", "") or "").strip()
    target_status = str(row.get("target_scope_status", "") or "").strip()
    if benchmark_status not in {"disabled", "skipped_no_registry"}:
        row["benchmark_scope_status"] = "skipped_alignment_ineligible"
        row["resolution_reason"] = "batch_doc_incomplete"
    if target_status not in {"disabled", "skipped_no_benchmark"}:
        row["target_scope_status"] = "skipped_ineligible" if row.get("target_gap_enabled", False) else "disabled"
    row["alignment_eligible"] = False
    row["counted_in_batch_benchmark_total"] = False
    row["counted_in_batch_target_total"] = False
    return row


def build_registry_resolution_payload(
    *,
    run_id: str,
    registry_path: Path,
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    resolution_rows = [dict(row) for row in rows]
    return {
        "run_id": run_id,
        "registry_path": str(registry_path),
        "docs_total": len(resolution_rows),
        "registry_found_total": sum(1 for row in resolution_rows if row.get("registry_found")),
        "benchmark_enabled_requested_total": sum(1 for row in resolution_rows if row.get("benchmark_enabled")),
        "target_gap_enabled_requested_total": sum(1 for row in resolution_rows if row.get("target_gap_enabled")),
        "resolution_reason_breakdown": dict(Counter(str(row.get("resolution_reason", "")) for row in resolution_rows)),
        "rows": resolution_rows,
    }


def build_batch_scope_rows(
    scope_rows: Sequence[Dict[str, Any]],
    doc_results: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    result_by_doc = {str(row.get("doc_id", "")): row for row in doc_results}
    rows: List[Dict[str, Any]] = []
    for item in scope_rows:
        doc_id = str(item.get("doc_id", "") or "").strip()
        result = result_by_doc.get(doc_id, {})
        benchmark_raw = int(result.get("benchmark_missing_true_total_raw", result.get("benchmark_missing_true_total", 0)) or 0)
        target_raw = int(result.get("target_missing_total_raw", result.get("target_missing_total", 0)) or 0)
        rows.append(
            {
                "job_id": str(item.get("job_id", "") or result.get("job_id", "") or "").strip(),
                "doc_id": doc_id,
                "company": str(item.get("company", "") or "").strip(),
                "run_id": str(result.get("run_id", "") or "").strip(),
                "run_status": str(result.get("run_status", "") or "").strip(),
                "run_dir": str(result.get("run_dir", "") or "").strip(),
                "benchmark_scope_status": str(item.get("benchmark_scope_status", "") or "").strip(),
                "target_scope_status": str(item.get("target_scope_status", "") or "").strip(),
                "registry_found": bool(item.get("registry_found", False)),
                "benchmark_enabled": bool(item.get("benchmark_enabled", False)),
                "target_gap_enabled": bool(item.get("target_gap_enabled", False)),
                "benchmark_path_exists": bool(item.get("benchmark_path_exists", False)),
                "benchmark_path": str(item.get("benchmark_path", "") or "").strip(),
                "resolution_reason": str(item.get("resolution_reason", "") or "").strip(),
                "alignment_eligible": bool(item.get("alignment_eligible", False)),
                "counted_in_batch_benchmark_total": bool(item.get("counted_in_batch_benchmark_total", False)),
                "counted_in_batch_target_total": bool(item.get("counted_in_batch_target_total", False)),
                "benchmark_missing_true_total_raw": benchmark_raw,
                "benchmark_missing_true_total_counted": benchmark_raw if item.get("counted_in_batch_benchmark_total", False) else 0,
                "target_missing_total_raw": target_raw,
                "target_missing_total_counted": target_raw if item.get("counted_in_batch_target_total", False) else 0,
                "review_total": int(result.get("review_total", 0) or 0),
                "source_backed_gap_total_before": int(result.get("source_backed_gap_total_before", 0) or 0),
                "source_backed_gap_total_after": int(result.get("source_backed_gap_total_after", 0) or 0),
                "safe_to_apply_total": int(result.get("safe_to_apply_total", 0) or 0),
                "applied_total": int(result.get("applied_total", 0) or 0),
                "closed_total": int(result.get("closed_total", 0) or 0),
                "metadata_contract_pass": bool(result.get("metadata_contract_pass", False)),
                "pages_skipped_pass": bool(result.get("pages_skipped_pass", False)),
                "full_run_contract_pass": bool(result.get("full_run_contract_pass", False)),
                "exit_code": int(result.get("exit_code", 0) or 0),
                "duration_seconds": float(result.get("duration_seconds", 0.0) or 0.0),
                "notes": str(item.get("notes", "") or "").strip(),
            }
        )
    return rows


def build_batch_scope_summary(run_id: str, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    scope_rows = list(rows)
    benchmark_breakdown = Counter(str(row.get("benchmark_scope_status", "") or "").strip() for row in scope_rows)
    target_breakdown = Counter(str(row.get("target_scope_status", "") or "").strip() for row in scope_rows)
    benchmark_in_scope = sum(int(row.get("benchmark_missing_true_total_counted", 0) or 0) for row in scope_rows)
    benchmark_raw = sum(int(row.get("benchmark_missing_true_total_raw", 0) or 0) for row in scope_rows)
    target_in_scope = sum(int(row.get("target_missing_total_counted", 0) or 0) for row in scope_rows)
    target_raw = sum(int(row.get("target_missing_total_raw", 0) or 0) for row in scope_rows)
    return {
        "run_id": run_id,
        "docs_in_batch_total": len(scope_rows),
        "docs_benchmark_enabled_total": sum(1 for row in scope_rows if row.get("benchmark_scope_status") == "enabled"),
        "docs_benchmark_skipped_total": sum(1 for row in scope_rows if row.get("benchmark_scope_status") != "enabled"),
        "docs_target_scope_enabled_total": sum(1 for row in scope_rows if row.get("target_scope_status") == "enabled"),
        "docs_target_scope_skipped_total": sum(1 for row in scope_rows if row.get("target_scope_status") != "enabled"),
        "benchmark_missing_true_total_in_scope": benchmark_in_scope,
        "benchmark_missing_true_total_out_of_scope_ignored": max(benchmark_raw - benchmark_in_scope, 0),
        "target_missing_total_in_scope": target_in_scope,
        "target_missing_total_out_of_scope_ignored": max(target_raw - target_in_scope, 0),
        "benchmark_scope_breakdown": dict(benchmark_breakdown),
        "target_scope_breakdown": dict(target_breakdown),
    }


def build_batch_pages_skipped_audit(run_id: str, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    page_rows = [row for row in rows if str(row.get("run_status", "")).strip() == "success"]
    pages_total = sum(int(row.get("pages_total", 0) or 0) for row in page_rows)
    pages_with_tables = sum(int(row.get("pages_with_tables", 0) or 0) for row in page_rows)
    pages_skipped = sum(int(row.get("pages_skipped_as_non_table", 0) or 0) for row in page_rows)
    expected_pages_skipped = max(pages_total - pages_with_tables, 0)
    return {
        "run_id": run_id,
        "docs_counted_total": len(page_rows),
        "pages_total": pages_total,
        "pages_with_tables": pages_with_tables,
        "pages_skipped_as_non_table": pages_skipped,
        "expected_pages_skipped": expected_pages_skipped,
        "pass": pages_skipped == expected_pages_skipped,
    }


def collect_missing_core_outputs(run_dir: Path | None) -> List[str]:
    if not run_dir:
        return list(CORE_SUMMARY_FILES)
    return [filename for filename in CORE_SUMMARY_FILES if not (run_dir / filename).exists()]


def infer_doc_failure_kind(row: Dict[str, Any]) -> str:
    run_status = str(row.get("run_status", "") or "").strip()
    error_message = str(row.get("error_message", "") or "").strip()
    if run_status == "success":
        return ""
    if run_status == "timed_out":
        return "timeout"
    if run_status in {"pending", "not_started"}:
        return "not_started"
    if run_status == "in_progress":
        return "in_progress"
    if run_status == "skipped":
        if error_message == "missing_input_paths":
            return "missing_input_paths"
        if error_message == "batch_aborted_before_start":
            return "batch_aborted_before_start"
        return "skipped"
    try:
        exit_code = int(row.get("exit_code", 0) or 0)
    except (TypeError, ValueError):
        exit_code = 0
    if exit_code == 124 or error_message.startswith("system_exit:124"):
        return "timeout"
    if "KeyboardInterrupt" in error_message:
        return "interrupted"
    if row.get("missing_required_outputs"):
        return "missing_required_outputs"
    return "failed"


def finalize_doc_result_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(payload)
    required_outputs = [str(item).strip() for item in row.get("required_core_outputs", CORE_SUMMARY_FILES) if str(item).strip()]
    missing_outputs = [str(item).strip() for item in row.get("missing_required_outputs", []) if str(item).strip()]
    row["required_core_outputs"] = required_outputs
    row["missing_required_outputs"] = sorted(set(missing_outputs))
    row["missing_required_output_count"] = len(row["missing_required_outputs"])
    row["entered_processing_scope"] = bool(row.get("entered_processing_scope", False))
    duration_value = float(row.get("duration_seconds", 0.0) or 0.0)
    row["duration_seconds"] = round(max(duration_value, 0.0), 3)
    run_status = str(row.get("run_status", "") or "").strip()
    row["lifecycle_state"] = {
        "pending": "queued",
        "not_started": "queued",
        "in_progress": "in_progress",
        "success": "finished",
        "failed": "failed",
        "skipped": "skipped",
        "timed_out": "timed_out",
    }.get(run_status, "queued")
    row["failure_kind"] = infer_doc_failure_kind(row)
    return row


def build_doc_result_template(
    *,
    batch_run_id: str,
    entry: Dict[str, Any],
    output_root: Path,
    resolution: Dict[str, Any],
) -> Dict[str, Any]:
    return finalize_doc_result_payload(
        {
            "batch_run_id": batch_run_id,
            "job_id": str(entry.get("job_id", "") or "").strip(),
            "doc_id": str(entry.get("doc_id", "") or "").strip(),
            "company": str(entry.get("company", "") or "").strip(),
            "output_root": str(output_root),
            "run_id": "",
            "run_status": "pending",
            "lifecycle_state": "queued",
            "failure_kind": "",
            "entered_processing_scope": False,
            "run_dir": "",
            "exit_code": "",
            "started_at": "",
            "finished_at": "",
            "duration_seconds": 0.0,
            "error_message": "",
            "required_core_outputs": list(CORE_SUMMARY_FILES),
            "missing_required_outputs": [],
            "benchmark_scope_status": str(resolution.get("benchmark_scope_status", "") or "").strip(),
            "target_scope_status": str(resolution.get("target_scope_status", "") or "").strip(),
            "metadata_contract_pass": False,
            "pages_skipped_pass": False,
            "full_run_contract_pass": False,
            "benchmark_missing_true_total": 0,
            "benchmark_missing_true_total_raw": 0,
            "target_missing_total": 0,
            "target_missing_total_raw": 0,
            "review_total": 0,
            "pages_total": 0,
            "pages_with_tables": 0,
            "pages_skipped_as_non_table": 0,
            "source_backed_gap_total_before": 0,
            "source_backed_gap_total_after": 0,
            "safe_to_apply_total": 0,
            "applied_total": 0,
            "closed_total": 0,
        }
    )


def finalize_doc_lifecycle_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(payload)
    child_pid = row.get("child_pid", "")
    try:
        row["child_pid"] = int(child_pid) if str(child_pid).strip() else ""
    except (TypeError, ValueError):
        row["child_pid"] = ""
    row["duration_seconds"] = round(max(float(row.get("duration_seconds", 0.0) or 0.0), 0.0), 3)
    row["timeout_hit"] = bool(row.get("timeout_hit", False))
    row["cleanup_performed"] = bool(row.get("cleanup_performed", False))
    row["orphan_process_detected"] = bool(row.get("orphan_process_detected", False))
    row["cleaned_up_pids"] = sorted(
        {
            int(item)
            for item in row.get("cleaned_up_pids", [])
            if str(item).strip()
        }
    )
    return row


def build_doc_lifecycle_template(
    *,
    batch_run_id: str,
    entry: Dict[str, Any],
) -> Dict[str, Any]:
    return finalize_doc_lifecycle_payload(
        {
            "batch_run_id": batch_run_id,
            "job_id": str(entry.get("job_id", "") or "").strip(),
            "doc_id": str(entry.get("doc_id", "") or "").strip(),
            "company": str(entry.get("company", "") or "").strip(),
            "command": "",
            "child_pid": "",
            "started_at": "",
            "finished_at": "",
            "duration_seconds": 0.0,
            "exit_code": "",
            "timeout_hit": False,
            "stdout_path": "",
            "stderr_path": "",
            "lifecycle_state": "queued",
            "run_status": "pending",
            "run_id": "",
            "cleanup_performed": False,
            "cleaned_up_pids": [],
            "orphan_process_detected": False,
            "error_message": "",
        }
    )


def build_doc_lifecycle_in_progress(
    *,
    batch_run_id: str,
    entry: Dict[str, Any],
    template_path: Path,
    output_root: Path,
    batch_lite: bool,
    log_level: str,
    started_at: datetime,
) -> Dict[str, Any]:
    doc_args = build_single_doc_args(
        entry=entry,
        template_path=template_path,
        output_root=output_root,
        batch_lite=batch_lite,
        log_level=log_level,
    )
    command = [sys.executable, "-m", "standardize.cli", *doc_args]
    return finalize_doc_lifecycle_payload(
        {
            **build_doc_lifecycle_template(batch_run_id=batch_run_id, entry=entry),
            "command": render_command(command),
            "stdout_path": str(output_root / "batch_child_stdout.log"),
            "stderr_path": str(output_root / "batch_child_stderr.log"),
            "started_at": started_at.isoformat(),
            "lifecycle_state": "in_progress",
            "run_status": "in_progress",
        }
    )


def merge_lifecycle_row(
    lifecycle_row: Dict[str, Any],
    result_row: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(lifecycle_row)
    supervision = finalize_doc_lifecycle_payload(result_row.get("supervision", {})) if isinstance(result_row.get("supervision", {}), dict) else {}
    run_status = str(result_row.get("run_status", "") or "").strip()
    merged.update(
        {
            "run_id": str(result_row.get("run_id", "") or "").strip(),
            "run_status": run_status or str(merged.get("run_status", "") or "").strip(),
            "lifecycle_state": run_status or str(supervision.get("lifecycle_state", "") or merged.get("lifecycle_state", "") or "").strip(),
            "started_at": str(supervision.get("started_at", "") or result_row.get("started_at", "") or merged.get("started_at", "") or "").strip(),
            "finished_at": str(supervision.get("finished_at", "") or result_row.get("finished_at", "") or merged.get("finished_at", "") or "").strip(),
            "duration_seconds": float(supervision.get("duration_seconds", result_row.get("duration_seconds", merged.get("duration_seconds", 0.0))) or 0.0),
            "exit_code": supervision.get("exit_code", result_row.get("exit_code", merged.get("exit_code", ""))),
            "timeout_hit": bool(supervision.get("timeout_hit", False)) or run_status == "timed_out",
            "stdout_path": str(supervision.get("stdout_path", "") or merged.get("stdout_path", "") or "").strip(),
            "stderr_path": str(supervision.get("stderr_path", "") or merged.get("stderr_path", "") or "").strip(),
            "command": str(supervision.get("command", "") or merged.get("command", "") or "").strip(),
            "child_pid": supervision.get("child_pid", merged.get("child_pid", "")),
            "cleanup_performed": bool(supervision.get("cleanup_performed", merged.get("cleanup_performed", False))),
            "cleaned_up_pids": list(supervision.get("cleaned_up_pids", merged.get("cleaned_up_pids", [])) or []),
            "orphan_process_detected": bool(supervision.get("orphan_process_detected", merged.get("orphan_process_detected", False))),
            "error_message": str(result_row.get("error_message", "") or supervision.get("error_message", "") or merged.get("error_message", "") or "").strip(),
        }
    )
    return finalize_doc_lifecycle_payload(merged)


def elapsed_seconds(started_at_text: str, finished_at: datetime) -> float:
    try:
        started_at = datetime.fromisoformat(str(started_at_text).strip())
    except (TypeError, ValueError):
        return 0.0
    return max((finished_at - started_at).total_seconds(), 0.0)


def build_doc_stage_status_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = finalize_doc_result_payload(row)
    return {
        "batch_run_id": str(payload.get("batch_run_id", "") or "").strip(),
        "job_id": str(payload.get("job_id", "") or "").strip(),
        "doc_id": str(payload.get("doc_id", "") or "").strip(),
        "company": str(payload.get("company", "") or "").strip(),
        "status": str(payload.get("run_status", "") or "").strip(),
        "run_status": str(payload.get("run_status", "") or "").strip(),
        "lifecycle_state": str(payload.get("lifecycle_state", "") or "").strip(),
        "entered_processing_scope": bool(payload.get("entered_processing_scope", False)),
        "failure_kind": str(payload.get("failure_kind", "") or "").strip(),
        "run_id": str(payload.get("run_id", "") or "").strip(),
        "run_dir": str(payload.get("run_dir", "") or "").strip(),
        "started_at": str(payload.get("started_at", "") or "").strip(),
        "finished_at": str(payload.get("finished_at", "") or "").strip(),
        "duration_seconds": float(payload.get("duration_seconds", 0.0) or 0.0),
        "exit_code": payload.get("exit_code", ""),
        "error_message": str(payload.get("error_message", "") or "").strip(),
        "missing_required_outputs": list(payload.get("missing_required_outputs", [])),
    }


def write_doc_status_artifacts(output_root: Path, row: Dict[str, Any]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    payload = finalize_doc_result_payload(row)
    stage_payload = build_doc_stage_status_payload(payload)
    completion_payload = dict(payload)
    completion_payload["completed"] = str(payload.get("run_status", "") or "").strip() in {"success", "failed", "skipped", "timed_out"}
    completion_payload["status"] = str(payload.get("run_status", "") or "").strip()
    write_json(output_root / "batch_doc_stage_status.json", stage_payload)
    write_json(output_root / "batch_doc_completion_summary.json", completion_payload)


def build_batch_completion_summary(
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    rows: Sequence[Dict[str, Any]],
    completed: bool = True,
    process_exited: bool = True,
    aborted: bool = False,
    batch_error_message: str = "",
    missing_required_batch_outputs: Sequence[str] | None = None,
) -> Dict[str, Any]:
    result_rows = list(rows)
    status_breakdown = Counter(str(row.get("run_status", "") or "").strip() for row in result_rows)
    docs_succeeded = int(status_breakdown.get("success", 0))
    docs_timed_out = int(status_breakdown.get("timed_out", 0))
    docs_failed = int(status_breakdown.get("failed", 0)) + docs_timed_out
    docs_skipped = int(status_breakdown.get("skipped", 0))
    docs_pending = int(status_breakdown.get("pending", 0)) + int(status_breakdown.get("not_started", 0))
    docs_in_progress = int(status_breakdown.get("in_progress", 0))
    missing_required_outputs = [
        {
            "doc_id": str(row.get("doc_id", "") or "").strip(),
            "run_status": str(row.get("run_status", "") or "").strip(),
            "failure_kind": str(row.get("failure_kind", "") or "").strip(),
            "missing_outputs": list(row.get("missing_required_outputs", [])),
        }
        for row in result_rows
        if list(row.get("missing_required_outputs", []))
        and str(row.get("run_status", "") or "").strip() in {"success", "failed", "skipped", "timed_out"}
    ]
    missing_required_batch_outputs = sorted({str(item).strip() for item in (missing_required_batch_outputs or []) if str(item).strip()})
    terminal_states = {"success", "failed", "completed_with_failures", "aborted"}
    if not process_exited:
        status = "in_progress"
    elif aborted:
        status = "aborted"
    elif batch_error_message or missing_required_batch_outputs:
        status = "failed"
    elif docs_failed or docs_skipped:
        status = "completed_with_failures"
    else:
        status = "success"
    docs_started_total = sum(
        1
        for row in result_rows
        if str(row.get("started_at", "") or "").strip() or bool(row.get("entered_processing_scope", False))
    )
    return {
        "run_id": run_id,
        "batch_run_id": run_id,
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "finished_at": finished_at.astimezone(timezone.utc).isoformat(),
        "duration_seconds": round(max((finished_at - started_at).total_seconds(), 0.0), 3),
        "docs_total": len(result_rows),
        "docs_succeeded_total": docs_succeeded,
        "docs_failed_total": docs_failed,
        "docs_skipped_total": docs_skipped,
        "docs_timed_out_total": docs_timed_out,
        "docs_succeeded": docs_succeeded,
        "docs_failed": docs_failed,
        "docs_skipped": docs_skipped,
        "docs_pending_total": docs_pending,
        "docs_in_progress_total": docs_in_progress,
        "docs_started_total": docs_started_total,
        "status_breakdown": dict(status_breakdown),
        "completed": bool(process_exited and status in terminal_states),
        "status": status,
        "failed_doc_ids": [str(row.get("doc_id", "") or "").strip() for row in result_rows if str(row.get("run_status", "") or "").strip() in {"failed", "timed_out"}],
        "skipped_doc_ids": [str(row.get("doc_id", "") or "").strip() for row in result_rows if str(row.get("run_status", "") or "").strip() == "skipped"],
        "timed_out_doc_ids": [str(row.get("doc_id", "") or "").strip() for row in result_rows if str(row.get("run_status", "") or "").strip() == "timed_out"],
        "pending_doc_ids": [str(row.get("doc_id", "") or "").strip() for row in result_rows if str(row.get("run_status", "") or "").strip() in {"pending", "not_started"}],
        "in_progress_doc_ids": [str(row.get("doc_id", "") or "").strip() for row in result_rows if str(row.get("run_status", "") or "").strip() == "in_progress"],
        "missing_required_outputs": missing_required_outputs,
        "missing_required_batch_outputs": missing_required_batch_outputs,
        "batch_error_message": str(batch_error_message or "").strip(),
        "process_exited": bool(process_exited),
        "success": status == "success",
    }


def build_batch_metadata_contract_summary(
    *,
    run_id: str,
    batch_payloads: Iterable[tuple[str, Dict[str, Any] | None]],
    doc_results: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    checked_files: List[str] = []
    missing_run_id_files: List[str] = []
    mismatched_run_id_files: List[Dict[str, Any]] = []
    for filename, payload in batch_payloads:
        checked_files.append(filename)
        payload = payload or {}
        found_run_id = str(payload.get("run_id", "") or "").strip()
        if not found_run_id:
            missing_run_id_files.append(filename)
        elif found_run_id != run_id:
            mismatched_run_id_files.append({"file": filename, "found_run_id": found_run_id})
    per_doc_rows = []
    per_doc_failures = 0
    for row in doc_results:
        metadata_contract_judged = bool(row.get("entered_processing_scope", False)) and str(row.get("run_status", "") or "").strip() == "success"
        doc_payload = {
            "job_id": str(row.get("job_id", "") or "").strip(),
            "doc_id": str(row.get("doc_id", "") or "").strip(),
            "run_id": str(row.get("run_id", "") or "").strip(),
            "run_status": str(row.get("run_status", "") or "").strip(),
            "metadata_contract_pass": bool(row.get("metadata_contract_pass", False)),
            "metadata_contract_judged": metadata_contract_judged,
        }
        if metadata_contract_judged and not doc_payload["metadata_contract_pass"]:
            per_doc_failures += 1
        per_doc_rows.append(doc_payload)
    checked_files = sorted(set(checked_files))
    missing_run_id_files = sorted(set(missing_run_id_files))
    mismatched_run_id_files.sort(key=lambda item: str(item.get("file", "")))
    return {
        "run_id": run_id,
        "checked_summary_files": checked_files,
        "summary_files_checked": checked_files,
        "missing_run_id_files": missing_run_id_files,
        "mismatched_run_id_files": mismatched_run_id_files,
        "per_doc_rows": per_doc_rows,
        "docs_judged_total": sum(1 for row in per_doc_rows if row.get("metadata_contract_judged")),
        "per_doc_contract_fail_total": per_doc_failures,
        "pass": not missing_run_id_files and not mismatched_run_id_files and per_doc_failures == 0,
    }


def build_batch_orchestrator_audit(
    *,
    run_id: str,
    completion_summary: Dict[str, Any],
    doc_results: Sequence[Dict[str, Any]],
    missing_required_batch_outputs: Sequence[str] | None = None,
) -> Dict[str, Any]:
    rows = list(doc_results)
    terminal_state = str(completion_summary.get("status", "") or "").strip()
    terminal_statuses = {"success", "failed", "completed_with_failures", "aborted"}
    terminal_doc_statuses = {"success", "failed", "skipped", "timed_out"}
    docs_with_terminal_status_total = sum(
        1 for row in rows if str(row.get("run_status", "") or "").strip() in terminal_doc_statuses
    )
    docs_without_terminal_status_total = max(len(rows) - docs_with_terminal_status_total, 0)
    missing_required_batch_outputs = sorted(
        {str(item).strip() for item in (missing_required_batch_outputs or []) if str(item).strip()}
    )
    fail_closed = bool(completion_summary.get("process_exited", False)) and terminal_state in terminal_statuses and docs_without_terminal_status_total == 0
    return {
        "run_id": run_id,
        "batch_run_id": run_id,
        "process_exited": bool(completion_summary.get("process_exited", False)),
        "terminal_state_written": terminal_state in terminal_statuses,
        "terminal_state": terminal_state,
        "fail_closed": fail_closed,
        "docs_total": len(rows),
        "docs_with_terminal_status_total": docs_with_terminal_status_total,
        "docs_without_terminal_status_total": docs_without_terminal_status_total,
        "missing_required_batch_outputs": missing_required_batch_outputs,
        "pass": fail_closed and not missing_required_batch_outputs,
    }


def build_batch_supervisor_audit(
    *,
    run_id: str,
    lifecycle_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    rows = [finalize_doc_lifecycle_payload(row) for row in lifecycle_rows]
    started_rows = [row for row in rows if row.get("child_pid", "")]
    cleaned_up_pids = sorted(
        {
            int(pid)
            for row in rows
            for pid in row.get("cleaned_up_pids", [])
            if str(pid).strip()
        }
    )
    timed_out_doc_ids = [
        str(row.get("doc_id", "") or "").strip()
        for row in rows
        if bool(row.get("timeout_hit", False))
    ]
    child_processes_orphaned_total = sum(1 for row in rows if bool(row.get("orphan_process_detected", False)))
    child_processes_in_progress_total = sum(
        1
        for row in started_rows
        if str(row.get("lifecycle_state", "") or "").strip() in {"queued", "in_progress"}
    )
    return {
        "run_id": run_id,
        "batch_run_id": run_id,
        "child_processes_started_total": len(started_rows),
        "child_processes_terminated_total": len(cleaned_up_pids),
        "child_processes_orphaned_total": child_processes_orphaned_total,
        "timed_out_doc_ids": timed_out_doc_ids,
        "cleaned_up_pids": cleaned_up_pids,
        "child_processes_in_progress_total": child_processes_in_progress_total,
        "pass": child_processes_orphaned_total == 0 and child_processes_in_progress_total == 0,
    }


def build_source_backed_gap_batch_summary(
    *,
    run_id: str,
    doc_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    source_backed_gap_total_before = 0
    source_backed_gap_total_after = 0
    safe_to_apply_total = 0
    applied_total = 0
    closed_total = 0
    docs_counted_total = 0
    for item in doc_rows:
        rows.append(dict(item))
        if item.get("target_scope_status") != "enabled":
            continue
        docs_counted_total += 1
        source_backed_gap_total_before += int(item.get("source_backed_gap_total_before", 0) or 0)
        source_backed_gap_total_after += int(item.get("source_backed_gap_total_after", 0) or 0)
        safe_to_apply_total += int(item.get("safe_to_apply_total", 0) or 0)
        applied_total += int(item.get("applied_total", 0) or 0)
        closed_total += int(item.get("closed_total", 0) or 0)
    return {
        "run_id": run_id,
        "docs_counted_total": docs_counted_total,
        "source_backed_gap_total_before": source_backed_gap_total_before,
        "source_backed_gap_total_after": source_backed_gap_total_after,
        "safe_to_apply_total": safe_to_apply_total,
        "applied_total": applied_total,
        "closed_total": closed_total,
        "rows": rows,
    }


def normalize_reason_codes(value: Any) -> str:
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = [part.strip() for part in text.split(",") if part.strip()]
        if isinstance(parsed, list):
            items = [str(item).strip() for item in parsed if str(item).strip()]
        else:
            items = [str(parsed).strip()]
    return "|".join(sorted(set(items)))


def normalize_subtable_key(row: Dict[str, Any]) -> str:
    return str(row.get("logical_subtable_id", "") or row.get("table_id", "") or "").strip()


def expand_merged_ids(row: Dict[str, Any], field_name: str, fallback_field: str) -> List[str]:
    text = str(row.get(field_name, "") or "").strip()
    values: List[str] = []
    if text:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = [part.strip() for part in text.split(",") if part.strip()]
        if isinstance(parsed, list):
            values.extend(str(item).strip() for item in parsed if str(item).strip())
        elif parsed not in (None, ""):
            values.append(str(parsed).strip())
    if not values:
        fallback = str(row.get(fallback_field, "") or "").strip()
        if fallback:
            values.append(fallback)
    return sorted(set(values))


def pass2_cluster_key(row: Dict[str, Any]) -> Tuple[str, int, str, str, str, str]:
    return (
        str(row.get("doc_id", "")).strip(),
        int(row.get("page_no", 0) or 0),
        normalize_bbox(row.get("bbox_normalized", "") or row.get("bbox", "")),
        normalize_subtable_key(row),
        str(row.get("category", "")).strip(),
        normalize_reason_codes(row.get("reason_codes", "")),
    )


def duplicate_group_stats(rows: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    counter: Dict[Tuple[str, int, str, str, str, str], int] = {}
    for row in rows:
        key = pass2_cluster_key(row)
        counter[key] = counter.get(key, 0) + 1
    duplicate_groups = sum(1 for count in counter.values() if count > 1)
    duplicate_tasks = sum(count for count in counter.values() if count > 1)
    return duplicate_groups, duplicate_tasks


def dedupe_reocr_rows_pass2(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, int, str, str, str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(pass2_cluster_key(row), []).append(dict(row))

    deduped: List[Dict[str, Any]] = []
    for cluster_key, cluster_rows in grouped.items():
        ordered = sorted(
            cluster_rows,
            key=lambda row: (
                granularity_rank(str(row.get("granularity", "")).strip()),
                -float(row.get("priority_score", 0.0) or 0.0),
                str(row.get("task_id", "")),
            ),
        )
        selected = dict(ordered[0])
        merged_task_ids: List[str] = []
        merged_review_ids: List[str] = []
        for cluster_row in ordered:
            merged_task_ids.extend(expand_merged_ids(cluster_row, "merged_task_ids", "task_id"))
            merged_review_ids.extend(expand_merged_ids(cluster_row, "merged_review_ids", "source_review_id"))
        merged_task_ids = sorted(set(merged_task_ids))
        merged_review_ids = sorted(set(merged_review_ids))
        reason_codes_key = normalize_reason_codes(selected.get("reason_codes", ""))
        selected["cluster_id"] = stable_id("REOCR_CLUSTER_PASS2_", list(cluster_key) + merged_task_ids)
        selected["merged_task_ids"] = json.dumps(merged_task_ids, ensure_ascii=False)
        selected["merged_review_ids"] = json.dumps(merged_review_ids, ensure_ascii=False)
        selected["merged_task_count"] = max(len(merged_task_ids) - 1, 0)
        selected["bbox_normalized"] = cluster_key[2]
        selected["reason_codes"] = json.dumps(reason_codes_key.split("|") if reason_codes_key else [], ensure_ascii=False)
        selected["dedupe_pass"] = "pass2"
        deduped.append(selected)

    deduped.sort(
        key=lambda row: (
            str(row.get("doc_id", "")),
            int(row.get("page_no", 0) or 0),
            granularity_rank(str(row.get("granularity", "")).strip()),
            -float(row.get("priority_score", 0.0) or 0.0),
            str(row.get("task_id", "")),
        )
    )
    return deduped


def build_reocr_pass2_audit(
    *,
    run_id: str,
    before_rows: Sequence[Dict[str, Any]],
    after_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    groups_before, tasks_before = duplicate_group_stats(before_rows)
    groups_after, tasks_after = duplicate_group_stats(after_rows)
    if groups_before == 0 and tasks_before == 0:
        assessment_reason = "already_deduped"
        passed = True
    elif groups_after < groups_before or tasks_after < tasks_before:
        assessment_reason = "materially_improved"
        passed = True
    else:
        assessment_reason = "not_improved"
        passed = False
    return {
        "run_id": run_id,
        "rows_total_before_pass2": len(before_rows),
        "rows_total_after_pass2": len(after_rows),
        "duplicate_bbox_groups_before_pass2": groups_before,
        "duplicate_bbox_groups_after_pass2": groups_after,
        "duplicate_bbox_tasks_before_pass2": tasks_before,
        "duplicate_bbox_tasks_after_pass2": tasks_after,
        "merged_task_count": max(len(before_rows) - len(after_rows), 0),
        "assessment_reason": assessment_reason,
        "pass": passed,
    }


def build_source_backed_gap_by_doc_rows(scope_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in scope_rows:
        rows.append(
            {
                "job_id": str(row.get("job_id", "")).strip(),
                "doc_id": str(row.get("doc_id", "")).strip(),
                "company": str(row.get("company", "")).strip(),
                "target_scope_status": str(row.get("target_scope_status", "")).strip(),
                "counted_in_batch_target_total": bool(row.get("counted_in_batch_target_total", False)),
                "source_backed_gap_total_before": int(row.get("source_backed_gap_total_before", 0) or 0),
                "source_backed_gap_total_after": int(row.get("source_backed_gap_total_after", 0) or 0),
                "safe_to_apply_total": int(row.get("safe_to_apply_total", 0) or 0),
                "applied_total": int(row.get("applied_total", 0) or 0),
                "closed_total": int(row.get("closed_total", 0) or 0),
            }
        )
    return rows


def build_batch_hardening_summary(
    *,
    run_id: str,
    scope_summary: Dict[str, Any],
    completion_summary: Dict[str, Any],
    metadata_contract_summary: Dict[str, Any],
    pages_audit: Dict[str, Any],
    reocr_pass2_audit: Dict[str, Any],
    source_backed_summary: Dict[str, Any],
    run_matrix_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "docs_in_batch_total": int(scope_summary.get("docs_in_batch_total", 0) or 0),
        "docs_succeeded_total": int(completion_summary.get("docs_succeeded_total", 0) or 0),
        "docs_failed_total": int(completion_summary.get("docs_failed_total", 0) or 0),
        "docs_timed_out_total": int(completion_summary.get("docs_timed_out_total", 0) or 0),
        "docs_skipped_total": int(completion_summary.get("docs_skipped_total", 0) or 0),
        "docs_benchmark_enabled_total": int(scope_summary.get("docs_benchmark_enabled_total", 0) or 0),
        "docs_benchmark_skipped_total": int(scope_summary.get("docs_benchmark_skipped_total", 0) or 0),
        "docs_target_scope_enabled_total": int(scope_summary.get("docs_target_scope_enabled_total", 0) or 0),
        "docs_target_scope_skipped_total": int(scope_summary.get("docs_target_scope_skipped_total", 0) or 0),
        "benchmark_missing_true_total": int(scope_summary.get("benchmark_missing_true_total_in_scope", 0) or 0),
        "benchmark_missing_true_total_out_of_scope_ignored": int(scope_summary.get("benchmark_missing_true_total_out_of_scope_ignored", 0) or 0),
        "target_missing_total": int(scope_summary.get("target_missing_total_in_scope", 0) or 0),
        "target_missing_total_out_of_scope_ignored": int(scope_summary.get("target_missing_total_out_of_scope_ignored", 0) or 0),
        "review_total": sum(int(row.get("review_total", 0) or 0) for row in run_matrix_rows if str(row.get("run_status", "")) == "success"),
        "metadata_contract_pass": bool(metadata_contract_summary.get("pass", False)),
        "pages_skipped_pass": bool(pages_audit.get("pass", False)),
        "reocr_duplicate_bbox_groups_before_pass2": int(reocr_pass2_audit.get("duplicate_bbox_groups_before_pass2", 0) or 0),
        "reocr_duplicate_bbox_groups_after_pass2": int(reocr_pass2_audit.get("duplicate_bbox_groups_after_pass2", 0) or 0),
        "reocr_duplicate_bbox_tasks_before_pass2": int(reocr_pass2_audit.get("duplicate_bbox_tasks_before_pass2", 0) or 0),
        "reocr_duplicate_bbox_tasks_after_pass2": int(reocr_pass2_audit.get("duplicate_bbox_tasks_after_pass2", 0) or 0),
        "reocr_pass2_assessment_reason": str(reocr_pass2_audit.get("assessment_reason", "") or "").strip(),
        "source_backed_gap_total_before": int(source_backed_summary.get("source_backed_gap_total_before", 0) or 0),
        "source_backed_gap_total_after": int(source_backed_summary.get("source_backed_gap_total_after", 0) or 0),
        "completed": bool(completion_summary.get("completed", False)),
        "success": bool(completion_summary.get("success", False)),
        "batch_status": str(completion_summary.get("status", "") or "").strip(),
    }


def dumps_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str] | None = None) -> None:
    rows = [dict(row) for row in rows]
    resolved_fieldnames = list(fieldnames or (rows[0].keys() if rows else []))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: serialize_csv_value(row.get(key)) for key in resolved_fieldnames})


def serialize_csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return value if value is not None else ""


def safe_write_json(path: Path, payload: Dict[str, Any], write_errors: List[Dict[str, Any]]) -> None:
    try:
        write_json(path, payload)
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Failed to write %s", path)
        write_errors.append({"file": path.name, "error": f"{exc.__class__.__name__}: {exc}"})


def safe_write_csv(path: Path, rows: Sequence[Dict[str, Any]], write_errors: List[Dict[str, Any]], fieldnames: Sequence[str] | None = None) -> None:
    try:
        write_csv(path, rows, fieldnames=fieldnames)
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Failed to write %s", path)
        write_errors.append({"file": path.name, "error": f"{exc.__class__.__name__}: {exc}"})


def list_missing_batch_outputs(batch_run_dir: Path) -> List[str]:
    return [filename for filename in REQUIRED_BATCH_OUTPUT_FILES if not (batch_run_dir / filename).exists()]


def write_batch_progress_artifacts(
    batch_run_dir: Path,
    completion_summary: Dict[str, Any],
    doc_results: Sequence[Dict[str, Any]],
    lifecycle_rows: Sequence[Dict[str, Any]] | None = None,
) -> None:
    lifecycle_rows = [finalize_doc_lifecycle_payload(row) for row in (lifecycle_rows or [])]
    write_json(batch_run_dir / "batch_completion_summary.json", completion_summary)
    write_json(
        batch_run_dir / "batch_orchestrator_audit.json",
        build_batch_orchestrator_audit(
            run_id=str(completion_summary.get("batch_run_id", "") or completion_summary.get("run_id", "") or "").strip(),
            completion_summary=completion_summary,
            doc_results=doc_results,
            missing_required_batch_outputs=list_missing_batch_outputs(batch_run_dir),
        ),
    )
    write_json(
        batch_run_dir / "batch_supervisor_audit.json",
        build_batch_supervisor_audit(
            run_id=str(completion_summary.get("batch_run_id", "") or completion_summary.get("run_id", "") or "").strip(),
            lifecycle_rows=lifecycle_rows,
        ),
    )
    write_csv(batch_run_dir / "batch_run_matrix.csv", doc_results, fieldnames=DOC_RESULT_FIELDNAMES)
    write_csv(batch_run_dir / "doc_lifecycle_manifest.csv", lifecycle_rows, fieldnames=DOC_LIFECYCLE_FIELDNAMES)


def make_batch_run_id() -> str:
    return f"BATCH_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def sort_registry_entries(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(row: Dict[str, Any]) -> Tuple[int, str, str]:
        job_id = str(row.get("job_id", "") or "").strip()
        try:
            numeric = int(job_id)
        except ValueError:
            numeric = 999999
        return numeric, str(row.get("doc_id", "") or ""), str(row.get("company", "") or "")

    return sorted((dict(row) for row in rows), key=sort_key)


def filter_registry_entries(
    registry: Dict[str, Dict[str, Any]],
    doc_ids: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    doc_id_set = {doc_id.strip() for doc_id in doc_ids or [] if doc_id.strip()}
    rows = []
    for row in sort_registry_entries(registry.values()):
        if not row.get("batch_enabled", True):
            continue
        if doc_id_set and str(row.get("doc_id", "")).strip() not in doc_id_set:
            continue
        rows.append(row)
    return rows


def list_run_dirs(base_dir: Path) -> List[Path]:
    if not base_dir.exists():
        return []
    return sorted(
        [
            path
            for path in base_dir.iterdir()
            if path.is_dir() and not path.name.startswith("_") and (path / "run_summary.json").exists()
        ],
        key=lambda path: path.stat().st_mtime,
    )


def locate_new_run_dir(output_root: Path, previous_run_dirs: Sequence[Path]) -> Path | None:
    previous_set = {path.resolve() for path in previous_run_dirs}
    current = list_run_dirs(output_root)
    for path in reversed(current):
        if path.resolve() not in previous_set:
            return path
    return current[-1] if current else None


def build_single_doc_args(
    *,
    entry: Dict[str, Any],
    template_path: Path,
    output_root: Path,
    batch_lite: bool,
    log_level: str,
) -> List[str]:
    args = [
        "--input-dir",
        str(entry.get("input_dir", "")),
        "--template",
        str(template_path),
        "--output-dir",
        str(output_root),
        "--source-image-dir",
        str(entry.get("source_image_dir", "")),
        "--provider-priority",
        "aliyun,tencent",
        "--enable-conflict-merge",
        "--enable-period-normalization",
        "--enable-dedupe",
        "--enable-validation",
        "--enable-integrity-check",
        "--enable-validation-aware-conflicts",
        "--emit-reocr-tasks",
        "--enable-label-canonicalization",
        "--enable-derived-facts",
        "--emit-run-manifest",
        "--artifact-manifest-mode",
        "core",
        "--enable-main-statement-specialization",
        "--enable-single-period-role-inference",
        "--enable-export-target-scoping",
        "--batch-mode",
        "--log-level",
        log_level,
    ]
    if batch_lite:
        args.append("--batch-lite")
    if not bool(entry.get("target_gap_enabled", True)):
        args.append("--disable-target-gap")
    if bool(entry.get("benchmark_enabled", False)) and bool(entry.get("benchmark_path_exists", False)):
        args.extend(
            [
                "--benchmark-workbook",
                str(entry.get("benchmark_path", "")),
                "--emit-benchmark-report",
                "--enable-benchmark-alignment-repair",
            ]
        )
    if not batch_lite:
        args.extend(
            [
                "--emit-routing-plan",
                "--enable-mapping-suggestions",
                "--enable-review-pack",
                "--emit-review-actions-template",
                "--materialize-reocr-inputs",
                "--emit-delta-report",
                "--emit-promotion-template",
                "--emit-stage6-kpis",
                "--emit-stage7-kpis",
            ]
        )
    return args


def render_command(command: Sequence[str]) -> str:
    return subprocess.list2cmdline([str(part) for part in command])


def process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:  # pragma: no cover
            return False
        output = str(result.stdout or "").strip()
        return bool(output) and "No tasks are running" not in output and f'"{pid}"' in output
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def cleanup_process_tree(process: subprocess.Popen[Any]) -> Dict[str, Any]:
    pid = int(getattr(process, "pid", 0) or 0)
    cleaned_up_pids: List[int] = []
    cleanup_performed = False
    error_message = ""
    if pid <= 0:
        return {
            "cleanup_performed": cleanup_performed,
            "cleaned_up_pids": cleaned_up_pids,
            "orphan_process_detected": False,
            "cleanup_error": error_message,
        }
    try:
        if os.name == "nt":
            result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
            )
            cleanup_performed = result.returncode == 0
            if cleanup_performed:
                cleaned_up_pids.append(pid)
            elif result.returncode not in {128, 255}:
                error_message = str(result.stderr or result.stdout or "").strip()
        else:  # pragma: no cover
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                cleanup_performed = True
                cleaned_up_pids.append(pid)
            except ProcessLookupError:
                cleanup_performed = True
            except Exception as exc:
                error_message = f"{exc.__class__.__name__}: {exc}"
            if process_exists(pid):
                try:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                    cleanup_performed = True
                    if pid not in cleaned_up_pids:
                        cleaned_up_pids.append(pid)
                except ProcessLookupError:
                    cleanup_performed = True
                except Exception as exc:
                    if not error_message:
                        error_message = f"{exc.__class__.__name__}: {exc}"
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    except Exception as exc:  # pragma: no cover
        if not error_message:
            error_message = f"{exc.__class__.__name__}: {exc}"
    orphan_process_detected = process_exists(pid)
    return {
        "cleanup_performed": cleanup_performed,
        "cleaned_up_pids": cleaned_up_pids,
        "orphan_process_detected": orphan_process_detected,
        "cleanup_error": error_message,
    }


def supervise_child_process(
    *,
    command: Sequence[str],
    timeout_seconds: int,
    stdout_path: Path,
    stderr_path: Path,
) -> Dict[str, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    popen_kwargs: Dict[str, Any] = {
        "cwd": str(REPO_ROOT),
        "stdin": subprocess.DEVNULL,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:  # pragma: no cover
        popen_kwargs["start_new_session"] = True
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = subprocess.Popen(command, stdout=stdout_handle, stderr=stderr_handle, **popen_kwargs)
        timeout_hit = False
        cleanup_payload = {
            "cleanup_performed": False,
            "cleaned_up_pids": [],
            "orphan_process_detected": False,
            "cleanup_error": "",
        }
        exit_code = ""
        error_message = ""
        deadline = time.monotonic() + timeout_seconds
        while True:
            polled = process.poll()
            if polled is not None:
                exit_code = int(polled)
                break
            if time.monotonic() >= deadline:
                timeout_hit = True
                cleanup_payload = cleanup_process_tree(process)
                exit_code = 124
                error_message = f"timeout:{timeout_seconds}s"
                break
            time.sleep(0.2)
    finished_at = datetime.now(timezone.utc)
    if exit_code == "" and process.returncode is not None:
        exit_code = int(process.returncode)
    if exit_code == "":
        exit_code = 1
    if not error_message and cleanup_payload.get("cleanup_error"):
        error_message = str(cleanup_payload.get("cleanup_error", "") or "").strip()
    lifecycle_state = "timed_out" if timeout_hit else ("success" if int(exit_code or 0) == 0 else "failed")
    return finalize_doc_lifecycle_payload(
        {
            "command": render_command(command),
            "child_pid": int(getattr(process, "pid", 0) or 0),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": max((finished_at - started_at).total_seconds(), 0.0),
            "exit_code": int(exit_code or 0),
            "timeout_hit": timeout_hit,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "lifecycle_state": lifecycle_state,
            "cleanup_performed": bool(cleanup_payload.get("cleanup_performed", False)),
            "cleaned_up_pids": list(cleanup_payload.get("cleaned_up_pids", [])),
            "orphan_process_detected": bool(cleanup_payload.get("orphan_process_detected", False)),
            "error_message": error_message,
        }
    )


def dispatch_single_doc_subprocess(
    *,
    doc_args: Sequence[str],
    timeout_seconds: int,
    output_root: Path,
) -> Dict[str, Any]:
    command = [sys.executable, "-m", "standardize.cli", *doc_args]
    return supervise_child_process(
        command=command,
        timeout_seconds=timeout_seconds,
        stdout_path=output_root / "batch_child_stdout.log",
        stderr_path=output_root / "batch_child_stderr.log",
    )


def run_single_doc(
    *,
    batch_run_id: str,
    entry: Dict[str, Any],
    registry: Dict[str, Dict[str, Any]],
    template_path: Path,
    batch_run_dir: Path,
    batch_lite: bool,
    log_level: str,
) -> Dict[str, Any]:
    output_root = batch_run_dir / str(entry.get("doc_id", "doc")).strip()
    output_root.mkdir(parents=True, exist_ok=True)
    resolution = resolve_benchmark_entry(doc_id=str(entry.get("doc_id", "")), registry=registry, job_id=str(entry.get("job_id", "")))
    result = build_doc_result_template(batch_run_id=batch_run_id, entry=entry, output_root=output_root, resolution=resolution)
    started_at = datetime.now(timezone.utc)
    result["started_at"] = started_at.isoformat()
    result["entered_processing_scope"] = True
    result["run_status"] = "in_progress"
    result["lifecycle_state"] = "in_progress"
    previous_run_dirs = list_run_dirs(output_root)

    input_dir = Path(str(entry.get("input_dir", "") or ""))
    source_image_dir = Path(str(entry.get("source_image_dir", "") or ""))
    if not input_dir.exists() or not source_image_dir.exists():
        finished_at = datetime.now(timezone.utc)
        result.update(
            {
                "entered_processing_scope": False,
                "run_status": "skipped",
                "lifecycle_state": "skipped",
                "exit_code": 0,
                "finished_at": finished_at.isoformat(),
                "duration_seconds": max((finished_at - started_at).total_seconds(), 0.0),
                "benchmark_scope_status": resolution.get("benchmark_scope_status", "skipped_no_registry"),
                "target_scope_status": resolution.get("target_scope_status", "skipped_no_benchmark"),
                "metadata_contract_pass": False,
                "pages_skipped_pass": False,
                "full_run_contract_pass": False,
                "error_message": "missing_input_paths",
            }
        )
        return finalize_doc_result_payload(result)

    doc_args = build_single_doc_args(
        entry=entry,
        template_path=template_path,
        output_root=output_root,
        batch_lite=batch_lite,
        log_level=log_level,
    )
    exit_code = 1
    error_message = ""
    supervision: Dict[str, Any] = build_doc_lifecycle_template(batch_run_id=batch_run_id, entry=entry)
    try:
        supervision = dispatch_single_doc_subprocess(
            doc_args=doc_args,
            timeout_seconds=get_doc_timeout_seconds(),
            output_root=output_root,
        )
        exit_code = int(supervision.get("exit_code", 1))
        error_message = str(supervision.get("error_message", "") or "").strip()
    except SystemExit as exc:  # pragma: no cover
        try:
            exit_code = int(exc.code or 1)
        except (TypeError, ValueError):
            exit_code = 1
        error_message = f"system_exit:{exc.code}"
        supervision.update({"exit_code": exit_code, "error_message": error_message, "lifecycle_state": "failed"})
    except Exception as exc:  # pragma: no cover
        exit_code = 1
        error_message = f"{exc.__class__.__name__}: {exc}"
        supervision.update({"exit_code": exit_code, "error_message": error_message, "lifecycle_state": "failed"})
        LOGGER.exception("Batch doc run failed for %s", entry.get("doc_id", ""))
        traceback.print_exc()

    finished_at = datetime.now(timezone.utc)
    run_dir = locate_new_run_dir(output_root, previous_run_dirs)
    run_summary = read_json(run_dir / "run_summary.json") if run_dir else {}
    benchmark_summary = read_json(run_dir / "benchmark_summary.json") if run_dir else {}
    benchmark_alignment_summary = read_json(run_dir / "benchmark_alignment_summary.json") if run_dir else {}
    metadata_contract_summary = read_json(run_dir / "metadata_contract_summary.json") if run_dir else {}
    pages_skipped_audit = read_json(run_dir / "pages_skipped_metric_audit.json") if run_dir else {}
    reocr_dedupe_audit = read_json(run_dir / "reocr_dedupe_audit.json") if run_dir else {}
    source_backed_summary = read_json(run_dir / "source_backed_gap_closure_summary.json") if run_dir else {}
    full_run_contract_summary = read_json(run_dir / "full_run_contract_summary.json") if run_dir else {}
    missing_core_outputs = collect_missing_core_outputs(run_dir)
    final_scope = finalize_scope_status(resolution, benchmark_summary, benchmark_alignment_summary) if run_dir and run_summary else finalize_incomplete_scope_status(resolution)

    if exit_code == 0 and run_dir and run_summary and not missing_core_outputs:
        run_status = "success"
    elif exit_code == 124 or error_message.startswith("timeout:") or error_message.startswith("system_exit:124"):
        run_status = "timed_out"
    else:
        run_status = "failed"
    result.update(
        {
            "run_id": str(run_summary.get("run_id", "") or (run_dir.name if run_dir else "")).strip(),
            "run_status": run_status,
            "run_dir": str(run_dir) if run_dir else "",
            "exit_code": exit_code,
            "duration_seconds": max((finished_at - started_at).total_seconds(), 0.0),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "benchmark_scope_status": final_scope.get("benchmark_scope_status", ""),
            "target_scope_status": final_scope.get("target_scope_status", ""),
            "metadata_contract_pass": bool(metadata_contract_summary.get("pass", False)),
            "pages_skipped_pass": bool(pages_skipped_audit.get("pass", False)),
            "full_run_contract_pass": int(full_run_contract_summary.get("contract_fail_total", 0) or 0) == 0 if full_run_contract_summary else False,
            "error_message": error_message,
            "missing_required_outputs": missing_core_outputs if run_status != "success" else [],
            "benchmark_missing_true_total": int(run_summary.get("benchmark_missing_true_total", 0) or 0),
            "benchmark_missing_true_total_raw": int(run_summary.get("benchmark_missing_true_total_raw", run_summary.get("benchmark_missing_true_total", 0)) or 0),
            "target_missing_total": int(run_summary.get("target_missing_total", 0) or 0),
            "target_missing_total_raw": int(run_summary.get("target_missing_total_raw", run_summary.get("target_missing_total", 0)) or 0),
            "review_total": int(run_summary.get("review_total", 0) or 0),
            "pages_total": int(run_summary.get("pages_total", 0) or 0),
            "pages_with_tables": int(run_summary.get("pages_with_tables", 0) or 0),
            "pages_skipped_as_non_table": int(pages_skipped_audit.get("pages_skipped_as_non_table", run_summary.get("pages_skipped_as_non_table", 0)) or 0),
            "source_backed_gap_total_before": int(source_backed_summary.get("source_backed_gap_total_before", 0) or 0),
            "source_backed_gap_total_after": int(source_backed_summary.get("source_backed_gap_total_after", 0) or 0),
            "safe_to_apply_total": int(source_backed_summary.get("safe_to_apply_total", 0) or 0),
            "applied_total": int(source_backed_summary.get("applied_total", 0) or 0),
            "closed_total": int(source_backed_summary.get("closed_total", 0) or 0),
            "supervision": finalize_doc_lifecycle_payload(
                {
                    **supervision,
                    "batch_run_id": batch_run_id,
                    "job_id": str(entry.get("job_id", "") or "").strip(),
                    "doc_id": str(entry.get("doc_id", "") or "").strip(),
                    "company": str(entry.get("company", "") or "").strip(),
                    "run_id": str(run_summary.get("run_id", "") or (run_dir.name if run_dir else "")).strip(),
                    "run_status": run_status,
                    "lifecycle_state": run_status if run_status in {"success", "failed", "skipped", "timed_out"} else str(supervision.get("lifecycle_state", "") or "").strip(),
                }
            ),
        }
    )
    return finalize_doc_result_payload(result)


def collect_reocr_rows(doc_results: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for result in doc_results:
        if str(result.get("run_status", "")).strip() != "success":
            continue
        run_dir_value = str(result.get("run_dir", "") or "").strip()
        if not run_dir_value:
            continue
        run_dir = Path(run_dir_value)
        path = run_dir / "reocr_task_pruned_deduped.csv"
        for row in read_csv(path):
            payload = dict(row)
            payload["job_id"] = str(result.get("job_id", "") or "").strip()
            rows.append(payload)
    return rows


def build_final_scope_rows(
    selected_entries: Sequence[Dict[str, Any]],
    doc_results: Sequence[Dict[str, Any]],
    registry: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    result_by_doc = {str(row.get("doc_id", "") or "").strip(): row for row in doc_results}
    rows: List[Dict[str, Any]] = []
    for entry in selected_entries:
        doc_id = str(entry.get("doc_id", "") or "").strip()
        result = result_by_doc.get(doc_id, {})
        resolution = resolve_benchmark_entry(doc_id=doc_id, registry=registry, job_id=str(entry.get("job_id", "")))
        run_dir_value = str(result.get("run_dir", "") or "").strip()
        if str(result.get("run_status", "") or "").strip() == "success" and run_dir_value:
            run_dir = Path(run_dir_value)
            rows.append(
                finalize_scope_status(
                    resolution,
                    benchmark_summary=read_json(run_dir / "benchmark_summary.json"),
                    alignment_summary=read_json(run_dir / "benchmark_alignment_summary.json"),
                )
            )
            continue
        rows.append(finalize_incomplete_scope_status(resolution))
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the repo-local multi-document standardization batch.")
    parser.add_argument("--template", required=True, help="Path to the standard accounting workbook template.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_BATCH_OUTPUT_ROOT),
        help=f"Directory that will receive the fresh batch run subdirectory. Defaults to ./{repo_relative(DEFAULT_BATCH_OUTPUT_ROOT)}",
    )
    parser.add_argument(
        "--registry",
        default=str(DEFAULT_REGISTRY_PATH),
        help=f"Repo-local benchmark/corpus registry YAML. Defaults to ./{repo_relative(DEFAULT_REGISTRY_PATH)}",
    )
    parser.add_argument("--doc-ids", default="", help="Optional comma-separated doc ids to include.")
    parser.add_argument("--batch-mode", action="store_true", help="Acknowledge multi-document batch execution.")
    parser.add_argument("--batch-lite", action="store_true", help="Skip heavy optional per-doc artifacts and keep core outputs only.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, e.g. INFO or DEBUG.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    cli.configure_logging(args.log_level)

    if not args.batch_mode:
        parser.error("--batch-mode is required for the multi-document batch runner.")

    template_path = Path(args.template).resolve()
    if not template_path.exists():
        parser.error(f"Template workbook does not exist: {template_path}")

    registry_path = Path(args.registry).resolve()
    registry = load_benchmark_registry(registry_path)
    if not registry:
        parser.error(f"Registry is missing or empty: {registry_path}")

    doc_ids = [token.strip() for token in str(args.doc_ids or "").split(",") if token.strip()]
    selected_entries = filter_registry_entries(registry, doc_ids)
    if not selected_entries:
        parser.error("No registry entries matched the requested batch selection.")

    run_id = make_batch_run_id()
    base_output_dir = Path(args.output_dir).resolve()
    batch_run_dir = base_output_dir / run_id
    batch_run_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)

    initial_resolution_rows = [
        resolve_benchmark_entry(doc_id=str(entry.get("doc_id", "")), registry=registry, job_id=str(entry.get("job_id", "")))
        for entry in selected_entries
    ]
    registry_resolution = build_registry_resolution_payload(run_id=run_id, registry_path=registry_path, rows=initial_resolution_rows)
    write_json(batch_run_dir / "benchmark_registry_resolution.json", registry_resolution)

    doc_results: List[Dict[str, Any]] = []
    lifecycle_rows: List[Dict[str, Any]] = []
    result_index: Dict[str, int] = {}
    for entry, resolution in zip(selected_entries, initial_resolution_rows):
        output_root = batch_run_dir / str(entry.get("doc_id", "doc")).strip()
        template_result = build_doc_result_template(batch_run_id=run_id, entry=entry, output_root=output_root, resolution=resolution)
        doc_results.append(template_result)
        lifecycle_rows.append(build_doc_lifecycle_template(batch_run_id=run_id, entry=entry))
        result_index[template_result["doc_id"]] = len(doc_results) - 1
        write_doc_status_artifacts(output_root, template_result)

    write_batch_progress_artifacts(
        batch_run_dir,
        build_batch_completion_summary(
            run_id=run_id,
            started_at=started_at,
            finished_at=started_at,
            rows=doc_results,
            completed=False,
            process_exited=False,
        ),
        doc_results,
        lifecycle_rows,
    )

    completed = False
    batch_error_message = ""
    interrupted = False

    try:
        for entry in selected_entries:
            doc_id = str(entry.get("doc_id", "") or "").strip()
            idx = result_index[doc_id]
            progress_row = dict(doc_results[idx])
            progress_row.update(
                {
                    "run_status": "in_progress",
                    "lifecycle_state": "in_progress",
                    "entered_processing_scope": True,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "finished_at": "",
                    "duration_seconds": 0.0,
                    "error_message": "",
                    "failure_kind": "",
                    "missing_required_outputs": [],
                }
            )
            doc_results[idx] = finalize_doc_result_payload(progress_row)
            lifecycle_rows[idx] = build_doc_lifecycle_in_progress(
                batch_run_id=run_id,
                entry=entry,
                template_path=template_path,
                output_root=batch_run_dir / doc_id,
                batch_lite=bool(args.batch_lite),
                log_level=str(args.log_level),
                started_at=datetime.fromisoformat(str(doc_results[idx].get("started_at", "") or datetime.now(timezone.utc).isoformat())),
            )
            write_doc_status_artifacts(batch_run_dir / doc_id, doc_results[idx])
            write_batch_progress_artifacts(
                batch_run_dir,
                build_batch_completion_summary(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    rows=doc_results,
                    completed=False,
                    process_exited=False,
                ),
                doc_results,
                lifecycle_rows,
            )

            LOGGER.info("Batch doc %s (%s): starting.", entry.get("doc_id", ""), entry.get("company", ""))
            try:
                result = run_single_doc(
                    batch_run_id=run_id,
                    entry=entry,
                    registry=registry,
                    template_path=template_path,
                    batch_run_dir=batch_run_dir,
                    batch_lite=bool(args.batch_lite),
                    log_level=str(args.log_level),
                )
            except KeyboardInterrupt as exc:  # pragma: no cover
                interrupted = True
                batch_error_message = f"KeyboardInterrupt: {exc}"
                now = datetime.now(timezone.utc)
                failed_row = dict(doc_results[idx])
                failed_row.update(
                    {
                        "run_status": "failed",
                        "lifecycle_state": "failed",
                        "finished_at": now.isoformat(),
                        "duration_seconds": elapsed_seconds(str(failed_row.get("started_at", "") or ""), now),
                        "error_message": batch_error_message,
                        "missing_required_outputs": list(CORE_SUMMARY_FILES),
                    }
                )
                doc_results[idx] = finalize_doc_result_payload(failed_row)
                lifecycle_rows[idx] = merge_lifecycle_row(lifecycle_rows[idx], doc_results[idx])
                write_doc_status_artifacts(batch_run_dir / doc_id, doc_results[idx])
                raise
            except Exception as exc:  # pragma: no cover
                LOGGER.exception("Batch orchestration failed while processing doc %s", doc_id)
                now = datetime.now(timezone.utc)
                failed_row = dict(doc_results[idx])
                failed_row.update(
                    {
                        "run_status": "failed",
                        "lifecycle_state": "failed",
                        "finished_at": now.isoformat(),
                        "duration_seconds": elapsed_seconds(str(failed_row.get("started_at", "") or ""), now),
                        "error_message": f"orchestrator_exception:{exc.__class__.__name__}: {exc}",
                        "missing_required_outputs": list(CORE_SUMMARY_FILES),
                    }
                )
                doc_results[idx] = finalize_doc_result_payload(failed_row)
                lifecycle_rows[idx] = merge_lifecycle_row(lifecycle_rows[idx], doc_results[idx])
            else:
                doc_results[idx] = finalize_doc_result_payload(result)
                lifecycle_rows[idx] = merge_lifecycle_row(lifecycle_rows[idx], doc_results[idx])

            write_doc_status_artifacts(batch_run_dir / doc_id, doc_results[idx])
            write_batch_progress_artifacts(
                batch_run_dir,
                build_batch_completion_summary(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    rows=doc_results,
                    completed=False,
                    process_exited=False,
                ),
                doc_results,
                lifecycle_rows,
            )
            LOGGER.info(
                "Batch doc %s completed with status=%s exit_code=%s run_id=%s",
                doc_results[idx].get("doc_id", ""),
                doc_results[idx].get("run_status", ""),
                doc_results[idx].get("exit_code", 0),
                doc_results[idx].get("run_id", ""),
            )
        completed = True
    except KeyboardInterrupt as exc:  # pragma: no cover
        interrupted = True
        if not batch_error_message:
            batch_error_message = f"KeyboardInterrupt: {exc}"
        LOGGER.warning("Batch run interrupted: %s", batch_error_message)
    except Exception as exc:  # pragma: no cover
        batch_error_message = f"{exc.__class__.__name__}: {exc}"
        LOGGER.exception("Batch orchestrator failed")
    finally:
        finalization_time = datetime.now(timezone.utc)
        if not completed:
            for index, row in enumerate(doc_results):
                current_status = str(row.get("run_status", "") or "").strip()
                if current_status in {"success", "failed", "skipped", "timed_out"}:
                    continue
                updated_row = dict(row)
                updated_row["finished_at"] = finalization_time.isoformat()
                updated_row["duration_seconds"] = elapsed_seconds(str(updated_row.get("started_at", "") or ""), finalization_time)
                if updated_row.get("entered_processing_scope", False):
                    updated_row["run_status"] = "failed"
                    updated_row["lifecycle_state"] = "failed"
                    updated_row["error_message"] = batch_error_message or ("KeyboardInterrupt" if interrupted else "batch_orchestrator_incomplete")
                    updated_row["missing_required_outputs"] = list(CORE_SUMMARY_FILES)
                else:
                    updated_row["run_status"] = "skipped"
                    updated_row["lifecycle_state"] = "skipped"
                    updated_row["error_message"] = "batch_aborted_before_start"
                    updated_row["missing_required_outputs"] = []
                doc_results[index] = finalize_doc_result_payload(updated_row)
                lifecycle_rows[index] = merge_lifecycle_row(lifecycle_rows[index], doc_results[index])
                write_doc_status_artifacts(Path(str(doc_results[index].get("output_root", "") or batch_run_dir / updated_row.get("doc_id", "doc"))), doc_results[index])

        finished_at = datetime.now(timezone.utc)
        final_scope_rows = build_final_scope_rows(selected_entries, doc_results, registry)
        scope_rows = build_batch_scope_rows(final_scope_rows, doc_results)
        scope_summary = build_batch_scope_summary(run_id, scope_rows)
        pages_audit = build_batch_pages_skipped_audit(run_id, doc_results)
        source_backed_by_doc_rows = build_source_backed_gap_by_doc_rows(scope_rows)
        source_backed_summary = build_source_backed_gap_batch_summary(run_id=run_id, doc_rows=source_backed_by_doc_rows)
        reocr_before_rows = collect_reocr_rows(doc_results)
        reocr_after_rows = dedupe_reocr_rows_pass2(reocr_before_rows)
        reocr_pass2_audit = build_reocr_pass2_audit(run_id=run_id, before_rows=reocr_before_rows, after_rows=reocr_after_rows)
        completion_summary = build_batch_completion_summary(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            rows=doc_results,
            completed=True,
            process_exited=True,
            aborted=not completed,
            batch_error_message=batch_error_message,
        )
        batch_orchestrator_audit = build_batch_orchestrator_audit(
            run_id=run_id,
            completion_summary=completion_summary,
            doc_results=doc_results,
        )
        batch_supervisor_audit = build_batch_supervisor_audit(
            run_id=run_id,
            lifecycle_rows=lifecycle_rows,
        )
        batch_payloads = [
            ("benchmark_registry_resolution.json", registry_resolution),
            ("batch_scope_summary.json", scope_summary),
            ("batch_completion_summary.json", completion_summary),
            ("batch_orchestrator_audit.json", batch_orchestrator_audit),
            ("batch_supervisor_audit.json", batch_supervisor_audit),
            ("pages_skipped_metric_audit.json", pages_audit),
            ("reocr_dedupe_pass2_audit.json", reocr_pass2_audit),
            ("source_backed_gap_closure_summary.json", source_backed_summary),
        ]
        metadata_contract_summary = build_batch_metadata_contract_summary(
            run_id=run_id,
            batch_payloads=batch_payloads,
            doc_results=doc_results,
        )
        hardening_summary = build_batch_hardening_summary(
            run_id=run_id,
            scope_summary=scope_summary,
            completion_summary=completion_summary,
            metadata_contract_summary=metadata_contract_summary,
            pages_audit=pages_audit,
            reocr_pass2_audit=reocr_pass2_audit,
            source_backed_summary=source_backed_summary,
            run_matrix_rows=doc_results,
        )

        write_errors: List[Dict[str, Any]] = []
        safe_write_json(batch_run_dir / "benchmark_registry_resolution.json", registry_resolution, write_errors)
        safe_write_json(batch_run_dir / "batch_scope_summary.json", scope_summary, write_errors)
        safe_write_csv(batch_run_dir / "batch_scope_by_doc.csv", scope_rows, write_errors)
        safe_write_json(batch_run_dir / "pages_skipped_metric_audit.json", pages_audit, write_errors)
        safe_write_json(batch_run_dir / "reocr_dedupe_pass2_audit.json", reocr_pass2_audit, write_errors)
        safe_write_csv(batch_run_dir / "reocr_task_pruned_deduped_pass2.csv", reocr_after_rows, write_errors)
        safe_write_json(batch_run_dir / "source_backed_gap_closure_summary.json", source_backed_summary, write_errors)
        safe_write_csv(batch_run_dir / "source_backed_gap_closure_by_doc.csv", source_backed_by_doc_rows, write_errors)
        safe_write_json(batch_run_dir / "metadata_contract_summary.json", metadata_contract_summary, write_errors)
        safe_write_json(batch_run_dir / "hardening_summary.json", hardening_summary, write_errors)
        safe_write_json(batch_run_dir / "batch_completion_summary.json", completion_summary, write_errors)
        safe_write_json(batch_run_dir / "batch_orchestrator_audit.json", batch_orchestrator_audit, write_errors)
        safe_write_json(batch_run_dir / "batch_supervisor_audit.json", batch_supervisor_audit, write_errors)
        safe_write_csv(batch_run_dir / "batch_run_matrix.csv", doc_results, write_errors, fieldnames=DOC_RESULT_FIELDNAMES)
        safe_write_csv(batch_run_dir / "doc_lifecycle_manifest.csv", lifecycle_rows, write_errors, fieldnames=DOC_LIFECYCLE_FIELDNAMES)

        missing_required_batch_outputs = list_missing_batch_outputs(batch_run_dir)
        if missing_required_batch_outputs:
            completion_summary = build_batch_completion_summary(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                rows=doc_results,
                completed=True,
                process_exited=True,
                aborted=not completed,
                batch_error_message=batch_error_message,
                missing_required_batch_outputs=missing_required_batch_outputs,
            )
            batch_orchestrator_audit = build_batch_orchestrator_audit(
                run_id=run_id,
                completion_summary=completion_summary,
                doc_results=doc_results,
                missing_required_batch_outputs=missing_required_batch_outputs,
            )
            batch_supervisor_audit = build_batch_supervisor_audit(
                run_id=run_id,
                lifecycle_rows=lifecycle_rows,
            )
            hardening_summary = build_batch_hardening_summary(
                run_id=run_id,
                scope_summary=scope_summary,
                completion_summary=completion_summary,
                metadata_contract_summary=metadata_contract_summary,
                pages_audit=pages_audit,
                reocr_pass2_audit=reocr_pass2_audit,
                source_backed_summary=source_backed_summary,
                run_matrix_rows=doc_results,
            )
            safe_write_json(batch_run_dir / "batch_completion_summary.json", completion_summary, write_errors)
            safe_write_json(batch_run_dir / "batch_orchestrator_audit.json", batch_orchestrator_audit, write_errors)
            safe_write_json(batch_run_dir / "batch_supervisor_audit.json", batch_supervisor_audit, write_errors)
            safe_write_json(batch_run_dir / "hardening_summary.json", hardening_summary, write_errors)

        LOGGER.info("Batch run complete: %s", batch_run_dir)
        return 0 if completion_summary.get("success", False) else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
