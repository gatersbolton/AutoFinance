from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable

from .decisions import append_mapping_decision_file, apply_mapping_decision_to_output
from .export import write_json
from .policy import default_confidence_threshold
from .relations import SAFE_RELATION_TYPES, normalize_relation_type
from .store import LocalMappingStore


def eligible_for_accept_once_by_confidence(
    output_dir: str | Path,
    threshold: float | None = None,
) -> list[dict[str, Any]]:
    output_dir = Path(output_dir).resolve()
    preview = build_confidence_bulk_accept_preview(output_dir, threshold=threshold, write_file=False)
    return list(preview.get("candidates", []))


def build_confidence_bulk_accept_preview(
    output_dir: str | Path,
    *,
    threshold: float | None = None,
    before_alias_count: int | None = None,
    after_alias_count: int | None = None,
    write_file: bool = True,
) -> dict[str, Any]:
    output_dir = Path(output_dir).resolve()
    resolved_threshold = _normalize_threshold(default_confidence_threshold() if threshold is None else float(threshold))
    review_rows = _read_csv(output_dir / "mapping_review_items.csv")
    decided_raw_ids = _decided_raw_metric_ids(output_dir)
    candidates: list[dict[str, Any]] = []
    excluded_reasons: dict[str, int] = {}
    for row in review_rows:
        eligible, reason = _bulk_row_eligibility(row, threshold=resolved_threshold, decided_raw_ids=decided_raw_ids)
        if not eligible:
            excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
            continue
        candidates.append(_bulk_candidate_row(row))
    preview = {
        "pass": True,
        "threshold": resolved_threshold,
        "default_threshold": default_confidence_threshold(),
        "eligible_total": len(candidates),
        "excluded_total": sum(excluded_reasons.values()),
        "excluded_reasons": excluded_reasons,
        "candidates": candidates,
        "would_apply_decision": "accept_once",
        "future_decision": "accept_once",
        "mutated_mappings": False,
        "store_aliases_before": before_alias_count,
        "store_aliases_after": after_alias_count,
        "eligible": candidates,
    }
    if write_file:
        write_json(output_dir / "confidence_bulk_accept_preview.json", preview)
    return preview


def apply_confidence_bulk_accept(
    output_dir: str | Path,
    *,
    store: LocalMappingStore,
    job_id: str = "",
    doc_id: str = "",
    threshold: float | None = None,
    decisions_dir: str | Path | None = None,
    decided_by: str = "web_bulk_confidence",
) -> dict[str, Any]:
    output_dir = Path(output_dir).resolve()
    before_alias_count = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    preview = build_confidence_bulk_accept_preview(
        output_dir,
        threshold=threshold,
        before_alias_count=before_alias_count,
        after_alias_count=before_alias_count,
    )
    decisions: list[dict[str, Any]] = []
    touched_files: list[str] = []
    for candidate in preview.get("candidates", []):
        decision = store.record_decision(
            job_id=job_id,
            doc_id=doc_id or job_id,
            raw_metric_id=str(candidate.get("raw_metric_id", "") or ""),
            raw_metric_name=str(candidate.get("raw_metric_name", "") or ""),
            suggested_code=str(candidate.get("candidate_code", "") or ""),
            suggested_name=str(candidate.get("candidate_name", "") or ""),
            decision="accept_once",
            final_code=str(candidate.get("candidate_code", "") or ""),
            final_name=str(candidate.get("candidate_name", "") or ""),
            relation_type=str(candidate.get("relation_type", "") or "exact_alias"),
            confidence=_float(candidate.get("candidate_score")),
            decided_by=decided_by,
            note="confidence_bulk_accept_once",
        )
        decisions.append(decision)
        touched_files.extend(apply_mapping_decision_to_output(output_dir, decision))
        append_mapping_decision_file(output_dir, decision)
        if decisions_dir is not None:
            append_mapping_decision_file(decisions_dir, decision)
    after_alias_count = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    summary = {
        "pass": True,
        "threshold": preview.get("threshold"),
        "applied_total": len(decisions),
        "rejected_total": int(preview.get("excluded_total", 0)),
        "decision_type": "accept_once",
        "mutated_local_alias_store": after_alias_count != before_alias_count,
        "store_aliases_before": before_alias_count,
        "store_aliases_after": after_alias_count,
        "decisions_written": decisions,
        "touched_files": sorted(set(touched_files)),
    }
    write_json(output_dir / "confidence_bulk_accept_apply_summary.json", summary)
    if decisions_dir is not None:
        write_json(Path(decisions_dir) / "confidence_bulk_accept_apply_summary.json", summary)
    return summary


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_threshold(value: float) -> float:
    value = float(value)
    if value > 1:
        value = value / 100.0
    return max(0.0, min(value, 1.0))


def _decided_raw_metric_ids(output_dir: Path) -> set[str]:
    decided: set[str] = set()
    for row in _read_csv(output_dir / "mapping_decisions.csv"):
        raw_metric_id = str(row.get("raw_metric_id", "") or "")
        decision = str(row.get("decision", "") or "")
        if raw_metric_id and decision:
            decided.add(raw_metric_id)
    return decided


def _bulk_row_eligibility(row: dict[str, Any], *, threshold: float, decided_raw_ids: set[str]) -> tuple[bool, str]:
    raw_metric_id = str(row.get("raw_metric_id", "") or "")
    if raw_metric_id in decided_raw_ids or str(row.get("mapping_decision", "") or ""):
        return False, "already_decided"
    status = str(row.get("mapping_status", "") or "").strip()
    if status not in {"review_required", "unmapped"}:
        return False, "status_not_reviewable"
    candidate_code = str(row.get("candidate_code", "") or row.get("ai_suggestion_code", "") or "")
    if not candidate_code:
        return False, "missing_candidate_code"
    relation_type = normalize_relation_type(row.get("relation_type") or row.get("ai_relation_type") or "")
    if relation_type not in SAFE_RELATION_TYPES:
        return False, "unsafe_relation_type"
    confidence = _float(row.get("mapping_confidence") or row.get("ai_confidence") or row.get("candidate_score"))
    if confidence < threshold:
        return False, "confidence_below_threshold"
    return True, ""


def _bulk_candidate_row(row: dict[str, Any]) -> dict[str, Any]:
    confidence = _float(row.get("mapping_confidence") or row.get("ai_confidence") or row.get("candidate_score"))
    return {
        "review_item_id": row.get("review_item_id", ""),
        "raw_metric_id": row.get("raw_metric_id", ""),
        "raw_metric_name": row.get("原始指标名", ""),
        "candidate_code": row.get("candidate_code", "") or row.get("ai_suggestion_code", ""),
        "candidate_name": row.get("candidate_name", "") or row.get("ai_suggestion_name", ""),
        "candidate_score": confidence,
        "candidate_method": "llm_suggested" if row.get("ai_suggestion_code") else row.get("candidate_method", ""),
        "relation_type": normalize_relation_type(row.get("relation_type") or row.get("ai_relation_type") or ""),
        "mapping_status": row.get("mapping_status", ""),
        "would_apply_decision": "accept_once",
        "future_decision": "accept_once",
    }
