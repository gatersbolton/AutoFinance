from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from ..models import CellRecord, ConflictRecord, FactRecord, IssueRecord, MappingCandidateRecord, ReviewQueueRecord, ValidationResultRecord, compact_json
from ..validation.rules import has_amount_legality_issue
from .evidence import attach_review_evidence


def build_review_queue(
    facts: List[FactRecord],
    cells: List[CellRecord],
    issues: List[IssueRecord],
    conflicts: List[ConflictRecord],
    validations: List[ValidationResultRecord],
    mapping_candidates: List[MappingCandidateRecord],
    source_image_dir: Path | None,
    output_dir: Path,
    review_config: Dict[str, object],
    generate_evidence: bool,
) -> Tuple[List[ReviewQueueRecord], Dict[str, object]]:
    fact_by_id = {fact.fact_id: fact for fact in facts}
    fact_by_source = defaultdict(list)
    for fact in facts:
        fact_by_source[fact.source_cell_ref].append(fact)
    candidate_map = build_mapping_candidate_text(mapping_candidates)

    buckets: Dict[str, Dict[str, object]] = {}

    def ensure_item(fact: FactRecord, reason_code: str, conflict_id: str = "", validation_id: str = "") -> None:
        key = fact.fact_id or fact.source_cell_ref
        bucket = buckets.setdefault(
            key,
            {
                "fact": fact,
                "reason_codes": set(),
                "conflict_ids": set(),
                "validation_ids": set(),
            },
        )
        bucket["reason_codes"].add(reason_code)
        if conflict_id:
            bucket["conflict_ids"].add(conflict_id)
        if validation_id:
            bucket["validation_ids"].add(validation_id)

    for conflict in conflicts:
        if conflict.decision not in {"review_required", "unresolved"}:
            continue
        payload = parse_json(conflict.provider_values_json)
        for items in payload.values():
            for item in items:
                fact = fact_by_id.get(item.get("fact_id", ""))
                if fact:
                    ensure_item(fact, f"conflict:{conflict.decision}", conflict_id=conflict.conflict_id)

    for validation in validations:
        if validation.status not in {"fail", "review"}:
            continue
        for ref in validation.evidence_fact_refs:
            for fact in fact_by_source.get(ref, []):
                ensure_item(fact, f"validation:{validation.rule_name}:{validation.status}", validation_id=validation.validation_id)

    for fact in facts:
        if not fact.mapping_code:
            ensure_item(fact, "mapping:unmapped")
        if fact.unplaced_reason:
            ensure_item(fact, f"unplaced:{fact.unplaced_reason}")
        if fact.source_kind == "xlsx_fallback":
            ensure_item(fact, "source:xlsx_fallback")
        if has_amount_legality_issue(fact):
            ensure_item(fact, "quality:suspicious_numeric")
        if fact.conflict_decision in {"review_required", "unresolved"}:
            ensure_item(fact, f"conflict:{fact.conflict_decision}", conflict_id=fact.conflict_id)

    for issue in issues:
        if issue.issue_type != "suspicious_value":
            continue
        for fact in fact_by_source.get(issue.source_cell_ref, []):
            ensure_item(fact, "issue:suspicious_value")

    weights = review_config.get("reason_weights", {}) if isinstance(review_config, dict) else {}
    review_items: List[ReviewQueueRecord] = []
    for index, bucket in enumerate(sorted(buckets.values(), key=lambda item: (item["fact"].doc_id, item["fact"].page_no, item["fact"].fact_id)), start=1):
        fact = bucket["fact"]
        reason_codes = sorted(bucket["reason_codes"])
        priority_score = round(sum(float(weights.get(reason_code, weights.get(reason_code.split(":")[0], 1.0))) for reason_code in reason_codes), 6)
        mapping_text = candidate_map.get(fact.row_label_std or fact.row_label_raw, "")
        review_items.append(
            ReviewQueueRecord(
                review_id=f"REV{index:05d}",
                priority_score=priority_score,
                reason_codes=reason_codes,
                doc_id=fact.doc_id,
                page_no=fact.page_no,
                statement_type=fact.statement_type,
                row_label_raw=fact.row_label_raw,
                row_label_std=fact.row_label_std,
                period_key=fact.period_key,
                value_raw=fact.value_raw,
                value_num=fact.value_num,
                provider=fact.provider,
                source_file=source_file_for_fact(fact, cells),
                bbox="",
                related_fact_ids=[fact.fact_id],
                related_conflict_ids=sorted(bucket["conflict_ids"]),
                related_validation_ids=sorted(bucket["validation_ids"]),
                mapping_candidates=mapping_text,
                evidence_cell_path="",
                evidence_row_path="",
                evidence_table_path="",
                meta_json=compact_json({"source_cell_ref": fact.source_cell_ref}),
            )
        )
        fact.review_id = review_items[-1].review_id

    if generate_evidence:
        attach_review_evidence(review_items, cells, source_image_dir, output_dir, review_config)
    summary = {
        "review_total": len(review_items),
        "reason_breakdown": dict(Counter(reason for item in review_items for reason in item.reason_codes)),
        "with_evidence_total": sum(1 for item in review_items if item.evidence_cell_path or item.evidence_row_path or item.evidence_table_path),
    }
    return review_items, summary


def build_mapping_candidate_text(mapping_candidates: Sequence[MappingCandidateRecord]) -> Dict[str, str]:
    grouped: Dict[str, List[MappingCandidateRecord]] = defaultdict(list)
    for candidate in mapping_candidates:
        grouped[candidate.row_label_std or candidate.row_label_raw].append(candidate)
    result: Dict[str, str] = {}
    for key, values in grouped.items():
        ordered = sorted(values, key=lambda item: (item.candidate_rank, -item.candidate_score))
        result[key] = "; ".join(
            f"{item.candidate_code} {item.candidate_name} ({item.candidate_method},{item.candidate_score:.3f})"
            for item in ordered[:3]
        )
    return result


def source_file_for_fact(fact: FactRecord, cells: Sequence[CellRecord]) -> str:
    for cell in cells:
        if f":{cell.table_id}:" in fact.source_cell_ref and cell.doc_id == fact.doc_id and cell.page_no == fact.page_no and cell.provider == fact.provider:
            return cell.source_file
    return ""


def parse_json(value: str) -> Dict[str, object]:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}
