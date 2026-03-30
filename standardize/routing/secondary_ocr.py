from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple

from ..models import ConflictRecord, IssueRecord, FactRecord, ProviderComparisonRecord, ReOCRTaskRecord, ReviewQueueRecord, SecondaryOCRCandidateRecord, ValidationResultRecord, compact_json


def build_secondary_ocr_candidates(
    facts: List[FactRecord],
    issues: List[IssueRecord],
    validations: List[ValidationResultRecord],
    provider_comparisons: List[ProviderComparisonRecord],
    routing_config: Dict[str, Any],
) -> Tuple[List[SecondaryOCRCandidateRecord], Dict[str, Any]]:
    config = routing_config.get("post_ocr", {})
    facts_by_page: Dict[Tuple[str, int], List[FactRecord]] = defaultdict(list)
    issues_by_page: Dict[Tuple[str, int], List[IssueRecord]] = defaultdict(list)
    validations_by_page: Dict[Tuple[str, int], List[ValidationResultRecord]] = defaultdict(list)
    comparison_map = {(record.doc_id, record.page_no): record for record in provider_comparisons}

    for fact in facts:
        facts_by_page[(fact.doc_id, fact.page_no)].append(fact)
    for issue in issues:
        issues_by_page[(issue.doc_id, issue.page_no)].append(issue)
    for result in validations:
        validations_by_page[(result.doc_id, infer_page_no(result, facts_by_page))].append(result)

    records: List[SecondaryOCRCandidateRecord] = []
    for page_key in sorted(set(list(facts_by_page) + list(issues_by_page) + list(comparison_map))):
        page_facts = facts_by_page.get(page_key, [])
        page_issues = issues_by_page.get(page_key, [])
        page_validations = validations_by_page.get(page_key, [])
        comparison = comparison_map.get(page_key)
        providers_present = sorted({fact.provider for fact in page_facts})
        compared_pairs = comparison.compared_pairs if comparison else 0
        aligned_groups = comparison.aligned_groups if comparison else 0
        coverage = float(compared_pairs) / float(aligned_groups) if aligned_groups else 0.0
        review_ratio = safe_ratio(sum(1 for fact in page_facts if fact.status == "review"), len(page_facts))
        unknown_date_ratio = safe_ratio(sum(1 for fact in page_facts if fact.report_date_norm == "unknown_date"), len(page_facts))
        suspicious_count = sum(1 for issue in page_issues if issue.issue_type == "suspicious_value")
        validation_fail_count = sum(1 for result in page_validations if result.status == "fail")
        statement_counter = Counter(fact.statement_type for fact in page_facts)
        dominant_statement = statement_counter.most_common(1)[0][0] if statement_counter else "unknown"
        mapped_fact_count = sum(1 for fact in page_facts if fact.mapping_code)
        has_xlsx_fallback = any(fact.source_kind == "xlsx_fallback" for fact in page_facts)

        trigger_score = 0.0
        trigger_reasons: List[str] = []
        if suspicious_count:
            trigger_score += float(config.get("weights", {}).get("suspicious_cells", 0.25))
            trigger_reasons.append("suspicious_numeric_cells")
        if validation_fail_count:
            trigger_score += float(config.get("weights", {}).get("validation_fail", 0.3))
            trigger_reasons.append("validation_fail")
        if review_ratio >= float(config.get("review_ratio_threshold", 0.15)):
            trigger_score += float(config.get("weights", {}).get("review_ratio", 0.2))
            trigger_reasons.append("review_ratio_high")
        if unknown_date_ratio >= float(config.get("unknown_date_ratio_threshold", 0.2)):
            trigger_score += float(config.get("weights", {}).get("unknown_date_ratio", 0.2))
            trigger_reasons.append("unknown_date_ratio_high")
        if dominant_statement == "unknown":
            trigger_score += float(config.get("weights", {}).get("unknown_statement", 0.15))
            trigger_reasons.append("statement_type_unknown")
        if compared_pairs == 0:
            trigger_score += float(config.get("weights", {}).get("compare_gap", 0.25))
            trigger_reasons.append(comparison.reason if comparison else "provider_compare_missing")
        if mapped_fact_count == 0 and dominant_statement in {"balance_sheet", "income_statement", "cash_flow"}:
            trigger_score += float(config.get("weights", {}).get("missing_key_facts", 0.2))
            trigger_reasons.append("missing_key_mapped_facts")
        if has_xlsx_fallback:
            trigger_score += float(config.get("weights", {}).get("xlsx_fallback", 0.2))
            trigger_reasons.append("xlsx_fallback_only")

        if len(providers_present) >= 2:
            recommend = False
            reason = "all_supported_providers_already_present"
        else:
            recommend = trigger_score >= float(config.get("selection_threshold", 0.35))
            reason = "high_risk_page" if recommend else "risk_below_threshold"

        records.append(
            SecondaryOCRCandidateRecord(
                doc_id=page_key[0],
                page_no=page_key[1],
                providers_present=",".join(providers_present),
                provider_comparison_coverage=round(coverage, 6),
                trigger_score=round(trigger_score, 6),
                trigger_reasons=trigger_reasons,
                recommend_secondary_ocr=recommend,
                reason=reason,
                meta_json=compact_json(
                    {
                        "review_ratio": review_ratio,
                        "unknown_date_ratio": unknown_date_ratio,
                        "validation_fail_count": validation_fail_count,
                        "suspicious_count": suspicious_count,
                    }
                ),
            )
        )

    plan = {
        "pages_total": len(records),
        "recommended_total": sum(1 for record in records if record.recommend_secondary_ocr),
        "coverage_average": safe_ratio(sum(record.provider_comparison_coverage for record in records), len(records)),
    }
    return records, plan


def build_reocr_tasks(
    review_items: List[ReviewQueueRecord],
    conflicts: List[ConflictRecord],
    reocr_config: Dict[str, Any],
) -> Tuple[List[ReOCRTaskRecord], Dict[str, Any]]:
    config = reocr_config.get("reocr", {})
    tasks: List[ReOCRTaskRecord] = []
    for index, item in enumerate(sorted(review_items, key=lambda row: (-row.priority_score, row.doc_id, row.page_no, row.review_id)), start=1):
        bbox_payload = parse_bbox_payload(item.bbox)
        granularity = choose_granularity(item, bbox_payload)
        suggested_provider = choose_provider(item.provider)
        task_bbox = select_bbox_for_granularity(bbox_payload, granularity)
        meta = parse_meta(item.meta_json)
        table_id, logical_subtable_id = parse_source_ref(meta.get("source_cell_ref", ""))
        tasks.append(
            ReOCRTaskRecord(
                task_id=f"REOCR{index:05d}",
                granularity=granularity,
                doc_id=item.doc_id,
                page_no=item.page_no,
                table_id=table_id,
                logical_subtable_id=logical_subtable_id,
                bbox=json.dumps(task_bbox, ensure_ascii=False) if task_bbox else "",
                reason_codes=item.reason_codes,
                suggested_provider=suggested_provider,
                priority_score=round(item.priority_score, 6),
                expected_benefit=expected_benefit(item.reason_codes),
                source_review_id=item.review_id,
                meta_json=compact_json(
                    {
                        "provider": item.provider,
                        "has_bbox": bool(task_bbox),
                        "conflict_ids": item.related_conflict_ids,
                    }
                ),
            )
        )

    summary = {
        "tasks_total": len(tasks),
        "granularity_breakdown": dict(Counter(task.granularity for task in tasks)),
        "provider_breakdown": dict(Counter(task.suggested_provider for task in tasks)),
    }
    return tasks, summary


def choose_granularity(item: ReviewQueueRecord, bbox_payload: Dict[str, List[int]]) -> str:
    reasons = set(item.reason_codes)
    if not any(bbox_payload.values()):
        return "page"
    if any(reason.startswith("validation:subtotal_check") for reason in reasons):
        return "row" if bbox_payload.get("row_bbox") else "table"
    if any(reason.startswith("conflict:") for reason in reasons):
        return "cell" if bbox_payload.get("cell_bbox") else "row"
    if "source:xlsx_fallback" in reasons:
        return "page"
    if "mapping:unmapped" in reasons:
        return "row" if bbox_payload.get("row_bbox") else "table"
    if "quality:suspicious_numeric" in reasons or "issue:suspicious_value" in reasons:
        return "cell" if bbox_payload.get("cell_bbox") else "row"
    return "row" if bbox_payload.get("row_bbox") else "table"


def expected_benefit(reason_codes: List[str]) -> str:
    if any(reason.startswith("conflict:") for reason in reason_codes):
        return "resolve_provider_conflict"
    if any(reason.startswith("validation:") for reason in reason_codes):
        return "reduce_validation_failures"
    if "mapping:unmapped" in reason_codes:
        return "improve_mapping_readability"
    if "quality:suspicious_numeric" in reason_codes:
        return "improve_numeric_legibility"
    return "improve_review_clarity"


def choose_provider(current_provider: str) -> str:
    if current_provider == "aliyun_table":
        return "tencent_table_v3"
    if current_provider == "tencent_table_v3":
        return "aliyun_table"
    return "manual_or_custom_crop"


def select_bbox_for_granularity(bbox_payload: Dict[str, List[int]], granularity: str) -> List[int]:
    if granularity == "cell":
        return bbox_payload.get("cell_bbox", [])
    if granularity == "row":
        return bbox_payload.get("row_bbox", []) or bbox_payload.get("cell_bbox", [])
    if granularity == "table":
        return bbox_payload.get("table_bbox", []) or bbox_payload.get("row_bbox", []) or bbox_payload.get("cell_bbox", [])
    return []


def infer_page_no(result: ValidationResultRecord, facts_by_page: Dict[Tuple[str, int], List[FactRecord]]) -> int:
    for (doc_id, page_no), facts in facts_by_page.items():
        if doc_id != result.doc_id:
            continue
        if any(fact.source_cell_ref in set(result.evidence_fact_refs) for fact in facts):
            return page_no
    return 0


def parse_bbox_payload(value: str) -> Dict[str, List[int]]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_meta(value: str) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_source_ref(source_ref: str) -> Tuple[str, str]:
    parts = source_ref.split(":")
    if len(parts) < 4:
        return "", ""
    return parts[3], ""


def safe_ratio(numerator: float, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
