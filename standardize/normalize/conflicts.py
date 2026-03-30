from __future__ import annotations

import copy
import json
from collections import Counter, defaultdict
from itertools import combinations
from typing import Dict, Iterable, List, Tuple

from ..models import ConflictDecisionAuditRecord, ConflictRecord, FactRecord, ProviderComparisonRecord, ValidationImpactRecord, compact_json
from ..validation import run_validation


def resolve_conflicts(
    facts: List[FactRecord],
    provider_priority: List[str],
    enabled: bool,
) -> Tuple[List[FactRecord], List[ConflictRecord], List[ProviderComparisonRecord]]:
    grouped: Dict[Tuple[str, int, str, str, str, str], List[FactRecord]] = defaultdict(list)
    page_fact_map: Dict[Tuple[str, int], List[FactRecord]] = defaultdict(list)

    for fact in facts:
        fact.comparison_status = fact.comparison_status or "uncompared"
        fact.comparison_reason = fact.comparison_reason or "not_compared_yet"
        page_key = (fact.doc_id, fact.page_no)
        page_fact_map[page_key].append(fact)
        grouped[build_alignment_key(fact)].append(fact)

    conflicts: List[ConflictRecord] = []
    priority_index = {provider: index for index, provider in enumerate(provider_priority)}
    page_metrics: Dict[Tuple[str, int], Dict[str, int]] = defaultdict(
        lambda: {
            "aligned_groups": 0,
            "compared_pairs": 0,
            "equal_pairs": 0,
            "conflict_pairs": 0,
            "uncomparable_groups": 0,
            "facts_missing_alignment_key": 0,
        }
    )

    conflict_counter = 0
    for key, items in grouped.items():
        page_key = (key[0], key[1])
        providers = {item.provider for item in items}
        if not all(key[2:]):
            page_metrics[page_key]["facts_missing_alignment_key"] += len(items)
            continue
        if len(providers) <= 1:
            continue

        page_metrics[page_key]["aligned_groups"] += 1
        provider_groups = group_items_by_provider(items)
        comparable_pairs = build_provider_pairs(provider_groups)
        if not comparable_pairs:
            for item in items:
                item.comparison_status = "uncomparable"
                item.comparison_reason = "no_comparable_values"
            page_metrics[page_key]["uncomparable_groups"] += 1
            continue

        unique_pair_values = {pair["left_value"] for pair in comparable_pairs}
        unique_pair_values.update(pair["right_value"] for pair in comparable_pairs)
        page_metrics[page_key]["compared_pairs"] += len(comparable_pairs)

        if len(unique_pair_values) <= 1:
            for item in items:
                item.comparison_status = "equal"
                item.comparison_reason = "normalized_values_equal"
            page_metrics[page_key]["equal_pairs"] += len(comparable_pairs)
            continue

        page_metrics[page_key]["conflict_pairs"] += len(comparable_pairs)
        conflict_counter += 1
        conflict_id = f"CF{conflict_counter:05d}"
        chosen = choose_fact(items, priority_index)
        provider_values = serialize_provider_values(items)
        magnitude_ratio = compute_group_magnitude_ratio(items)

        if enabled:
            if chosen is None:
                for item in items:
                    item.status = "review"
                    item.comparison_status = "conflict"
                    item.comparison_reason = "unable_to_decide"
                    item.conflict_id = conflict_id
                    item.conflict_decision = "review_required"
                    if "provider_conflict" not in item.issue_flags:
                        item.issue_flags.append("provider_conflict")
                decision = "review_required"
                accepted_provider = ""
                reason = "unable_to_decide"
                accepted_fact_id = ""
            else:
                for item in items:
                    item.conflict_id = conflict_id
                    if item is chosen:
                        if item.status == "conflict":
                            item.status = "observed"
                        item.comparison_status = "accepted"
                        item.comparison_reason = "chosen_for_export"
                        item.conflict_decision = "accepted"
                        continue
                    item.status = "conflict"
                    item.comparison_status = "conflict"
                    item.comparison_reason = "rejected_by_provider_compare"
                    item.conflict_decision = "rejected"
                    if "provider_conflict" not in item.issue_flags:
                        item.issue_flags.append("provider_conflict")
                decision = "accepted"
                accepted_provider = chosen.provider
                accepted_fact_id = chosen.fact_id
                reason = "highest_quality_numeric" if chosen.value_num is not None else "priority_fallback"
        else:
            for item in items:
                item.comparison_status = "conflict"
                item.comparison_reason = "conflict_detected_merge_disabled"
                item.conflict_id = conflict_id
                item.conflict_decision = "detected"
            decision = "detected"
            accepted_provider = ""
            accepted_fact_id = ""
            reason = "conflict_detected_merge_disabled"

        conflicts.append(
            ConflictRecord(
                doc_id=key[0],
                page_no=key[1],
                logical_subtable_id=items[0].logical_subtable_id,
                table_semantic_key=key[2],
                statement_type=items[0].statement_type,
                row_label_std=key[3],
                column_semantic_key=key[4],
                period_key=items[0].period_key,
                provider_values_json=compact_json(provider_values),
                decision=decision,
                accepted_provider=accepted_provider,
                reason=reason,
                meta_json=compact_json(
                    {
                        "providers": sorted(providers),
                        "provider_logical_subtable_ids": {
                            provider: [item.logical_subtable_id for item in provider_groups[provider]]
                            for provider in sorted(provider_groups)
                        },
                        "compared_pairs": comparable_pairs,
                    }
                ),
                conflict_id=conflict_id,
                compared_pair_count=len(comparable_pairs),
                providers=",".join(sorted(providers)),
                candidate_values_json=compact_json(provider_values),
                magnitude_ratio=magnitude_ratio,
                validation_delta="",
                needs_review=decision in {"review_required", "detected"},
                accepted_fact_id=accepted_fact_id,
            )
        )

    comparison_records: List[ProviderComparisonRecord] = []
    for page_key, page_items in sorted(page_fact_map.items()):
        providers_present = sorted({item.provider for item in page_items})
        metrics = page_metrics[page_key]
        reason = summarize_page_reason(providers_present, metrics)
        if len(providers_present) <= 1:
            for item in page_items:
                item.comparison_status = "single_provider"
                item.comparison_reason = "single_provider_only"
        comparison_records.append(
            ProviderComparisonRecord(
                doc_id=page_key[0],
                page_no=page_key[1],
                providers_present=",".join(providers_present),
                aligned_groups=metrics["aligned_groups"],
                compared_pairs=metrics["compared_pairs"],
                equal_pairs=metrics["equal_pairs"],
                conflict_pairs=metrics["conflict_pairs"],
                uncomparable_groups=metrics["uncomparable_groups"],
                reason=reason,
                meta_json=compact_json(
                    {
                        "facts_missing_alignment_key": metrics["facts_missing_alignment_key"],
                        "providers_present": providers_present,
                    }
                ),
            )
        )

    return facts, conflicts, comparison_records


def enrich_conflicts(
    facts: List[FactRecord],
    conflicts: List[ConflictRecord],
    provider_priority: List[str],
    validation_config: Dict[str, object],
    conflict_config: Dict[str, object],
    merge_enabled: bool,
    validation_aware_enabled: bool,
) -> Tuple[List[FactRecord], List[ConflictRecord], List[ConflictDecisionAuditRecord], List[ValidationImpactRecord]]:
    facts_by_id = {fact.fact_id: fact for fact in facts}
    audits: List[ConflictDecisionAuditRecord] = []
    impacts: List[ValidationImpactRecord] = []
    enriched: List[ConflictRecord] = []
    priority_index = {provider: index for index, provider in enumerate(provider_priority)}

    for conflict in conflicts:
        candidate_facts = select_conflict_candidates(conflict, facts_by_id)
        if len(candidate_facts) <= 1:
            conflict.decision = "unresolved"
            conflict.reason = "insufficient_candidates_after_dedupe"
            conflict.needs_review = True
            conflict.accepted_fact_id = ""
            enriched.append(conflict)
            audits.append(
                ConflictDecisionAuditRecord(
                    conflict_id=conflict.conflict_id,
                    doc_id=conflict.doc_id,
                    page_no=conflict.page_no,
                    statement_type=conflict.statement_type,
                    period_key=conflict.period_key,
                    providers=conflict.providers,
                    compared_pair_count=conflict.compared_pair_count,
                    candidate_values_json=conflict.candidate_values_json or conflict.provider_values_json,
                    magnitude_ratio=conflict.magnitude_ratio,
                    decision=conflict.decision,
                    decision_reason=conflict.reason,
                    accepted_fact_id="",
                    needs_review=True,
                    validation_delta="",
                    meta_json="",
                )
            )
            continue

        decision, accepted, reason, validation_delta, candidate_impacts, magnitude_ratio = evaluate_conflict(
            conflict=conflict,
            candidate_facts=candidate_facts,
            all_facts=facts,
            provider_priority=priority_index,
            validation_config=validation_config,
            conflict_config=conflict_config,
            validation_aware_enabled=validation_aware_enabled,
        )
        impacts.extend(candidate_impacts)
        conflict.decision = decision
        conflict.reason = reason
        conflict.accepted_provider = accepted.provider if accepted else ""
        conflict.accepted_fact_id = accepted.fact_id if accepted else ""
        conflict.needs_review = decision in {"review_required", "unresolved"}
        conflict.validation_delta = compact_json(validation_delta) if validation_delta else ""
        conflict.magnitude_ratio = magnitude_ratio
        if accepted:
            conflict.candidate_values_json = compact_json(serialize_provider_values(candidate_facts))
        enriched.append(conflict)

        apply_conflict_decision_to_facts(candidate_facts, conflict, accepted, merge_enabled)
        audits.append(
            ConflictDecisionAuditRecord(
                conflict_id=conflict.conflict_id,
                doc_id=conflict.doc_id,
                page_no=conflict.page_no,
                statement_type=conflict.statement_type,
                period_key=conflict.period_key,
                providers=conflict.providers,
                compared_pair_count=conflict.compared_pair_count,
                candidate_values_json=conflict.candidate_values_json or conflict.provider_values_json,
                magnitude_ratio=magnitude_ratio,
                decision=decision,
                decision_reason=reason,
                accepted_fact_id=accepted.fact_id if accepted else "",
                needs_review=conflict.needs_review,
                validation_delta=conflict.validation_delta,
                meta_json=compact_json(
                    {
                        "candidate_fact_ids": [fact.fact_id for fact in candidate_facts],
                        "merge_enabled": merge_enabled,
                    }
                ),
            )
        )

    return facts, enriched, audits, impacts


def evaluate_conflict(
    conflict: ConflictRecord,
    candidate_facts: List[FactRecord],
    all_facts: List[FactRecord],
    provider_priority: Dict[str, int],
    validation_config: Dict[str, object],
    conflict_config: Dict[str, object],
    validation_aware_enabled: bool,
) -> Tuple[str, FactRecord | None, str, Dict[str, object], List[ValidationImpactRecord], float | None]:
    numeric_candidates = [fact for fact in candidate_facts if fact.value_num is not None]
    magnitude_ratio = compute_group_magnitude_ratio(candidate_facts)
    review_threshold = float(conflict_config.get("magnitude_ratio_review_threshold", 10.0))
    force_review_threshold = float(conflict_config.get("magnitude_ratio_force_review_threshold", 100.0))

    if not numeric_candidates:
        return "unresolved", None, "no_numeric_candidate_available", {}, [], magnitude_ratio

    if len(numeric_candidates) == 1:
        chosen = numeric_candidates[0]
        if any(has_noise(item) or item.value_num is None for item in candidate_facts if item is not chosen):
            return "accepted_with_rule_support", chosen, "single_clean_numeric_candidate", {}, [], magnitude_ratio
        return "review_required", None, "single_numeric_candidate_without_support", {}, [], magnitude_ratio

    if magnitude_ratio is not None and magnitude_ratio >= force_review_threshold:
        return "review_required", None, "magnitude_ratio_exceeds_force_threshold", {}, [], magnitude_ratio

    impacts: List[ValidationImpactRecord] = []
    validation_delta: Dict[str, object] = {}
    if validation_aware_enabled:
        impacts = evaluate_validation_impacts(conflict, candidate_facts, all_facts, validation_config)
        by_fact = {impact.candidate_fact_id: impact for impact in impacts}
        ordered = sorted(
            [impact for impact in impacts if impact.candidate_fact_id],
            key=lambda item: (item.fail_count, item.review_count, -item.delta_score, provider_priority.get(item.candidate_provider, 999)),
        )
        if ordered:
            best = ordered[0]
            second = ordered[1] if len(ordered) > 1 else None
            validation_delta = {
                "best_provider": best.candidate_provider,
                "best_fail_count": best.fail_count,
                "best_review_count": best.review_count,
                "candidates": [
                    {
                        "provider": impact.candidate_provider,
                        "fact_id": impact.candidate_fact_id,
                        "fail_count": impact.fail_count,
                        "review_count": impact.review_count,
                        "delta_score": impact.delta_score,
                    }
                    for impact in impacts
                ],
            }
            if second and (best.fail_count + 1 <= second.fail_count or (best.fail_count == second.fail_count and best.review_count + 2 <= second.review_count)):
                chosen = next((fact for fact in candidate_facts if fact.fact_id == best.candidate_fact_id), None)
                if chosen is not None and (magnitude_ratio is None or magnitude_ratio < review_threshold):
                    return "accepted_with_validation_support", chosen, "validation_delta_supports_candidate", validation_delta, impacts, magnitude_ratio

    ordered_numeric = sorted(
        numeric_candidates,
        key=lambda item: (
            -rule_quality_score(item),
            provider_priority.get(item.provider, 999) * -1,
            item.fact_id,
        ),
        reverse=True,
    )
    best_numeric = ordered_numeric[0]
    second_numeric = ordered_numeric[1] if len(ordered_numeric) > 1 else None

    if magnitude_ratio is not None and magnitude_ratio >= review_threshold:
        return "review_required", None, "magnitude_ratio_requires_review", validation_delta, impacts, magnitude_ratio

    if second_numeric and rule_quality_score(best_numeric) >= rule_quality_score(second_numeric) + 2 and has_noise(second_numeric):
        return "accepted_with_rule_support", best_numeric, "quality_and_noise_support_candidate", validation_delta, impacts, magnitude_ratio

    if any(has_noise(item) for item in candidate_facts if item is not best_numeric) and not has_noise(best_numeric):
        return "accepted_with_rule_support", best_numeric, "clean_numeric_beats_noisy_candidate", validation_delta, impacts, magnitude_ratio

    return "review_required", None, "multiple_numeric_candidates_need_review", validation_delta, impacts, magnitude_ratio


def evaluate_validation_impacts(
    conflict: ConflictRecord,
    candidate_facts: List[FactRecord],
    all_facts: List[FactRecord],
    validation_config: Dict[str, object],
) -> List[ValidationImpactRecord]:
    impacts: List[ValidationImpactRecord] = []
    scoped_facts = [
        fact
        for fact in all_facts
        if fact.doc_id == conflict.doc_id
        and fact.period_key == conflict.period_key
        and fact.statement_type == conflict.statement_type
    ]
    if not scoped_facts:
        scoped_facts = [fact for fact in all_facts if fact.doc_id == conflict.doc_id and fact.period_key == conflict.period_key]

    for candidate in candidate_facts:
        candidate_view = []
        for fact in scoped_facts:
            clone = copy.copy(fact)
            if fact.conflict_id == conflict.conflict_id:
                if fact.fact_id == candidate.fact_id:
                    if clone.status in {"conflict", "review"}:
                        clone.status = "observed"
                else:
                    clone.status = "conflict"
            candidate_view.append(clone)
        results, _summary = run_validation(candidate_view, validation_config)
        impacted_rules = [
            result.rule_name
            for result in results
            if result.status in {"fail", "review"}
            and any(ref in set(get_conflict_fact_refs(candidate_facts)) for ref in result.evidence_fact_refs)
        ]
        fail_count = sum(1 for result in results if result.status == "fail")
        review_count = sum(1 for result in results if result.status == "review")
        delta_score = float(max(0, 20 - fail_count * 4 - review_count))
        impacts.append(
            ValidationImpactRecord(
                conflict_id=conflict.conflict_id,
                candidate_provider=candidate.provider,
                candidate_fact_id=candidate.fact_id,
                doc_id=conflict.doc_id,
                statement_type=conflict.statement_type,
                period_key=conflict.period_key,
                fail_count=fail_count,
                review_count=review_count,
                impacted_rules_json=compact_json(Counter(impacted_rules)),
                delta_score=round(delta_score, 6),
                meta_json=compact_json({"impacted_rules": impacted_rules}),
            )
        )
    return impacts


def apply_conflict_decision_to_facts(
    candidate_facts: List[FactRecord],
    conflict: ConflictRecord,
    accepted_fact: FactRecord | None,
    merge_enabled: bool,
) -> None:
    for fact in candidate_facts:
        fact.conflict_id = conflict.conflict_id
        fact.conflict_decision = conflict.decision
        if conflict.decision in {"review_required", "unresolved"}:
            fact.status = "review"
            fact.comparison_status = "conflict"
            fact.comparison_reason = conflict.reason
            if "provider_conflict" not in fact.issue_flags:
                fact.issue_flags.append("provider_conflict")
            continue

        if accepted_fact is not None and merge_enabled and fact.fact_id == accepted_fact.fact_id:
            if fact.status in {"conflict", "review"}:
                fact.status = "observed"
            fact.comparison_status = "accepted"
            fact.comparison_reason = conflict.reason
            continue

        if accepted_fact is not None and merge_enabled:
            fact.status = "conflict"
            fact.comparison_status = "conflict"
            fact.comparison_reason = conflict.reason
            if "provider_conflict" not in fact.issue_flags:
                fact.issue_flags.append("provider_conflict")
        else:
            fact.status = "review"
            fact.comparison_status = "conflict"
            fact.comparison_reason = conflict.reason


def select_conflict_candidates(conflict: ConflictRecord, facts_by_id: Dict[str, FactRecord]) -> List[FactRecord]:
    candidate_facts: List[FactRecord] = []
    payload = parse_json_payload(conflict.provider_values_json)
    for items in payload.values():
        for item in items:
            fact_id = item.get("fact_id", "")
            if fact_id and fact_id in facts_by_id:
                candidate_facts.append(facts_by_id[fact_id])
    if candidate_facts:
        return candidate_facts
    meta_payload = parse_json_payload(conflict.meta_json)
    pair_refs = meta_payload.get("compared_pairs", [])
    for pair in pair_refs:
        for fact_id_key in ("left_fact_id", "right_fact_id"):
            fact_id = pair.get(fact_id_key, "")
            if fact_id and fact_id in facts_by_id:
                candidate_facts.append(facts_by_id[fact_id])
    deduped: List[FactRecord] = []
    seen = set()
    for fact in candidate_facts:
        if fact.fact_id in seen:
            continue
        deduped.append(fact)
        seen.add(fact.fact_id)
    return deduped


def build_alignment_key(fact: FactRecord) -> Tuple[str, int, str, str, str, str]:
    return (
        fact.doc_id,
        fact.page_no,
        fact.table_semantic_key or "",
        fact.row_label_std or "",
        fact.column_semantic_key or "",
        fact.period_role_norm or fact.period_role_raw or "",
    )


def group_items_by_provider(items: Iterable[FactRecord]) -> Dict[str, List[FactRecord]]:
    grouped: Dict[str, List[FactRecord]] = defaultdict(list)
    for item in items:
        grouped[item.provider].append(item)
    return grouped


def build_provider_pairs(provider_groups: Dict[str, List[FactRecord]]) -> List[Dict[str, object]]:
    comparable_pairs: List[Dict[str, object]] = []
    for left_provider, right_provider in combinations(sorted(provider_groups), 2):
        left_item = best_comparable_item(provider_groups[left_provider])
        right_item = best_comparable_item(provider_groups[right_provider])
        left_value = normalize_fact_value(left_item) if left_item else None
        right_value = normalize_fact_value(right_item) if right_item else None
        if left_value is None or right_value is None:
            continue
        comparable_pairs.append(
            {
                "left_provider": left_provider,
                "right_provider": right_provider,
                "left_value": left_value,
                "right_value": right_value,
                "left_fact_id": left_item.fact_id if left_item else "",
                "right_fact_id": right_item.fact_id if right_item else "",
            }
        )
    return comparable_pairs


def best_comparable_item(items: List[FactRecord]) -> FactRecord | None:
    comparable = [item for item in items if normalize_fact_value(item) is not None]
    if comparable:
        return sorted(comparable, key=lambda item: (-value_quality_score(item), item.fact_id or item.source_cell_ref))[0]
    return None


def choose_fact(items: List[FactRecord], priority_index: Dict[str, int]) -> FactRecord | None:
    scored = sorted(
        items,
        key=lambda item: (
            -value_quality_score(item),
            -date_specificity_score(item.report_date_norm),
            -status_score(item.status),
            -source_kind_score(item.source_kind),
            -(1 if item.mapping_code else 0),
            -len(item.col_header_path or []),
            priority_index.get(item.provider, 999),
            item.fact_id or item.source_cell_ref,
        ),
    )
    if not scored or normalize_fact_value(scored[0]) is None:
        return None
    return scored[0]


def value_quality_score(item: FactRecord) -> int:
    if item.value_num is not None and not has_noise(item):
        return 5
    if item.value_num is not None:
        return 4
    if item.value_raw and item.status not in {"review", "conflict"}:
        return 2
    if item.value_raw:
        return 1
    return 0


def rule_quality_score(item: FactRecord) -> int:
    score = 0
    if item.value_num is not None:
        score += 3
    if not has_noise(item):
        score += 2
    if item.status == "observed":
        score += 1
    if item.mapping_code:
        score += 1
    if item.report_date_norm and item.report_date_norm not in {"unknown_date", ""}:
        score += 1
    return score


def has_noise(item: FactRecord) -> bool:
    flags = set(item.issue_flags or [])
    return any(
        flag in flags
        for flag in (
            "numeric_parse_failed",
            "contains_chinese_noise",
            "contains_alpha_noise",
            "seal_or_stamp_noise",
            "expected_numeric_but_unparseable",
        )
    )


def date_specificity_score(report_date_norm: str) -> int:
    if not report_date_norm or report_date_norm == "unknown_date":
        return 0
    if len(report_date_norm) == 10 and report_date_norm[4] == "-" and report_date_norm[7] == "-":
        return 3
    if report_date_norm.endswith("月"):
        return 2
    if report_date_norm.endswith("年度"):
        return 1
    return 1


def status_score(status: str) -> int:
    order = {
        "observed": 5,
        "repaired": 4,
        "inferred": 3,
        "blank": 2,
        "review": 1,
        "conflict": 0,
    }
    return order.get(status, 0)


def source_kind_score(source_kind: str) -> int:
    order = {
        "json": 2,
        "": 1,
        "xlsx_fallback": 0,
    }
    return order.get(source_kind, 0)


def serialize_provider_values(items: List[FactRecord]) -> Dict[str, List[Dict[str, object]]]:
    payload: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for item in items:
        payload[item.provider].append(
            {
                "fact_id": item.fact_id,
                "logical_subtable_id": item.logical_subtable_id,
                "table_semantic_key": item.table_semantic_key,
                "column_semantic_key": item.column_semantic_key,
                "value_raw": item.value_raw,
                "value_num": item.value_num,
                "status": item.status,
                "issue_flags": item.issue_flags,
                "period_key": item.period_key,
            }
        )
    return dict(payload)


def summarize_page_reason(providers_present: List[str], metrics: Dict[str, int]) -> str:
    if len(providers_present) <= 1:
        return "single_provider_only"
    if metrics["aligned_groups"] == 0:
        if metrics["facts_missing_alignment_key"]:
            return "missing_alignment_key"
        return "no_aligned_pairs_found"
    if metrics["compared_pairs"] == 0:
        return "no_comparable_values"
    return "compared"


def normalize_fact_value(fact: FactRecord | None):
    if fact is None:
        return None
    if fact.value_num is not None:
        return round(float(fact.value_num), 8)
    if fact.value_raw:
        return fact.value_raw.strip()
    return None


def compute_group_magnitude_ratio(items: List[FactRecord]) -> float | None:
    values = [abs(float(item.value_num)) for item in items if item.value_num not in (None, 0)]
    if len(values) < 2:
        return None
    minimum = min(values)
    maximum = max(values)
    if minimum == 0:
        return None
    return round(maximum / minimum, 6)


def get_conflict_fact_refs(items: List[FactRecord]) -> List[str]:
    return [item.source_cell_ref for item in items]


def parse_json_payload(payload: str) -> Dict[str, object]:
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return {}
