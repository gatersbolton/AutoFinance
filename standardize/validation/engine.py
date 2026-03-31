from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple

from ..models import FactRecord, ValidationResultRecord, compact_json
from .rules import compute_tolerance, find_best_fact_by_aliases, has_amount_legality_issue, is_ratio_fact, is_total_label, within_tolerance


def run_validation(
    facts: List[FactRecord],
    validation_config: Dict[str, Any],
) -> Tuple[List[ValidationResultRecord], Dict[str, Any]]:
    results: List[ValidationResultRecord] = []
    counter = 0

    def next_id() -> str:
        nonlocal counter
        counter += 1
        return f"VAL{counter:06d}"

    results.extend(run_balance_equation(facts, validation_config.get("balance_equation", {}), next_id))
    results.extend(run_subtotal_checks(facts, validation_config.get("subtotal_rules", {}), next_id))
    results.extend(run_ratio_checks(facts, validation_config.get("ratio_rules", {}), next_id))
    results.extend(run_amount_legality_checks(facts, validation_config.get("amount_legality", {}), next_id))

    status_counter = Counter(result.status for result in results)
    rule_counter = Counter(result.rule_name for result in results)
    summary = {
        "validation_total": len(results),
        "validation_pass_total": status_counter.get("pass", 0),
        "validation_fail_total": status_counter.get("fail", 0),
        "validation_review_total": status_counter.get("review", 0),
        "validation_skipped_total": status_counter.get("skipped", 0),
        "validation_reason_breakdown": dict(rule_counter),
    }
    return results, summary


def run_balance_equation(
    facts: List[FactRecord],
    config: Dict[str, Any],
    next_id,
) -> List[ValidationResultRecord]:
    aliases = config.get("aliases", {})
    results: List[ValidationResultRecord] = []
    grouped: Dict[Tuple[str, str], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        if fact.period_key and fact.value_num is not None and fact.status not in {"review", "conflict", "suppressed"}:
            grouped[(fact.doc_id, fact.period_key)].append(fact)

    for (doc_id, period_key), items in grouped.items():
        candidates = [fact for fact in items if fact.statement_type == "balance_sheet"]
        assets = find_best_fact_by_aliases(candidates, aliases.get("assets_total", ["资产总计"]))
        liabilities = find_best_fact_by_aliases(candidates, aliases.get("liabilities_total", ["负债合计"]))
        equity = find_best_fact_by_aliases(candidates, aliases.get("equity_total", ["所有者权益合计", "股东权益合计"]))
        if not assets or not liabilities or not equity:
            results.append(
                ValidationResultRecord(
                    validation_id=next_id(),
                    doc_id=doc_id,
                    statement_type="balance_sheet",
                    period_key=period_key,
                    rule_name="balance_equation",
                    rule_type="equation",
                    lhs_value=assets.value_num if assets else None,
                    rhs_value=(liabilities.value_num + equity.value_num) if liabilities and equity else None,
                    diff_value=None,
                    tolerance=None,
                    status="skipped",
                    evidence_fact_refs=[
                        fact.source_cell_ref
                        for fact in (assets, liabilities, equity)
                        if fact is not None
                    ],
                    message="Missing one or more required balance sheet totals.",
                    meta_json=compact_json({"doc_id": doc_id}),
                )
            )
            continue

        lhs_value = float(assets.value_num or 0.0)
        rhs_value = float(liabilities.value_num or 0.0) + float(equity.value_num or 0.0)
        tolerance = compute_tolerance([lhs_value, rhs_value], config.get("tolerance", {}))
        status = "pass" if within_tolerance(lhs_value, rhs_value, tolerance) else "fail"
        results.append(
            ValidationResultRecord(
                validation_id=next_id(),
                doc_id=doc_id,
                statement_type="balance_sheet",
                period_key=period_key,
                rule_name="balance_equation",
                rule_type="equation",
                lhs_value=lhs_value,
                rhs_value=rhs_value,
                diff_value=lhs_value - rhs_value,
                tolerance=tolerance,
                status=status,
                evidence_fact_refs=[assets.source_cell_ref, liabilities.source_cell_ref, equity.source_cell_ref],
                message="资产总计应等于负债合计加所有者权益合计。",
                meta_json=compact_json(
                    {
                        "assets_fact_id": assets.fact_id,
                        "liabilities_fact_id": liabilities.fact_id,
                        "equity_fact_id": equity.fact_id,
                    }
                ),
            )
        )
    return results


def run_subtotal_checks(
    facts: List[FactRecord],
    config: Dict[str, Any],
    next_id,
) -> List[ValidationResultRecord]:
    results: List[ValidationResultRecord] = []
    min_detail_rows = int(config.get("min_detail_rows", 2))
    grouped: Dict[Tuple[str, int, str, str, str], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        if fact.value_num is None or fact.status in {"review", "conflict", "suppressed"}:
            continue
        grouped[(fact.doc_id, fact.page_no, fact.logical_subtable_id, fact.period_key, fact.column_semantic_key)].append(fact)

    for key, items in grouped.items():
        ordered = sorted(items, key=lambda fact: (fact.source_row_start, fact.source_col_start))
        previous_total_row = -1
        for index, fact in enumerate(ordered):
            if not is_total_label(fact.row_label_std):
                continue
            detail_rows = [
                candidate
                for candidate in ordered
                if previous_total_row < candidate.source_row_start < fact.source_row_start and not is_total_label(candidate.row_label_std)
            ]
            previous_total_row = fact.source_row_start
            if len(detail_rows) < min_detail_rows:
                results.append(
                    ValidationResultRecord(
                        validation_id=next_id(),
                        doc_id=key[0],
                        statement_type=fact.statement_type,
                        period_key=fact.period_key,
                        rule_name="subtotal_check",
                        rule_type="subtotal",
                        lhs_value=fact.value_num,
                        rhs_value=None,
                        diff_value=None,
                        tolerance=None,
                        status="skipped",
                        evidence_fact_refs=[fact.source_cell_ref],
                        message="Subtotal row found but detail structure is insufficient for conservative validation.",
                        meta_json=compact_json({"logical_subtable_id": fact.logical_subtable_id}),
                    )
                )
                continue
            rhs_value = sum(float(candidate.value_num or 0.0) for candidate in detail_rows)
            lhs_value = float(fact.value_num or 0.0)
            tolerance = compute_tolerance([lhs_value, rhs_value], config.get("tolerance", {}))
            status = "pass" if within_tolerance(lhs_value, rhs_value, tolerance) else "fail"
            results.append(
                ValidationResultRecord(
                    validation_id=next_id(),
                    doc_id=key[0],
                    statement_type=fact.statement_type,
                    period_key=fact.period_key,
                    rule_name="subtotal_check",
                    rule_type="subtotal",
                    lhs_value=lhs_value,
                    rhs_value=rhs_value,
                    diff_value=lhs_value - rhs_value,
                    tolerance=tolerance,
                    status=status,
                    evidence_fact_refs=[candidate.source_cell_ref for candidate in detail_rows] + [fact.source_cell_ref],
                    message="Subtotal/total row compared with preceding detail rows in the same logical subtable.",
                    meta_json=compact_json({"logical_subtable_id": fact.logical_subtable_id, "detail_rows": len(detail_rows)}),
                )
            )
    return results


def run_ratio_checks(
    facts: List[FactRecord],
    config: Dict[str, Any],
    next_id,
) -> List[ValidationResultRecord]:
    results: List[ValidationResultRecord] = []
    expected_total = float(config.get("expected_total", 1.0))
    grouped: Dict[Tuple[str, int, str, str], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        if fact.value_num is None or fact.status in {"review", "conflict", "suppressed"}:
            continue
        if not is_ratio_fact(fact):
            continue
        grouped[(fact.doc_id, fact.page_no, fact.logical_subtable_id, fact.period_key)].append(fact)

    for key, items in grouped.items():
        total_facts = [fact for fact in items if is_total_label(fact.row_label_std)]
        if not total_facts:
            continue
        for total_fact in total_facts:
            tolerance = float(config.get("tolerance", 0.02))
            lhs_value = float(total_fact.value_num or 0.0)
            status = "pass" if within_tolerance(lhs_value, expected_total, tolerance) else "fail"
            results.append(
                ValidationResultRecord(
                    validation_id=next_id(),
                    doc_id=key[0],
                    statement_type=total_fact.statement_type,
                    period_key=total_fact.period_key,
                    rule_name="ratio_total_check",
                    rule_type="ratio",
                    lhs_value=lhs_value,
                    rhs_value=expected_total,
                    diff_value=lhs_value - expected_total,
                    tolerance=tolerance,
                    status=status,
                    evidence_fact_refs=[total_fact.source_cell_ref],
                    message="Ratio total should be close to 100%.",
                    meta_json=compact_json({"logical_subtable_id": total_fact.logical_subtable_id}),
                )
            )
    return results


def run_amount_legality_checks(
    facts: List[FactRecord],
    config: Dict[str, Any],
    next_id,
) -> List[ValidationResultRecord]:
    results: List[ValidationResultRecord] = []
    for fact in facts:
        if not has_amount_legality_issue(fact):
            continue
        if fact.status == "suppressed":
            continue
        status = config.get("status_when_noise_detected", "review")
        results.append(
            ValidationResultRecord(
                validation_id=next_id(),
                doc_id=fact.doc_id,
                statement_type=fact.statement_type,
                period_key=fact.period_key,
                rule_name="amount_legality",
                rule_type="quality",
                lhs_value=fact.value_num,
                rhs_value=None,
                diff_value=None,
                tolerance=None,
                status=status,
                evidence_fact_refs=[fact.source_cell_ref],
                message="Amount-like field contains suspicious noise or parsing failure.",
                meta_json=compact_json({"issue_flags": fact.issue_flags, "fact_id": fact.fact_id}),
            )
        )
    return results
