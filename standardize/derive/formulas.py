from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from ..models import FactRecord, RelationRecord, compact_json
from ..stable_ids import stable_id
from .relations import seed_formula_candidates_from_relations


def derive_formula_facts(
    facts: Sequence[FactRecord],
    formula_rules: Dict[str, Any] | None = None,
    relation_records: Sequence[RelationRecord] | None = None,
    enabled: bool = False,
) -> Tuple[List[FactRecord], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    formula_rules = formula_rules or {}
    relation_records = relation_records or []
    if not enabled:
        return [], [], {"enabled": False, "derived_facts_total": 0, "derived_conflicts_total": 0}, []

    rules = list(formula_rules.get("rules", []))
    rules.extend(seed_formula_candidates_from_relations(relation_records))
    by_key: Dict[Tuple[str, str, str], List[FactRecord]] = defaultdict(list)
    observed_index: Dict[Tuple[str, str, str], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        if fact.status == "suppressed":
            continue
        if not fact.mapping_code:
            continue
        key = (fact.mapping_code, fact.statement_type, fact.period_key)
        by_key[key].append(fact)
        observed_index[key].append(fact)

    derived_facts: List[FactRecord] = []
    audit_rows: List[Dict[str, Any]] = []
    conflict_rows: List[Dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict) or not rule.get("enabled", True):
            continue
        target_code = str(rule.get("target_code", "")).strip()
        target_name = str(rule.get("target_name", "")).strip()
        rule_id = str(rule.get("rule_id", target_code or "rule")).strip()
        if not target_code:
            continue
        periods = sorted({fact.period_key for fact in facts if fact.period_key})
        statement_types = list(rule.get("statement_types", []))
        if not statement_types:
            statement_types = sorted({fact.statement_type for fact in facts if fact.statement_type})
        for statement_type in statement_types:
            for period_key in periods:
                candidate_facts = build_candidate_facts(
                    facts=facts,
                    rule=rule,
                    target_code=target_code,
                    target_name=target_name,
                    statement_type=statement_type,
                    period_key=period_key,
                    by_key=by_key,
                )
                if not candidate_facts:
                    continue
                for derived_fact, audit_row in candidate_facts:
                    observed = observed_index.get((target_code, statement_type, period_key), [])
                    stronger = choose_strong_observed(observed)
                    if stronger and stronger.value_num is not None and derived_fact.value_num is not None:
                        if round(float(stronger.value_num), 6) != round(float(derived_fact.value_num), 6):
                            conflict_rows.append(
                                {
                                    "target_code": target_code,
                                    "target_name": target_name,
                                    "statement_type": statement_type,
                                    "period_key": period_key,
                                    "rule_id": rule_id,
                                    "observed_fact_id": stronger.fact_id,
                                    "observed_value_num": stronger.value_num,
                                    "derived_fact_id": derived_fact.fact_id,
                                    "derived_value_num": derived_fact.value_num,
                                    "decision": "prefer_observed",
                                    "run_id": "",
                                    "meta_json": compact_json(
                                        {
                                            "source_fact_ids": audit_row.get("source_fact_ids", []),
                                            "source_kind": "derived_formula",
                                        }
                                    ),
                                }
                            )
                            derived_fact.unplaced_reason = "derived_conflict_with_observed"
                    audit_rows.append(audit_row)
                    derived_facts.append(derived_fact)

    summary = {
        "enabled": True,
        "rules_total": len(rules),
        "derived_facts_total": len(derived_facts),
        "derived_conflicts_total": len(conflict_rows),
        "exportable_derived_total": sum(1 for fact in derived_facts if not fact.unplaced_reason),
    }
    return derived_facts, audit_rows, summary, conflict_rows


def build_candidate_facts(
    facts: Sequence[FactRecord],
    rule: Dict[str, Any],
    target_code: str,
    target_name: str,
    statement_type: str,
    period_key: str,
    by_key: Dict[Tuple[str, str, str], List[FactRecord]],
) -> List[Tuple[FactRecord, Dict[str, Any]]]:
    rule_type = str(rule.get("rule_type", "")).strip()
    children = list(rule.get("children", []))
    if not children:
        return []
    child_facts: List[FactRecord] = []
    signed_child_facts: List[Tuple[int, FactRecord]] = []
    for child in children:
        sign = 1
        child_code = ""
        if isinstance(child, dict):
            child_code = str(child.get("code", "")).strip()
            sign = -1 if str(child.get("sign", "+")).strip() == "-" else 1
        else:
            child_code = str(child).strip()
        if not child_code:
            continue
        matched = select_best_fact(by_key.get((child_code, statement_type, period_key), []))
        if matched is None or matched.value_num is None:
            if rule_type in {"sum", "additive_subtractive_formula"}:
                return []
            continue
        child_facts.append(matched)
        signed_child_facts.append((sign, matched))

    if not child_facts:
        return []

    if rule_type == "copy_if_single_nonempty_child":
        if len(child_facts) != 1:
            return []
        value_num = child_facts[0].value_num
    elif rule_type in {"sum", "sum_if_present"}:
        value_num = sum(float(fact.value_num or 0.0) for fact in child_facts)
    elif rule_type == "additive_subtractive_formula":
        value_num = sum(sign * float(fact.value_num or 0.0) for sign, fact in signed_child_facts)
    else:
        return []

    anchor = child_facts[0]
    derived_fact = FactRecord(
        doc_id=anchor.doc_id,
        page_no=anchor.page_no,
        provider="derived_formula",
        statement_type=statement_type,
        statement_name_raw=anchor.statement_name_raw,
        logical_subtable_id=anchor.logical_subtable_id,
        table_semantic_key=anchor.table_semantic_key,
        row_label_raw=target_name or anchor.row_label_raw,
        row_label_std=target_name or anchor.row_label_std,
        row_label_norm=target_name or anchor.row_label_norm,
        row_label_canonical_candidate=target_name or anchor.row_label_canonical_candidate,
        col_header_raw=anchor.col_header_raw,
        col_header_path=list(anchor.col_header_path),
        column_semantic_key=anchor.column_semantic_key,
        period_role_raw=anchor.period_role_raw,
        report_date_raw=anchor.report_date_raw,
        period_key=period_key,
        value_raw="",
        value_num=round(float(value_num), 6),
        value_type="amount",
        unit_raw=anchor.unit_raw,
        unit_multiplier=anchor.unit_multiplier,
        source_cell_ref=f"derived:{target_code}:{period_key}:{rule.get('rule_id', '')}",
        status="derived_resolved",
        mapping_code=target_code,
        mapping_name=target_name,
        mapping_method="derived_formula",
        mapping_confidence=1.0,
        issue_flags=[],
        fact_id=stable_id("DF_", [target_code, statement_type, period_key, rule.get("rule_id", "")]),
        report_date_norm=anchor.report_date_norm,
        period_role_norm=anchor.period_role_norm,
        period_source_level=anchor.period_source_level,
        period_reason=anchor.period_reason,
        duplicate_group_id="",
        kept_fact_id="",
        comparison_status="derived",
        comparison_reason="derived_formula",
        source_kind="derived_formula",
        statement_group_key=anchor.statement_group_key,
        source_row_start=0,
        source_row_end=0,
        source_col_start=0,
        source_col_end=0,
        mapping_relation_type="",
        mapping_review_required=False,
        conflict_id="",
        conflict_decision="",
        unplaced_reason="",
        review_id="",
        suppression_reason="",
        override_source="",
        parent_review_id="",
        parent_task_id="",
    )
    audit_row = {
        "rule_id": str(rule.get("rule_id", "")).strip(),
        "target_code": target_code,
        "target_name": target_name,
        "statement_type": statement_type,
        "period_key": period_key,
        "rule_type": rule_type,
        "source_fact_ids": [fact.fact_id for fact in child_facts],
        "derived_fact_id": derived_fact.fact_id,
        "derived_value_num": derived_fact.value_num,
        "safety_level": str(rule.get("safety_level", "deterministic")).strip(),
        "run_id": "",
        "meta_json": compact_json(
            {
                "child_mapping_codes": [fact.mapping_code for fact in child_facts],
                "source_kind": "derived_formula",
            }
        ),
    }
    return [(derived_fact, audit_row)]


def select_best_fact(facts: Iterable[FactRecord]) -> FactRecord | None:
    candidates = [fact for fact in facts if fact.value_num is not None and fact.status != "suppressed"]
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda fact: (
            1 if fact.source_kind != "derived_formula" else 0,
            1 if fact.conflict_decision in {"accepted_with_validation_support", "accepted_with_rule_support", "accepted"} else 0,
            1 if fact.status == "observed" else 0,
            1 if fact.mapping_code else 0,
        ),
        reverse=True,
    )[0]


def choose_strong_observed(facts: Iterable[FactRecord]) -> FactRecord | None:
    observed = [fact for fact in facts if fact.source_kind != "derived_formula" and fact.value_num is not None]
    if not observed:
        return None
    return select_best_fact(observed)
