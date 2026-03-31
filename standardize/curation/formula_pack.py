from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Sequence, Tuple

from ..models import FactRecord


def build_formula_rule_impact(
    derived_facts: Sequence[FactRecord],
    derived_conflicts: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    impact_by_rule = defaultdict(lambda: {"newly_exportable_facts": 0, "amount_coverage_gain": 0.0, "conflicts_introduced": 0})
    placement_rows: List[Dict[str, Any]] = []
    for fact in derived_facts:
        rule_id = extract_rule_id(fact)
        impact_by_rule[rule_id]["amount_coverage_gain"] += abs(float(fact.value_num or 0.0))
        if not fact.unplaced_reason:
            impact_by_rule[rule_id]["newly_exportable_facts"] += 1
        placement_rows.append(
            {
                "rule_id": rule_id,
                "fact_id": fact.fact_id,
                "mapping_code": fact.mapping_code,
                "mapping_name": fact.mapping_name,
                "statement_type": fact.statement_type,
                "period_key": fact.period_key,
                "value_num": fact.value_num,
                "exportable": not bool(fact.unplaced_reason),
                "unplaced_reason": fact.unplaced_reason,
            }
        )
    for row in derived_conflicts:
        impact_by_rule[row.get("rule_id", "")]["conflicts_introduced"] += 1
    summary = {
        "rules_total": len(impact_by_rule),
        "rule_impact": {
            rule_id: {
                "newly_exportable_facts": values["newly_exportable_facts"],
                "amount_coverage_gain": round(values["amount_coverage_gain"], 6),
                "conflicts_introduced": values["conflicts_introduced"],
            }
            for rule_id, values in sorted(impact_by_rule.items())
        },
    }
    return summary, placement_rows


def extract_rule_id(fact: FactRecord) -> str:
    parts = (fact.source_cell_ref or "").split(":")
    if len(parts) >= 4:
        return parts[3]
    return fact.source_cell_ref or "derived_formula"
