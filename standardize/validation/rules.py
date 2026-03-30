from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from ..models import FactRecord
from ..normalize.text import normalize_label_for_matching


TOTAL_LABEL_TOKENS = ("合计", "小计", "总计")


def normalized_fact_name(fact: FactRecord) -> str:
    return normalize_label_for_matching(fact.mapping_name or fact.row_label_std or fact.row_label_raw)


def matches_aliases(fact: FactRecord, aliases: Iterable[str]) -> bool:
    fact_name = normalized_fact_name(fact)
    return any(fact_name == normalize_label_for_matching(alias) for alias in aliases)


def find_best_fact_by_aliases(facts: List[FactRecord], aliases: Iterable[str]) -> Optional[FactRecord]:
    matches = [fact for fact in facts if matches_aliases(fact, aliases)]
    if not matches:
        return None
    return sorted(matches, key=lambda fact: (0 if fact.status == "observed" else 1, fact.source_row_start))[0]


def within_tolerance(lhs_value: float, rhs_value: float, tolerance: float) -> bool:
    return abs(lhs_value - rhs_value) <= tolerance


def compute_tolerance(values: Iterable[float], config: Dict[str, float]) -> float:
    absolute = float(config.get("absolute", 1.0))
    relative = float(config.get("relative", 0.0001))
    max_value = max((abs(value) for value in values), default=0.0)
    return max(absolute, max_value * relative)


def is_total_label(label: str) -> bool:
    normalized = normalize_label_for_matching(label)
    return any(token in normalized for token in TOTAL_LABEL_TOKENS)


def is_ratio_fact(fact: FactRecord) -> bool:
    if fact.value_type == "ratio":
        return True
    return "比例" in fact.column_semantic_key or "比例" in fact.col_header_raw


def has_amount_legality_issue(fact: FactRecord) -> bool:
    issue_flags = set(fact.issue_flags or [])
    return any(
        flag in issue_flags
        for flag in (
            "numeric_parse_failed",
            "contains_chinese_noise",
            "contains_alpha_noise",
            "seal_or_stamp_noise",
            "expected_numeric_but_unparseable",
        )
    )
