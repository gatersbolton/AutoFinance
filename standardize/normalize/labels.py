from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict, List, Tuple

from ..models import FactRecord, compact_json
from .text import clean_text


ENUMERATION_PATTERNS = [
    re.compile(r"^\s*[一二三四五六七八九十]+[、,，.]"),
    re.compile(r"^\s*[(（]?\d+[)）.]"),
]


def apply_label_canonicalization(
    facts: List[FactRecord],
    rules: Dict[str, Any] | None = None,
    enabled: bool = False,
) -> Tuple[List[FactRecord], List[Dict[str, Any]], Dict[str, Any]]:
    rules = rules or {}
    if not enabled:
        for fact in facts:
            base = clean_text(fact.row_label_std or fact.row_label_raw)
            fact.row_label_norm = base
            fact.row_label_canonical_candidate = base
            fact.normalization_rule_ids = []
        return facts, [], {"enabled": False, "rows_changed": 0, "statement_breakdown": {}}

    audit_rows: List[Dict[str, Any]] = []
    statement_counter = Counter()
    rows_changed = 0
    for fact in facts:
        raw_label = clean_text(fact.row_label_std or fact.row_label_raw)
        normalized, canonical_candidate, applied_rules = canonicalize_label(
            raw_label,
            fact.statement_type,
            rules,
        )
        fact.row_label_norm = normalized
        fact.row_label_canonical_candidate = canonical_candidate or normalized
        fact.normalization_rule_ids = applied_rules
        if normalized != raw_label or canonical_candidate != raw_label:
            rows_changed += 1
        statement_counter[fact.statement_type] += 1
        audit_rows.append(
            {
                "doc_id": fact.doc_id,
                "page_no": fact.page_no,
                "statement_type": fact.statement_type,
                "row_label_raw": fact.row_label_raw,
                "row_label_std": fact.row_label_std,
                "row_label_norm": fact.row_label_norm,
                "row_label_canonical_candidate": fact.row_label_canonical_candidate,
                "normalization_rule_ids": "|".join(applied_rules),
                "fact_id": fact.fact_id,
                "source_cell_ref": fact.source_cell_ref,
                "run_id": "",
                "meta_json": compact_json(
                    {
                        "statement_group_key": fact.statement_group_key,
                        "table_semantic_key": fact.table_semantic_key,
                    }
                ),
            }
        )

    summary = {
        "enabled": True,
        "rows_total": len(facts),
        "rows_changed": rows_changed,
        "statement_breakdown": dict(statement_counter),
        "high_value_rows_changed": sum(1 for fact in facts if fact.normalization_rule_ids and fact.value_num is not None and abs(float(fact.value_num or 0.0)) > 0),
    }
    return facts, audit_rows, summary


def canonicalize_label(value: str, statement_type: str, rules: Dict[str, Any]) -> Tuple[str, str, List[str]]:
    text = clean_text(value)
    applied_rules: List[str] = []
    if not text:
        return "", "", applied_rules

    changed = True
    while changed:
        changed = False
        for pattern in ENUMERATION_PATTERNS:
            new_text = pattern.sub("", text).strip()
            if new_text != text:
                text = new_text
                applied_rules.append("strip_enumeration")
                changed = True
                break

    general_prefixes = rules.get("general_prefixes", [])
    statement_rules = rules.get("statement_rules", {}).get(statement_type, {})
    prefixes = statement_rules.get("prefixes", general_prefixes)
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
            applied_rules.append(f"strip_prefix:{prefix}")
            break

    text = text.rstrip(":：;,，")
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[:：]", "", text)
    text = re.sub(r"[,，]", "", text)
    normalized = text
    if normalized != clean_text(value):
        applied_rules.append("normalize_punctuation")

    synonym_map = statement_rules.get("synonyms", {})
    canonical_candidate = synonym_map.get(normalized, normalized)
    if canonical_candidate != normalized:
        applied_rules.append(f"statement_synonym:{normalized}->{canonical_candidate}")

    return normalized, canonical_candidate, applied_rules
