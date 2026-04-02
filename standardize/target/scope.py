from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict, List, Sequence, Set, Tuple

from ..models import FactRecord


def scope_facts_to_targets(
    facts: Sequence[FactRecord],
    benchmark_payload: Dict[str, Any] | None = None,
    rules: Dict[str, Any] | None = None,
) -> Tuple[List[FactRecord], List[Dict[str, Any]], Dict[str, Any]]:
    rules = rules or {}
    benchmark_codes = collect_benchmark_codes(benchmark_payload or {})
    scoped_facts = list(facts)
    scope_rows: List[Dict[str, Any]] = []
    counts = Counter()
    for fact in scoped_facts:
        scope, reason = classify_target_scope(fact, benchmark_codes, rules)
        fact.target_scope = scope
        fact.target_scope_reason = reason
        counts[scope] += 1
        scope_rows.append(
            {
                "run_id": "",
                "fact_id": fact.fact_id,
                "doc_id": fact.doc_id,
                "page_no": fact.page_no,
                "statement_type": fact.statement_type,
                "mapping_code": fact.mapping_code,
                "mapping_name": fact.mapping_name,
                "row_label_raw": fact.row_label_raw,
                "row_label_std": fact.row_label_std,
                "row_label_norm": fact.row_label_norm,
                "row_label_canonical_candidate": fact.row_label_canonical_candidate,
                "period_key": fact.period_key,
                "value_num": fact.value_num,
                "target_scope": scope,
                "target_scope_reason": reason,
            }
        )
    summary = {
        "run_id": "",
        "facts_total": len(scoped_facts),
        "scope_breakdown": dict(counts),
    }
    return scoped_facts, scope_rows, summary


def classify_target_scope(fact: FactRecord, benchmark_codes: Set[str], rules: Dict[str, Any]) -> Tuple[str, str]:
    label = (fact.row_label_canonical_candidate or fact.row_label_norm or fact.row_label_std or fact.row_label_raw or "").strip()
    label_lower = label.lower()
    main_statement_types = set(rules.get("main_statement_types", ["balance_sheet", "income_statement", "cash_flow", "changes_in_equity"]))
    note_statements = set(rules.get("note_statement_types", ["note"]))
    note_detail_config = rules.get("note_detail_patterns", {}) or {}
    non_target_config = rules.get("non_target_patterns", {}) or {}
    company_patterns = compile_patterns(rules.get("company_name_patterns", []) or note_detail_config.get("company_like", []))
    aging_patterns = compile_patterns(rules.get("aging_bucket_patterns", []) or note_detail_config.get("aging_bucket", []))
    note_detail_patterns = compile_patterns(rules.get("detail_header_patterns", []) or note_detail_config.get("note_detail_headers", []))
    note_aggregation_patterns = compile_patterns(rules.get("note_aggregation_patterns", []) or rules.get("note_aggregation_keywords", []))
    if not company_patterns:
        company_patterns = compile_patterns(non_target_config.get("company_like", []))
    if not aging_patterns:
        aging_patterns = compile_patterns(non_target_config.get("aging_bucket", []))
    non_target_patterns = compile_patterns(rules.get("non_target_noise_patterns", []) or non_target_config.get("note_noise", []))
    promoted_rules = list(rules.get("promoted_rules", []))

    for rule in promoted_rules:
        if not isinstance(rule, dict):
            continue
        if rule.get("mapping_code") and rule.get("mapping_code") == fact.mapping_code:
            return str(rule.get("target_scope", "main_export_target")), "promoted_target_rule"

    if fact.source_kind == "derived_formula":
        return "derived_target", "source_kind:derived_formula"
    if fact.mapping_code and fact.mapping_code in benchmark_codes:
        return "main_export_target", "benchmark_row_membership"
    if fact.statement_type in main_statement_types and fact.mapping_code:
        return "main_export_target", f"main_statement:{fact.statement_type}"
    if matches_any(label, note_aggregation_patterns):
        return "note_aggregation", "note_aggregation_pattern"
    if fact.statement_type in note_statements and (matches_any(label, company_patterns) or matches_any(label, aging_patterns) or matches_any(label, note_detail_patterns)):
        return "note_detail", "note_detail_pattern"
    if fact.statement_type in note_statements:
        return "note_detail", "statement_type:note"
    if matches_any(label, company_patterns) or matches_any(label, aging_patterns):
        return "note_detail", "detail_bucket_pattern"
    if matches_any(label, non_target_patterns) or not label_lower:
        return "non_target_noise", "non_target_noise_pattern"
    if fact.statement_type in main_statement_types:
        return "main_export_target", f"main_statement_unmapped:{fact.statement_type}"
    return "non_target_noise", "fallback_non_target"


def collect_benchmark_codes(benchmark_payload: Dict[str, Any]) -> Set[str]:
    codes = set()
    for row in benchmark_payload.get("rows", []):
        if row.get("mapping_code"):
            codes.add(row["mapping_code"])
    for row in benchmark_payload.get("cell_rows", []):
        if row.get("mapping_code"):
            codes.add(row["mapping_code"])
    for row in benchmark_payload.get("missing_rows", []):
        if row.get("mapping_code"):
            codes.add(row["mapping_code"])
    return codes


def compile_patterns(values: Sequence[str]) -> List[re.Pattern[str]]:
    patterns: List[re.Pattern[str]] = []
    for value in values or []:
        text = str(value).strip()
        if not text:
            continue
        patterns.append(re.compile(text, re.IGNORECASE))
    return patterns


def matches_any(text: str, patterns: Sequence[re.Pattern[str]]) -> bool:
    return any(pattern.search(text or "") for pattern in patterns)
