from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Sequence, Tuple

from ..models import FactRecord, ProviderPage, compact_json
from ..normalize.text import clean_text


def specialize_statement_types(
    facts: List[FactRecord],
    provider_pages: Sequence[ProviderPage],
    statement_rules: Dict[str, Any] | None = None,
    enabled: bool = False,
) -> Tuple[List[FactRecord], List[Dict[str, Any]], Dict[str, Any]]:
    statement_rules = statement_rules or {}
    active_facts = [fact for fact in facts if fact.status != "suppressed"]
    before_unknown = sum(1 for fact in active_facts if fact.statement_type == "unknown")
    before_unknown_value = sum(1 for fact in active_facts if fact.statement_type == "unknown" and fact.value_num is not None)
    if not enabled:
        return facts, [], {
            "enabled": False,
            "unknown_statement_type_total_before": before_unknown,
            "unknown_statement_type_total_after": before_unknown,
            "unknown_statement_type_value_bearing_total_before": before_unknown_value,
            "unknown_statement_type_value_bearing_total_after": before_unknown_value,
        }

    page_context = build_page_context(provider_pages)
    grouped = defaultdict(list)
    for fact in facts:
        grouped[(fact.doc_id, fact.page_no, fact.logical_subtable_id)].append(fact)

    audit_rows: List[Dict[str, Any]] = []
    after_unknown = 0
    after_unknown_value = 0
    change_counter = Counter()
    for group_key, group_facts in grouped.items():
        chosen_type, score, source, reason, debug = classify_group(group_facts, page_context.get((group_key[0], group_key[1]), {}), statement_rules)
        for fact in group_facts:
            old_type = fact.statement_type or "unknown"
            if chosen_type:
                fact.statement_type = chosen_type
            fact.statement_type_source = source
            fact.statement_type_score = round(score, 6)
            fact.statement_type_reason = reason
            if old_type != fact.statement_type:
                change_counter[f"{old_type}->{fact.statement_type}"] += 1
            if fact.statement_type == "unknown":
                after_unknown += 1
                if fact.value_num is not None:
                    after_unknown_value += 1
            audit_rows.append(
                {
                    "doc_id": fact.doc_id,
                    "page_no": fact.page_no,
                    "logical_subtable_id": fact.logical_subtable_id,
                    "fact_id": fact.fact_id,
                    "row_label_raw": fact.row_label_raw,
                    "row_label_std": fact.row_label_std,
                    "value_num": fact.value_num,
                    "statement_type_before": old_type,
                    "statement_type_after": fact.statement_type,
                    "statement_type_source": source,
                    "statement_type_score": round(score, 6),
                    "statement_type_reason": reason,
                    "meta_json": compact_json(debug),
                    "run_id": "",
                }
            )

    summary = {
        "enabled": True,
        "groups_total": len(grouped),
        "unknown_statement_type_total_before": before_unknown,
        "unknown_statement_type_total_after": after_unknown,
        "unknown_statement_type_value_bearing_total_before": before_unknown_value,
        "unknown_statement_type_value_bearing_total_after": after_unknown_value,
        "changes": dict(change_counter),
    }
    return facts, audit_rows, summary


def build_page_context(provider_pages: Sequence[ProviderPage]) -> Dict[Tuple[str, int], Dict[str, Any]]:
    context: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for page in provider_pages:
        key = (page.doc_id, page.page_no)
        lines = [clean_text(line) for line in page.context_lines if clean_text(line)]
        context[key] = {"lines": lines, "page_text": clean_text(page.page_text), "page_no": page.page_no}
    return context


def classify_group(group_facts: Sequence[FactRecord], page_context: Dict[str, Any], rules: Dict[str, Any]) -> Tuple[str, float, str, str, Dict[str, Any]]:
    score_map = {name: 0.0 for name in ["balance_sheet", "income_statement", "cash_flow", "changes_in_equity", "note", "unknown"]}
    reasons: List[str] = []
    lines = list(page_context.get("lines", []))
    joined_context = " ".join(lines + [page_context.get("page_text", "")])
    row_labels = [clean_text(f.row_label_canonical_candidate or f.row_label_norm or f.row_label_std or f.row_label_raw) for f in group_facts]
    header_text = " ".join(clean_text(item) for fact in group_facts for item in (fact.col_header_path or []) if clean_text(item))

    for statement_type, keywords in (rules.get("title_keywords", {}) or {}).items():
        for keyword in keywords:
            if keyword and keyword in joined_context:
                score_map[statement_type] += 25.0
                reasons.append(f"title:{statement_type}:{keyword}")
    if any(keyword in joined_context for keyword in rules.get("note_titles", [])):
        score_map["note"] += 35.0
        reasons.append("note_title")
    for statement_type, signatures in (rules.get("header_signatures", {}) or {}).items():
        for signature in signatures:
            items = [clean_text(item) for item in signature if clean_text(item)]
            if items and all(item in header_text for item in items):
                score_map[statement_type] += 18.0
                reasons.append(f"header_signature:{statement_type}")
    for statement_type, labels in (rules.get("row_patterns", {}) or {}).items():
        matches = sum(1 for label in row_labels if label in labels)
        if matches:
            score_map[statement_type] += min(30.0, matches * 9.0)
            reasons.append(f"row_pattern:{statement_type}:{matches}")

    numbering_rules = rules.get("main_statement_numbering", {}) or {}
    if any(any(label.startswith(prefix) for prefix in numbering_rules.get("income_statement_prefixes", [])) for label in row_labels):
        score_map["income_statement"] += 6.0
        reasons.append("income_statement_numbering")
    if any(any(label.startswith(prefix) for prefix in numbering_rules.get("cash_flow_prefixes", [])) for label in row_labels):
        score_map["cash_flow"] += 6.0
        reasons.append("cash_flow_numbering")
    if any(marker in joined_context for marker in rules.get("note_detail_markers", [])):
        score_map["note"] += 12.0
        reasons.append("note_detail_marker")

    page_no = int(page_context.get("page_no", 0) or 0)
    if page_no and page_no <= int(rules.get("main_statement_page_cutoff", 8)):
        for statement_type in ("balance_sheet", "income_statement", "cash_flow", "changes_in_equity"):
            if score_map[statement_type] > 0:
                score_map[statement_type] += 2.0
        reasons.append("early_page_bonus")

    current_types = Counter(fact.statement_type for fact in group_facts if fact.statement_type and fact.statement_type != "unknown")
    if current_types:
        current_type, count = current_types.most_common(1)[0]
        score_map[current_type] += min(8.0, count * 2.0)
        reasons.append(f"existing_type_hint:{current_type}")

    ordered = sorted(score_map.items(), key=lambda item: item[1], reverse=True)
    top_type, top_score = ordered[0]
    second_score = ordered[1][1] if len(ordered) > 1 else -1.0
    min_score = float(rules.get("classification_min_score", 12.0))
    margin = float(rules.get("classification_margin", 4.0))
    if top_score < min_score or (top_score - second_score) < margin:
        if current_types:
            chosen_type = current_types.most_common(1)[0][0]
            return chosen_type, top_score, "fallback_existing", "kept_existing_type", {"scores": score_map, "row_labels": row_labels[:10], "headers": header_text}
        return "unknown", top_score, "ambiguous", "ambiguous_scores", {"scores": score_map, "row_labels": row_labels[:10], "headers": header_text}
    return top_type, top_score, "scored_specialization", ";".join(reasons[:6]) or "scored_specialization", {"scores": score_map, "row_labels": row_labels[:10], "headers": header_text}
