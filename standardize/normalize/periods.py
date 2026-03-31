from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from ..discover import list_provider_dirs
from ..models import FactRecord, ProviderPage, compact_json
from .statements import infer_period_role
from .text import clean_text, normalize_label_for_matching


EXACT_DATE_PATTERNS = (
    re.compile(r"(?P<year>20\d{2})[-/\.](?P<month>\d{1,2})[-/\.](?P<day>\d{1,2})"),
    re.compile(r"(?P<year>20\d{2})年\s*(?P<month>\d{1,2})月\s*(?P<day>\d{1,2})日"),
)
ANNUAL_PATTERNS = (
    re.compile(r"(?P<year>20\d{2})年度"),
)
RANGE_PATTERNS = (
    re.compile(r"(?P<year>20\d{2})年\s*(?P<start>\d{1,2})(?:月)?\s*[-~至]\s*(?P<end>\d{1,2})月"),
    re.compile(r"(?P<year>20\d{2})年\s*(?P<start>\d{1,2})月\s*[-~至]\s*(?P<end>\d{1,2})月"),
)


def apply_period_normalization(
    facts: List[FactRecord],
    provider_pages: List[ProviderPage],
    input_dir: Path,
    keyword_config: Dict[str, Any],
    period_config: Dict[str, Any],
    enabled: bool,
) -> List[FactRecord]:
    grouped_pages: Dict[str, List[ProviderPage]] = defaultdict(list)
    for page in provider_pages:
        grouped_pages[page.doc_id].append(page)

    doc_contexts = {
        doc_id: scan_document_period_context(
            input_dir=input_dir,
            doc_id=doc_id,
            provider_pages=pages,
            keyword_config=keyword_config,
            period_config=period_config,
        )
        for doc_id, pages in grouped_pages.items()
    }

    for fact in facts:
        fact.statement_group_key = build_statement_group_key(fact)
        fact.period_role_norm = normalize_period_role(fact, keyword_config)
        if not fact.report_date_norm:
            fact.report_date_norm = fact.report_date_raw or "unknown_date"
        if not enabled:
            fact.period_source_level = fact.period_source_level or "page"
            fact.period_reason = fact.period_reason or "stage1_default"
            fact.period_key = f"{fact.report_date_norm or 'unknown_date'}__{fact.period_role_norm or 'unknown'}"

    if not enabled:
        return facts

    statement_contexts = build_statement_contexts(facts, doc_contexts, period_config)
    for fact in facts:
        context = doc_contexts.get(fact.doc_id, {})
        page_context = context.get("pages", {}).get(fact.page_no, {})
        header_candidates = extract_period_candidates(
            " ".join([fact.statement_name_raw, fact.col_header_raw, " ".join(fact.col_header_path)]),
            source_level="header",
            page_no=fact.page_no,
            keyword_config=keyword_config,
            period_config=period_config,
        )
        chosen, source_level, reason = choose_fact_date_candidate(
            fact=fact,
            header_candidates=header_candidates,
            page_context=page_context,
            statement_context=statement_contexts.get((fact.doc_id, fact.statement_group_key), {}),
            doc_context=context,
            period_config=period_config,
        )
        fact.report_date_raw = chosen.get("raw", fact.report_date_raw or "")
        fact.report_date_norm = chosen.get("norm", "unknown_date") or "unknown_date"
        fact.period_source_level = source_level
        fact.period_reason = reason
        fact.period_key = f"{fact.report_date_norm}__{fact.period_role_norm or 'unknown'}"

    return facts


def scan_document_period_context(
    input_dir: Path,
    doc_id: str,
    provider_pages: List[ProviderPage],
    keyword_config: Dict[str, Any],
    period_config: Dict[str, Any],
) -> Dict[str, Any]:
    page_lines: Dict[int, List[Tuple[str, str]]] = defaultdict(list)
    for page in provider_pages:
        for line in page.context_lines:
            cleaned = clean_text(line)
            if cleaned:
                page_lines[page.page_no].append(("page_context", cleaned))
        for line in split_text_lines(page.page_text):
            page_lines[page.page_no].append(("page_text", line))

    for provider_name in list_provider_dirs(input_dir):
        result_path = input_dir / provider_name / doc_id / "result.json"
        if not result_path.exists():
            continue
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        for page in payload.get("pages", []):
            page_no = page.get("page_number")
            if page_no is None:
                continue
            for line in split_text_lines(page.get("text", "")):
                page_lines[int(page_no)].append((f"result_json:{provider_name}", line))

    doc_lines = [("doc_id", clean_text(doc_id))]
    all_candidates: List[Dict[str, Any]] = []
    page_contexts: Dict[int, Dict[str, Any]] = {}
    for page_no, items in page_lines.items():
        candidates: List[Dict[str, Any]] = []
        for source_level, text in items:
            candidates.extend(
                extract_period_candidates(
                    text=text,
                    source_level=source_level,
                    page_no=page_no,
                    keyword_config=keyword_config,
                    period_config=period_config,
                )
            )
        page_contexts[page_no] = {
            "candidates": candidates,
            "line_count": len(items),
        }
        all_candidates.extend(candidates)

    for source_level, text in doc_lines:
        all_candidates.extend(
            extract_period_candidates(
                text=text,
                source_level=source_level,
                page_no=0,
                keyword_config=keyword_config,
                period_config=period_config,
            )
        )

    counts = Counter(candidate["norm"] for candidate in all_candidates if candidate.get("norm"))
    doc_best = {
        "exact": select_best_candidate(all_candidates, ["exact"], counts, period_config),
        "range": select_best_candidate(all_candidates, ["range"], counts, period_config),
        "annual": select_best_candidate(all_candidates, ["annual"], counts, period_config),
    }
    for page_no, page_context in page_contexts.items():
        candidates = page_context["candidates"]
        page_context["best"] = {
            "exact": select_best_candidate(candidates, ["exact"], counts, period_config),
            "range": select_best_candidate(candidates, ["range"], counts, period_config),
            "annual": select_best_candidate(candidates, ["annual"], counts, period_config),
        }
        page_context["reason"] = summarize_candidate_reason(candidates, page_context["best"])

    return {
        "pages": page_contexts,
        "doc_best": doc_best,
        "candidate_counts": dict(counts),
    }


def build_statement_contexts(
    facts: List[FactRecord],
    doc_contexts: Dict[str, Dict[str, Any]],
    period_config: Dict[str, Any],
) -> Dict[Tuple[str, str], Dict[str, Dict[str, Any]]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    counts_by_doc = {
        doc_id: Counter(context.get("candidate_counts", {}))
        for doc_id, context in doc_contexts.items()
    }
    for fact in facts:
        page_best = doc_contexts.get(fact.doc_id, {}).get("pages", {}).get(fact.page_no, {}).get("best", {})
        for kind, candidate in page_best.items():
            if candidate:
                grouped[(fact.doc_id, fact.statement_group_key)].append(candidate)

    statement_contexts: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    for key, candidates in grouped.items():
        counts = counts_by_doc.get(key[0], Counter())
        statement_best = {
            "exact": select_best_candidate(candidates, ["exact"], counts, period_config),
            "range": select_best_candidate(candidates, ["range"], counts, period_config),
            "annual": select_best_candidate(candidates, ["annual"], counts, period_config),
        }
        doc_best = doc_contexts.get(key[0], {}).get("doc_best", {})
        if not statement_best["exact"] and doc_best.get("exact"):
            statement_best["exact"] = doc_best["exact"]
        if not statement_best["annual"] and doc_best.get("annual"):
            statement_best["annual"] = doc_best["annual"]
        if not statement_best["range"] and doc_best.get("range"):
            statement_best["range"] = doc_best["range"]
        statement_contexts[key] = statement_best
    return statement_contexts


def choose_fact_date_candidate(
    fact: FactRecord,
    header_candidates: List[Dict[str, Any]],
    page_context: Dict[str, Any],
    statement_context: Dict[str, Dict[str, Any]],
    doc_context: Dict[str, Any],
    period_config: Dict[str, Any],
) -> Tuple[Dict[str, Any], str, str]:
    counts = Counter(doc_context.get("candidate_counts", {}))
    preferred_kinds = preferred_date_kinds(fact)
    header_choice = select_best_candidate(header_candidates, preferred_kinds, counts, period_config)
    if header_choice:
        return header_choice, "header", "header_date_candidate"

    for kind in preferred_kinds:
        for source_level, context in (
            ("page", page_context.get("best", {})),
            ("statement", statement_context or {}),
            ("doc", doc_context.get("doc_best", {})),
        ):
            candidate = context.get(kind)
            if candidate:
                return candidate, source_level, f"{source_level}_{kind}_candidate"

    return {"raw": "", "norm": "unknown_date", "kind": "unknown"}, "inferred", infer_unknown_reason(fact, page_context, statement_context, doc_context)


def preferred_date_kinds(fact: FactRecord) -> List[str]:
    role = fact.period_role_norm or fact.period_role_raw or ""
    if role in {"期初数", "期末数"}:
        return ["exact", "range", "annual"]
    if role in {"本期", "上期", "本年累计", "上年累计"}:
        return ["range", "annual", "exact"]
    if fact.statement_type in {"income_statement", "cash_flow", "equity_statement", "changes_in_equity"}:
        return ["range", "annual", "exact"]
    return ["exact", "annual", "range"]


def infer_unknown_reason(
    fact: FactRecord,
    page_context: Dict[str, Any],
    statement_context: Dict[str, Dict[str, Any]],
    doc_context: Dict[str, Any],
) -> str:
    if not page_context or not page_context.get("candidates"):
        if not statement_context and not doc_context.get("doc_best"):
            return "no_statement_date_found"
        return "no_page_date_found"
    if page_context.get("reason") == "ambiguous_multiple_dates":
        return "ambiguous_multiple_dates"
    if not (fact.period_role_norm or fact.period_role_raw):
        return "no_period_role_found"
    return "no_page_date_found"


def select_best_candidate(
    candidates: Iterable[Dict[str, Any]],
    kinds: List[str],
    counts: Counter,
    period_config: Dict[str, Any],
) -> Dict[str, Any]:
    filtered = [candidate for candidate in candidates if candidate.get("kind") in kinds]
    if not filtered:
        return {}
    scored = sorted(
        filtered,
        key=lambda candidate: (
            -(candidate.get("score", 0) + counts.get(candidate.get("norm", ""), 0) * float(period_config.get("repeat_bonus", 2.0))),
            -kind_specificity(candidate.get("kind", "")),
            candidate.get("page_no", 999),
            candidate.get("norm", ""),
        ),
    )
    tie_threshold = float(period_config.get("tie_threshold", 5.0))
    if len(scored) > 1:
        first = scored[0]
        second = scored[1]
        first_score = first.get("score", 0) + counts.get(first.get("norm", ""), 0) * float(period_config.get("repeat_bonus", 2.0))
        second_score = second.get("score", 0) + counts.get(second.get("norm", ""), 0) * float(period_config.get("repeat_bonus", 2.0))
        if first.get("norm") != second.get("norm") and abs(first_score - second_score) < tie_threshold:
            return {}
    return scored[0]


def summarize_candidate_reason(candidates: List[Dict[str, Any]], best: Dict[str, Dict[str, Any]]) -> str:
    if not candidates:
        return "no_page_date_found"
    if not any(best.values()):
        return "ambiguous_multiple_dates"
    return "page_candidate_selected"


def extract_period_candidates(
    text: str,
    source_level: str,
    page_no: int,
    keyword_config: Dict[str, Any],
    period_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    cleaned = clean_text(text)
    if not cleaned:
        return []

    candidates: List[Dict[str, Any]] = []
    for pattern in EXACT_DATE_PATTERNS:
        for match in pattern.finditer(cleaned):
            raw = match.group(0)
            norm = f"{int(match.group('year')):04d}-{int(match.group('month')):02d}-{int(match.group('day')):02d}"
            candidates.append(build_candidate(raw, norm, "exact", cleaned, source_level, page_no, keyword_config, period_config))
    for pattern in RANGE_PATTERNS:
        for match in pattern.finditer(cleaned):
            raw = match.group(0)
            norm = f"{int(match.group('year')):04d}年{int(match.group('start'))}月-{int(match.group('end'))}月"
            candidates.append(build_candidate(raw, norm, "range", cleaned, source_level, page_no, keyword_config, period_config))
    for pattern in ANNUAL_PATTERNS:
        for match in pattern.finditer(cleaned):
            raw = match.group(0)
            year = int(match.group("year"))
            norm = f"{year:04d}年度"
            if any(candidate["norm"] == norm for candidate in candidates):
                continue
            candidates.append(build_candidate(raw, norm, "annual", cleaned, source_level, page_no, keyword_config, period_config))
    return candidates


def build_candidate(
    raw: str,
    norm: str,
    kind: str,
    text: str,
    source_level: str,
    page_no: int,
    keyword_config: Dict[str, Any],
    period_config: Dict[str, Any],
) -> Dict[str, Any]:
    source_weights = period_config.get("source_weights", {})
    financial_keywords = flatten_keywords(keyword_config.get("statement_titles", {}))
    bonuses = period_config.get("line_scoring", {})

    score = float(source_weights.get(source_level, source_weights.get(source_level.split(":", 1)[0], 20.0)))
    if any(keyword in text for keyword in financial_keywords):
        score += float(bonuses.get("financial_statement_bonus", 18.0))
    if "截至" in text:
        score += float(bonuses.get("as_of_bonus", 10.0))
    if "财务报表" in text or "附注" in text:
        score += float(bonuses.get("financial_statement_bonus", 18.0))
    if "报告日期" in text:
        score -= float(bonuses.get("report_date_penalty", 40.0))
    if "审计报告" in text and not any(keyword in text for keyword in financial_keywords):
        score -= float(bonuses.get("audit_report_penalty", 20.0))

    return {
        "raw": raw,
        "norm": norm,
        "kind": kind,
        "text": text,
        "source_level": source_level,
        "page_no": page_no,
        "score": score,
    }


def flatten_keywords(statement_titles: Dict[str, List[str]]) -> List[str]:
    keywords: List[str] = []
    for values in statement_titles.values():
        keywords.extend(values)
    return keywords


def normalize_period_role(fact: FactRecord, keyword_config: Dict[str, Any]) -> str:
    header_role = infer_period_role(fact.col_header_path or [], keyword_config)
    if header_role != "unknown":
        return header_role
    if fact.period_role_raw and fact.period_role_raw != "unknown":
        return fact.period_role_raw
    return "unknown"


def build_statement_group_key(fact: FactRecord) -> str:
    parts = [normalize_label_for_matching(fact.statement_type or "unknown")]
    normalized_name = normalize_label_for_matching(fact.statement_name_raw)
    canonical_name = canonical_statement_name(fact.statement_type, normalized_name)
    if canonical_name:
        parts.append(canonical_name[:40])
    elif fact.table_semantic_key:
        header_signature = fact.table_semantic_key.split("|r:", 1)[0]
        parts.append(normalize_label_for_matching(header_signature)[:80])
    return "|".join(part for part in parts if part)


def canonical_statement_name(statement_type: str, normalized_name: str) -> str:
    if not normalized_name:
        return ""
    if statement_type == "note" and "附注" in normalized_name:
        return "附注"
    if statement_type == "balance_sheet":
        return "资产负债表"
    if statement_type == "income_statement":
        return "利润表"
    if statement_type == "cash_flow":
        return "现金流量表"
    if statement_type in {"equity_statement", "changes_in_equity"}:
        return "所有者权益变动表"
    return normalized_name


def split_text_lines(text: object) -> List[str]:
    if text is None:
        return []
    raw = str(text)
    lines: List[str] = []
    for piece in raw.splitlines():
        cleaned = clean_text(piece)
        if cleaned:
            lines.append(cleaned)
    if not lines and clean_text(raw):
        lines.append(clean_text(raw))
    return lines


def kind_specificity(kind: str) -> int:
    return {
        "exact": 3,
        "range": 2,
        "annual": 1,
    }.get(kind, 0)
