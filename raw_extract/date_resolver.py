from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from standardize.models import LogicalSubtable, ProviderPage
from standardize.normalize.text import clean_text

from .models import FillDateResolution, ItemDateResolution


EXACT_DATE_PATTERNS = (
    re.compile(r"(?P<year>20\d{2})[-/.](?P<month>\d{1,2})[-/.](?P<day>\d{1,2})"),
    re.compile(r"(?P<year>20\d{2})年\s*(?P<month>\d{1,2})月\s*(?P<day>\d{1,2})日"),
)
ANNUAL_RE = re.compile(r"(?P<year>20\d{2})年度")
MONTH_RE = re.compile(r"(?P<year>20\d{2})年\s*(?P<month>\d{1,2})月")
DATE_AUTO_ACCEPT_SCORE = 80
DATE_COMPETING_SCORE_GAP = 5


@dataclass
class DateCandidate:
    raw: str
    fill_date: str
    kind: str
    method: str
    source_text: str
    score: int

    def row(self) -> Dict[str, Any]:
        return {
            "raw": self.raw,
            "fill_date": self.fill_date,
            "kind": self.kind,
            "method": self.method,
            "source_text": self.source_text,
            "score": self.score,
        }


def resolve_fill_date(
    *,
    subtable: LogicalSubtable,
    page: Optional[ProviderPage],
    input_dir: Path,
    source_image_dir: Optional[Path] = None,
    default_fill_date: str = "",
) -> FillDateResolution:
    issue_flags: List[str] = []
    candidates: List[DateCandidate] = []

    meta_date = normalize_exact_date(subtable.statement_meta.report_date_norm)
    if meta_date:
        candidates.append(
            DateCandidate(
                raw=subtable.statement_meta.report_date_raw or meta_date,
                fill_date=meta_date,
                kind="exact",
                method="statement_meta_exact",
                source_text=subtable.statement_meta.statement_name_raw,
                score=130,
            )
        )

    for source_name, text, base_score in iter_fill_date_texts(subtable, page):
        candidates.extend(extract_date_candidates(text, source_name, base_score))

    for filename in iter_filename_texts(source_image_dir):
        candidates.extend(extract_date_candidates(filename, "filename", 25))
    candidates.extend(extract_date_candidates(str(input_dir), "input_path", 15))

    default_candidate = default_fill_date_candidate(default_fill_date)
    if default_candidate:
        candidates.append(default_candidate)

    if not candidates:
        issue_flags.append("missing_fill_date")
        return FillDateResolution(
            doc_id=subtable.doc_id,
            provider=subtable.provider,
            page_no=subtable.page_no,
            table_id=subtable.table_id,
            logical_subtable_id=subtable.logical_subtable_id,
            fill_date="",
            method="unknown",
            candidates=[],
            issue_flags=issue_flags,
        )

    exact_values = {candidate.fill_date for candidate in candidates if candidate.kind == "exact"}
    if len(exact_values) > 1:
        issue_flags.append("ambiguous_date")

    ordered = sorted(candidates, key=lambda item: (-item.score, item.kind != "exact", item.fill_date))
    best = ordered[0]
    competing = [
        candidate
        for candidate in ordered[1:]
        if candidate.fill_date != best.fill_date and candidate.score >= best.score - DATE_COMPETING_SCORE_GAP
    ]
    if best.score < DATE_AUTO_ACCEPT_SCORE or competing:
        issue_flags.append("missing_fill_date")
        issue_flags.append("low_confidence_date" if best.score < DATE_AUTO_ACCEPT_SCORE else "ambiguous_date")
        return FillDateResolution(
            doc_id=subtable.doc_id,
            provider=subtable.provider,
            page_no=subtable.page_no,
            table_id=subtable.table_id,
            logical_subtable_id=subtable.logical_subtable_id,
            fill_date="",
            method="needs_review_low_confidence" if best.score < DATE_AUTO_ACCEPT_SCORE else "needs_review_ambiguous",
            source_text=best.source_text,
            candidates=[candidate.row() for candidate in candidates],
            issue_flags=list(dict.fromkeys(issue_flags)),
        )
    return FillDateResolution(
        doc_id=subtable.doc_id,
        provider=subtable.provider,
        page_no=subtable.page_no,
        table_id=subtable.table_id,
        logical_subtable_id=subtable.logical_subtable_id,
        fill_date=best.fill_date,
        method=best.method,
        source_text=best.source_text,
        candidates=[candidate.row() for candidate in candidates],
        issue_flags=issue_flags,
    )


def resolve_item_date(
    *,
    fill_date: str,
    header_path: Iterable[str],
    period_role_raw: str,
    statement_type: str,
    fill_date_method: str = "",
) -> ItemDateResolution:
    raw_text = clean_text(" ".join([period_role_raw, *[str(item) for item in header_path]]))
    explicit = first_exact_date(raw_text)
    if explicit:
        return ItemDateResolution(
            item_date=explicit,
            period_start_date=explicit,
            period_end_date=explicit,
            period_role_raw=period_role_raw or raw_text,
            period_role_norm="explicit_date",
            method="explicit_date_header",
        )

    fill = normalize_exact_date(fill_date)
    if not fill:
        return ItemDateResolution(
            item_date="",
            period_start_date="",
            period_end_date="",
            period_role_raw=period_role_raw or raw_text,
            period_role_norm=normalize_period_role(raw_text),
            method="missing_fill_date",
            issue_flags=["missing_item_date"],
        )

    fill_year = int(fill[:4])
    role_norm = normalize_period_role(raw_text)
    is_flow = statement_type in {"income_statement", "cash_flow", "equity_statement", "changes_in_equity"}
    annual_context = fill.endswith("-12-31") or "annual_to_year_end" in fill_date_method or "年度" in raw_text

    if role_norm in {"ending", "current_point"}:
        return ItemDateResolution(fill, fill, fill, period_role_raw or raw_text, role_norm, "period_role_to_fill_date")
    if role_norm == "beginning":
        start = f"{fill_year:04d}-01-01"
        return ItemDateResolution(start, start, start, period_role_raw or raw_text, role_norm, "period_role_to_year_start")
    if role_norm == "previous_ending":
        previous = f"{fill_year - 1:04d}-12-31"
        return ItemDateResolution(previous, previous, previous, period_role_raw or raw_text, role_norm, "period_role_to_previous_year_end")
    if role_norm in {"current_period", "current_year"} or (role_norm == "amount" and is_flow and annual_context):
        start = f"{fill_year:04d}-01-01"
        return ItemDateResolution(start, start, fill, period_role_raw or raw_text, role_norm, "annual_flow_to_year_start")
    if role_norm in {"previous_period", "previous_year"}:
        start = f"{fill_year - 1:04d}-01-01"
        end = f"{fill_year - 1:04d}-12-31"
        return ItemDateResolution(start, start, end, period_role_raw or raw_text, role_norm, "prior_annual_flow_to_previous_year_start")

    return ItemDateResolution(
        item_date="",
        period_start_date="",
        period_end_date="",
        period_role_raw=period_role_raw or raw_text,
        period_role_norm=role_norm,
        method="ambiguous_item_date",
        issue_flags=["missing_item_date", "ambiguous_item_date"],
    )


def normalize_period_role(text: str) -> str:
    value = clean_text(text)
    if not value:
        return "unknown"
    if any(token in value for token in ("上年年末", "上年末", "上期期末")):
        return "previous_ending"
    if any(token in value for token in ("期初", "年初")):
        return "beginning"
    if any(token in value for token in ("期末", "年末", "本期末", "期末余额")):
        return "ending"
    if any(token in value for token in ("上年同期", "上期", "上年累计", "上年数")):
        return "previous_period"
    if any(token in value for token in ("本年累计", "本期", "本年", "本期金额")):
        return "current_period"
    if value in {"金额", "本期金额"} or value.endswith("金额"):
        return "amount"
    return "unknown"


def extract_date_candidates(text: str, source: str, base_score: int) -> List[DateCandidate]:
    cleaned = clean_text(text)
    if not cleaned:
        return []
    score = base_score
    if any(token in cleaned for token in ("资产负债表", "利润表", "现金流量表", "所有者权益变动表")):
        score += 25
    if "截至" in cleaned:
        score += 12
    if "审计报告" in cleaned and not any(token in cleaned for token in ("资产负债表", "利润表", "现金流量表")):
        score -= 20

    rows: List[DateCandidate] = []
    for pattern in EXACT_DATE_PATTERNS:
        for match in pattern.finditer(cleaned):
            normalized = build_date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
            if normalized:
                rows.append(DateCandidate(match.group(0), normalized, "exact", f"{source}_exact", cleaned, score + 30))

    for match in ANNUAL_RE.finditer(cleaned):
        year = int(match.group("year"))
        rows.append(DateCandidate(match.group(0), f"{year:04d}-12-31", "annual", f"{source}_annual_to_year_end", cleaned, score))
    return rows


def iter_fill_date_texts(subtable: LogicalSubtable, page: Optional[ProviderPage]) -> Iterable[tuple[str, str, int]]:
    yield "statement_title", subtable.statement_meta.statement_name_raw, 95
    header_text = " ".join(" ".join(path) for path in subtable.header_paths.values())
    yield "table_header", header_text, 85
    if page:
        for line in page.context_lines:
            yield "page_context", line, 80
        yield "page_text", page.page_text, 60


def iter_filename_texts(source_image_dir: Optional[Path]) -> Iterable[str]:
    if not source_image_dir:
        return []
    if source_image_dir.is_file():
        return [source_image_dir.name]
    if not source_image_dir.exists():
        return [source_image_dir.name]
    return [path.name for path in sorted(source_image_dir.iterdir()) if path.is_file()]


def default_fill_date_candidate(value: str) -> Optional[DateCandidate]:
    if not value:
        return None
    candidates = extract_date_candidates(value, "default_fill_date", 10)
    if candidates:
        return candidates[0]
    exact = normalize_exact_date(value)
    if exact:
        return DateCandidate(value, exact, "exact", "default_fill_date", value, 10)
    return None


def normalize_exact_date(value: str) -> str:
    text = clean_text(value)
    if not text or text == "unknown_date":
        return ""
    for pattern in EXACT_DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            return build_date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
    return text if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text) else ""


def first_exact_date(text: str) -> str:
    for pattern in EXACT_DATE_PATTERNS:
        match = pattern.search(clean_text(text))
        if match:
            return build_date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
    return ""


def build_date(year: int, month: int, day: int) -> str:
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return ""
