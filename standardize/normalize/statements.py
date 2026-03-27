from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from ..models import ProviderPage, StatementMeta
from .text import clean_text


DATE_RE = re.compile(r"(?P<year>\d{4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日")


def classify_statement(page: ProviderPage, keyword_config: Dict[str, Any]) -> StatementMeta:
    """Infer statement type, name, report date, and unit from page context."""

    candidate_lines = [clean_text(line) for line in page.context_lines if clean_text(line)]
    candidate_lines.extend(clean_text(line) for line in str(page.page_text).splitlines() if clean_text(line))

    statement_name_raw = ""
    statement_type = "unknown"
    for line in candidate_lines:
        matched_type = match_statement_type(line, keyword_config.get("statement_titles", {}))
        if matched_type:
            statement_type = matched_type
            statement_name_raw = line
            break

    report_date_raw = ""
    report_date_norm = "unknown_date"
    for line in candidate_lines:
        match = DATE_RE.search(line)
        if match:
            report_date_raw = match.group(0)
            report_date_norm = f"{int(match.group('year')):04d}-{int(match.group('month')):02d}-{int(match.group('day')):02d}"
            break

    unit_raw = ""
    unit_multiplier = 1.0
    units = keyword_config.get("units", {})
    for line in candidate_lines:
        normalized = clean_text(line)
        for unit_name, multiplier in units.items():
            if unit_name in normalized:
                unit_raw = unit_name
                unit_multiplier = float(multiplier)
                break
        if unit_raw:
            break

    return StatementMeta(
        statement_type=statement_type,
        statement_name_raw=statement_name_raw,
        report_date_raw=report_date_raw,
        report_date_norm=report_date_norm,
        unit_raw=unit_raw,
        unit_multiplier=unit_multiplier,
    )


def match_statement_type(line: str, statement_titles: Dict[str, List[str]]) -> str:
    normalized = clean_text(line)
    for statement_type, keywords in statement_titles.items():
        for keyword in keywords:
            if keyword in normalized:
                return statement_type
    return ""


def infer_period_role(header_path: List[str], keyword_config: Dict[str, Any]) -> str:
    normalized_items = [clean_text(item) for item in header_path if clean_text(item)]
    period_roles = keyword_config.get("period_roles", {})
    for item in normalized_items:
        for role_name, keywords in period_roles.items():
            if item == role_name:
                return role_name
            if any(keyword in item for keyword in keywords):
                return role_name
    return "unknown"
