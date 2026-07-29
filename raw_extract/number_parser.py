from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import List, Optional

from .models import NumberParseResult


NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
ALPHA_CONFUSIONS = str.maketrans({"O": "0", "o": "0", "I": "1", "l": "1"})
BLANK_TOKENS = {"", "-", "--", "—", "–", "―", "/", "N/A", "NA", "n/a", "na", "不适用", "无"}


def parse_metric_number(text: object, *, expected_numeric: bool = True) -> NumberParseResult:
    raw = "" if text is None else str(text)
    normalized = normalize_numeric_text(raw)
    issue_flags: List[str] = []

    if normalized in BLANK_TOKENS:
        return NumberParseResult(value=None, value_type="blank", normalized_text="")

    parsed = _parse_normalized(normalized, issue_flags)
    if parsed is None and expected_numeric:
        repaired = normalize_numeric_text(normalized.translate(ALPHA_CONFUSIONS))
        if repaired != normalized and _safe_alpha_repair_candidate(normalized):
            repaired_flags: List[str] = []
            parsed = _parse_normalized(repaired, repaired_flags)
            if parsed is not None:
                issue_flags.extend(repaired_flags)
                issue_flags.append("repaired_numeric")
                normalized = repaired

    if parsed is None:
        if expected_numeric:
            issue_flags.append("numeric_parse_failed")
        if normalized.count(".") > 1:
            issue_flags.append("suspicious_numeric")
        return NumberParseResult(
            value=None,
            value_type="text",
            normalized_text=normalized,
            issue_flags=_dedupe(issue_flags),
            suspicious_reason="unparseable_numeric" if expected_numeric else "",
        )

    value, value_type = parsed
    if has_abnormal_comma_grouping(normalized):
        issue_flags.append("suspicious_numeric")
    return NumberParseResult(
        value=value,
        value_type=value_type,
        normalized_text=normalized,
        issue_flags=_dedupe(issue_flags),
        suspicious_reason="abnormal_comma_grouping" if "suspicious_numeric" in issue_flags else "",
    )


def normalize_numeric_text(text: str) -> str:
    value = unicodedata.normalize("NFKC", str(text or ""))
    value = value.strip()
    value = value.replace("，", ",").replace("。", ".").replace("．", ".")
    value = value.replace("（", "(").replace("）", ")")
    value = re.sub(r"\s+", "", value)
    value = value.strip(":：;；,，")
    return value


def has_abnormal_comma_grouping(text: str) -> bool:
    token = text.strip()
    if "," not in token:
        return False
    if token.startswith("(") and token.endswith(")"):
        token = token[1:-1]
    if token.startswith("-"):
        token = token[1:]
    if token.endswith("%"):
        token = token[:-1]
    integer = token.split(".", 1)[0]
    groups = integer.split(",")
    if len(groups) <= 1:
        return False
    if not (1 <= len(groups[0]) <= 3):
        return True
    return any(len(group) != 3 for group in groups[1:])


def _parse_normalized(text: str, issue_flags: List[str]) -> Optional[tuple[Decimal, str]]:
    token = text
    negative = False
    if token.startswith("(") and token.endswith(")"):
        negative = True
        token = token[1:-1]
    percent = token.endswith("%")
    if percent:
        token = token[:-1]
    token = token.replace(",", "")
    if token.startswith("+"):
        token = token[1:]
    if negative:
        token = f"-{token}"
    if token.count(".") > 1:
        issue_flags.append("suspicious_numeric")
        return None
    if not NUMBER_RE.fullmatch(token):
        return None
    try:
        value = Decimal(token)
    except InvalidOperation:
        return None
    if percent:
        value = value / Decimal("100")
        return value, "ratio"
    return value, "amount"


def _safe_alpha_repair_candidate(text: str) -> bool:
    if not re.search(r"\d", text):
        return False
    alpha_count = len(re.findall(r"[A-Za-z]", text))
    return 0 < alpha_count <= 2


def _dedupe(values: List[str]) -> List[str]:
    return list(dict.fromkeys(item for item in values if item))
