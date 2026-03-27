from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .text import clean_text


PERCENT_RE = re.compile(r"^-?\(?[\d,]+\.\d+%?\)?$|^-?\(?[\d,]+%?\)?$")
INTEGER_RE = re.compile(r"^-?\d+$")
NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
SUSPICIOUS_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
ALPHA_RE = re.compile(r"[A-Za-z]")
DIGIT_RE = re.compile(r"\d")
REPAIR_TRANSLATION = str.maketrans(
    {
        "，": ",",
        "．": ".",
        "。": ".",
        "！": "1",
        "I": "1",
        "l": "1",
        "O": "0",
    }
)


def looks_like_integer(value: str) -> bool:
    text = clean_text(value)
    if not text:
        return False
    text = text.replace(",", "")
    return bool(INTEGER_RE.fullmatch(text))


def looks_like_numeric(value: str) -> bool:
    text = clean_text(value)
    if not text:
        return False
    normalized = normalize_number_token(text)
    return bool(NUMBER_RE.fullmatch(normalized))


def normalize_number_token(text: str) -> str:
    token = clean_text(text)
    negative = False
    if token.startswith("(") and token.endswith(")"):
        negative = True
        token = token[1:-1]
    if token.endswith("%"):
        token = token[:-1]
    token = token.replace(",", "")
    token = token.replace(" ", "")
    if negative and token and not token.startswith("-"):
        token = f"-{token}"
    return token


def analyze_numeric_text(text: str, expected_numeric: bool) -> Dict[str, Any]:
    """Parse OCR cell text into a stable numeric/text representation."""

    raw = "" if text is None else str(text)
    cleaned = clean_text(raw)
    issue_flags: List[str] = []
    suspicious_reasons: List[str] = []
    repair_status = "raw"
    repaired_from = ""

    if not cleaned:
        return {
            "normalized_text": "",
            "value_num": None,
            "value_type": "blank",
            "repair_status": "cleaned" if raw else "raw",
            "is_suspicious": False,
            "suspicious_reason": "",
            "issue_flags": issue_flags,
            "meta": {},
        }

    if expected_numeric:
        if cleaned in {"章", "米"}:
            suspicious_reasons.append("seal_or_stamp_noise")
        if len(cleaned) == 1 and ALPHA_RE.search(cleaned):
            suspicious_reasons.append("single_letter_noise")
        if SUSPICIOUS_CJK_RE.search(cleaned) and DIGIT_RE.search(cleaned):
            suspicious_reasons.append("contains_chinese_noise")
        if ALPHA_RE.search(cleaned) and DIGIT_RE.search(cleaned):
            suspicious_reasons.append("contains_alpha_noise")

    parsed = try_parse_numeric(cleaned)
    normalized_text = cleaned
    value_num = parsed
    value_type = infer_value_type(cleaned, parsed)

    if expected_numeric and parsed is None:
        repaired = clean_text(cleaned.translate(REPAIR_TRANSLATION))
        if repaired != cleaned:
            repaired_parsed = try_parse_numeric(repaired)
            if repaired_parsed is not None:
                repaired_from = cleaned
                normalized_text = repaired
                value_num = repaired_parsed
                value_type = infer_value_type(repaired, repaired_parsed)
                repair_status = "repaired"
                issue_flags.append("repaired_numeric")
        if value_num is None:
            repair_status = "unresolved"
            issue_flags.append("numeric_parse_failed")
    elif cleaned != raw:
        repair_status = "cleaned"

    is_suspicious = bool(suspicious_reasons)
    if expected_numeric and value_num is None and value_type != "blank":
        is_suspicious = True
        suspicious_reasons.append("expected_numeric_but_unparseable")

    return {
        "normalized_text": normalized_text,
        "value_num": value_num,
        "value_type": value_type,
        "repair_status": repair_status,
        "is_suspicious": is_suspicious,
        "suspicious_reason": "|".join(dict.fromkeys(suspicious_reasons)),
        "issue_flags": list(dict.fromkeys(issue_flags)),
        "meta": {
            "repaired_from": repaired_from,
            "expected_numeric": expected_numeric,
        },
    }


def try_parse_numeric(text: str) -> Optional[float]:
    token = clean_text(text)
    if not token:
        return None
    percent = token.endswith("%")
    token = normalize_number_token(token)
    if not NUMBER_RE.fullmatch(token):
        return None
    value = float(token)
    if percent:
        value = value / 100.0
    return value


def infer_value_type(text: str, value_num: Optional[float]) -> str:
    if not clean_text(text):
        return "blank"
    if value_num is None:
        return "text"
    if clean_text(text).endswith("%"):
        return "ratio"
    return "amount"

