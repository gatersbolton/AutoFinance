from __future__ import annotations

import re
from typing import Any


PERIOD_ROLE_LABELS_ZH = {
    "begin": "期初数",
    "beginning": "期初数",
    "end": "期末数",
    "ending": "期末数",
    "previous_ending": "上期期末",
    "current_point": "当前时点",
    "current_period": "本期",
    "current_year": "本年",
    "previous_period": "上期",
    "previous_year": "上年",
    "amount": "金额",
    "explicit_date": "明确日期",
}

_NUMERIC_LIKE_RE = re.compile(r"[-+]?[\d,，]+(?:\.\d+)?%?")
_PERIOD_ROLE_KEYWORDS = (
    "期初",
    "年初",
    "期末",
    "年末",
    "上期期末",
    "当前时点",
    "本期",
    "上期",
    "本年",
    "上年",
    "本月",
    "累计",
)


def is_numeric_like_text(value: Any) -> bool:
    text = _compact_text(value)
    return bool(text and _NUMERIC_LIKE_RE.fullmatch(text))


def is_invalid_metric_name(value: Any) -> bool:
    text = _compact_text(value).strip(" :：;；,，.。")
    return not text or is_numeric_like_text(text)


def display_period_role(period_role_norm: Any, period_role_raw: Any = "") -> str:
    norm = _compact_text(period_role_norm)
    raw = str(period_role_raw or "").strip()
    if norm and norm != "unknown":
        label = PERIOD_ROLE_LABELS_ZH.get(norm)
        if label:
            return label
        if _is_displayable_period_role_raw(norm):
            return norm
        return ""
    if _is_displayable_period_role_raw(raw):
        return raw
    return ""


def has_temporal_key(item_date: Any, period_role_norm: Any = "", period_role_raw: Any = "") -> bool:
    if str(item_date or "").strip():
        return True
    norm = _compact_text(period_role_norm).lower()
    if norm in {"", "unknown", "amount", "金额"}:
        return _is_displayable_period_role_raw(period_role_raw)
    return norm == "explicit_date" or _is_displayable_period_role_raw(norm) or norm in {
        "begin",
        "beginning",
        "end",
        "ending",
        "previous_ending",
        "current_point",
        "current_period",
        "current_year",
        "previous_period",
        "previous_year",
    }


def _is_displayable_period_role_raw(value: Any) -> bool:
    text = str(value or "").strip()
    if not text or text == "unknown":
        return False
    if "/" in text or " / " in text:
        return False
    compact = _compact_text(text)
    if is_numeric_like_text(compact):
        return False
    if len(compact) > 16:
        return False
    return any(keyword in compact for keyword in _PERIOD_ROLE_KEYWORDS)


def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip())
