from __future__ import annotations

import re


WHITESPACE_RE = re.compile(r"\s+")
FULLWIDTH_TRANSLATION = str.maketrans(
    {
        "，": ",",
        "：": ":",
        "；": ";",
        "（": "(",
        "）": ")",
        "【": "[",
        "】": "]",
        "％": "%",
        "－": "-",
        "—": "-",
        "　": " ",
        "．": ".",
        "、": ",",
    }
)


def clean_text(value: object) -> str:
    """Normalize whitespace and common punctuation without dropping evidence."""

    text = "" if value is None else str(value)
    text = text.translate(FULLWIDTH_TRANSLATION)
    text = text.replace("\u200b", "")
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def normalize_label_for_matching(value: object) -> str:
    text = clean_text(value)
    text = text.rstrip(":")
    text = text.replace(" ", "")
    return text

