from __future__ import annotations

try:
    from standardize.mapping.alias_miner import normalize_subject_label as _normalize_subject_label
except Exception:  # pragma: no cover
    _normalize_subject_label = None


def normalize_metric_name(value: object) -> str:
    if _normalize_subject_label is not None:
        return _normalize_subject_label(value)
    text = "" if value is None else str(value)
    for old, new in {
        "（": "(",
        "）": ")",
        "：": ":",
        "　": " ",
        "\u200b": "",
    }.items():
        text = text.replace(old, new)
    text = "".join(text.split()).rstrip(":")
    for prefix in ("其中:", "其中", "减:", "减", "加:", "加"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
    return text.strip()
