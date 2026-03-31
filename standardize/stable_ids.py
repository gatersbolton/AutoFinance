from __future__ import annotations

import hashlib
import json
from typing import Iterable, Sequence


def _normalize_part(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_normalize_part(item) for item in value) + "]"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return str(value).strip()


def stable_hash(parts: Sequence[object], length: int = 12) -> str:
    payload = "||".join(_normalize_part(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:length]


def stable_id(prefix: str, parts: Sequence[object], length: int = 12) -> str:
    return f"{prefix}{stable_hash(parts, length=length)}"


def fact_id_parts(fact) -> list[object]:
    return [
        fact.doc_id,
        fact.page_no,
        fact.provider,
        fact.source_cell_ref,
        fact.row_label_std or fact.row_label_raw,
        fact.column_semantic_key,
        fact.period_key,
    ]


def conflict_id_parts(
    doc_id: str,
    page_no: int,
    table_semantic_key: str,
    row_label_std: str,
    column_semantic_key: str,
    period_key: str,
) -> list[object]:
    return [doc_id, page_no, table_semantic_key, row_label_std, column_semantic_key, period_key]


def review_id_parts(fact_id: str, reason_codes: Iterable[str]) -> list[object]:
    return [fact_id, sorted(str(code) for code in reason_codes if str(code).strip())]


def task_id_parts(review_id: str, granularity: str, bbox) -> list[object]:
    return [review_id, granularity, bbox]


def action_id_parts(review_id: str, action_type: str, action_value: str) -> list[object]:
    return [review_id, action_type, action_value]
