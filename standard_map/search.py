from __future__ import annotations

import re
from typing import Any

from .models import StandardRegistry, StandardTerm
from .registry import load_standard_registry


_CODE_PATTERN = re.compile(r"^(?:ZT[_-]?)?(\d+)$", re.IGNORECASE)
_FALLBACK_PINYIN = {
    "短": "duan",
    "期": "qi",
    "借": "jie",
    "款": "kuan",
    "货": "huo",
    "币": "bi",
    "资": "zi",
    "金": "jin",
    "应": "ying",
    "付": "fu",
    "票": "piao",
    "据": "ju",
    "收": "shou",
    "账": "zhang",
    "存": "cun",
    "交": "jiao",
    "税": "shui",
    "费": "fei",
    "职": "zhi",
    "工": "gong",
    "薪": "xin",
    "酬": "chou",
    "营": "ying",
    "业": "ye",
    "入": "ru",
    "主": "zhu",
    "利": "li",
    "润": "run",
    "净": "jing",
    "其": "qi",
    "他": "ta",
    "项": "xiang",
    "现": "xian",
    "等": "deng",
    "价": "jia",
    "物": "wu",
}


def normalize_standard_code(value: str) -> str:
    text = str(value or "").strip().upper().replace("-", "_")
    match = _CODE_PATTERN.match(text)
    if not match:
        return text
    return f"ZT_{int(match.group(1)):03d}"


def build_standard_term_search_index(registry: StandardRegistry | None = None) -> list[dict[str, Any]]:
    registry = registry or load_standard_registry()
    aliases_by_code: dict[str, set[str]] = {term.code: set() for term in registry.terms}
    legacy_aliases_by_code: dict[str, set[str]] = {term.code: set() for term in registry.terms}
    for term in registry.terms:
        aliases_by_code.setdefault(term.code, set()).update(term.aliases)
        legacy_aliases_by_code.setdefault(term.code, set()).update(term.legacy_aliases)
    for alias in registry.aliases:
        target = legacy_aliases_by_code if alias.alias_type == "legacy_alias" else aliases_by_code
        target.setdefault(alias.term.code, set()).add(alias.alias)

    entries: list[dict[str, Any]] = []
    for term in registry.terms:
        aliases = sorted(value for value in aliases_by_code.get(term.code, set()) if value and value != term.name)
        legacy_aliases = sorted(value for value in legacy_aliases_by_code.get(term.code, set()) if value and value != term.name)
        code_tail = term.code.split("_")[-1] if "_" in term.code else term.code
        code_number = str(int(code_tail)) if code_tail.isdigit() else code_tail.lstrip("0")
        pinyin_texts = [term.name, *aliases, *legacy_aliases]
        entries.append(
            {
                "code": term.code,
                "name": term.name,
                "display_label": f"{term.code} {term.name}",
                "aliases": aliases,
                "legacy_aliases": legacy_aliases,
                "code_key": term.code.lower(),
                "code_tail": code_tail.lower(),
                "code_number": code_number.lower(),
                "name_key": _normalize_query(term.name),
                "alias_keys": [_normalize_query(value) for value in aliases],
                "legacy_alias_keys": [_normalize_query(value) for value in legacy_aliases],
                "pinyin_initials": sorted({_pinyin_initials(value) for value in pinyin_texts if _pinyin_initials(value)}),
                "pinyin_full": sorted({_pinyin_full(value) for value in pinyin_texts if _pinyin_full(value)}),
            }
        )
    return entries


def search_standard_terms(
    query: str,
    *,
    registry: StandardRegistry | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    q = _normalize_query(query)
    if not q:
        return [_result(entry, "默认排序", 0) for entry in build_standard_term_search_index(registry)[:limit]]

    normalized_code = normalize_standard_code(q)
    scored: list[dict[str, Any]] = []
    for entry in build_standard_term_search_index(registry):
        score, reason = _score_entry(q, normalized_code, entry)
        if score <= 0:
            continue
        scored.append(_result(entry, reason, score))
    scored.sort(key=lambda item: (-int(item["score"]), str(item["code"])))
    return scored[: max(1, min(limit, 20))]


def _score_entry(query: str, normalized_code: str, entry: dict[str, Any]) -> tuple[int, str]:
    if normalized_code == entry["code"]:
        return 100, "标准编码"
    if query == entry["code_key"]:
        return 100, "标准编码"
    if query == entry["code_tail"]:
        return 96, "编码数字"
    if query == entry["code_number"]:
        return 94, "编码数字"
    if query in entry["code_key"] or query in entry["code_tail"]:
        return 86, "编码片段"
    if query == entry["name_key"]:
        return 88, "标准名称"
    if query in entry["name_key"]:
        return 82, "名称片段"
    if any(query == alias for alias in entry["alias_keys"]):
        return 80, "别名"
    if any(query in alias for alias in entry["alias_keys"]):
        return 76, "别名片段"
    if any(query == alias for alias in entry["legacy_alias_keys"]):
        return 78, "旧名称"
    if any(query in alias for alias in entry["legacy_alias_keys"]):
        return 74, "旧名称片段"
    if any(query == initials for initials in entry["pinyin_initials"]):
        return 84, "拼音首字母"
    if any(initials.startswith(query) or query in initials for initials in entry["pinyin_initials"]):
        return 72, "拼音首字母"
    if any(query == full for full in entry["pinyin_full"]):
        return 82, "完整拼音"
    if any(full.startswith(query) or query in full for full in entry["pinyin_full"]):
        return 70, "完整拼音"
    return 0, ""


def _result(entry: dict[str, Any], reason: str, score: int) -> dict[str, Any]:
    return {
        "code": entry["code"],
        "name": entry["name"],
        "display_label": entry["display_label"],
        "match_reason": reason,
        "score": score,
    }


def _normalize_query(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "").replace("\t", "").replace("\n", "").replace("-", "_")


def _pinyin_full(value: str) -> str:
    try:
        from pypinyin import lazy_pinyin

        return "".join(lazy_pinyin(value, errors="ignore")).lower()
    except Exception:
        return "".join(_FALLBACK_PINYIN.get(char, "") for char in str(value or "")).lower()


def _pinyin_initials(value: str) -> str:
    try:
        from pypinyin import Style, lazy_pinyin

        return "".join(lazy_pinyin(value, style=Style.FIRST_LETTER, errors="ignore")).lower()
    except Exception:
        return "".join(_FALLBACK_PINYIN.get(char, "")[:1] for char in str(value or "") if char in _FALLBACK_PINYIN).lower()
