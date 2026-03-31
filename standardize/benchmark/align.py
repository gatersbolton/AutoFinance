from __future__ import annotations

from typing import Any, Dict, List, Tuple


def align_benchmark_headers(benchmark_headers: List[str], export_period_headers: List[str], rules: Dict[str, Any] | None = None) -> Dict[str, Dict[str, str]]:
    rules = rules or {}
    legacy_role_map = rules.get(
        "legacy_role_map",
        {
            "期初": "期初数",
            "期末": "期末数",
            "本期": "本期",
            "上期": "上期",
            "本年累计": "本年累计",
        },
    )
    results: Dict[str, Dict[str, str]] = {}
    for header in benchmark_headers:
        if not header or header == "科目名称":
            continue
        if "__" in header:
            if header in export_period_headers:
                results[header] = {"aligned_period_key": header, "reason": "direct_period_key_match"}
            else:
                results[header] = {"aligned_period_key": "", "reason": "period_key_not_found"}
            continue
        role = legacy_role_map.get(header, "")
        if not role:
            results[header] = {"aligned_period_key": "", "reason": "legacy_header_unsupported"}
            continue
        exact_date_matches = [
            value
            for value in export_period_headers
            if value.endswith(f"__{role}") and not value.startswith("unknown_date__") and "-" in value.split("__", 1)[0]
        ]
        if len(exact_date_matches) == 1:
            results[header] = {"aligned_period_key": exact_date_matches[0], "reason": "legacy_role_exact_date_match"}
            continue
        all_matches = [value for value in export_period_headers if value.endswith(f"__{role}")]
        if len(all_matches) == 1:
            results[header] = {"aligned_period_key": all_matches[0], "reason": "legacy_role_unique_match"}
        elif len(all_matches) > 1:
            results[header] = {"aligned_period_key": "", "reason": "ambiguous_period_alignment"}
        else:
            results[header] = {"aligned_period_key": "", "reason": "no_matching_period_for_legacy_role"}
    return results
