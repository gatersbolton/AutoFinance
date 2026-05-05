from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Sequence

from .models import RawMetricCandidate, RawMetricIssue


def build_summary(
    *,
    run_id: str,
    docs_processed: int,
    providers_seen: Sequence[str],
    candidates: Sequence[RawMetricCandidate],
    accepted: Sequence[RawMetricCandidate],
    issues: Sequence[RawMetricIssue],
    output_files: Sequence[str],
    load_error_total: int = 0,
) -> Dict[str, Any]:
    issue_counter = Counter(issue.issue_type for issue in issues if issue.issue_type)
    summary = {
        "pass": bool(docs_processed > 0 and len(accepted) > 0),
        "run_id": run_id,
        "docs_processed": docs_processed,
        "providers_seen": sorted(set(providers_seen)),
        "candidates_total": len(candidates),
        "accepted_metrics_total": len(accepted),
        "issue_total": len(issues),
        "rows_with_company_total": sum(1 for row in accepted if row.company_name and row.company_name != "UNKNOWN_COMPANY"),
        "rows_with_fill_date_total": sum(1 for row in accepted if row.fill_date),
        "rows_with_item_date_total": sum(1 for row in accepted if row.item_date),
        "issue_breakdown": dict(issue_counter),
        "top_issue_categories": dict(issue_counter.most_common(10)),
        "load_error_total": load_error_total,
        "output_files": list(output_files),
        "no_zt_mapping_confirmed": no_zt_mapping_confirmed(candidates),
    }
    summary["pass"] = bool(summary["pass"] and summary["no_zt_mapping_confirmed"])
    return summary


def no_zt_mapping_confirmed(candidates: Sequence[RawMetricCandidate]) -> bool:
    for candidate in candidates:
        if "ZT_" in candidate.metric_name:
            return False
    return True
