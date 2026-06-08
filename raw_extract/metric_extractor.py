from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from standardize.models import CellRecord, LogicalSubtable, ProviderPage
from standardize.normalize.headers import joined_header_path
from standardize.normalize.statements import infer_period_role
from standardize.normalize.tables import extract_row_label
from standardize.normalize.text import clean_text

from .date_resolver import resolve_fill_date, resolve_item_date
from .models import CompanyResolution, FillDateResolution, RawMetricCandidate, RawMetricIssue
from .number_parser import parse_metric_number


ENUMERATION_PATTERNS = (
    re.compile(r"^\s*[一二三四五六七八九十]+[、,，.．]\s*"),
    re.compile(r"^\s*[（(]?\d+[）).、,，]\s*"),
)


def extract_raw_metric_candidates(
    *,
    subtables: Sequence[LogicalSubtable],
    pages: Sequence[ProviderPage],
    company: CompanyResolution,
    input_dir: Path,
    source_image_dir: Optional[Path],
    provider_priority: Sequence[str],
    default_fill_date: str = "",
    include_blank: bool = False,
    include_ratios: bool = True,
) -> Tuple[List[RawMetricCandidate], List[FillDateResolution]]:
    page_lookup = {(page.provider, page.page_no, page.doc_id): page for page in pages}
    priority_rank = {provider: rank for rank, provider in enumerate(provider_priority)}
    evidence_path = resolve_evidence_path(source_image_dir)
    candidates: List[RawMetricCandidate] = []
    date_audits: List[FillDateResolution] = []

    for subtable in subtables:
        page = page_lookup.get((subtable.provider, subtable.page_no, subtable.doc_id))
        fill_resolution = resolve_fill_date(
            subtable=subtable,
            page=page,
            input_dir=input_dir,
            source_image_dir=source_image_dir,
            default_fill_date=default_fill_date,
        )
        date_audits.append(fill_resolution)
        context_by_row = build_row_contexts(subtable)

        for row_idx in range(subtable.header_row_count, len(subtable.grid)):
            row_cells = subtable.grid[row_idx]
            row_label_raw = extract_row_label(row_cells, subtable.row_label_col, subtable.value_cols, subtable.line_no_col)
            metric_name = clean_metric_name(row_label_raw)
            text_confidence = extract_row_label_confidence(row_cells, subtable.row_label_col, subtable.value_cols, subtable.line_no_col)
            has_values = any(not row_cells[col_idx].is_empty for col_idx in subtable.value_cols if col_idx < len(row_cells))
            if not metric_name and not has_values:
                continue

            for value_col in subtable.value_cols:
                if value_col >= len(row_cells):
                    continue
                cell = row_cells[value_col]
                if cell.is_empty and not include_blank:
                    continue

                number_info = parse_metric_number(cell.text_raw or cell.text_clean, expected_numeric=True)
                if number_info.value_type == "blank" and not include_blank:
                    continue

                header_path = subtable.header_paths.get(value_col, [])
                period_role_raw = infer_period_role(header_path, {"period_roles": _period_keyword_config()})
                if period_role_raw == "unknown":
                    period_role_raw = joined_header_path(header_path)
                item_resolution = resolve_item_date(
                    fill_date=fill_resolution.fill_date,
                    header_path=header_path,
                    period_role_raw=period_role_raw,
                    statement_type=subtable.statement_meta.statement_type,
                    fill_date_method=fill_resolution.method,
                )

                issue_flags = collect_candidate_issue_flags(
                    metric_name=metric_name,
                    company=company,
                    fill_resolution=fill_resolution,
                    item_issue_flags=item_resolution.issue_flags,
                    cell=cell,
                    number_issue_flags=number_info.issue_flags,
                    value_type=number_info.value_type,
                    include_ratios=include_ratios,
                )
                candidate = RawMetricCandidate(
                    candidate_id=build_candidate_id(
                        [
                            subtable.doc_id,
                            subtable.provider,
                            subtable.page_no,
                            subtable.table_id,
                            subtable.logical_subtable_id,
                            row_idx,
                            value_col,
                            cell.text_raw,
                        ]
                    ),
                    fill_date=fill_resolution.fill_date,
                    item_date=item_resolution.item_date,
                    company_name=company.company_name,
                    metric_name=metric_name,
                    metric_value=number_info.value,
                    value_raw=cell.text_raw,
                    value_type=number_info.value_type,
                    unit_raw=subtable.statement_meta.unit_raw,
                    provider=subtable.provider,
                    doc_id=subtable.doc_id,
                    source_file=subtable.source_file,
                    page_no=subtable.page_no,
                    table_id=subtable.table_id,
                    logical_subtable_id=subtable.logical_subtable_id,
                    row_index=cell.row_start,
                    col_index=cell.col_start,
                    row_label_raw=row_label_raw,
                    row_label_clean=metric_name,
                    row_context_path=context_by_row.get(row_idx, ""),
                    header_path=joined_header_path(header_path),
                    period_role_raw=period_role_raw,
                    period_role_norm=item_resolution.period_role_norm,
                    period_start_date=item_resolution.period_start_date,
                    period_end_date=item_resolution.period_end_date,
                    date_resolution_method="|".join(part for part in [fill_resolution.method, item_resolution.method] if part),
                    company_resolution_method=company.method,
                    source_cell_ref=build_source_cell_ref(cell),
                    bbox_json=cell.bbox_json,
                    text_confidence=text_confidence,
                    value_confidence=cell.ocr_conf,
                    confidence=cell.ocr_conf,
                    evidence_path=evidence_path,
                    issue_flags=issue_flags,
                    provider_rank=priority_rank.get(subtable.provider, 9999),
                )
                candidate.duplicate_key = build_duplicate_key(candidate, subtable)
                candidates.append(candidate)

    return candidates, date_audits


def select_accepted_candidates(
    candidates: Sequence[RawMetricCandidate],
    *,
    include_blank: bool,
    include_ratios: bool,
) -> Tuple[List[RawMetricCandidate], List[RawMetricIssue]]:
    issues: List[RawMetricIssue] = []
    eligible: List[RawMetricCandidate] = []
    for candidate in candidates:
        if not candidate.metric_name:
            candidate.selection_status = "rejected_missing_metric_name"
            continue
        if candidate.value_type == "ratio" and not include_ratios:
            candidate.selection_status = "rejected_ratio"
            issues.append(issue_from_candidate(candidate, "ratio_excluded", "info", "Ratio candidate excluded by --include-ratios false."))
            continue
        if candidate.metric_value is None and not include_blank:
            candidate.selection_status = "rejected_no_numeric_value"
            continue
        eligible.append(candidate)

    grouped: Dict[str, List[RawMetricCandidate]] = defaultdict(list)
    for candidate in eligible:
        grouped[candidate.duplicate_key].append(candidate)

    accepted: List[RawMetricCandidate] = []
    for duplicate_key, group in grouped.items():
        ordered = sorted(group, key=lambda item: (item.provider_rank, item.page_no, item.source_cell_ref, item.candidate_id))
        chosen = ordered[0]
        chosen.accepted = True
        chosen.selection_status = "accepted"
        accepted.append(chosen)

        if len(group) > 1:
            for duplicate in ordered[1:]:
                duplicate.selection_status = "duplicate_candidate"
                duplicate.issue_flags = dedupe_flags([*duplicate.issue_flags, "duplicate_candidate"])
                issues.append(issue_from_candidate(duplicate, "duplicate_candidate", "info", "Duplicate candidate superseded by provider priority."))

        conflict_values = numeric_values_by_provider(group)
        if len({value for value in conflict_values.values()}) > 1:
            issues.append(
                issue_from_candidate(
                    chosen,
                    "provider_value_conflict",
                    "warning",
                    "Providers disagree on the numeric value for this raw metric slot; preferred provider retained.",
                    meta={
                        "duplicate_key": duplicate_key,
                        "provider_values": conflict_values,
                        "accepted_provider": chosen.provider,
                    },
                )
            )
            chosen.issue_flags = dedupe_flags([*chosen.issue_flags, "provider_value_conflict"])

    for candidate in candidates:
        for issue_flag in candidate.issue_flags:
            if issue_flag in {"ratio_excluded", "duplicate_candidate"}:
                continue
            severity = "warning" if issue_flag not in {"missing_bbox", "duplicate_candidate"} else "info"
            issues.append(issue_from_candidate(candidate, issue_flag, severity, f"Candidate issue: {issue_flag}"))

    return sorted(accepted, key=lambda item: (item.doc_id, item.page_no, item.table_id, item.row_index, item.col_index, item.provider_rank)), issues


def clean_metric_name(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    changed = True
    while changed:
        changed = False
        for pattern in ENUMERATION_PATTERNS:
            updated = pattern.sub("", text).strip()
            if updated != text:
                text = updated
                changed = True
                break
    text = text.strip(" :：;；,，.。")
    text = re.sub(r"\s+", "", text)
    return text


def build_row_contexts(subtable: LogicalSubtable) -> Dict[int, str]:
    context_by_row: Dict[int, str] = {}
    active_context: List[str] = []
    for row_idx in range(subtable.header_row_count, len(subtable.grid)):
        row_cells = subtable.grid[row_idx]
        label = clean_metric_name(extract_row_label(row_cells, subtable.row_label_col, subtable.value_cols, subtable.line_no_col))
        has_value = any(not row_cells[col_idx].is_empty for col_idx in subtable.value_cols if col_idx < len(row_cells))
        if label and not has_value:
            active_context = [label]
            context_by_row[row_idx] = ""
            continue
        context_by_row[row_idx] = " / ".join(active_context)
    return context_by_row


def extract_row_label_confidence(
    row_cells: Sequence[CellRecord],
    row_label_col: int,
    value_cols: Sequence[int],
    line_no_col: Optional[int],
) -> Optional[float]:
    if row_label_col < len(row_cells) and row_cells[row_label_col].text_clean:
        return row_cells[row_label_col].ocr_conf
    values = [
        cell.ocr_conf
        for col_idx, cell in enumerate(row_cells)
        if col_idx not in value_cols
        and col_idx != line_no_col
        and cell.text_clean
        and cell.ocr_conf is not None
    ]
    return min(values) if values else None


def collect_candidate_issue_flags(
    *,
    metric_name: str,
    company: CompanyResolution,
    fill_resolution: FillDateResolution,
    item_issue_flags: Sequence[str],
    cell: CellRecord,
    number_issue_flags: Sequence[str],
    value_type: str,
    include_ratios: bool,
) -> List[str]:
    flags: List[str] = []
    if not metric_name:
        flags.append("weak_metric_name")
    elif len(metric_name) <= 1 or metric_name.isdigit():
        flags.append("weak_metric_name")
    flags.extend(company.issue_flags)
    flags.extend(fill_resolution.issue_flags)
    flags.extend(item_issue_flags)
    flags.extend(number_issue_flags)
    if not cell.bbox_json:
        flags.append("missing_bbox")
    if value_type == "ratio" and not include_ratios:
        flags.append("ratio_excluded")
    return dedupe_flags(flags)


def build_duplicate_key(candidate: RawMetricCandidate, subtable: LogicalSubtable) -> str:
    parts = [
        candidate.doc_id,
        subtable.statement_meta.statement_type,
        clean_metric_name(candidate.metric_name),
        candidate.row_context_path,
        candidate.fill_date,
        candidate.item_date,
        candidate.period_role_norm,
    ]
    return "|".join(str(part or "") for part in parts)


def numeric_values_by_provider(group: Sequence[RawMetricCandidate]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for candidate in group:
        if candidate.metric_value is None:
            continue
        values[candidate.provider] = numeric_signature(candidate.metric_value)
    return values


def numeric_signature(value: float) -> str:
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


def issue_from_candidate(
    candidate: RawMetricCandidate,
    issue_type: str,
    severity: str,
    message: str,
    meta: Optional[Dict[str, Any]] = None,
) -> RawMetricIssue:
    return RawMetricIssue(
        issue_id=build_candidate_id([candidate.candidate_id, issue_type, message]),
        issue_type=issue_type,
        severity=severity,
        message=message,
        doc_id=candidate.doc_id,
        provider=candidate.provider,
        source_file=candidate.source_file,
        page_no=candidate.page_no,
        table_id=candidate.table_id,
        logical_subtable_id=candidate.logical_subtable_id,
        source_cell_ref=candidate.source_cell_ref,
        metric_name=candidate.metric_name,
        fill_date=candidate.fill_date,
        item_date=candidate.item_date,
        value_raw=candidate.value_raw,
        candidate_id=candidate.candidate_id,
        meta_json=_compact_json(meta or {}),
    )


def build_source_cell_ref(cell: CellRecord) -> str:
    return f"{cell.doc_id}:{cell.page_no}:{cell.provider}:{cell.table_id}:{cell.row_start}-{cell.row_end}:{cell.col_start}-{cell.col_end}"


def build_candidate_id(parts: Iterable[Any]) -> str:
    payload = "|".join(str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def resolve_evidence_path(source_image_dir: Optional[Path]) -> str:
    if not source_image_dir:
        return ""
    if source_image_dir.is_file():
        return str(source_image_dir)
    if source_image_dir.exists():
        for path in sorted(source_image_dir.iterdir()):
            if path.is_file() and path.suffix.lower() in {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}:
                return str(path)
    return ""


def dedupe_flags(flags: Sequence[str]) -> List[str]:
    return list(dict.fromkeys(flag for flag in flags if flag))


def _period_keyword_config() -> Dict[str, List[str]]:
    return {
        "期初数": ["期初数", "年初数", "年初", "期初"],
        "期末数": ["期末数", "年末数", "期末余额", "本期末", "年末", "期末"],
        "本期": ["本期", "本期金额", "本年累计", "本年数", "本年"],
        "上期": ["上期", "上期金额", "上年同期", "上年数"],
        "本年累计": ["本年累计", "本年累计数"],
        "上年累计": ["上年累计", "上年累计数"],
    }


def _compact_json(value: Dict[str, Any]) -> str:
    if not value:
        return ""
    import json

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
