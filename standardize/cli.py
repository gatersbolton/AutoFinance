from __future__ import annotations

import argparse
import csv
import json
import logging
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import yaml

from .discover import SUPPORTED_TABLE_PROVIDERS, TEXT_ONLY_PROVIDERS, discover_provider_sources, list_provider_dirs
from .models import ConflictRecord, DiscoveredSource, FactRecord, IssueRecord, MappingReviewRecord, dataclass_row
from .models import CellRecord
from .normalize.conflicts import resolve_conflicts
from .normalize.export import export_template
from .normalize.mapping import apply_subject_mapping, load_alias_mapping, load_template_subjects
from .normalize.statements import classify_statement
from .normalize.tables import extract_facts, standardize_page
from .providers import load_aliyun_page, load_tencent_page, load_xlsx_fallback_page


LOGGER = logging.getLogger(__name__)
PACKAGE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = PACKAGE_DIR / "config"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standardize financial statement OCR outputs.")
    parser.add_argument("--input-dir", default="outputs", help="Directory containing OCR provider outputs.")
    parser.add_argument("--template", required=True, help="Path to the standard accounting template workbook.")
    parser.add_argument("--output-dir", default="normalized", help="Directory for normalized outputs.")
    parser.add_argument(
        "--provider-priority",
        default="aliyun,tencent",
        help="Comma-separated provider families or provider names, e.g. aliyun,tencent or aliyun_table,tencent_table_v3.",
    )
    parser.add_argument("--enable-conflict-merge", action="store_true", help="Enable cross-provider conflict resolution.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, e.g. INFO or DEBUG.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    input_dir = Path(args.input_dir).resolve()
    template_path = Path(args.template).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    provider_config = load_yaml(CONFIG_DIR / "provider_priority.yml")
    statement_config = load_yaml(CONFIG_DIR / "statement_keywords.yml")

    provider_priority = expand_provider_priority(args.provider_priority, provider_config)
    LOGGER.info("Using provider priority: %s", provider_priority)

    found_provider_dirs = list_provider_dirs(input_dir)
    skipped_text = [provider for provider in found_provider_dirs if provider in TEXT_ONLY_PROVIDERS]
    unsupported = [provider for provider in found_provider_dirs if provider not in SUPPORTED_TABLE_PROVIDERS and provider not in TEXT_ONLY_PROVIDERS]
    for provider in skipped_text:
        LOGGER.info("Skipping text-only provider %s for structured extraction.", provider)
    for provider in unsupported:
        LOGGER.warning("Skipping unsupported provider directory %s.", provider)

    selected_providers = [provider for provider in provider_priority if provider in SUPPORTED_TABLE_PROVIDERS]
    sources: List[DiscoveredSource] = []
    for provider in selected_providers:
        provider_sources = discover_provider_sources(input_dir, provider)
        LOGGER.info("Discovered %s page sources for provider %s.", len(provider_sources), provider)
        sources.extend(provider_sources)

    all_cells: List[CellRecord] = []
    all_facts: List[FactRecord] = []
    all_issues: List[IssueRecord] = []
    page_errors: List[Dict[str, Any]] = []
    provider_hits: Dict[str, int] = {}

    for source in sorted(sources, key=lambda item: (item.doc_id, item.page_no, provider_priority.index(item.provider))):
        try:
            page = load_provider_page(source)
            provider_hits[page.provider] = provider_hits.get(page.provider, 0) + 1
            statement_meta = classify_statement(page, statement_config)
            page_cells, logical_subtables, page_issues = standardize_page(page, statement_meta, statement_config)
            page_facts, fact_issues = extract_facts(logical_subtables, statement_config)

            all_cells.extend(page_cells)
            all_facts.extend(page_facts)
            all_issues.extend(page_issues)
            all_issues.extend(fact_issues)

            LOGGER.info(
                "Processed %s page %s provider %s: %s cells, %s facts, %s issues.",
                source.doc_id,
                source.page_no,
                source.provider,
                len(page_cells),
                len(page_facts),
                len(page_issues) + len(fact_issues),
            )
        except Exception as exc:  # pragma: no cover - defensive logging path
            LOGGER.exception("Failed to process %s page %s provider %s", source.doc_id, source.page_no, source.provider)
            page_errors.append(
                {
                    "doc_id": source.doc_id,
                    "page_no": source.page_no,
                    "provider": source.provider,
                    "error": str(exc),
                }
            )

    subjects, subject_sheet, header_row = load_template_subjects(template_path)
    alias_mapping = load_alias_mapping(CONFIG_DIR / "subject_aliases.yml", subjects)
    all_facts, mapping_review = apply_subject_mapping(all_facts, subjects, alias_mapping)
    all_facts, conflicts = resolve_conflicts(all_facts, provider_priority, args.enable_conflict_merge)

    export_stats = export_template(
        template_path=template_path,
        output_path=output_dir / "会计报表_填充结果.xlsx",
        facts=all_facts,
    )

    write_dataclass_csv(output_dir / "cells.csv", all_cells, CellRecord)
    write_dataclass_csv(output_dir / "facts.csv", all_facts, FactRecord)
    write_dataclass_csv(output_dir / "issues.csv", all_issues, IssueRecord)
    write_dataclass_csv(output_dir / "conflicts.csv", conflicts, ConflictRecord)
    write_dataclass_csv(output_dir / "mapping_review.csv", mapping_review, MappingReviewRecord)

    summary = {
        "docs_processed": len({source.doc_id for source in sources}),
        "pages_discovered": len(sources),
        "providers_seen": provider_hits,
        "skipped_text_providers": skipped_text,
        "unsupported_providers": unsupported,
        "cells_count": len(all_cells),
        "facts_count": len(all_facts),
        "issues_count": len(all_issues),
        "conflicts_count": len(conflicts),
        "mapping_review_count": len(mapping_review),
        "template_sheet": subject_sheet,
        "template_header_row": header_row,
        "export_stats": export_stats,
        "page_errors": page_errors,
    }
    write_json(output_dir / "summary.json", summary)
    LOGGER.info("Wrote normalized outputs to %s", output_dir)
    return 0


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def load_yaml(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}


def expand_provider_priority(spec: str, provider_config: Dict[str, Any]) -> List[str]:
    tokens = [token.strip() for token in spec.split(",") if token.strip()]
    families = provider_config.get("families", {}) or {}
    default_priority = provider_config.get("default_priority", []) or []
    expanded: List[str] = []

    if not tokens:
        tokens = list(default_priority)

    for token in tokens:
        if token in families:
            expanded.extend(families[token])
        else:
            expanded.append(token)

    deduped: List[str] = []
    for provider in expanded:
        if provider not in deduped:
            deduped.append(provider)
    return deduped


def load_provider_page(source: DiscoveredSource):
    if source.provider == "aliyun_table" and source.raw_file:
        return load_aliyun_page(source)
    if source.provider == "tencent_table_v3" and source.raw_file:
        return load_tencent_page(source)
    if source.artifact_file:
        return load_xlsx_fallback_page(source)
    raise ValueError(f"No supported source found for {source.doc_id} page {source.page_no} provider {source.provider}")


def write_dataclass_csv(path: Path, rows: Iterable[Any], model_cls) -> None:
    rows = list(rows)
    fieldnames = [item.name for item in fields(model_cls)]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dataclass_row(row))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
