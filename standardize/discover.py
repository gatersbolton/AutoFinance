from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from .models import DiscoveredSource


LOGGER = logging.getLogger(__name__)

SUPPORTED_TABLE_PROVIDERS = {
    "aliyun_table": "aliyun",
    "tencent_table_v3": "tencent",
}
TEXT_ONLY_PROVIDERS = {
    "aliyun_text",
    "tencent_text",
}
PAGE_PATTERN = re.compile(r"page_(\d{4})", re.IGNORECASE)


def parse_page_number(value: str) -> Optional[int]:
    match = PAGE_PATTERN.search(value)
    if not match:
        return None
    return int(match.group(1))


def load_result_index(result_json_path: Path) -> Dict[int, Dict[str, Any]]:
    """Read page-level metadata from a provider result.json file."""

    if not result_json_path.exists():
        return {}

    payload = json.loads(result_json_path.read_text(encoding="utf-8"))
    pages = payload.get("pages", [])
    return {
        int(page["page_number"]): page
        for page in pages
        if isinstance(page, dict) and page.get("page_number") is not None
    }


def list_provider_dirs(input_dir: Path) -> List[str]:
    if not input_dir.exists():
        return []
    return sorted(path.name for path in input_dir.iterdir() if path.is_dir())


def discover_provider_sources(input_dir: Path, provider: str) -> List[DiscoveredSource]:
    """Discover doc/page sources for a single provider directory."""

    provider_dir = input_dir / provider
    if not provider_dir.exists():
        return []

    provider_family = SUPPORTED_TABLE_PROVIDERS.get(provider, provider.split("_", 1)[0])
    discovered: List[DiscoveredSource] = []

    for doc_dir in sorted(path for path in provider_dir.iterdir() if path.is_dir()):
        result_json_path = doc_dir / "result.json"
        result_pages = load_result_index(result_json_path)
        page_numbers = collect_candidate_pages(doc_dir, result_pages)

        for page_no in sorted(page_numbers):
            page_meta = result_pages.get(page_no, {})
            notes: List[str] = []
            raw_file = resolve_raw_file(doc_dir, page_no, provider, page_meta)
            artifact_file = resolve_artifact_file(doc_dir, page_no, page_meta)

            if not raw_file and artifact_file:
                notes.append("missing_raw_json_using_xlsx_fallback")
            elif not raw_file:
                notes.append("missing_raw_json")

            discovered.append(
                DiscoveredSource(
                    doc_id=doc_dir.name,
                    page_no=page_no,
                    provider=provider,
                    provider_family=provider_family,
                    provider_dir=str(doc_dir),
                    raw_file=str(raw_file) if raw_file else None,
                    artifact_file=str(artifact_file) if artifact_file else None,
                    result_json_file=str(result_json_path) if result_json_path.exists() else None,
                    result_page_meta=page_meta,
                    notes=notes,
                )
            )

    return discovered


def collect_candidate_pages(doc_dir: Path, result_pages: Dict[int, Dict[str, Any]]) -> Set[int]:
    page_numbers: Set[int] = set(result_pages.keys())
    for path in doc_dir.rglob("*"):
        if not path.is_file():
            continue
        page_no = parse_page_number(path.name)
        if page_no is not None:
            page_numbers.add(page_no)
    return page_numbers


def resolve_raw_file(
    doc_dir: Path,
    page_no: int,
    provider: str,
    page_meta: Dict[str, Any],
) -> Optional[Path]:
    """Resolve raw json path with result.json as a hint rather than a source of truth."""

    raw_dir = doc_dir / "raw"
    if not raw_dir.exists():
        return None

    hint = page_meta.get("raw_file")
    if isinstance(hint, str):
        hinted_path = doc_dir / Path(hint)
        if hinted_path.exists():
            return hinted_path
        LOGGER.debug("Ignoring missing raw_file hint %s for %s page %s", hint, provider, page_no)

    exact = raw_dir / f"page_{page_no:04d}.json"
    if exact.exists():
        return exact

    candidates = sorted(raw_dir.glob(f"page_{page_no:04d}_*.json"))
    for candidate in candidates:
        if provider.split("_", 1)[0] in candidate.stem:
            return candidate
    if candidates:
        return candidates[0]
    return None


def resolve_artifact_file(doc_dir: Path, page_no: int, page_meta: Dict[str, Any]) -> Optional[Path]:
    artifacts_dir = doc_dir / "artifacts"
    if not artifacts_dir.exists():
        return None

    artifact_files = page_meta.get("artifact_files")
    if isinstance(artifact_files, list):
        for item in artifact_files:
            artifact_path = doc_dir / Path(item)
            if artifact_path.exists():
                return artifact_path

    exact = artifacts_dir / f"page_{page_no:04d}.xlsx"
    if exact.exists():
        return exact

    candidates = sorted(artifacts_dir.glob(f"page_{page_no:04d}*.xlsx"))
    if candidates:
        return candidates[0]
    return None

