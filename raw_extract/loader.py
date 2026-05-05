from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import yaml

from standardize.discover import SUPPORTED_TABLE_PROVIDERS, discover_provider_sources, list_provider_dirs
from standardize.models import DiscoveredSource, ProviderPage

from .provider_adapters import load_provider_page


PACKAGE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = PACKAGE_DIR.parent / "standardize" / "config"


def load_existing_provider_pages(
    *,
    input_dir: Path,
    provider_priority: Sequence[str],
    doc_id_override: str = "",
    debug: bool = False,
) -> Tuple[List[ProviderPage], List[DiscoveredSource], List[Dict[str, Any]]]:
    providers = discover_extractable_providers(input_dir, provider_priority)
    sources: List[DiscoveredSource] = []
    pages: List[ProviderPage] = []
    load_errors: List[Dict[str, Any]] = []

    for provider in providers:
        provider_sources = discover_provider_sources(input_dir, provider)
        for source in provider_sources:
            load_source = replace(source, doc_id=doc_id_override) if doc_id_override else source
            sources.append(load_source)
            try:
                pages.append(load_provider_page(load_source, doc_id_override=doc_id_override))
            except Exception as exc:  # pragma: no cover - exercised by real fixture drift
                load_errors.append(
                    {
                        "provider": provider,
                        "doc_id": load_source.doc_id,
                        "page_no": load_source.page_no,
                        "raw_file": load_source.raw_file or "",
                        "artifact_file": load_source.artifact_file or "",
                        "error": str(exc),
                    }
                )
                if debug:
                    raise
    return pages, sources, load_errors


def discover_extractable_providers(input_dir: Path, provider_priority: Sequence[str]) -> List[str]:
    present = list_provider_dirs(input_dir)
    ordered: List[str] = []
    for provider in provider_priority:
        if provider in present and provider not in ordered:
            ordered.append(provider)
    for provider in present:
        if provider in SUPPORTED_TABLE_PROVIDERS and provider not in ordered:
            ordered.append(provider)
        elif provider.lower().startswith("xlsx") and provider not in ordered:
            ordered.append(provider)
    return ordered


def expand_provider_priority(spec: str) -> List[str]:
    tokens = [token.strip() for token in str(spec or "").split(",") if token.strip()]
    provider_config = load_yaml(CONFIG_DIR / "provider_priority.yml")
    families = provider_config.get("families", {}) or {}
    default_priority = provider_config.get("default_priority", []) or []
    if not tokens:
        tokens = list(default_priority)

    expanded: List[str] = []
    for token in tokens:
        if token in families:
            expanded.extend(str(item) for item in families[token])
        else:
            expanded.append(token)

    deduped: List[str] = []
    for provider in expanded:
        if provider not in deduped:
            deduped.append(provider)
    return deduped


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}


def collect_source_files(sources: Iterable[DiscoveredSource]) -> List[str]:
    paths = set()
    for source in sources:
        for field in ("raw_file", "artifact_file", "result_json_file"):
            value = getattr(source, field, None)
            if value:
                paths.add(str(value))
    return sorted(paths)


def load_registry_metadata(doc_id: str) -> Dict[str, Any]:
    registry_path = PACKAGE_DIR.parent / "benchmarks" / "registry.yml"
    if not registry_path.exists() or not doc_id:
        return {}
    payload = load_yaml(registry_path)
    entries = payload.get("documents", payload if isinstance(payload, list) else [])
    if not isinstance(entries, list):
        return {}
    for entry in entries:
        if isinstance(entry, dict) and str(entry.get("doc_id", "")) == doc_id:
            return dict(entry)
    return {}


def infer_doc_id(input_dir: Path, explicit_doc_id: str = "") -> str:
    if explicit_doc_id.strip():
        return explicit_doc_id.strip()
    if input_dir.name == "ocr_outputs" and input_dir.parent.name:
        return input_dir.parent.name
    return input_dir.name


def write_debug_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
