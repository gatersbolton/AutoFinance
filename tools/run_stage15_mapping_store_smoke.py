from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from project_paths import (
    RAW_METRICS_GENERATED_ROOT,
    REPO_ROOT,
    STANDARD_METRICS_GENERATED_ROOT,
    STANDARD_TERMS_PATH,
    WEB_MAPPING_STORE_PATH,
    WEB_MAPPING_STORE_ROOT,
)
from standard_map.confidence import build_confidence_bulk_accept_preview
from standard_map.decisions import append_mapping_decision_file, apply_mapping_decision_to_output
from standard_map.mapper import run_standard_mapping
from standard_map.policy import default_confidence_threshold
from standard_map.store import LocalMappingStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Stage 15 local mapping-store smoke workflow.")
    parser.add_argument("--doc-id", default="D01")
    parser.add_argument("--threshold", type=float, default=default_confidence_threshold())
    args = parser.parse_args(argv)

    WEB_MAPPING_STORE_ROOT.mkdir(parents=True, exist_ok=True)
    raw_metrics = _latest_raw_metrics(args.doc_id)
    output_base = STANDARD_METRICS_GENERATED_ROOT / args.doc_id / "stage15_smoke"
    output_base.mkdir(parents=True, exist_ok=True)
    store = LocalMappingStore(WEB_MAPPING_STORE_PATH)

    first_run = _run_mapping(raw_metrics, output_base, args.doc_id, "first")
    review_rows = _read_csv(Path(first_run.output_dir) / "mapping_review_items.csv")
    review_candidates = [row for row in review_rows if str(row.get("mapping_status", "")) in {"review_required", "unmapped"}]
    if not review_candidates:
        raise RuntimeError("Stage 15 smoke requires at least one D01 review_required or unmapped mapping item.")

    reject_item = review_candidates[0]
    accept_once_item = review_candidates[1] if len(review_candidates) > 1 else reject_item
    remember_item = next(
        (
            row
            for row in review_candidates
            if row.get("candidate_code")
            and row.get("candidate_name")
            and (row.get("relation_type") or row.get("issue_reason", "").endswith("_relation_requires_review"))
        ),
        next((row for row in review_candidates if row.get("candidate_code") and row.get("candidate_name")), review_candidates[-1]),
    )

    decisions = []
    decisions.append(
        _record_and_apply(
            store,
            first_run.output_dir,
            item=reject_item,
            decision="reject",
            final_code="",
            final_name="",
            note="stage15 smoke reject",
        )
    )
    accept_once_code, accept_once_name = _target_for_item(accept_once_item)
    decisions.append(
        _record_and_apply(
            store,
            first_run.output_dir,
            item=accept_once_item,
            decision="accept_once",
            final_code=accept_once_code,
            final_name=accept_once_name,
            note="stage15 smoke accept once",
        )
    )
    remember_code, remember_name = _target_for_item(remember_item)
    local_aliases_before_remember = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    decisions.append(
        _record_and_apply(
            store,
            first_run.output_dir,
            item=remember_item,
            decision="accept_and_remember",
            final_code=remember_code,
            final_name=remember_name,
            note="stage15 smoke remember",
        )
    )
    store.export_aliases()
    store.export_decision_audit()
    store.write_snapshot(Path(first_run.output_dir) / "mapping_store_snapshot.yml")

    rerun = _run_mapping(raw_metrics, output_base, args.doc_id, "rerun")
    remember_name_raw = str(remember_item.get("原始指标名", ""))
    remembered_alias_reused_pass = any(
        row.raw.metric_name == remember_name_raw
        and row.mapping_method == "local_alias"
        and row.standard_code == remember_code
        for row in rerun.rows
    )

    aliases_before_preview = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    preview = build_confidence_bulk_accept_preview(
        rerun.output_dir,
        threshold=args.threshold,
        before_alias_count=aliases_before_preview,
        after_alias_count=aliases_before_preview,
    )
    aliases_after_preview = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    store.write_snapshot(Path(rerun.output_dir) / "mapping_store_snapshot.yml")

    output_files = sorted(
        {
            *first_run.output_files,
            *rerun.output_files,
            str(WEB_MAPPING_STORE_PATH),
            str(WEB_MAPPING_STORE_ROOT / "local_aliases_export.yml"),
            str(WEB_MAPPING_STORE_ROOT / "mapping_decisions_audit.csv"),
        }
    )
    path_hygiene_pass = _path_hygiene_pass(output_files)
    summary = {
        "pass": bool(remembered_alias_reused_pass and not preview["mutated_mappings"] and path_hygiene_pass),
        "store_path": str(WEB_MAPPING_STORE_PATH),
        "raw_metrics_path": str(raw_metrics),
        "first_run_output_dir": first_run.output_dir,
        "rerun_output_dir": rerun.output_dir,
        "decisions_total": len(decisions),
        "rejected_total": sum(1 for row in decisions if row["decision"] == "reject"),
        "accept_once_total": sum(1 for row in decisions if row["decision"] == "accept_once"),
        "accept_and_remember_total": sum(1 for row in decisions if row["decision"] == "accept_and_remember"),
        "local_aliases_total": aliases_after_preview,
        "local_aliases_added_by_smoke": max(0, aliases_after_preview - local_aliases_before_remember),
        "remembered_alias": remember_name_raw,
        "remembered_alias_target_code": remember_code,
        "remembered_alias_reused_pass": remembered_alias_reused_pass,
        "confidence_threshold_default": default_confidence_threshold(),
        "confidence_preview_total": preview["eligible_total"],
        "confidence_preview_mutated_store": aliases_before_preview != aliases_after_preview,
        "output_files": output_files,
        "path_hygiene_pass": path_hygiene_pass,
    }
    summary_path = WEB_MAPPING_STORE_ROOT / "stage15_mapping_store_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary["pass"] else 1


def _latest_raw_metrics(doc_id: str) -> Path:
    candidates = sorted(
        (RAW_METRICS_GENERATED_ROOT / doc_id).glob("RUN_*/raw_metrics.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No generated raw_metrics.csv found for {doc_id}.")
    return candidates[0].resolve()


def _run_mapping(raw_metrics: Path, output_base: Path, doc_id: str, pass_name: str):
    args = SimpleNamespace(
        input=str(raw_metrics),
        output_dir=str(output_base),
        mapping_registry=str(STANDARD_TERMS_PATH),
        mapping_store_path=str(WEB_MAPPING_STORE_PATH),
        doc_id=doc_id,
        company_name="",
        debug=False,
    )
    return run_standard_mapping(
        args=args,
        cli_args=[
            "--input",
            str(raw_metrics),
            "--output-dir",
            str(output_base),
            "--doc-id",
            doc_id,
            "--mapping-store-path",
            str(WEB_MAPPING_STORE_PATH),
            "--stage15-smoke-pass",
            pass_name,
        ],
    )


def _record_and_apply(
    store: LocalMappingStore,
    output_dir: str,
    *,
    item: dict[str, str],
    decision: str,
    final_code: str,
    final_name: str,
    note: str,
) -> dict[str, object]:
    payload = store.record_decision(
        job_id="stage15_smoke",
        doc_id="D01",
        raw_metric_id=str(item.get("raw_metric_id", "")),
        raw_metric_name=str(item.get("原始指标名", "")),
        suggested_code=str(item.get("candidate_code", "")),
        suggested_name=str(item.get("candidate_name", "")),
        decision=decision,
        final_code=final_code,
        final_name=final_name,
        relation_type=str(item.get("relation_type", "") or "exact_alias"),
        confidence=_float_or_none(item.get("mapping_confidence") or item.get("candidate_score")),
        decided_by="stage15_smoke",
        note=note,
    )
    apply_mapping_decision_to_output(output_dir, payload)
    append_mapping_decision_file(Path(output_dir), payload)
    return payload


def _target_for_item(item: dict[str, str]) -> tuple[str, str]:
    code = str(item.get("candidate_code", "") or "")
    name = str(item.get("candidate_name", "") or "")
    if code and name:
        return code, name
    raw_name = str(item.get("原始指标名", "") or "")
    if raw_name == "应付账款":
        return "ZT_013", "应付账款"
    return "ZT_001", "货币资金"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float_or_none(value: object) -> float | None:
    try:
        text = str(value or "").strip()
        return float(text) if text else None
    except ValueError:
        return None


def _path_hygiene_pass(output_files: list[str]) -> bool:
    generated_root = (REPO_ROOT / "data" / "generated").resolve()
    for raw in output_files:
        path = Path(raw).resolve()
        try:
            path.relative_to(generated_root)
        except ValueError:
            return False
    forbidden_root_files = [
        REPO_ROOT / "local_mappings.sqlite",
        REPO_ROOT / "local_aliases_export.yml",
        REPO_ROOT / "mapping_decisions_audit.csv",
        REPO_ROOT / "stage15_mapping_store_summary.json",
    ]
    return not any(path.exists() for path in forbidden_root_files)


if __name__ == "__main__":
    raise SystemExit(main())
