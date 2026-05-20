from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from project_paths import RAW_METRICS_GENERATED_ROOT, REPO_ROOT, STANDARD_METRICS_GENERATED_ROOT, WEB_MAPPING_STORE_ROOT
from standard_map.confidence import apply_confidence_bulk_accept, build_confidence_bulk_accept_preview
from standard_map.decisions import append_mapping_decision_file, apply_mapping_decision_to_output
from standard_map.llm import load_deepseek_config
from standard_map.mapper import run_standard_mapping
from standard_map.store import LocalMappingStore


SUMMARY_PATH = WEB_MAPPING_STORE_ROOT / "stage15_2_llm_mapping_summary.json"
SMOKE_STORE_PATH = WEB_MAPPING_STORE_ROOT / "stage15_2_smoke_local_mappings.sqlite"


def main() -> int:
    WEB_MAPPING_STORE_ROOT.mkdir(parents=True, exist_ok=True)
    raw_path = _write_raw_fixture()
    output_base = STANDARD_METRICS_GENERATED_ROOT / "stage15_2_smoke"
    shutil.rmtree(output_base, ignore_errors=True)
    store_path = SMOKE_STORE_PATH
    if store_path.exists():
        store_path.unlink()
    store = LocalMappingStore(store_path)
    before_aliases = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")

    first_run = _run_mapping(raw_path, output_base, llm_mock=True)
    suggestions = _read_csv(Path(first_run.output_dir) / "llm_suggestions.csv")
    if not suggestions:
        raise RuntimeError("Mock smoke expected at least one LLM suggestion.")

    preview = build_confidence_bulk_accept_preview(first_run.output_dir, threshold=0.90, before_alias_count=before_aliases, after_alias_count=before_aliases)
    apply_summary = apply_confidence_bulk_accept(first_run.output_dir, store=store, job_id="stage15_2_smoke", doc_id="stage15_2_smoke", threshold=0.90)
    after_bulk_aliases = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    decisions = _read_csv(Path(first_run.output_dir) / "mapping_decisions.csv")
    if not decisions or any(row.get("decision") != "accept_once" for row in decisions):
        raise RuntimeError("Bulk confidence smoke expected only accept_once decisions.")
    if after_bulk_aliases != before_aliases:
        raise RuntimeError("Bulk confidence accept mutated local alias store.")

    remembered_source = next(row for row in first_run.rows if row.raw.metric_name == "总收入")
    remember = store.record_decision(
        job_id="stage15_2_smoke",
        doc_id="stage15_2_smoke",
        raw_metric_id=remembered_source.raw.raw_metric_id,
        raw_metric_name=remembered_source.raw.metric_name,
        suggested_code=remembered_source.standard_code,
        suggested_name=remembered_source.standard_name,
        decision="accept_and_remember",
        final_code=remembered_source.standard_code,
        final_name=remembered_source.standard_name,
        relation_type=remembered_source.relation_type or "same_as",
        confidence=remembered_source.confidence,
        decided_by="stage15_2_smoke",
        note="stage15_2_accept_and_remember_smoke",
    )
    apply_mapping_decision_to_output(first_run.output_dir, remember)
    append_mapping_decision_file(Path(first_run.output_dir), remember)
    store.export_aliases()
    store.export_decision_audit()

    rerun_raw_path = _write_raw_fixture(rows=[_row("总收入", "999")], run_name="RUN_MOCK_RERUN")
    rerun = _run_mapping(rerun_raw_path, output_base, llm_mock=False, disable_llm=True)
    remembered_alias_reused_pass = rerun.rows[0].mapping_method == "local_alias" and rerun.rows[0].standard_code == "ST_001"

    live_llm_executed = False
    if os.environ.get("STAGE15_2_LIVE_LLM_SMOKE", "").strip().lower() in {"1", "true", "yes", "on"}:
        live_config = load_deepseek_config()
        live_llm_executed = bool(live_config.enabled and live_config.has_valid_api_key)
        if live_llm_executed:
            live_output_base = STANDARD_METRICS_GENERATED_ROOT / "stage15_2_live_smoke"
            live_raw_path = _write_raw_fixture(rows=[_row("总收入", "100")], run_name="RUN_LIVE")
            _run_mapping(live_raw_path, live_output_base, enable_llm=True, llm_mock=False)

    output_files = sorted(
        str(path)
        for path in [
            Path(first_run.output_dir) / "llm_suggestions.csv",
            Path(first_run.output_dir) / "llm_suggestion_audit.csv",
            Path(first_run.output_dir) / "llm_mapping_summary.json",
            Path(first_run.output_dir) / "confidence_bulk_accept_preview.json",
            Path(first_run.output_dir) / "confidence_bulk_accept_apply_summary.json",
            WEB_MAPPING_STORE_ROOT / "llm_suggestions.csv",
            WEB_MAPPING_STORE_ROOT / "llm_suggestion_audit.csv",
            WEB_MAPPING_STORE_ROOT / "local_aliases_export.yml",
            WEB_MAPPING_STORE_ROOT / "mapping_decisions_audit.csv",
        ]
        if path.exists()
    )
    path_hygiene_pass = not any(
        (REPO_ROOT / name).exists()
        for name in [
            "llm_suggestions.csv",
            "llm_suggestion_audit.csv",
            "llm_mapping_summary.json",
            "confidence_bulk_accept_preview.json",
            "confidence_bulk_accept_apply_summary.json",
        ]
    )
    final_aliases = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    llm_mapping = json.loads((Path(first_run.output_dir) / "llm_mapping_summary.json").read_text(encoding="utf-8"))
    summary = {
        "pass": bool(
            suggestions
            and preview.get("eligible_total", 0) >= 1
            and apply_summary.get("applied_total", 0) >= 1
            and not apply_summary.get("mutated_local_alias_store")
            and final_aliases > before_aliases
            and remembered_alias_reused_pass
            and path_hygiene_pass
        ),
        "llm_enabled": True,
        "live_llm_executed": live_llm_executed,
        "mock_llm_used": True,
        "suggestions_total": len(suggestions),
        "cached_suggestions_total": llm_mapping.get("cached_suggestions_total", 0),
        "invalid_responses_total": llm_mapping.get("invalid_responses_total", 0),
        "confidence_threshold_default": 0.90,
        "confidence_preview_total": preview.get("eligible_total", 0),
        "confidence_bulk_accept_applied_total": apply_summary.get("applied_total", 0),
        "bulk_accept_mutated_local_alias_store": apply_summary.get("mutated_local_alias_store", True),
        "accept_and_remember_total": 1,
        "remembered_alias_reused_pass": remembered_alias_reused_pass,
        "output_files": output_files,
        "path_hygiene_pass": path_hygiene_pass,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary["pass"] else 1


def _run_mapping(raw_path: Path, output_base: Path, *, llm_mock: bool = False, disable_llm: bool = False, enable_llm: bool = False):
    args = argparse.Namespace(
        input=str(raw_path),
        output_dir=str(output_base),
        mapping_registry="config/standard_terms.yml",
        mapping_store_path=str(SMOKE_STORE_PATH),
        doc_id="stage15_2_smoke",
        company_name="",
        enable_llm_mapping=enable_llm,
        disable_llm_mapping=disable_llm,
        llm_mock=llm_mock,
        disable_llm_cache=False,
        debug=False,
    )
    return run_standard_mapping(args=args, cli_args=["--input", str(raw_path), "--output-dir", str(output_base)])


def _write_raw_fixture(*, rows: list[dict[str, object]] | None = None, run_name: str = "RUN_MOCK") -> Path:
    run_dir = RAW_METRICS_GENERATED_ROOT / "stage15_2_smoke" / run_name
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = rows or [_row("总收入", "100"), _row("营业额", "200"), _row("上半年营收", "300"), _row("奇怪项目XYZ", "400")]
    raw_path = run_dir / "raw_metrics.csv"
    with raw_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["填表日期", "当前条目日期", "公司名", "指标名", "指标数值"])
        writer.writeheader()
        writer.writerows(rows)
    with (run_dir / "raw_metrics_detailed.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source_cell_ref", "page_no", "bbox_json", "evidence_path", "source_file", "provider", "doc_id", "value_type", "header_path"])
        writer.writeheader()
        for index, _ in enumerate(rows, start=1):
            writer.writerow(
                {
                    "source_cell_ref": f"stage15_2_smoke:1:mock:0:{index}-{index}:1-1",
                    "page_no": "1",
                    "bbox_json": "",
                    "evidence_path": "",
                    "source_file": "",
                    "provider": "mock",
                    "doc_id": "stage15_2_smoke",
                    "value_type": "amount",
                    "header_path": "利润表/本期金额",
                }
            )
    return raw_path


def _row(metric_name: str, value: str) -> dict[str, object]:
    return {
        "填表日期": "2022-12-31",
        "当前条目日期": "2022-12-31",
        "公司名": "Stage15测试公司",
        "指标名": metric_name,
        "指标数值": value,
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


if __name__ == "__main__":
    raise SystemExit(main())
