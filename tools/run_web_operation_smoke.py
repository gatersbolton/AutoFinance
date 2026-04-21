from __future__ import annotations

import csv
import json
import sys
import threading
import time
from pathlib import Path

from fastapi.testclient import TestClient
from openpyxl import Workbook

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project_paths import DEFAULT_TEMPLATE_PATH, REPO_ROOT, WEB_GENERATED_ROOT
from webapp.config import WebAppSettings
from webapp.db import get_job, init_db, update_job
from webapp.main import create_app
from webapp.review import get_review_dir, load_review_items
from webapp.runner import run_worker_once


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_fake_workbook(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Sheet1"
    worksheet["A1"] = "smoke"
    workbook.create_sheet("_meta_summary")
    workbook.save(path)


def _create_smoke_input(settings: WebAppSettings) -> Path:
    fixture_root = settings.uploads_root / "_smoke_fixture" / "CASE_SMOKE"
    input_dir = fixture_root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.pdf").write_bytes(b"%PDF-1.4\n%smoke\n")

    ocr_dir = fixture_root / "ocr_outputs"
    doc_dir = ocr_dir / "aliyun_table" / "demo_doc"
    raw_dir = doc_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "page_0001.json").write_text("{}", encoding="utf-8")
    (doc_dir / "result.json").write_text(
        json.dumps(
            {
                "provider": "aliyun_table",
                "pages": [{"page_number": 1, "text": "smoke", "raw_file": "raw/page_0001.json"}],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return ocr_dir


def _prepare_review_outputs(settings: WebAppSettings, job_id: str) -> None:
    job = get_job(settings, job_id)
    if job is None:
        raise RuntimeError(f"missing job: {job_id}")
    update_job(settings, job_id, status="needs_review", current_stage="completed", progress_summary="smoke review ready")
    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_fake_workbook(output_dir / "会计报表_填充结果.xlsx")
    _write_fake_workbook(output_dir / "review_workbook.xlsx")
    _write_json(
        output_dir / "run_summary.json",
        {
            "run_id": "RUN_SMOKE_001",
            "review_total": 2,
            "validation_fail_total": 1,
            "mapped_facts_ratio": 0.4,
            "exportable_facts_total": 3,
            "integrity_fail_total": 0,
        },
    )
    _write_json(output_dir / "pipeline_completion_summary.json", {"status": "success", "last_successful_stage": "export"})
    _write_json(output_dir / "artifact_integrity.json", {"integrity_fail_total": 0, "integrity_review_total": 0})
    _write_json(output_dir / "review_summary.json", {"review_total": 2})
    _write_json(output_dir / "validation_summary.json", {"validation_fail_total": 1})
    _write_json(Path(job.result_dir) / "job_summary.json", {"job_id": job_id})
    _write_json(Path(job.result_dir) / "job_quality_summary.json", {"final_job_status": "needs_review"})
    _write_json(Path(job.result_dir) / "job_log_bundle.json", {"log_files": []})

    review_pack_dir = output_dir / "review_pack"
    review_pack_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = review_pack_dir / "REV_smoke_1_cell.png"
    evidence_path.write_bytes(
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
        b"\x00\x00\x00\x0cIDAT\x08\xd7c\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    _write_csv(
        output_dir / "review_queue.csv",
        [
            {
                "review_id": "REV_smoke_1",
                "priority_score": "6.0",
                "reason_codes": json.dumps(["mapping:unmapped"], ensure_ascii=False),
                "doc_id": "CASE_SMOKE",
                "page_no": "1",
                "statement_type": "balance_sheet",
                "row_label_raw": "货币资金",
                "row_label_std": "货币资金",
                "period_key": "2022-12-31__期末数",
                "value_raw": "100",
                "value_num": "100.0",
                "provider": "aliyun_table",
                "source_file": str(output_dir / "raw" / "page_0001.json"),
                "bbox": "",
                "related_fact_ids": json.dumps(["F_SMOKE_001"], ensure_ascii=False),
                "related_conflict_ids": json.dumps([], ensure_ascii=False),
                "related_validation_ids": json.dumps(["VAL_SMOKE_001"], ensure_ascii=False),
                "mapping_candidates": "ZT_001 货币资金 (manual,0.99)",
                "evidence_cell_path": str(evidence_path),
                "evidence_row_path": "",
                "evidence_table_path": "",
                "meta_json": json.dumps({"source_cell_ref": "CASE_SMOKE:1:aliyun_table:0:1-1:1-1"}, ensure_ascii=False),
            }
        ],
        [
            "review_id",
            "priority_score",
            "reason_codes",
            "doc_id",
            "page_no",
            "statement_type",
            "row_label_raw",
            "row_label_std",
            "period_key",
            "value_raw",
            "value_num",
            "provider",
            "source_file",
            "bbox",
            "related_fact_ids",
            "related_conflict_ids",
            "related_validation_ids",
            "mapping_candidates",
            "evidence_cell_path",
            "evidence_row_path",
            "evidence_table_path",
            "meta_json",
        ],
    )
    _write_csv(
        output_dir / "issues.csv",
        [
            {
                "doc_id": "CASE_SMOKE",
                "page_no": "2",
                "provider": "aliyun_table",
                "source_file": str(output_dir / "raw" / "page_0002.json"),
                "table_id": "0",
                "logical_subtable_id": "0_sub1",
                "source_cell_ref": "CASE_SMOKE:2:aliyun_table:0:2-2:2-2",
                "issue_type": "suspicious_value",
                "severity": "warning",
                "message": "expected_numeric_but_unparseable",
                "text_raw": "-",
                "text_clean": "-",
                "status": "open",
                "meta_json": "{}",
            }
        ],
        [
            "doc_id",
            "page_no",
            "provider",
            "source_file",
            "table_id",
            "logical_subtable_id",
            "source_cell_ref",
            "issue_type",
            "severity",
            "message",
            "text_raw",
            "text_clean",
            "status",
            "meta_json",
        ],
    )
    _write_csv(
        output_dir / "validation_results.csv",
        [
            {
                "validation_id": "VAL_SMOKE_001",
                "doc_id": "CASE_SMOKE",
                "statement_type": "balance_sheet",
                "period_key": "2022-12-31__期末数",
                "rule_name": "subtotal_check",
                "rule_type": "equation",
                "lhs_value": "1",
                "rhs_value": "2",
                "diff_value": "1",
                "tolerance": "0.01",
                "status": "fail",
                "evidence_fact_refs": json.dumps(["CASE_SMOKE:1:aliyun_table:0:1-1:1-1"], ensure_ascii=False),
                "message": "subtotal mismatch",
                "meta_json": "{}",
            }
        ],
        [
            "validation_id",
            "doc_id",
            "statement_type",
            "period_key",
            "rule_name",
            "rule_type",
            "lhs_value",
            "rhs_value",
            "diff_value",
            "tolerance",
            "status",
            "evidence_fact_refs",
            "message",
            "meta_json",
        ],
    )


def _poll_operation(client: TestClient, job_id: str, operation_id: str, timeout_seconds: int = 180) -> dict[str, object]:
    started = time.time()
    while time.time() - started < timeout_seconds:
        payload = client.get(f"/jobs/{job_id}/operations/{operation_id}").json()["operation"]
        if str(payload.get("status", "")) in {"succeeded", "failed", "cancelled"}:
            return payload
        time.sleep(0.5)
    raise TimeoutError(f"operation did not finish in {timeout_seconds} seconds")


def main() -> int:
    settings = WebAppSettings(
        env_mode="dev",
        runtime_root=WEB_GENERATED_ROOT,
        uploads_root=WEB_GENERATED_ROOT / "uploads",
        jobs_root=WEB_GENERATED_ROOT / "jobs",
        results_root=WEB_GENERATED_ROOT / "results",
        logs_root=WEB_GENERATED_ROOT / "logs",
        db_path=WEB_GENERATED_ROOT / "webapp.sqlite3",
        corpus_root=REPO_ROOT / "data" / "corpus",
        template_path=DEFAULT_TEMPLATE_PATH,
        secret_path=REPO_ROOT / "data" / "secrets" / "secret",
        enable_local_worker=False,
        auto_run_upload_ocr=False,
        auth_required=False,
        queue_backend="local",
        operation_timeout_seconds=120,
    )
    settings.ensure_directories()
    init_db(settings)
    smoke_input_dir = _create_smoke_input(settings)

    summary_path = WEB_GENERATED_ROOT / "web_operation_queue_summary.json"
    with TestClient(create_app(settings)) as client:
        create_response = client.post(
            "/jobs",
            data={
                "mode": "existing_ocr_outputs",
                "display_name": "stage10_3_smoke",
                "existing_ocr_path": str(smoke_input_dir),
            },
            follow_redirects=False,
        )
        if create_response.status_code != 303:
            raise RuntimeError(f"failed to create smoke job: {create_response.status_code}")
        job_id = create_response.headers["location"].rstrip("/").split("/")[-1]
        _prepare_review_outputs(settings, job_id)

        dashboard_response = client.get(f"/jobs/{job_id}/review")
        if dashboard_response.status_code != 200:
            raise RuntimeError("review dashboard failed to load")
        job = get_job(settings, job_id)
        if job is None:
            raise RuntimeError("smoke job missing after create")
        items, _ = load_review_items(settings, job)
        issue_item = next(item for item in items if item.source_type == "issue")

        client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_smoke_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "smoke ignore",
                "reviewer_name": "smoke",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        review_items_page = client.get(f"/jobs/{job_id}/review/items")
        if "review_item_id" not in review_items_page.text:
            raise RuntimeError("review items page did not render expected content")
        client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": issue_item.review_item_id,
                "action_type": "defer",
                "action_value": "",
                "reviewer_note": "smoke defer",
                "reviewer_name": "smoke",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)

        started = time.perf_counter()
        enqueue_response = client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
        returned_quickly = (time.perf_counter() - started) < 1.5 and enqueue_response.status_code == 303
        latest_operation = client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        operation_id = str(latest_operation["operation_id"])

        duplicate_response = client.post(
            f"/jobs/{job_id}/review/apply-and-rerun",
            follow_redirects=False,
            headers={"accept": "application/json"},
        )
        duplicate_operation_blocked = duplicate_response.status_code == 409

        worker_thread = threading.Thread(target=run_worker_once, args=(settings,), daemon=True)
        worker_thread.start()
        final_operation = _poll_operation(client, job_id, operation_id, timeout_seconds=180)
        worker_thread.join(timeout=5)

        operation_dir = settings.jobs_root / job_id / "review" / "operations" / operation_id
        operation_summary_exists = (operation_dir / "review_operation_summary.json").exists()
        operation_timeline_exists = (operation_dir / "operation_stage_timeline.json").exists()

        job_detail_response = client.get(f"/jobs/{job_id}")
        job_detail_shows_result = operation_id in job_detail_response.text and "最近一次后台操作" in job_detail_response.text

        latest_rerun_summary_path = settings.results_root / job_id / "reruns" / "rerun_001" / "review_rerun_summary.json"
        rerun_summary = {}
        if latest_rerun_summary_path.exists():
            rerun_summary = json.loads(latest_rerun_summary_path.read_text(encoding="utf-8"))
        final_job_status = str(rerun_summary.get("final_job_status", final_operation.get("status", "")) or "")

        result_paths = [str(path) for path in final_operation.get("result_paths", [])]
        path_hygiene_pass = True
        for raw_path in [*final_operation.get("log_paths", []), *result_paths]:
            resolved = Path(raw_path)
            if not resolved.is_absolute():
                resolved = (REPO_ROOT / resolved).resolve()
            if not str(resolved).startswith(str(WEB_GENERATED_ROOT.resolve())):
                path_hygiene_pass = False
                break

        summary = {
            "job_id": job_id,
            "operation_id": operation_id,
            "operation_type": final_operation.get("operation_type", ""),
            "queue_backend": settings.queue_backend,
            "status": final_operation.get("status", ""),
            "duration_seconds": final_operation.get("duration_seconds", 0),
            "returned_quickly": returned_quickly,
            "polling_pass": str(final_operation.get("status", "")) in {"succeeded", "failed", "cancelled"},
            "cancel_supported": bool(final_operation.get("cancel_supported", False)),
            "retry_supported": bool(final_operation.get("retry_supported", False)),
            "duplicate_operation_blocked": duplicate_operation_blocked,
            "final_job_status": final_job_status,
            "result_paths": result_paths,
            "path_hygiene_pass": path_hygiene_pass,
            "operation_summary_exists": operation_summary_exists,
            "operation_timeline_exists": operation_timeline_exists,
            "job_detail_shows_result": job_detail_shows_result,
        }
        summary["pass"] = all(
            [
                summary["returned_quickly"],
                summary["polling_pass"],
                summary["duplicate_operation_blocked"],
                summary["path_hygiene_pass"],
                summary["operation_summary_exists"],
                summary["operation_timeline_exists"],
                summary["job_detail_shows_result"],
            ]
        )
        _write_json(summary_path, summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
