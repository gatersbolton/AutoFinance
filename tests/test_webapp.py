from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient
from openpyxl import Workbook

from project_paths import REPO_ROOT
from webapp.config import WebAppSettings
from webapp.db import get_job, init_db, list_review_actions, update_job
from webapp.jobs import discover_output_files
from webapp.main import create_app
from webapp.models import JobRecord
from webapp.review import export_review_actions, get_review_dir, load_review_items, filter_review_items
from webapp.runner import run_worker_once


class WebAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.tempdir.name)
        self.corpus_root = self.temp_path / "corpus"
        self.runtime_root = self.temp_path / "generated" / "web"
        self.template_path = REPO_ROOT / "data" / "templates" / "会计报表.xlsx"
        self.secret_path = self.temp_path / "secret"
        self.settings = self.make_settings()
        self.settings.ensure_directories()
        init_db(self.settings)
        self.sample_input_dir = self._create_minimal_ocr_input()
        self.client_cm = TestClient(create_app(self.settings))
        self.client = self.client_cm.__enter__()

    def tearDown(self) -> None:
        self.client_cm.__exit__(None, None, None)
        self.tempdir.cleanup()

    def make_settings(self, **overrides) -> WebAppSettings:
        defaults = dict(
            env_mode="dev",
            runtime_root=self.runtime_root,
            uploads_root=self.runtime_root / "uploads",
            jobs_root=self.runtime_root / "jobs",
            results_root=self.runtime_root / "results",
            logs_root=self.runtime_root / "logs",
            db_path=self.runtime_root / "webapp.sqlite3",
            corpus_root=self.corpus_root,
            template_path=self.template_path,
            secret_path=self.secret_path,
            enable_local_worker=False,
            auto_run_upload_ocr=False,
            worker_poll_seconds=1,
            job_timeout_seconds=120,
            auth_required=False,
            admin_password="",
        )
        defaults.update(overrides)
        return WebAppSettings(**defaults)

    def _create_minimal_ocr_input(self) -> Path:
        target_doc_root = self.corpus_root / "CASE1"
        input_pdf_dir = target_doc_root / "input"
        input_pdf_dir.mkdir(parents=True, exist_ok=True)
        (input_pdf_dir / "sample.pdf").write_bytes(b"%PDF-1.4\n%mock\n")

        ocr_dir = target_doc_root / "ocr_outputs"
        doc_dir = ocr_dir / "aliyun_table" / "demo_doc"
        raw_dir = doc_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "page_0001.json").write_text("{}", encoding="utf-8")
        (doc_dir / "result.json").write_text(
            json.dumps(
                {
                    "provider": "aliyun_table",
                    "pages": [{"page_number": 1, "text": "mock", "raw_file": "raw/page_0001.json"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return ocr_dir

    def _create_job(self, display_name: str = "mock job") -> str:
        response = self.client.post(
            "/jobs",
            data={
                "mode": "existing_ocr_outputs",
                "display_name": display_name,
                "existing_ocr_path": str(self.sample_input_dir),
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        return response.headers["location"].rstrip("/").split("/")[-1]

    def _write_fake_workbook(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "Sheet1"
        worksheet["A1"] = "mock"
        workbook.create_sheet("_meta_summary")
        workbook.save(path)

    def _write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_csv(self, path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _prepare_review_job(self, *, include_optional: bool = True, outside_evidence: bool = False) -> str:
        job_id = self._create_job("review job")
        job = get_job(self.settings, job_id)
        self.assertIsNotNone(job)
        update_job(self.settings, job_id, status="needs_review", current_stage="completed", progress_summary="ready for review")
        output_dir = Path(job.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        self._write_fake_workbook(output_dir / "会计报表_填充结果.xlsx")
        self._write_json(output_dir / "run_summary.json", {"review_total": 2, "validation_fail_total": 1})
        self._write_json(output_dir / "pipeline_completion_summary.json", {"status": "success", "last_successful_stage": "export"})
        self._write_json(output_dir / "artifact_integrity.json", {"integrity_fail_total": 0, "integrity_review_total": 0})
        self._write_json(output_dir / "review_summary.json", {"review_total": 2})
        self._write_json(output_dir / "validation_summary.json", {"validation_fail_total": 1})
        self._write_json(Path(job.result_dir) / "job_summary.json", {"job_id": job_id})
        self._write_json(Path(job.result_dir) / "job_quality_summary.json", {"final_job_status": "needs_review"})
        self._write_json(Path(job.result_dir) / "job_log_bundle.json", {"log_files": []})

        review_pack_dir = output_dir / "review_pack"
        review_pack_dir.mkdir(parents=True, exist_ok=True)
        inside_evidence = review_pack_dir / "REV_case_1_cell.png"
        inside_evidence.write_bytes(b"mockpng")
        outside_path = self.temp_path / "outside_evidence.png"
        outside_path.write_bytes(b"outside")
        evidence_path = outside_path if outside_evidence else inside_evidence

        self._write_csv(
            output_dir / "review_queue.csv",
            [
                {
                    "review_id": "REV_case_1",
                    "priority_score": "6.0",
                    "reason_codes": json.dumps(["mapping:unmapped"], ensure_ascii=False),
                    "doc_id": "CASE1",
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
                    "related_fact_ids": json.dumps(["F_001"], ensure_ascii=False),
                    "related_conflict_ids": json.dumps(["CON_001"], ensure_ascii=False),
                    "related_validation_ids": json.dumps(["VAL_001"], ensure_ascii=False),
                    "mapping_candidates": "ZT_001 货币资金 (manual,0.99)",
                    "evidence_cell_path": str(evidence_path),
                    "evidence_row_path": "",
                    "evidence_table_path": "",
                    "meta_json": json.dumps({"source_cell_ref": "CASE1:1:aliyun_table:0:1-1:1-1"}, ensure_ascii=False),
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
        self._write_fake_workbook(output_dir / "review_workbook.xlsx")

        if include_optional:
            self._write_csv(
                output_dir / "issues.csv",
                [
                    {
                        "doc_id": "CASE1",
                        "page_no": "2",
                        "provider": "aliyun_table",
                        "source_file": str(output_dir / "raw" / "page_0002.json"),
                        "table_id": "0",
                        "logical_subtable_id": "0_sub1",
                        "source_cell_ref": "CASE1:2:aliyun_table:0:2-2:2-2",
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
            self._write_csv(
                output_dir / "validation_results.csv",
                [
                    {
                        "validation_id": "VAL_001",
                        "doc_id": "CASE1",
                        "statement_type": "balance_sheet",
                        "period_key": "2022-12-31__期末数",
                        "rule_name": "subtotal_check",
                        "rule_type": "equation",
                        "lhs_value": "1",
                        "rhs_value": "2",
                        "diff_value": "1",
                        "tolerance": "0.01",
                        "status": "fail",
                        "evidence_fact_refs": json.dumps(["CASE1:3:aliyun_table:0:3-3:3-3"], ensure_ascii=False),
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
            self._write_csv(
                output_dir / "mapping_candidates.csv",
                [
                    {
                        "doc_id": "CASE1",
                        "page_no": "1",
                        "provider": "aliyun_table",
                        "statement_type": "balance_sheet",
                        "row_label_raw": "货币资金",
                        "row_label_std": "货币资金",
                        "normalized_label": "货币资金",
                        "candidate_code": "ZT_001",
                        "candidate_name": "货币资金",
                        "candidate_rank": "1",
                        "candidate_score": "0.99",
                        "candidate_method": "manual",
                        "relation_type": "",
                        "review_required": "True",
                        "source_cell_ref": "CASE1:1:aliyun_table:0:1-1:1-1",
                        "meta_json": "{}",
                    }
                ],
                [
                    "doc_id",
                    "page_no",
                    "provider",
                    "statement_type",
                    "row_label_raw",
                    "row_label_std",
                    "normalized_label",
                    "candidate_code",
                    "candidate_name",
                    "candidate_rank",
                    "candidate_score",
                    "candidate_method",
                    "relation_type",
                    "review_required",
                    "source_cell_ref",
                    "meta_json",
                ],
            )
        return job_id

    def _fake_subprocess(self, profile: str = "clean_success"):
        def _runner(*, command, stdout_path: Path, stderr_path: Path, timeout_seconds: int):
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_path.write_text(f"fake run for {profile}\n", encoding="utf-8")
            stderr_path.write_text("", encoding="utf-8")

            if "-m" in command and "standardize.cli" in command:
                output_dir = Path(command[command.index("--output-dir") + 1])
                self._write_fake_standardize_outputs(output_dir, profile)
                return mock.Mock(returncode=0)

            return mock.Mock(returncode=0)

        return _runner

    def _fake_failed_subprocess(self, raw_error: str):
        def _runner(*, command, stdout_path: Path, stderr_path: Path, timeout_seconds: int):
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_path.write_text("", encoding="utf-8")
            stderr_path.write_text(raw_error, encoding="utf-8")
            return mock.Mock(returncode=1)

        return _runner

    def _write_fake_standardize_outputs(self, output_dir: Path, profile: str) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        self._write_fake_workbook(output_dir / "会计报表_填充结果.xlsx")
        self._write_json(
            output_dir / "run_summary.json",
            {
                "run_id": "RUN_TEST_001",
                "integrity_fail_total": 0,
                "review_total": 0,
                "validation_fail_total": 0,
            },
        )
        self._write_json(
            output_dir / "artifact_integrity.json",
            {
                "run_id": "RUN_TEST_001",
                "integrity_fail_total": 0,
                "integrity_review_total": 0,
                "checks_total": 3,
            },
        )
        self._write_json(
            output_dir / "validation_summary.json",
            {"run_id": "RUN_TEST_001", "validation_fail_total": 0},
        )
        self._write_json(
            output_dir / "review_summary.json",
            {"run_id": "RUN_TEST_001", "review_total": 0},
        )
        self._write_json(
            output_dir / "pipeline_completion_summary.json",
            {"run_id": "RUN_TEST_001", "status": "success", "current_stage": "", "last_successful_stage": "export"},
        )
        self._write_json(
            output_dir / "full_run_contract_summary.json",
            {"run_id": "RUN_TEST_001", "contract_fail_total": 0},
        )
        (output_dir / "issues.csv").write_text("issue\n", encoding="utf-8")
        (output_dir / "validation_results.csv").write_text("result\n", encoding="utf-8")

        if profile == "warning":
            self._write_json(
                output_dir / "artifact_integrity.json",
                {
                    "run_id": "RUN_TEST_001",
                    "integrity_fail_total": 0,
                    "integrity_review_total": 2,
                    "checks_total": 3,
                },
            )
        elif profile == "needs_review":
            self._write_json(
                output_dir / "review_summary.json",
                {"run_id": "RUN_TEST_001", "review_total": 4},
            )
            self._write_json(
                output_dir / "validation_summary.json",
                {"run_id": "RUN_TEST_001", "validation_fail_total": 2},
            )
            (output_dir / "review_queue.csv").write_text("review\n", encoding="utf-8")
            self._write_fake_workbook(output_dir / "review_workbook.xlsx")
        elif profile == "missing_workbook":
            (output_dir / "会计报表_填充结果.xlsx").unlink(missing_ok=True)

    def test_app_starts_and_home_page_returns_200(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("AutoFinance Web MVP", response.text)

    def test_new_job_page_returns_200(self):
        response = self.client.get("/jobs/new")
        self.assertEqual(response.status_code, 200)
        self.assertIn("新建任务", response.text)

    def test_create_standardize_only_job_from_existing_ocr_path(self):
        job_id = self._create_job("CASE1 smoke")
        job = get_job(self.settings, job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job.status, "queued")

    def test_invalid_upload_extension_is_rejected(self):
        response = self.client.post(
            "/jobs",
            data={"mode": "upload_pdf", "display_name": "bad upload"},
            files={"uploaded_files": ("notes.txt", b"not a pdf", "text/plain")},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("不支持的上传文件类型", response.text)

    def test_succeeded_with_warnings_job_quality_classification(self):
        job_id = self._create_job("warning job")
        with mock.patch("webapp.runner._run_subprocess", side_effect=self._fake_subprocess(profile="warning")):
            run_worker_once(self.settings)
        job = get_job(self.settings, job_id)
        self.assertEqual(job.status, "succeeded_with_warnings")
        quality_summary_path = Path(job.result_dir) / "job_quality_summary.json"
        self.assertTrue(quality_summary_path.exists())
        quality_summary = json.loads(quality_summary_path.read_text(encoding="utf-8"))
        self.assertEqual(quality_summary["final_job_status"], "succeeded_with_warnings")
        self.assertEqual(quality_summary["artifact_integrity_review_total"], 2)
        self.assertEqual(quality_summary["command_exit_code"], 0)

    def test_failed_command_classification_and_error_translation(self):
        job_id = self._create_job("failed job")
        raw_error = "Template workbook does not exist: mock-template.xlsx"
        with mock.patch("webapp.runner._run_subprocess", side_effect=self._fake_failed_subprocess(raw_error)):
            run_worker_once(self.settings)
        job = get_job(self.settings, job_id)
        self.assertEqual(job.status, "failed")
        self.assertIn("模板文件不存在", job.user_friendly_error)
        quality_summary = json.loads((Path(job.result_dir) / "job_quality_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(quality_summary["final_job_status"], "failed")
        self.assertIn("模板文件不存在", quality_summary["user_friendly_error"])

    def test_job_detail_page_contains_chinese_user_facing_summary(self):
        job_id = self._create_job("needs review job")
        with mock.patch("webapp.runner._run_subprocess", side_effect=self._fake_subprocess(profile="needs_review")):
            run_worker_once(self.settings)
        response = self.client.get(f"/jobs/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("处理状态", response.text)
        self.assertIn("是否成功生成会计报表", response.text)
        self.assertIn("下一步建议", response.text)
        self.assertIn("已生成结果，但建议复核", response.text)

    def test_output_discovery_handles_missing_optional_files(self):
        job = JobRecord(
            job_id="job_manual",
            display_name="manual",
            mode="existing_ocr_outputs",
            provider_mode="cloud_first",
            input_path=str(self.sample_input_dir),
            source_image_dir="",
            upload_dir="",
            ocr_output_dir="",
            template_path=str(self.template_path),
            output_dir=str(self.runtime_root / "jobs" / "job_manual" / "standardize"),
            result_dir=str(self.runtime_root / "results" / "job_manual"),
            log_dir=str(self.runtime_root / "logs" / "job_manual"),
            provider_priority="aliyun,tencent",
            status="succeeded",
            current_stage="completed",
            progress_summary="done",
            created_at="",
            updated_at="",
            started_at="",
            finished_at="",
            error_message="",
            raw_error_message="",
            user_friendly_error="",
            recommended_action="",
            run_id="RUN_TEST_001",
            command_executed="",
            exit_code=0,
            timeout_seconds=120,
        )
        output_dir = Path(job.output_dir)
        result_dir = Path(job.result_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        result_dir.mkdir(parents=True, exist_ok=True)
        self._write_fake_workbook(output_dir / "会计报表_填充结果.xlsx")
        (output_dir / "run_summary.json").write_text("{}", encoding="utf-8")
        artifacts = {item.slug: item for item in discover_output_files(job)}
        self.assertTrue(artifacts["filled_workbook"].exists)
        self.assertFalse(artifacts["review_workbook"].exists)
        self.assertFalse(artifacts["quality_summary"].exists)

    def test_no_generated_web_files_go_into_repo_root(self):
        job_id = self._create_job("path smoke")
        with mock.patch("webapp.runner._run_subprocess", side_effect=self._fake_subprocess(profile="warning")):
            run_worker_once(self.settings)
        job = get_job(self.settings, job_id)
        self.assertTrue(str(Path(job.output_dir)).startswith(str(self.settings.jobs_root)))
        self.assertTrue(str(Path(job.result_dir)).startswith(str(self.settings.results_root)))
        self.assertTrue(str(self.settings.db_path).startswith(str(self.settings.runtime_root)))
        self.assertFalse((REPO_ROOT / job.job_id).exists())

    def test_job_detail_status_endpoint_returns_payload(self):
        job_id = self._create_job("status smoke")
        with mock.patch("webapp.runner._run_subprocess", side_effect=self._fake_subprocess(profile="warning")):
            run_worker_once(self.settings)
        status_response = self.client.get(f"/jobs/{job_id}/status")
        self.assertEqual(status_response.status_code, 200)
        payload = status_response.json()
        self.assertEqual(payload["job"]["job_id"], job_id)
        self.assertIn("quality_summary", payload)
        self.assertIn("output_files", payload)

    def test_review_dashboard_route_returns_200_for_job_with_review_files(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.get(f"/jobs/{job_id}/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn("复核看板", response.text)
        self.assertIn("待复核总数", response.text)
        self.assertIn("校验失败", response.text)
        self.assertIn("OCR 可疑", response.text)

    def test_review_dashboard_handles_missing_optional_files(self):
        job_id = self._prepare_review_job(include_optional=False)
        response = self.client.get(f"/jobs/{job_id}/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn("不可用", response.text)

    def test_review_item_loader_parses_review_queue_csv(self):
        job_id = self._prepare_review_job(include_optional=False)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        review_item = next(item for item in items if item.source_type == "review_queue")
        self.assertEqual(review_item.review_item_id, "REV_case_1")
        self.assertEqual(review_item.reason_code, "mapping:unmapped")
        self.assertEqual(review_item.mapping_code, "ZT_001")

    def test_review_item_loader_parses_issues_and_validation_results(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        source_types = {item.source_type for item in items}
        self.assertIn("issue", source_types)
        self.assertIn("validation", source_types)

    def test_review_item_filtering_supports_source_type_and_status(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "defer",
                "action_value": "",
                "reviewer_note": "later",
                "reviewer_name": "tester",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        deferred = filter_review_items(items, status="deferred")
        mapping_items = filter_review_items(items, source_type="review_queue")
        self.assertEqual(len(deferred), 1)
        self.assertEqual(deferred[0].review_item_id, "REV_case_1")
        self.assertEqual(len(mapping_items), 1)

    def test_submitting_review_action_stores_it(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "false alert",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        actions = list_review_actions(self.settings, job_id)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action_type, "ignore")
        self.assertEqual(actions[0].reviewer_name, "auditor")

    def test_exporting_review_actions_creates_csv_xlsx_json_under_job_review_dir(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "defer",
                "action_value": "",
                "reviewer_note": "queue later",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        job = get_job(self.settings, job_id)
        result = export_review_actions(self.settings, job)
        review_dir = get_review_dir(job)
        self.assertTrue((review_dir / "review_actions_filled.csv").exists())
        self.assertTrue((review_dir / "review_actions_filled.xlsx").exists())
        self.assertTrue((review_dir / "review_action_export_summary.json").exists())
        self.assertTrue(str(result["csv_path"]).startswith(str(self.settings.jobs_root)))

    def test_evidence_path_outside_allowed_directories_is_rejected(self):
        job_id = self._prepare_review_job(include_optional=False, outside_evidence=True)
        response = self.client.get(f"/jobs/{job_id}/review/evidence/REV_case_1/cell")
        self.assertEqual(response.status_code, 404)

    def test_review_exports_do_not_go_into_repo_root(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "request_reocr",
                "action_value": "",
                "reviewer_note": "need targeted reocr",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        job = get_job(self.settings, job_id)
        review_dir = get_review_dir(job)
        export_review_actions(self.settings, job)
        self.assertTrue(str(review_dir).startswith(str(self.settings.jobs_root)))
        self.assertFalse((REPO_ROOT / "review_actions_filled.csv").exists())
        self.assertFalse((REPO_ROOT / "review_actions_filled.xlsx").exists())

    def test_auth_disabled_in_dev_works(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)

    def test_auth_required_in_prod_without_password_fails_startup(self):
        settings = self.make_settings(env_mode="prod", auth_required=True, admin_password="")
        with self.assertRaisesRegex(RuntimeError, "WEBAPP_ADMIN_PASSWORD"):
            with TestClient(create_app(settings)):
                pass

    def test_auth_enabled_requires_basic_auth(self):
        settings = self.make_settings(env_mode="prod", auth_required=True, admin_password="secret-pass")
        with TestClient(create_app(settings)) as client:
            unauthorized = client.get("/")
            self.assertEqual(unauthorized.status_code, 401)
            authorized = client.get("/", auth=("admin", "secret-pass"))
            self.assertEqual(authorized.status_code, 200)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
