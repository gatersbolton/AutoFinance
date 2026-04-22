from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient
from openpyxl import Workbook

from project_paths import REPO_ROOT
from scripts.deployment_check import main as deployment_check_main
from webapp.config import WebAppSettings
from webapp.deployment import run_deployment_preflight
from webapp.db import (
    get_job,
    get_review_operation,
    init_db,
    list_review_actions,
    update_job,
    update_review_operation,
)
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

    def _write_secret_file(self, *, aliyun: bool = True, tencent: bool = False, secret_value: str = "demo-secret") -> None:
        lines: list[str] = []
        if aliyun:
            lines.extend(
                [
                    "aliyun:",
                    "  AccessKeyId: demo-id",
                    f"  AccessKeySecret: {secret_value}",
                ]
            )
        if tencent:
            lines.extend(
                [
                    "",
                    "tencent:",
                    "  SecretId: demo-tencent-id",
                    f"  SecretKey: {secret_value}",
                ]
            )
        self.secret_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

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

    def _run_next_worker_item(self) -> None:
        run_worker_once(self.settings)

    def _prepare_review_job(self, *, include_optional: bool = True, outside_evidence: bool = False) -> str:
        job_id = self._create_job("review job")
        job = get_job(self.settings, job_id)
        self.assertIsNotNone(job)
        update_job(self.settings, job_id, status="needs_review", current_stage="completed", progress_summary="ready for review")
        output_dir = Path(job.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        self._write_fake_workbook(output_dir / "会计报表_填充结果.xlsx")
        self._write_json(
            output_dir / "run_summary.json",
            {
                "run_id": "RUN_REVIEW_JOB_001",
                "review_total": 2,
                "validation_fail_total": 1,
                "mapped_facts_ratio": 0.4,
                "exportable_facts_total": 3,
                "integrity_fail_total": 0,
            },
        )
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
        inside_evidence.write_bytes(
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
            b"\x00\x00\x00\x0cIDAT\x08\xd7c\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
        )
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

    def _fake_review_rerun(self, profile: str = "improved"):
        def _runner(
            *,
            settings,
            job,
            output_dir: Path,
            config_dir: Path,
            stdout_path: Path,
            stderr_path: Path,
            cancel_requested=None,
            timeout_seconds=None,
        ):
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_path.write_text(f"fake rerun for {profile}\n", encoding="utf-8")
            stderr_path.write_text("", encoding="utf-8")
            output_dir.mkdir(parents=True, exist_ok=True)
            self._write_fake_workbook(output_dir / "会计报表_填充结果.xlsx")
            review_total = 1 if profile == "improved" else 2
            validation_fail_total = 0 if profile == "improved" else 1
            mapped_ratio = 0.7 if profile == "improved" else 0.4
            exportable_total = 5 if profile == "improved" else 3
            self._write_json(
                output_dir / "run_summary.json",
                {
                    "run_id": "RUN_RERUN_001",
                    "review_total": review_total,
                    "validation_fail_total": validation_fail_total,
                    "mapped_facts_ratio": mapped_ratio,
                    "exportable_facts_total": exportable_total,
                    "integrity_fail_total": 0,
                },
            )
            self._write_json(output_dir / "artifact_integrity.json", {"run_id": "RUN_RERUN_001", "integrity_fail_total": 0, "integrity_review_total": 0})
            self._write_json(output_dir / "review_summary.json", {"run_id": "RUN_RERUN_001", "review_total": review_total})
            self._write_json(output_dir / "validation_summary.json", {"run_id": "RUN_RERUN_001", "validation_fail_total": validation_fail_total})
            self._write_json(output_dir / "pipeline_completion_summary.json", {"run_id": "RUN_RERUN_001", "status": "success", "last_successful_stage": "export"})
            return {
                "exit_code": 0,
                "logical_command": "python -m standardize.cli --output-dir ...",
                "runner_command": "python -c ...",
            }

        return _runner

    def _fake_failed_review_rerun(self):
        def _runner(
            *,
            settings,
            job,
            output_dir: Path,
            config_dir: Path,
            stdout_path: Path,
            stderr_path: Path,
            cancel_requested=None,
            timeout_seconds=None,
        ):
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_path.write_text("fake rerun failed\n", encoding="utf-8")
            stderr_path.write_text("rerun failed\n", encoding="utf-8")
            return {
                "exit_code": -1,
                "logical_command": "python -m standardize.cli --output-dir ...",
                "runner_command": "python -c ...",
                "cancelled": False,
            }

        return _runner

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

    def test_upload_pdf_missing_ocr_credentials_returns_chinese_error(self):
        response = self.client.post(
            "/jobs",
            data={
                "mode": "upload_pdf",
                "display_name": "missing secret",
                "upload_provider_mode": "aliyun_table",
            },
            files={"uploaded_files": ("demo.pdf", b"%PDF-1.4\n%mock\n", "application/pdf")},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("当前未配置阿里云 OCR 密钥", response.text)

    def test_create_upload_pdf_job_with_selected_provider_and_mock_runtime(self):
        settings = self.make_settings(auto_run_upload_ocr=True)
        settings.ensure_directories()
        init_db(settings)
        with mock.patch.dict(
            os.environ,
            {
                "WEBAPP_UPLOAD_OCR_MOCK_MODE": "copy_fixture",
                "WEBAPP_UPLOAD_OCR_MOCK_SOURCE_DIR": str(self.sample_input_dir),
            },
            clear=False,
        ):
            with TestClient(create_app(settings)) as client:
                response = client.post(
                    "/jobs",
                    data={
                        "mode": "upload_pdf",
                        "display_name": "upload demo",
                        "upload_provider_mode": "tencent_table_v3",
                    },
                    files={"uploaded_files": ("demo.pdf", b"%PDF-1.4\n%mock\n", "application/pdf")},
                    follow_redirects=False,
                )
        self.assertEqual(response.status_code, 303)
        job_id = response.headers["location"].rstrip("/").split("/")[-1]
        job = get_job(settings, job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job.provider_mode, "tencent_table_v3")
        self.assertEqual(job.status, "queued")
        self.assertTrue((settings.uploads_root / job_id / "demo.pdf").exists())

    def test_upload_pdf_worker_captures_ocr_and_standardize_logs_separately(self):
        settings = self.make_settings(auto_run_upload_ocr=True)
        settings.ensure_directories()
        init_db(settings)
        with mock.patch.dict(
            os.environ,
            {
                "WEBAPP_UPLOAD_OCR_MOCK_MODE": "copy_fixture",
                "WEBAPP_UPLOAD_OCR_MOCK_SOURCE_DIR": str(self.sample_input_dir),
            },
            clear=False,
        ):
            with TestClient(create_app(settings)) as client:
                response = client.post(
                    "/jobs",
                    data={
                        "mode": "upload_pdf",
                        "display_name": "upload run",
                        "upload_provider_mode": "cloud_first",
                    },
                    files={"uploaded_files": ("demo.pdf", b"%PDF-1.4\n%mock\n", "application/pdf")},
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                job_id = response.headers["location"].rstrip("/").split("/")[-1]
                with mock.patch("webapp.runner._run_subprocess", side_effect=self._fake_subprocess(profile="clean_success")):
                    run_worker_once(settings)
        job = get_job(settings, job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job.status, "succeeded")
        ocr_stdout = settings.logs_root / job_id / "ocr_stdout.txt"
        standardize_stdout = settings.logs_root / job_id / "standardize_stdout.txt"
        self.assertTrue(ocr_stdout.exists())
        self.assertTrue(standardize_stdout.exists())
        ocr_stage_summary = json.loads((settings.results_root / job_id / "ocr_stage_summary.json").read_text(encoding="utf-8"))
        self.assertTrue(ocr_stage_summary["used_mock"])
        self.assertFalse(ocr_stage_summary["cloud_ocr_executed"])
        log_bundle = json.loads((settings.results_root / job_id / "job_log_bundle.json").read_text(encoding="utf-8"))
        log_names = {item["name"] for item in log_bundle["log_files"]}
        self.assertIn("ocr_stdout.txt", log_names)
        self.assertIn("standardize_stdout.txt", log_names)

    def test_deployment_preflight_success_and_script_writes_summary(self):
        self._write_secret_file(aliyun=True)
        settings = self.make_settings(
            env_mode="prod",
            auth_required=True,
            admin_password="demo-pass",
            upload_ocr_method="aliyun_table",
        )
        settings.ensure_directories()
        summary = run_deployment_preflight(settings, deployment_profile="aliyun", min_free_bytes=1)
        self.assertTrue(summary["pass"])

        output_path = settings.runtime_root / "deployment_check_summary.json"
        with mock.patch.dict(
            os.environ,
            {
                "WEBAPP_ENV": "prod",
                "WEBAPP_AUTH_REQUIRED": "1",
                "WEBAPP_ADMIN_PASSWORD": "demo-pass",
                "WEBAPP_QUEUE_BACKEND": "local",
                "WEBAPP_UPLOAD_OCR_METHOD": "aliyun_table",
                "WEBAPP_RUNTIME_ROOT": str(settings.runtime_root),
                "WEBAPP_UPLOADS_ROOT": str(settings.uploads_root),
                "WEBAPP_JOBS_ROOT": str(settings.jobs_root),
                "WEBAPP_RESULTS_ROOT": str(settings.results_root),
                "WEBAPP_LOGS_ROOT": str(settings.logs_root),
                "WEBAPP_DB_PATH": str(settings.db_path),
                "WEBAPP_TEMPLATE_PATH": str(settings.template_path),
                "WEBAPP_SECRET_PATH": str(settings.secret_path),
            },
            clear=False,
        ):
            exit_code = deployment_check_main(["--profile", "aliyun", "--output", str(output_path), "--min-free-mb", "1"])
        self.assertEqual(exit_code, 0)
        self.assertTrue(output_path.exists())

    def test_deployment_preflight_failure_when_prod_missing_password(self):
        settings = self.make_settings(
            env_mode="prod",
            auth_required=True,
            admin_password="",
            upload_ocr_method="aliyun_table",
        )
        summary = run_deployment_preflight(settings, deployment_profile="aliyun", min_free_bytes=1)
        self.assertFalse(summary["pass"])
        error_text = "\n".join(summary["errors"])
        self.assertIn("WEBAPP_ADMIN_PASSWORD", error_text)

    def test_system_status_api_does_not_expose_secret_values(self):
        self._write_secret_file(aliyun=True, secret_value="TOP_SECRET_STAGE11")
        response = self.client.get("/api/system-status")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("TOP_SECRET_STAGE11", response.text)
        self.assertNotIn("demo-id", response.text)

    def test_docker_compose_config_validates_when_available(self):
        if shutil.which("docker") is None:
            self.skipTest("docker not installed")
        compose_version = subprocess.run(
            ["docker", "compose", "version"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        if compose_version.returncode != 0:
            self.skipTest("docker compose plugin unavailable")
        result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.aliyun.yml", "config"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)

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
        self.assertIn("待复核项目", response.text)
        self.assertIn("已提交动作", response.text)
        self.assertIn("原始问题行", response.text)

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

    def test_review_dashboard_count_semantics_summary_is_generated(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.get(f"/jobs/{job_id}/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn("待复核项目", response.text)
        self.assertIn("已提交动作", response.text)
        self.assertIn("可处理项目", response.text)
        self.assertIn("原始问题行", response.text)
        self.assertIn("高优先级", response.text)
        job = get_job(self.settings, job_id)
        summary_path = get_review_dir(job) / "review_dashboard_counts_summary.json"
        self.assertTrue(summary_path.exists())
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["total_review_items"], 4)
        self.assertEqual(summary["source_artifact_rows_total"], 4)
        self.assertEqual(summary["actions_submitted_total"], 0)
        self.assertEqual(summary["actionable_items_total"], 4)

    def test_review_items_page_contains_chinese_action_labels(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.get(f"/jobs/{job_id}/review/items")
        self.assertEqual(response.status_code, 200)
        for text in (
            "暂缓处理",
            "忽略此项",
            "标记为非财务事实",
            "请求重新 OCR",
            "接受科目建议",
            "指定标准科目",
            "选择冲突赢家",
            "标记为误报",
        ):
            self.assertIn(text, response.text)

    def test_non_review_queue_items_get_stable_surrogate_review_item_id_and_source_ref(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items_first, _ = load_review_items(self.settings, job)
        items_second, _ = load_review_items(self.settings, job)
        issue_first = next(item for item in items_first if item.source_type == "issue")
        issue_second = next(item for item in items_second if item.source_type == "issue")
        self.assertEqual(issue_first.review_item_id, issue_second.review_item_id)
        self.assertEqual(issue_first.review_id, "")
        self.assertTrue(issue_first.review_item_id.startswith("issue_"))
        self.assertTrue(issue_first.source_ref)

    def test_review_action_compatibility_summary_is_generated(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        review_item = next(item for item in items if item.source_type == "review_queue")
        issue_item = next(item for item in items if item.source_type == "issue")
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": review_item.review_item_id,
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close queue item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": issue_item.review_item_id,
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close issue item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        result = export_review_actions(self.settings, job)
        self.assertTrue(Path(result["compatibility_summary_path"]).exists())
        summary = json.loads(Path(result["compatibility_summary_path"]).read_text(encoding="utf-8"))
        self.assertEqual(summary["actions_total"], 2)
        self.assertEqual(summary["backend_ready_total"], 1)
        self.assertEqual(summary["backend_partial_total"], 1)

    def test_apply_review_actions_route_creates_apply_outputs(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        response = self.client.post(f"/jobs/{job_id}/review/apply", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self._run_next_worker_item()
        apply_status = self.client.get(f"/jobs/{job_id}/review/apply-status")
        self.assertEqual(apply_status.status_code, 200)
        latest_apply = apply_status.json()["latest_apply_summary"]
        self.assertEqual(latest_apply["applied_actions_total"], 1)
        self.assertEqual(latest_apply["rejected_actions_total"], 0)
        apply_dir = self.settings.jobs_root / job_id / "review" / latest_apply["apply_id"]
        self.assertTrue((apply_dir / "applied_review_actions.csv").exists())
        self.assertTrue((apply_dir / "review_apply_summary.json").exists())

    def test_rejected_unsupported_or_incompatible_actions_are_recorded(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        issue_item = next(item for item in items if item.source_type == "issue")
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": issue_item.review_item_id,
                "action_type": "defer",
                "action_value": "",
                "reviewer_note": "cannot backend apply",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        self.client.post(f"/jobs/{job_id}/review/apply", follow_redirects=False)
        self._run_next_worker_item()
        latest_apply = self.client.get(f"/jobs/{job_id}/review/apply-status").json()["latest_apply_summary"]
        apply_dir = self.settings.jobs_root / job_id / "review" / latest_apply["apply_id"]
        rejected_path = apply_dir / "rejected_review_actions.csv"
        self.assertTrue(rejected_path.exists())
        rejected_text = rejected_path.read_text(encoding="utf-8-sig")
        self.assertIn("review_id_not_found", rejected_text)
        self.assertIn(issue_item.review_item_id, rejected_text)

    def test_apply_and_rerun_creates_new_rerun_output_directory(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            response = self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        self.assertEqual(response.status_code, 303)
        rerun_dir = self.settings.jobs_root / job_id / "reruns" / "rerun_001" / "standardize"
        self.assertTrue(rerun_dir.exists())
        self.assertTrue((rerun_dir / "会计报表_填充结果.xlsx").exists())

    def test_review_rerun_delta_json_is_generated(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        delta_path = self.settings.results_root / job_id / "reruns" / "rerun_001" / "review_rerun_delta.json"
        self.assertTrue(delta_path.exists())
        delta = json.loads(delta_path.read_text(encoding="utf-8"))
        metric_map = {row["metric"]: row for row in delta["metrics"]}
        self.assertEqual(metric_map["review_total"]["before"], 2)
        self.assertEqual(metric_map["review_total"]["after"], 1)
        self.assertEqual(metric_map["validation_fail_total"]["after"], 0)

    def test_job_detail_shows_original_and_rerun_outputs(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        response = self.client.get(f"/jobs/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("结果版本", response.text)
        self.assertIn("original", response.text)
        self.assertIn("rerun_001", response.text)
        self.assertIn("当前推荐结果", response.text)

    def test_evidence_preview_fixture_has_real_preview_and_summary(self):
        job_id = self._prepare_review_job(include_optional=False)
        response = self.client.get(f"/jobs/{job_id}/review/evidence/REV_case_1/cell")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content[:8], b"\x89PNG\r\n\x1a\n")
        self.client.get(f"/jobs/{job_id}/review")
        job = get_job(self.settings, job_id)
        summary_path = get_review_dir(job) / "review_evidence_preview_summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertGreaterEqual(summary["evidence_preview_available_count"], 1)
        self.assertTrue(summary["pass"])

    def test_review_apply_and_rerun_outputs_stay_under_web_runtime_paths(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        apply_root = self.settings.jobs_root / job_id / "review"
        rerun_root = self.settings.jobs_root / job_id / "reruns" / "rerun_001"
        rerun_result_root = self.settings.results_root / job_id / "reruns" / "rerun_001"
        self.assertTrue(str(apply_root).startswith(str(self.settings.jobs_root)))
        self.assertTrue(str(rerun_root).startswith(str(self.settings.jobs_root)))
        self.assertTrue(str(rerun_result_root).startswith(str(self.settings.results_root)))
        self.assertFalse((REPO_ROOT / "rerun_001").exists())

    def test_review_workbench_summary_is_generated(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.get(f"/jobs/{job_id}/review")
        self.assertEqual(response.status_code, 200)
        for text in ("待复核总数", "已处理", "可自动应用", "有证据图片"):
            self.assertIn(text, response.text)
        job = get_job(self.settings, job_id)
        summary_path = get_review_dir(job) / "review_workbench_summary.json"
        self.assertTrue(summary_path.exists())
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["review_items_total"], 4)
        self.assertEqual(summary["backend_ready_total"], 1)
        self.assertEqual(summary["backend_partial_total"], 2)
        self.assertEqual(summary["backend_suggestion_only_total"], 1)
        self.assertEqual(summary["evidence_available_total"], 1)
        self.assertTrue(summary["pass"])

    def test_review_items_can_filter_by_apply_compatibility(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        review_item = next(item for item in items if item.source_type == "review_queue")
        issue_item = next(item for item in items if item.source_type == "issue")
        response = self.client.get(f"/jobs/{job_id}/review/items?apply_compatibility=backend_ready")
        self.assertEqual(response.status_code, 200)
        self.assertIn(review_item.review_item_id, response.text)
        self.assertNotIn(issue_item.review_item_id, response.text)

    def test_review_items_can_filter_by_evidence_available(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        review_item = next(item for item in items if item.source_type == "review_queue")
        issue_item = next(item for item in items if item.source_type == "issue")
        response = self.client.get(f"/jobs/{job_id}/review/items?evidence_available=yes")
        self.assertEqual(response.status_code, 200)
        self.assertIn(review_item.review_item_id, response.text)
        self.assertNotIn(issue_item.review_item_id, response.text)

    def test_bulk_defer_creates_actions_and_summary(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        selected_ids = [item.review_item_id for item in items[:3]]
        response = self.client.post(
            f"/jobs/{job_id}/review/bulk-action",
            data={
                "selected_review_item_ids": selected_ids,
                "action_type": "defer",
                "action_value": "",
                "reviewer_note": "bulk defer",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        actions = list_review_actions(self.settings, job_id)
        self.assertEqual(len(actions), 3)
        self.assertTrue(all(action.action_type == "defer" for action in actions))
        summary_path = get_review_dir(job) / "bulk_review_action_summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["requested_total"], 3)
        self.assertEqual(summary["applied_total"], 3)
        self.assertEqual(summary["rejected_total"], 0)

    def test_bulk_ignore_creates_actions(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        selected_ids = [item.review_item_id for item in items[:2]]
        response = self.client.post(
            f"/jobs/{job_id}/review/bulk-action",
            data={
                "selected_review_item_ids": selected_ids,
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "bulk ignore",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        actions = list_review_actions(self.settings, job_id)
        self.assertEqual(len(actions), 2)
        self.assertTrue(all(action.action_type == "ignore" for action in actions))

    def test_unsupported_bulk_action_items_are_rejected_with_reason(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        review_item = next(item for item in items if item.source_type == "review_queue")
        issue_item = next(item for item in items if item.source_type == "issue")
        response = self.client.post(
            f"/jobs/{job_id}/review/bulk-action",
            data={
                "selected_review_item_ids": [review_item.review_item_id, issue_item.review_item_id],
                "action_type": "accept_mapping_candidate",
                "action_value": "",
                "reviewer_note": "bulk mapping",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        summary = json.loads((get_review_dir(job) / "bulk_review_action_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["requested_total"], 2)
        self.assertEqual(summary["applied_total"], 1)
        self.assertEqual(summary["rejected_total"], 1)
        self.assertIn("compatibility_not_backend_ready", summary["rejected_reasons"])

    def test_review_apply_preview_summary_is_generated(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        items, _ = load_review_items(self.settings, job)
        review_item = next(item for item in items if item.source_type == "review_queue")
        issue_item = next(item for item in items if item.source_type == "issue")
        for item in (review_item, issue_item):
            self.client.post(
                f"/jobs/{job_id}/review/actions",
                data={
                    "review_item_id": item.review_item_id,
                    "action_type": "ignore",
                    "action_value": "",
                    "reviewer_note": "preview",
                    "reviewer_name": "auditor",
                    "next_url": f"/jobs/{job_id}/review/items",
                },
                follow_redirects=False,
            )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        response = self.client.get(f"/jobs/{job_id}/review/apply-preview", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        summary = json.loads((get_review_dir(job) / "review_apply_preview_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["actions_total"], 2)
        self.assertEqual(summary["backend_ready_total"], 1)
        self.assertEqual(summary["partial_total"], 1)
        self.assertEqual(summary["likely_applied_total"], 1)
        self.assertEqual(summary["likely_rejected_total"], 1)

    def test_review_rerun_delta_explained_is_generated(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        explained_path = self.settings.results_root / job_id / "reruns" / "rerun_001" / "review_rerun_delta_explained.json"
        self.assertTrue(explained_path.exists())
        explained = json.loads(explained_path.read_text(encoding="utf-8"))
        self.assertIn("headline_status_before", explained)
        self.assertIn("headline_status_after", explained)
        self.assertTrue(explained["user_friendly_summary_zh"])
        self.assertIn("recommended_next_action_zh", explained)

    def test_operation_summary_status_endpoint_returns_latest_operation(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "close item",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        response = self.client.get(f"/jobs/{job_id}/review/operation-status")
        self.assertEqual(response.status_code, 200)
        latest_operation = response.json()["latest_operation_summary"]
        self.assertEqual(latest_operation["operation_type"], "apply_and_rerun")
        self.assertEqual(latest_operation["status"], "succeeded")
        self.assertTrue(latest_operation["log_paths"])
        self.assertTrue((self.settings.jobs_root / job_id / "review" / "review_operation_summary.json").exists())

    def test_review_items_page_contains_stage_10_2_ux_labels_and_help(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.get(f"/jobs/{job_id}/review/items")
        self.assertEqual(response.status_code, 200)
        for text in ("接受科目建议", "当前兼容性", "可自动应用", "批量动作", "对选中条目执行批量动作"):
            self.assertIn(text, response.text)

    def test_review_workbench_artifacts_do_not_go_into_repo_root(self):
        job_id = self._prepare_review_job(include_optional=True)
        job = get_job(self.settings, job_id)
        self.client.get(f"/jobs/{job_id}/review")
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "preview apply",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        self.client.get(f"/jobs/{job_id}/review/apply-preview", follow_redirects=False)
        self.client.post(
            f"/jobs/{job_id}/review/bulk-action",
            data={
                "selected_review_item_ids": ["REV_case_1"],
                "action_type": "defer",
                "action_value": "",
                "reviewer_note": "bulk",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/apply", follow_redirects=False)
        self._run_next_worker_item()
        review_dir = get_review_dir(job)
        self.assertTrue((review_dir / "review_workbench_summary.json").exists())
        self.assertTrue((review_dir / "review_apply_preview_summary.json").exists())
        self.assertTrue((review_dir / "bulk_review_action_summary.json").exists())
        self.assertTrue((review_dir / "review_operation_summary.json").exists())
        self.assertFalse((REPO_ROOT / "review_workbench_summary.json").exists())
        self.assertFalse((REPO_ROOT / "review_apply_preview_summary.json").exists())
        self.assertFalse((REPO_ROOT / "bulk_review_action_summary.json").exists())
        self.assertFalse((REPO_ROOT / "review_operation_summary.json").exists())

    def test_operation_creation_returns_quickly_and_status_endpoint(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "queue quick",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        started = time.perf_counter()
        response = self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
        elapsed = time.perf_counter() - started
        self.assertEqual(response.status_code, 303)
        self.assertLess(elapsed, 1.5)
        latest_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        self.assertEqual(latest_operation["operation_type"], "apply_and_rerun")
        self.assertEqual(latest_operation["status"], "queued")
        detail = self.client.get(f"/jobs/{job_id}/operations/{latest_operation['operation_id']}")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["operation"]["status"], "queued")

    def test_operation_stage_timeline_is_generated_for_async_apply_and_rerun(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "timeline",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        latest_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        timeline_path = self.settings.jobs_root / job_id / "review" / "operations" / latest_operation["operation_id"] / "operation_stage_timeline.json"
        self.assertTrue(timeline_path.exists())
        timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
        stages = [event["stage"] for event in timeline["events"]]
        self.assertIn("created", stages)
        self.assertIn("queued", stages)
        self.assertIn("running", stages)
        self.assertIn("running_standardize", stages)
        self.assertTrue((self.settings.jobs_root / job_id / "review" / "operation_stage_timeline.json").exists())

    def test_operation_lock_prevents_duplicate_running_apply_and_rerun(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "duplicate lock",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        first = self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
        self.assertEqual(first.status_code, 303)
        second = self.client.post(
            f"/jobs/{job_id}/review/apply-and-rerun",
            follow_redirects=False,
            headers={"accept": "application/json"},
        )
        self.assertEqual(second.status_code, 409)
        self.assertEqual(second.json()["error"], "duplicate_operation_blocked")
        lock_summary = json.loads((self.settings.jobs_root / job_id / "review" / "operation_lock_summary.json").read_text(encoding="utf-8"))
        self.assertTrue(lock_summary["blocked"])
        self.assertTrue(lock_summary["blocked_by_operation_id"])

    def test_retry_failed_operation_creates_new_operation_and_retry_summary(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "retry failed",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_failed_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        failed_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        self.assertEqual(failed_operation["status"], "failed")

        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            retry_response = self.client.post(
                f"/jobs/{job_id}/operations/{failed_operation['operation_id']}/retry",
                follow_redirects=False,
            )
            self.assertEqual(retry_response.status_code, 303)
            self._run_next_worker_item()
        latest_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        self.assertEqual(latest_operation["status"], "succeeded")
        self.assertNotEqual(latest_operation["operation_id"], failed_operation["operation_id"])
        retry_summary = json.loads((self.settings.jobs_root / job_id / "review" / "operation_retry_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(retry_summary["source_operation_id"], failed_operation["operation_id"])
        self.assertEqual(retry_summary["new_operation_id"], latest_operation["operation_id"])

    def test_cancel_operation_route_marks_queued_operation_cancelled(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "cancel queued",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
        latest_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        cancel_response = self.client.post(
            f"/jobs/{job_id}/operations/{latest_operation['operation_id']}/cancel",
            follow_redirects=False,
        )
        self.assertEqual(cancel_response.status_code, 303)
        detail = self.client.get(f"/jobs/{job_id}/operations/{latest_operation['operation_id']}")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["operation"]["status"], "cancelled")

    def test_filter_dropdowns_show_chinese_labels_not_raw_enums(self):
        job_id = self._prepare_review_job(include_optional=True)
        response = self.client.get(f"/jobs/{job_id}/review/items")
        self.assertEqual(response.status_code, 200)
        self.assertIn('value="unresolved"', response.text)
        self.assertIn(">未处理</option>", response.text)
        self.assertIn(">复核队列</option>", response.text)
        self.assertIn(">科目映射问题</option>", response.text)
        self.assertIn(">可自动应用</option>", response.text)
        self.assertNotIn(">unresolved</option>", response.text)
        self.assertNotIn(">review_queue</option>", response.text)
        self.assertNotIn(">backend_ready</option>", response.text)

    def test_operation_status_and_type_are_shown_in_chinese(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "zh labels",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
        response = self.client.get(f"/jobs/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("应用复核并重新生成", response.text)
        self.assertIn("排队中", response.text)

    def test_operation_log_tail_route_is_restricted_to_allowed_job_dirs(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "log tail",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        self.client.post(f"/jobs/{job_id}/review/apply", follow_redirects=False)
        latest_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        allowed_log_path = self.settings.jobs_root / job_id / "review" / "operations" / latest_operation["operation_id"] / "operation.log"
        allowed_log_path.parent.mkdir(parents=True, exist_ok=True)
        allowed_log_path.write_text("allowed log\n", encoding="utf-8")
        outside_log_path = self.temp_path / "outside_operation.log"
        outside_log_path.write_text("outside log\n", encoding="utf-8")
        update_review_operation(
            self.settings,
            latest_operation["operation_id"],
            log_paths=[str(allowed_log_path), str(outside_log_path)],
        )
        response = self.client.get(f"/jobs/{job_id}/operations/{latest_operation['operation_id']}/logs")
        self.assertEqual(response.status_code, 200)
        log_tails = response.json()["log_tails"]
        self.assertEqual(len(log_tails), 1)
        self.assertIn("allowed log", log_tails[0]["tail"])

    def test_operation_artifacts_stay_under_generated_web_paths(self):
        job_id = self._prepare_review_job(include_optional=True)
        self.client.post(
            f"/jobs/{job_id}/review/actions",
            data={
                "review_item_id": "REV_case_1",
                "action_type": "ignore",
                "action_value": "",
                "reviewer_note": "artifacts under runtime",
                "reviewer_name": "auditor",
                "next_url": f"/jobs/{job_id}/review/items",
            },
            follow_redirects=False,
        )
        self.client.post(f"/jobs/{job_id}/review/export-actions", follow_redirects=False)
        with mock.patch("webapp.review._run_patched_standardize_cli", side_effect=self._fake_review_rerun()):
            self.client.post(f"/jobs/{job_id}/review/apply-and-rerun", follow_redirects=False)
            self._run_next_worker_item()
        latest_operation = self.client.get(f"/jobs/{job_id}/review/operation-status").json()["latest_operation_summary"]
        for raw_path in [*latest_operation.get("log_paths", []), *latest_operation.get("result_paths", [])]:
            resolved = Path(raw_path)
            if not resolved.is_absolute():
                resolved = REPO_ROOT / resolved
            self.assertTrue(str(resolved).startswith(str(self.runtime_root)))
        self.assertTrue((self.settings.jobs_root / job_id / "review" / "operations").exists())

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
