from __future__ import annotations

import argparse
import csv
import tempfile
import unittest
from pathlib import Path

from project_paths import RAW_METRICS_GENERATED_ROOT, STANDARD_METRICS_GENERATED_ROOT
from standard_map.cli import main as standard_map_main
from standard_map.models import STANDARD_OUTPUT_COLUMNS
from standard_map.mapper import run_standard_mapping


class StandardMapTests(unittest.TestCase):
    def setUp(self) -> None:
        RAW_METRICS_GENERATED_ROOT.mkdir(parents=True, exist_ok=True)
        STANDARD_METRICS_GENERATED_ROOT.mkdir(parents=True, exist_ok=True)

    def _write_raw_metrics(self, rows: list[dict[str, object]]) -> tuple[tempfile.TemporaryDirectory, Path]:
        tempdir = tempfile.TemporaryDirectory(dir=RAW_METRICS_GENERATED_ROOT)
        run_dir = Path(tempdir.name) / "RUN_TEST"
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "raw_metrics.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["填表日期", "当前条目日期", "公司名", "指标名", "指标数值"])
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        detailed = run_dir / "raw_metrics_detailed.csv"
        with detailed.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["source_cell_ref", "page_no", "bbox_json", "evidence_path", "source_file", "provider", "doc_id"],
            )
            writer.writeheader()
            for index, _ in enumerate(rows, start=1):
                writer.writerow(
                    {
                        "source_cell_ref": f"DTEST:1:aliyun_table:0:{index}-{index}:2-2",
                        "page_no": "1",
                        "bbox_json": '[{"x":1,"y":2}]',
                        "evidence_path": "data/corpus/DTEST/input/demo.pdf",
                        "source_file": "fixture.json",
                        "provider": "aliyun_table",
                        "doc_id": "DTEST",
                    }
                )
        return tempdir, path

    def _run_mapping(self, rows: list[dict[str, object]]):
        raw_temp, input_path = self._write_raw_metrics(rows)
        output_temp = tempfile.TemporaryDirectory(dir=STANDARD_METRICS_GENERATED_ROOT)
        args = argparse.Namespace(
            input=str(input_path),
            output_dir=output_temp.name,
            mapping_registry="config/standard_terms.yml",
            doc_id="DTEST",
            company_name="",
            debug=False,
        )
        result = run_standard_mapping(args=args, cli_args=["--input", str(input_path), "--output-dir", output_temp.name])
        return raw_temp, output_temp, result

    def test_exact_alias_mapping(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("现金及现金等价物", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "mapped")
            self.assertEqual(mapped.mapping_method, "alias")
            self.assertEqual(mapped.standard_code, "ZT_001")
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_legacy_alias_mapping(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("应交税金", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "mapped")
            self.assertEqual(mapped.mapping_method, "legacy_alias")
            self.assertEqual(mapped.standard_name, "应交税费")
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_ambiguous_relation_requires_review(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("往来款", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "review_required")
            self.assertEqual(mapped.mapping_method, "relation_review")
            self.assertIn("ambiguous", mapped.issue_reason)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_aggregate_relation_requires_review(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("应收票据及应收账款", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "review_required")
            self.assertEqual(mapped.mapping_method, "relation_review")
            self.assertIn("aggregate", mapped.issue_reason)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_unmapped_metric(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("不存在指标", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "unmapped")
            self.assertEqual(mapped.mapping_method, "none")
            self.assertTrue(mapped.review_required)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_output_chinese_headers_and_review_items(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("往来款", "100")])
        try:
            with (Path(result.output_dir) / "standardized_metrics.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                self.assertEqual(next(csv.reader(handle)), STANDARD_OUTPUT_COLUMNS)
            with (Path(result.output_dir) / "mapping_review_items.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["action_default"], "approve_mapping")
            self.assertIn("change_mapping", rows[0]["action_options"])
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_no_value_or_date_mutation(self):
        row = self._row("货币资金", "00123.4500")
        row["填表日期"] = "2022-12-31"
        row["当前条目日期"] = "2022-01-01"
        raw_temp, output_temp, result = self._run_mapping([row])
        try:
            with (Path(result.output_dir) / "standardized_metrics.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                mapped = next(csv.DictReader(handle))
            self.assertEqual(mapped["填表日期"], "2022-12-31")
            self.assertEqual(mapped["当前条目日期"], "2022-01-01")
            self.assertEqual(mapped["指标数值"], "00123.4500")
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_cli_runs(self):
        raw_temp, input_path = self._write_raw_metrics([self._row("货币资金", "100")])
        output_temp = tempfile.TemporaryDirectory(dir=STANDARD_METRICS_GENERATED_ROOT)
        try:
            exit_code = standard_map_main(
                [
                    "--input",
                    str(input_path),
                    "--output-dir",
                    output_temp.name,
                    "--mapping-registry",
                    "config/standard_terms.yml",
                    "--doc-id",
                    "DTEST",
                ]
            )
            self.assertEqual(exit_code, 0)
            self.assertTrue(list(Path(output_temp.name).glob("RUN_*/standardized_metrics.csv")))
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def _row(self, metric_name: str, metric_value: str) -> dict[str, object]:
        return {
            "填表日期": "2022-12-31",
            "当前条目日期": "2022-12-31",
            "公司名": "AAA有限公司",
            "指标名": metric_name,
            "指标数值": metric_value,
        }


if __name__ == "__main__":
    unittest.main()
