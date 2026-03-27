import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from openpyxl import load_workbook

from standardize import cli
from standardize.models import FactRecord, ProviderCell, ProviderPage, StatementMeta
from standardize.normalize.conflicts import resolve_conflicts
from standardize.normalize.mapping import load_template_subjects
from standardize.normalize.numbers import analyze_numeric_text
from standardize.normalize.tables import standardize_page
from standardize.providers.aliyun import extract_aliyun_data


class StandardizeTests(unittest.TestCase):
    def test_extract_aliyun_outer_data_string(self):
        raw = {
            "Data": json.dumps(
                {
                    "content": "资产负债表",
                    "prism_tablesInfo": [{"cellInfos": []}],
                },
                ensure_ascii=False,
            )
        }

        data = extract_aliyun_data(raw)

        self.assertEqual(data["content"], "资产负债表")
        self.assertIn("prism_tablesInfo", data)

    def test_dense_grid_preserves_blank_cells(self):
        page = ProviderPage(
            doc_id="demo",
            page_no=1,
            provider="aliyun_table",
            source_file="demo.json",
            source_kind="json",
            page_text="资产负债表",
            tables={
                "1": [
                    ProviderCell(table_id="1", row_start=0, row_end=0, col_start=0, col_end=0, text="项目"),
                    ProviderCell(table_id="1", row_start=0, row_end=0, col_start=2, col_end=2, text="金额"),
                    ProviderCell(table_id="1", row_start=1, row_end=1, col_start=0, col_end=0, text="货币资金"),
                    ProviderCell(table_id="1", row_start=1, row_end=1, col_start=2, col_end=2, text="100"),
                ]
            },
            context_lines=["资产负债表", "2022年12月31日", "单位:元"],
        )
        statement_meta = StatementMeta(
            statement_type="balance_sheet",
            statement_name_raw="资产负债表",
            report_date_raw="2022年12月31日",
            report_date_norm="2022-12-31",
            unit_raw="元",
            unit_multiplier=1.0,
        )

        cells, subtables, issues = standardize_page(
            page=page,
            statement_meta=statement_meta,
            keyword_config={},
        )

        self.assertEqual(len(cells), 6)
        self.assertEqual(sum(1 for cell in cells if cell.is_empty), 2)
        self.assertEqual(len(subtables), 1)
        self.assertEqual(len(issues), 0)

    def test_number_cleaning(self):
        self.assertEqual(analyze_numeric_text("396，149，420.62", expected_numeric=True)["value_num"], 396149420.62)
        self.assertEqual(analyze_numeric_text("(1,234.56)", expected_numeric=True)["value_num"], -1234.56)
        self.assertEqual(analyze_numeric_text("98.26%", expected_numeric=True)["value_num"], 0.9826)

    def test_suspicious_values(self):
        for value in ["务屏20,000,000.00", "t", "章"]:
            result = analyze_numeric_text(value, expected_numeric=True)
            self.assertTrue(result["is_suspicious"])
            self.assertTrue(result["suspicious_reason"])

    def test_template_subject_parsing(self):
        template_path = Path(__file__).resolve().parent.parent / "会计报表.xlsx"

        subjects, sheet_name, header_row = load_template_subjects(template_path)

        self.assertEqual(subjects[0].code, "ZT_001")
        self.assertEqual(subjects[0].canonical_name, "货币资金")
        self.assertTrue(sheet_name)
        self.assertGreaterEqual(header_row, 1)

    def test_conflict_grouping_uses_table_semantics(self):
        base_kwargs = {
            "doc_id": "demo",
            "page_no": 13,
            "statement_type": "note",
            "statement_name_raw": "2022年度财务报表附注",
            "row_label_raw": "合计",
            "row_label_std": "合计",
            "col_header_raw": "期末数",
            "col_header_path": ["期末数"],
            "column_semantic_key": "期末数",
            "period_role_raw": "期末数",
            "report_date_raw": "",
            "period_key": "unknown_date__期末数",
            "value_type": "amount",
            "unit_raw": "",
            "unit_multiplier": 1.0,
            "mapping_code": "",
            "mapping_name": "",
            "mapping_method": "",
            "mapping_confidence": None,
        }
        facts = [
            FactRecord(
                provider="aliyun_table",
                logical_subtable_id="4_sub1",
                table_semantic_key="note|h:单位名称,期末余额,账龄,比例|r:泰兴市智光环保科技有限,江苏智光创业投资有限公,泰州市科兴环境咨询有限",
                value_raw="735，449，914.02",
                value_num=735449914.02,
                source_cell_ref="demo:13:aliyun_table:4:6-6:1-1",
                status="observed",
                issue_flags=[],
                **base_kwargs,
            ),
            FactRecord(
                provider="tencent_table_v3",
                logical_subtable_id="4_sub1",
                table_semantic_key="note|h:项目,期初数,期末数|r:银行存款,其他货币资金,合计",
                value_raw="401,700,701.94",
                value_num=401700701.94,
                source_cell_ref="demo:13:tencent_table_v3:4:3-3:2-2",
                status="observed",
                issue_flags=[],
                **base_kwargs,
            ),
        ]

        resolved_facts, conflicts = resolve_conflicts(
            facts=facts,
            provider_priority=["aliyun_table", "tencent_table_v3"],
            enabled=True,
        )

        self.assertEqual(len(conflicts), 0)
        self.assertTrue(all(fact.status == "observed" for fact in resolved_facts))

    def test_page_0004_end_to_end(self):
        repo_root = Path(__file__).resolve().parent
        template_path = repo_root.parent / "会计报表.xlsx"

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            input_dir = tmpdir_path / "outputs"
            output_dir = tmpdir_path / "normalized"

            self.copy_sample_page_0004(repo_root, input_dir)

            exit_code = cli.main(
                [
                    "--input-dir",
                    str(input_dir),
                    "--template",
                    str(template_path),
                    "--output-dir",
                    str(output_dir),
                    "--provider-priority",
                    "aliyun,tencent",
                    "--enable-conflict-merge",
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "cells.csv").exists())
            self.assertTrue((output_dir / "facts.csv").exists())
            self.assertTrue((output_dir / "会计报表_填充结果.xlsx").exists())

            with (output_dir / "facts.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(any(row["row_label_raw"] == "货币资金" for row in rows))

            workbook = load_workbook(output_dir / "会计报表_填充结果.xlsx")
            worksheet = workbook[workbook.sheetnames[0]]
            headers = [worksheet.cell(row=3, column=idx).value for idx in range(1, worksheet.max_column + 1)]
            self.assertIn("2022-12-31__期初数", headers)
            self.assertIn("2022-12-31__期末数", headers)

    def copy_sample_page_0004(self, repo_root: Path, input_dir: Path) -> None:
        doc_name = "债务人审计报告-2022年"

        aliyun_raw_dir = input_dir / "aliyun_table" / doc_name / "raw"
        aliyun_raw_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            next((repo_root / "outputs" / "aliyun_table").glob(f"*/raw/page_0004.json")),
            aliyun_raw_dir / "page_0004.json",
        )

        tencent_doc_dir = input_dir / "tencent_table_v3" / doc_name
        tencent_raw_dir = tencent_doc_dir / "raw"
        tencent_artifacts_dir = tencent_doc_dir / "artifacts"
        tencent_raw_dir.mkdir(parents=True, exist_ok=True)
        tencent_artifacts_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy2(
            next((repo_root / "outputs" / "tencent_table_v3").glob("*/raw/page_0004_tencent.json")),
            tencent_raw_dir / "page_0004_tencent.json",
        )
        shutil.copy2(
            next((repo_root / "outputs" / "tencent_table_v3").glob("*/artifacts/page_0004.xlsx")),
            tencent_artifacts_dir / "page_0004.xlsx",
        )

        tencent_result = {
            "provider": "tencent_table_v3",
            "pages": [
                {
                    "page_number": 4,
                    "text": "资产负债表\n编制单位:泰兴市泰泽实业有限公司\n2022年12月31日\n单位:元",
                    "raw_file": "raw/page_0004.json",
                    "artifact_files": ["artifacts/page_0004.xlsx"],
                }
            ],
        }
        (tencent_doc_dir / "result.json").write_text(
            json.dumps(tencent_result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
