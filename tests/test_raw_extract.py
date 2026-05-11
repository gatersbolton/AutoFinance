from __future__ import annotations

import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from project_paths import RAW_METRICS_GENERATED_ROOT, REPO_ROOT
from raw_extract.cli import validate_output_base
from raw_extract.company_resolver import resolve_company_name
from raw_extract.date_resolver import resolve_item_date
from raw_extract.export import export_raw_metrics_run
from raw_extract.metric_extractor import extract_raw_metric_candidates, select_accepted_candidates
from raw_extract.models import MAIN_OUTPUT_COLUMNS, CompanyResolution, RawMetricCandidate
from raw_extract.number_parser import parse_metric_number
from raw_extract.table_rebuild import rebuild_logical_subtables
from standardize.models import DiscoveredSource, ProviderCell, ProviderPage
from standardize.providers.tencent import load_tencent_page, normalize_tencent_range


class RawExtractTests(unittest.TestCase):
    def test_number_parsing(self):
        self.assertEqual(parse_metric_number("396，149，420.62").value, 396149420.62)
        self.assertEqual(parse_metric_number("(1,234.56)").value, -1234.56)
        self.assertEqual(parse_metric_number("98.26%").value, 0.9826)
        abnormal = parse_metric_number("20000,000.00")
        self.assertEqual(abnormal.value, 20000000.0)
        self.assertIn("suspicious_numeric", abnormal.issue_flags)
        broken = parse_metric_number("2,029,298.849.12")
        self.assertIsNone(broken.value)
        self.assertIn("numeric_parse_failed", broken.issue_flags)

    def test_date_role_mapping(self):
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["期末数"], period_role_raw="期末数", statement_type="balance_sheet").item_date, "2022-12-31")
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["期初数"], period_role_raw="期初数", statement_type="balance_sheet").item_date, "2022-01-01")
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["年初数"], period_role_raw="年初数", statement_type="balance_sheet").item_date, "2022-01-01")
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["本年累计"], period_role_raw="本年累计", statement_type="income_statement").item_date, "2022-01-01")
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["本期"], period_role_raw="本期", statement_type="cash_flow").item_date, "2022-01-01")
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["上期"], period_role_raw="上期", statement_type="income_statement").item_date, "2021-01-01")
        self.assertEqual(resolve_item_date(fill_date="2022-12-31", header_path=["上年同期"], period_role_raw="上年同期", statement_type="income_statement").item_date, "2021-01-01")

    def test_company_resolver_from_prepared_by_label(self):
        page = ProviderPage(
            doc_id="DTEST",
            page_no=1,
            provider="aliyun_table",
            source_file="fixture.json",
            source_kind="json",
            page_text="资产负债表 编制单位：AAA有限公司 2022年12月31日 单位：元",
            tables={},
        )
        result = resolve_company_name(doc_id="DTEST", pages=[page], input_dir=Path("data/corpus/DTEST/ocr_outputs"))
        self.assertEqual(result.company_name, "AAA有限公司")
        self.assertEqual(result.method, "ocr_label")

    def test_raw_metric_extraction_from_tiny_grid(self):
        page = self._tiny_balance_sheet_page("aliyun_table")
        _, subtables, _ = rebuild_logical_subtables([page])
        company = resolve_company_name(doc_id="DTEST", pages=[page], input_dir=Path("data/corpus/DTEST/ocr_outputs"))
        candidates, _ = extract_raw_metric_candidates(
            subtables=subtables,
            pages=[page],
            company=company,
            input_dir=Path("data/corpus/DTEST/ocr_outputs"),
            source_image_dir=None,
            provider_priority=["aliyun_table"],
        )
        accepted, issues = select_accepted_candidates(candidates, include_blank=False, include_ratios=True)
        self.assertEqual(len(accepted), 2)
        self.assertEqual({row.metric_name for row in accepted}, {"货币资金"})
        self.assertIn("2022-01-01", {row.item_date for row in accepted})
        self.assertIn("2022-12-31", {row.item_date for row in accepted})
        self.assertFalse(any("ZT_" in row.metric_name for row in accepted))
        self.assertIsInstance(issues, list)

    def test_provider_priority_candidate_selection(self):
        low = self._candidate("tencent_table_v3", 1, 100.0)
        high = self._candidate("aliyun_table", 0, 100.0)
        accepted, issues = select_accepted_candidates([low, high], include_blank=False, include_ratios=True)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].provider, "aliyun_table")
        self.assertIn("duplicate_candidate", {issue.issue_type for issue in issues})

    def test_provider_conflict_issue(self):
        first = self._candidate("aliyun_table", 0, 100.0)
        second = self._candidate("tencent_table_v3", 1, 101.0)
        accepted, issues = select_accepted_candidates([first, second], include_blank=False, include_ratios=True)
        self.assertEqual(accepted[0].provider, "aliyun_table")
        self.assertIn("provider_value_conflict", {issue.issue_type for issue in issues})

    def test_tencent_table_range_does_not_treat_line_numbers_as_metric_names(self):
        self.assertEqual(normalize_tencent_range(0, 1), (0, 0))
        self.assertEqual(normalize_tencent_range(1, 2), (1, 1))

        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "page_0001.json"
            raw_path.write_text(
                json.dumps(
                    {
                        "TableDetections": [
                            {
                                "Type": 1,
                                "Cells": [
                                    self._tencent_cell("项目", 0, 1, 0, 1),
                                    self._tencent_cell("行次", 0, 1, 1, 2),
                                    self._tencent_cell("本年累计数", 0, 1, 2, 3),
                                    self._tencent_cell("上年累计数", 0, 1, 3, 4),
                                    self._tencent_cell("一、主营业务收入", 1, 2, 0, 1),
                                    self._tencent_cell("1", 1, 2, 1, 2),
                                    self._tencent_cell("251,143,230.20", 1, 2, 2, 3),
                                    self._tencent_cell("227,585,011.97", 1, 2, 3, 4),
                                    self._tencent_cell("管理费用", 2, 3, 0, 1),
                                    self._tencent_cell("2", 2, 3, 1, 2),
                                    self._tencent_cell("8,252,343.59", 2, 3, 2, 3),
                                    self._tencent_cell("8,410,412.77", 2, 3, 3, 4),
                                ],
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            page = load_tencent_page(
                DiscoveredSource(
                    doc_id="DTEST",
                    page_no=1,
                    provider="tencent_table_v3",
                    provider_family="tencent",
                    provider_dir=tmp,
                    raw_file=str(raw_path),
                    result_page_meta={"text": "利润及利润分配表\n编制单位：AAA有限公司\n2022年12月31日\n单位：元"},
                )
            )
            _, subtables, _ = rebuild_logical_subtables([page])
            company = resolve_company_name(doc_id="DTEST", pages=[page], input_dir=Path(tmp))
            candidates, _ = extract_raw_metric_candidates(
                subtables=subtables,
                pages=[page],
                company=company,
                input_dir=Path(tmp),
                source_image_dir=None,
                provider_priority=["tencent_table_v3"],
            )
            accepted, _ = select_accepted_candidates(candidates, include_blank=False, include_ratios=True)

        metric_names = {row.metric_name for row in accepted}
        self.assertIn("主营业务收入", metric_names)
        self.assertIn("管理费用", metric_names)
        self.assertNotIn("1", metric_names)
        self.assertNotIn("2", metric_names)

    def test_tencent_shared_table_polygon_is_not_used_as_cell_bbox(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "page_0001.json"
            shared_polygon = [{"X": 10, "Y": 10}, {"X": 500, "Y": 10}, {"X": 500, "Y": 400}, {"X": 10, "Y": 400}]
            raw_path.write_text(
                json.dumps(
                    {
                        "TableDetections": [
                            {
                                "Type": 1,
                                "Cells": [
                                    self._tencent_cell("项目", 0, 1, 0, 1, polygon=shared_polygon),
                                    self._tencent_cell("行次", 0, 1, 1, 2, polygon=shared_polygon),
                                    self._tencent_cell("货币资金", 1, 2, 0, 1, polygon=shared_polygon),
                                    self._tencent_cell("1", 1, 2, 1, 2, polygon=shared_polygon),
                                ],
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            page = load_tencent_page(
                DiscoveredSource(
                    doc_id="DTEST",
                    page_no=1,
                    provider="tencent_table_v3",
                    provider_family="tencent",
                    provider_dir=tmp,
                    raw_file=str(raw_path),
                )
            )

        self.assertTrue(page.tables["1"])
        self.assertTrue(all(cell.bbox is None for cell in page.tables["1"]))

    def test_output_chinese_headers_exactly(self):
        RAW_METRICS_GENERATED_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=RAW_METRICS_GENERATED_ROOT) as tmp:
            output_dir = Path(tmp)
            row = self._candidate("aliyun_table", 0, 100.0)
            export_raw_metrics_run(
                output_dir=output_dir,
                accepted=[row],
                candidates=[row],
                issues=[],
                date_audits=[],
                company_audits=[CompanyResolution(doc_id="DTEST", company_name="AAA有限公司", method="cli_override")],
                summary={"run_id": "RUN_TEST"},
                manifest={"run_id": "RUN_TEST"},
            )
            with (output_dir / "raw_metrics.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                self.assertEqual(next(reader), MAIN_OUTPUT_COLUMNS)

    def test_path_hygiene_under_raw_metrics(self):
        validate_output_base(RAW_METRICS_GENERATED_ROOT / "DTEST")
        with self.assertRaises(ValueError):
            validate_output_base(REPO_ROOT)

    def _tiny_balance_sheet_page(self, provider: str) -> ProviderPage:
        cells = [
            ProviderCell("1", 0, 0, 0, 0, "项目"),
            ProviderCell("1", 0, 0, 1, 1, "行次"),
            ProviderCell("1", 0, 0, 2, 2, "期初数"),
            ProviderCell("1", 0, 0, 3, 3, "期末数"),
            ProviderCell("1", 1, 1, 0, 0, "货币资金"),
            ProviderCell("1", 1, 1, 1, 1, "1"),
            ProviderCell("1", 1, 1, 2, 2, "100"),
            ProviderCell("1", 1, 1, 3, 3, "200"),
        ]
        return ProviderPage(
            doc_id="DTEST",
            page_no=1,
            provider=provider,
            source_file="fixture.json",
            source_kind="json",
            page_text="资产负债表\n编制单位：AAA有限公司\n2022年12月31日\n单位：元",
            tables={"1": cells},
            context_lines=["资产负债表", "编制单位：AAA有限公司", "2022年12月31日", "单位：元"],
        )

    def _candidate(self, provider: str, rank: int, value: float) -> RawMetricCandidate:
        row = RawMetricCandidate(
            candidate_id=f"{provider}_{value}",
            fill_date="2022-12-31",
            item_date="2022-12-31",
            company_name="AAA有限公司",
            metric_name="营业总额",
            metric_value=value,
            value_raw=str(value),
            value_type="amount",
            unit_raw="元",
            provider=provider,
            doc_id="DTEST",
            source_file="fixture.json",
            page_no=1,
            table_id="1",
            logical_subtable_id="1_sub1",
            row_index=1,
            col_index=2,
            row_label_raw="营业总额",
            row_label_clean="营业总额",
            row_context_path="",
            header_path="期末数",
            period_role_raw="期末数",
            period_role_norm="ending",
            period_start_date="2022-12-31",
            period_end_date="2022-12-31",
            date_resolution_method="test",
            company_resolution_method="test",
            source_cell_ref=f"DTEST:1:{provider}:1:1-1:2-2",
            bbox_json="",
            confidence=None,
            issue_flags=[],
            provider_rank=rank,
            duplicate_key="DTEST|income_statement|营业总额||2022-12-31|2022-12-31|ending",
        )
        return row

    def _tencent_cell(self, text: str, row_tl: int, row_br: int, col_tl: int, col_br: int, *, polygon: list[dict] | None = None) -> dict:
        return {
            "Text": text,
            "Confidence": 99,
            "Type": "body",
            "RowTl": row_tl,
            "RowBr": row_br,
            "ColTl": col_tl,
            "ColBr": col_br,
            "Polygon": polygon or [{"X": col_tl * 10, "Y": row_tl * 10}],
        }


if __name__ == "__main__":
    unittest.main()
