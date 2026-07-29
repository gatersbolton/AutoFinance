from __future__ import annotations

import argparse
import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from project_paths import REPO_ROOT
from raw_extract.cli import validate_output_base
from raw_extract.company_resolver import resolve_company_name
from raw_extract.date_resolver import resolve_fill_date, resolve_item_date
from raw_extract.export import export_raw_metrics_run
from raw_extract.metric_extractor import extract_raw_metric_candidates, select_accepted_candidates
from raw_extract.models import MAIN_OUTPUT_COLUMNS, CompanyResolution, RawMetricCandidate, display_period_role
from raw_extract.number_parser import parse_metric_number
from raw_extract.table_rebuild import rebuild_logical_subtables
from standardize.models import DiscoveredSource, ProviderCell, ProviderPage
from standardize.providers.tencent import load_tencent_page, normalize_tencent_range


class RawExtractTests(unittest.TestCase):
    def test_number_parsing(self):
        self.assertEqual(parse_metric_number("396，149，420.62").value, Decimal("396149420.62"))
        self.assertEqual(parse_metric_number("(1,234.56)").value, Decimal("-1234.56"))
        self.assertEqual(parse_metric_number("98.26%").value, Decimal("0.9826"))
        abnormal = parse_metric_number("20000,000.00")
        self.assertEqual(abnormal.value, Decimal("20000000.00"))
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
        self.assertEqual(display_period_role("beginning", "42,940,481.00"), "期初数")
        self.assertEqual(display_period_role("unknown", "42,940,481.00"), "")
        self.assertEqual(display_period_role("unknown", "期末余额"), "期末余额")

    def test_numeric_only_metric_names_are_rejected(self):
        row = self._candidate("tencent_table_v3", 0, 7132218.77)
        row.metric_name = "26"
        row.row_label_clean = "26"
        row.row_label_raw = "53,295,859.26"
        row.period_role_raw = "42,940,481.00"
        row.period_role_norm = "unknown"
        accepted, issues = select_accepted_candidates([row], include_blank=False, include_ratios=True)

        self.assertEqual(accepted, [])
        self.assertEqual(row.selection_status, "rejected_invalid_metric_name")
        self.assertIn("invalid_metric_name", {issue.issue_type for issue in issues})

    def test_missing_item_date_and_period_role_candidates_are_kept_for_review(self):
        row = self._candidate("tencent_table_v3", 0, 113866652.0)
        row.metric_name = "其他应付款"
        row.item_date = ""
        row.period_role_raw = ""
        row.period_role_norm = "unknown"

        accepted, issues = select_accepted_candidates([row], include_blank=False, include_ratios=True)

        self.assertEqual(accepted, [row])
        self.assertTrue(row.accepted)
        self.assertEqual(row.selection_status, "accepted_needs_temporal_review")
        self.assertIn("missing_temporal_key", row.issue_flags)
        self.assertIn("missing_temporal_key", {issue.issue_type for issue in issues})

    def test_amount_header_without_date_is_kept_for_temporal_review(self):
        row = self._candidate("tencent_table_v3", 0, 113866652.0)
        row.metric_name = "其他应付款"
        row.item_date = ""
        row.period_role_raw = "金额"
        row.period_role_norm = "amount"

        accepted, issues = select_accepted_candidates([row], include_blank=False, include_ratios=True)

        self.assertEqual(accepted, [row])
        self.assertEqual(row.selection_status, "accepted_needs_temporal_review")
        self.assertIn("missing_temporal_key", {issue.issue_type for issue in issues})

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
        self.assertEqual({row.text_confidence for row in accepted}, {0.91})
        self.assertEqual({row.value_confidence for row in accepted}, {0.84, 0.88})
        self.assertEqual({row.confidence for row in accepted}, {0.84, 0.88})
        self.assertIsInstance(issues, list)

    def test_raw_metric_amounts_are_normalized_to_yuan(self):
        page = self._tiny_balance_sheet_page("aliyun_table")
        page.page_text = page.page_text.replace("单位：元", "单位：万元")
        page.context_lines = [line.replace("单位：元", "单位：万元") for line in page.context_lines]
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
        accepted, _ = select_accepted_candidates(candidates, include_blank=False, include_ratios=True)

        self.assertEqual({row.unit_raw for row in accepted}, {"万元"})
        self.assertEqual({row.unit_multiplier for row in accepted}, {Decimal("10000.0")})
        self.assertEqual({row.metric_value for row in accepted}, {Decimal("1000000.0"), Decimal("2000000.0")})

    def test_filename_only_date_requires_review(self):
        page = self._tiny_balance_sheet_page("aliyun_table")
        page.page_text = "资产负债表\n编制单位：AAA有限公司\n单位：元"
        page.context_lines = ["资产负债表", "编制单位：AAA有限公司", "单位：元"]
        _, subtables, _ = rebuild_logical_subtables([page])

        resolution = resolve_fill_date(
            subtable=subtables[0],
            page=page,
            input_dir=Path("data/corpus/DTEST/2022年度/ocr_outputs"),
        )

        self.assertEqual(resolution.fill_date, "")
        self.assertEqual(resolution.method, "needs_review_low_confidence")
        self.assertIn("low_confidence_date", resolution.issue_flags)

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

    def test_accepted_candidates_keep_logical_subtables_contiguous(self):
        candidates = []
        for index, (subtable_id, row_index, metric_name) in enumerate(
            [
                ("1_sub1", 1, "资产1"),
                ("1_sub2", 1, "负债1"),
                ("1_sub1", 2, "资产2"),
                ("1_sub2", 2, "负债2"),
            ],
            start=1,
        ):
            candidate = self._candidate("aliyun_table", 0, float(index))
            candidate.candidate_id = f"candidate_{index}"
            candidate.logical_subtable_id = subtable_id
            candidate.row_index = row_index
            candidate.metric_name = metric_name
            candidate.source_cell_ref = f"DTEST:1:aliyun_table:1:{row_index}-{row_index}:2-2"
            candidate.duplicate_key = f"DTEST|balance_sheet|{metric_name}|{subtable_id}"
            candidates.append(candidate)

        accepted, _ = select_accepted_candidates(candidates, include_blank=False, include_ratios=True)

        self.assertEqual(
            [(row.logical_subtable_id, row.row_index, row.metric_name) for row in accepted],
            [
                ("1_sub1", 1, "资产1"),
                ("1_sub1", 2, "资产2"),
                ("1_sub2", 1, "负债1"),
                ("1_sub2", 2, "负债2"),
            ],
        )

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
        with tempfile.TemporaryDirectory() as tmp:
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
        with tempfile.TemporaryDirectory() as tmp:
            raw_metrics_root = Path(tmp) / "raw_metrics"
            validate_output_base(raw_metrics_root / "DTEST", raw_metrics_root=raw_metrics_root)
            with self.assertRaises(ValueError):
                validate_output_base(REPO_ROOT, raw_metrics_root=raw_metrics_root)

    def _tiny_balance_sheet_page(self, provider: str) -> ProviderPage:
        cells = [
            ProviderCell("1", 0, 0, 0, 0, "项目"),
            ProviderCell("1", 0, 0, 1, 1, "行次"),
            ProviderCell("1", 0, 0, 2, 2, "期初数"),
            ProviderCell("1", 0, 0, 3, 3, "期末数"),
            ProviderCell("1", 1, 1, 0, 0, "货币资金", confidence=0.91),
            ProviderCell("1", 1, 1, 1, 1, "1", confidence=0.99),
            ProviderCell("1", 1, 1, 2, 2, "100", confidence=0.84),
            ProviderCell("1", 1, 1, 3, 3, "200", confidence=0.88),
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
            text_confidence=None,
            value_confidence=None,
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
