import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from PIL import Image, ImageDraw
from openpyxl import load_workbook

from standardize import cli
from standardize.dedupe import assign_fact_ids, dedupe_facts
from standardize.feedback import apply_review_actions, build_delta_reports, export_review_actions_template, parse_review_actions_file
from standardize.integrity import run_artifact_integrity
from standardize.mapping.review import apply_subject_mapping
from standardize.models import AliasRecord, CellRecord, ConflictRecord, FactRecord, ProviderCell, ProviderPage, ReOCRTaskRecord, RelationRecord, StatementMeta, TemplateSubject, ValidationResultRecord
from standardize.normalize.conflicts import enrich_conflicts, resolve_conflicts
from standardize.normalize.export import export_template
from standardize.normalize.mapping import load_template_subjects
from standardize.normalize.numbers import analyze_numeric_text
from standardize.normalize.periods import apply_period_normalization
from standardize.normalize.tables import standardize_page
from standardize.overrides.periods import apply_period_overrides
from standardize.overrides.storage import ensure_override_store
from standardize.overrides.suppression import apply_suppression_overrides
from standardize.providers.aliyun import extract_aliyun_data
from standardize.review import build_review_queue
from standardize.routing.page_selector import build_page_selection
from standardize.routing.secondary_ocr import materialize_reocr_inputs
from standardize.validation import run_validation


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

    def test_period_normalization_parses_and_inherits(self):
        facts = [
            self.make_fact(
                doc_id="demo-2022年",
                page_no=1,
                statement_type="balance_sheet",
                statement_name_raw="资产负债表",
                col_header_raw="截至2022年12月31日 / 期末数",
                col_header_path=["截至2022年12月31日", "期末数"],
                period_role_raw="期末数",
                period_key="unknown_date__期末数",
                row_label_std="货币资金",
                report_date_raw="",
                report_date_norm="unknown_date",
            ),
            self.make_fact(
                doc_id="demo-2022年",
                page_no=2,
                statement_type="note",
                statement_name_raw="2022年度财务报表附注",
                col_header_raw="本期发生额",
                col_header_path=["本期发生额"],
                period_role_raw="本期",
                period_key="unknown_date__本期",
                row_label_std="管理费用",
                report_date_raw="",
                report_date_norm="unknown_date",
            ),
            self.make_fact(
                doc_id="demo-2022年",
                page_no=3,
                statement_type="note",
                statement_name_raw="财务报表附注",
                col_header_raw="期末数",
                col_header_path=["期末数"],
                period_role_raw="期末数",
                period_key="unknown_date__期末数",
                row_label_std="固定资产净额",
                report_date_raw="",
                report_date_norm="unknown_date",
            ),
        ]
        pages = [
            ProviderPage(
                doc_id="demo-2022年",
                page_no=1,
                provider="aliyun_table",
                source_file="page1.json",
                source_kind="json",
                page_text="资产负债表 2022年12月31日",
                tables={},
                context_lines=["资产负债表", "2022年12月31日"],
            ),
            ProviderPage(
                doc_id="demo-2022年",
                page_no=2,
                provider="aliyun_table",
                source_file="page2.json",
                source_kind="json",
                page_text="2022年度财务报表附注",
                tables={},
                context_lines=["2022年度财务报表附注"],
            ),
            ProviderPage(
                doc_id="demo-2022年",
                page_no=3,
                provider="aliyun_table",
                source_file="page3.json",
                source_kind="json",
                page_text="财务报表附注",
                tables={},
                context_lines=["财务报表附注"],
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "outputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            normalized = apply_period_normalization(
                facts=facts,
                provider_pages=pages,
                input_dir=input_dir,
                keyword_config=self.statement_config(),
                period_config={"source_weights": {"header": 120, "page_context": 95, "page_text": 70, "doc_id": 18}, "repeat_bonus": 2, "tie_threshold": 5},
                enabled=True,
            )

        by_page = {fact.page_no: fact for fact in normalized}
        self.assertEqual(by_page[1].report_date_norm, "2022-12-31")
        self.assertEqual(by_page[1].period_key, "2022-12-31__期末数")
        self.assertEqual(by_page[2].report_date_norm, "2022年度")
        self.assertEqual(by_page[2].period_key, "2022年度__本期")
        self.assertEqual(by_page[3].report_date_norm, "2022-12-31")
        self.assertEqual(by_page[3].period_source_level, "statement")

    def test_dedupe_prefers_explicit_date_over_unknown(self):
        facts = assign_fact_ids(
            [
                self.make_fact(
                    fact_id="",
                    report_date_norm="2022-12-31",
                    period_key="2022-12-31__期末数",
                    period_role_norm="期末数",
                    row_label_std="货币资金",
                    mapping_code="ZT_001",
                    mapping_name="货币资金",
                    value_num=100.0,
                    value_raw="100",
                ),
                self.make_fact(
                    fact_id="",
                    report_date_norm="unknown_date",
                    period_key="unknown_date__期末数",
                    period_role_norm="期末数",
                    row_label_std="货币资金",
                    mapping_code="ZT_001",
                    mapping_name="货币资金",
                    value_num=100.0,
                    value_raw="100",
                ),
            ]
        )

        deduped, duplicates = dedupe_facts(facts, ["aliyun_table", "tencent_table_v3"])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].period_key, "2022-12-31__期末数")
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(duplicates[0].dedupe_reason, "explicit_date_preferred_over_unknown_date")

    def test_provider_compare_metrics(self):
        equal_fact_a = self.make_fact(
            doc_id="demo",
            page_no=4,
            provider="aliyun_table",
            table_semantic_key="balance_sheet|h:项目,期末数",
            row_label_std="货币资金",
            row_label_raw="货币资金",
            column_semantic_key="期末数",
            period_role_raw="期末数",
            period_role_norm="期末数",
            period_key="2022-12-31__期末数",
            value_num=100.0,
            value_raw="100",
        )
        equal_fact_b = self.make_fact(
            doc_id="demo",
            page_no=4,
            provider="tencent_table_v3",
            table_semantic_key="balance_sheet|h:项目,期末数",
            row_label_std="货币资金",
            row_label_raw="货币资金",
            column_semantic_key="期末数",
            period_role_raw="期末数",
            period_role_norm="期末数",
            period_key="2022-12-31__期末数",
            value_num=100.0,
            value_raw="100",
        )
        conflict_fact_a = self.make_fact(
            doc_id="demo",
            page_no=5,
            provider="aliyun_table",
            table_semantic_key="note|h:项目,本期发生额",
            row_label_std="管理费用",
            row_label_raw="管理费用",
            column_semantic_key="本期发生额",
            period_role_raw="本期",
            period_role_norm="本期",
            period_key="2022年度__本期",
            value_num=10.0,
            value_raw="10",
        )
        conflict_fact_b = self.make_fact(
            doc_id="demo",
            page_no=5,
            provider="tencent_table_v3",
            table_semantic_key="note|h:项目,本期发生额",
            row_label_std="管理费用",
            row_label_raw="管理费用",
            column_semantic_key="本期发生额",
            period_role_raw="本期",
            period_role_norm="本期",
            period_key="2022年度__本期",
            value_num=12.0,
            value_raw="12",
        )
        facts = assign_fact_ids([equal_fact_a, equal_fact_b, conflict_fact_a, conflict_fact_b])

        resolved_facts, conflicts, comparisons = resolve_conflicts(
            facts=facts,
            provider_priority=["aliyun_table", "tencent_table_v3"],
            enabled=True,
        )

        self.assertEqual(len(conflicts), 1)
        comparison_by_page = {record.page_no: record for record in comparisons}
        self.assertGreater(comparison_by_page[4].compared_pairs, 0)
        self.assertEqual(comparison_by_page[4].equal_pairs, 1)
        self.assertEqual(comparison_by_page[5].conflict_pairs, 1)
        self.assertTrue(any(fact.comparison_status == "equal" for fact in resolved_facts if fact.page_no == 4))

    def test_conflict_grouping_uses_table_semantics(self):
        facts = assign_fact_ids(
            [
                self.make_fact(
                    doc_id="demo",
                    page_no=13,
                    provider="aliyun_table",
                    table_semantic_key="note|h:单位名称,期末余额,账龄,比例",
                    logical_subtable_id="4_sub1",
                    row_label_raw="合计",
                    row_label_std="合计",
                    column_semantic_key="期末数",
                    period_role_raw="期末数",
                    period_role_norm="期末数",
                    period_key="unknown_date__期末数",
                    value_raw="735，449，914.02",
                    value_num=735449914.02,
                ),
                self.make_fact(
                    doc_id="demo",
                    page_no=13,
                    provider="tencent_table_v3",
                    table_semantic_key="note|h:项目,期初数,期末数",
                    logical_subtable_id="4_sub1",
                    row_label_raw="合计",
                    row_label_std="合计",
                    column_semantic_key="期末数",
                    period_role_raw="期末数",
                    period_role_norm="期末数",
                    period_key="unknown_date__期末数",
                    value_raw="401,700,701.94",
                    value_num=401700701.94,
                ),
            ]
        )

        resolved_facts, conflicts, comparisons = resolve_conflicts(
            facts=facts,
            provider_priority=["aliyun_table", "tencent_table_v3"],
            enabled=True,
        )

        self.assertEqual(len(conflicts), 0)
        self.assertEqual(comparisons[0].compared_pairs, 0)
        self.assertEqual(comparisons[0].reason, "no_aligned_pairs_found")
        self.assertTrue(all(fact.status == "observed" for fact in resolved_facts))

    def test_validation_rules(self):
        facts = assign_fact_ids(
            [
                self.make_fact(mapping_code="ZT_A", mapping_name="资产总计", row_label_std="资产总计", statement_type="balance_sheet", period_key="2022-12-31__期末数", value_num=300.0, value_raw="300"),
                self.make_fact(mapping_code="ZT_B", mapping_name="负债合计", row_label_std="负债合计", statement_type="balance_sheet", period_key="2022-12-31__期末数", value_num=100.0, value_raw="100"),
                self.make_fact(mapping_code="ZT_C", mapping_name="所有者权益合计", row_label_std="所有者权益合计", statement_type="balance_sheet", period_key="2022-12-31__期末数", value_num=200.0, value_raw="200"),
                self.make_fact(logical_subtable_id="sub1", column_semantic_key="金额", row_label_std="明细1", source_row_start=1, value_num=60.0, value_raw="60"),
                self.make_fact(logical_subtable_id="sub1", column_semantic_key="金额", row_label_std="明细2", source_row_start=2, value_num=40.0, value_raw="40"),
                self.make_fact(logical_subtable_id="sub1", column_semantic_key="金额", row_label_std="合计", source_row_start=3, value_num=100.0, value_raw="100"),
                self.make_fact(logical_subtable_id="sub2", column_semantic_key="比例", row_label_std="合计", source_row_start=1, value_num=1.0, value_raw="100%", value_type="ratio"),
                self.make_fact(row_label_std="异常金额", value_raw="务屏20,000,000.00", value_num=None, issue_flags=["numeric_parse_failed", "contains_chinese_noise"], status="review"),
            ]
        )
        validation_config = {
            "balance_equation": {
                "aliases": {
                    "assets_total": ["资产总计"],
                    "liabilities_total": ["负债合计"],
                    "equity_total": ["所有者权益合计", "股东权益合计"],
                },
                "tolerance": {"absolute": 1, "relative": 0.0001},
            },
            "subtotal_rules": {"min_detail_rows": 2, "tolerance": {"absolute": 1, "relative": 0.0001}},
            "ratio_rules": {"expected_total": 1.0, "tolerance": 0.02},
            "amount_legality": {"status_when_noise_detected": "review"},
        }

        results, summary = run_validation(facts, validation_config)

        rule_names = [result.rule_name for result in results]
        self.assertIn("balance_equation", rule_names)
        self.assertIn("subtotal_check", rule_names)
        self.assertIn("ratio_total_check", rule_names)
        self.assertIn("amount_legality", rule_names)
        self.assertGreater(summary["validation_pass_total"], 0)

    def test_mapping_candidate_and_relation_review(self):
        subjects = [
            TemplateSubject(code="ZT_001", canonical_name="货币资金", row_index=4, sheet_name="Sheet1", source_value="ZT_001 货币资金"),
            TemplateSubject(code="ZT_024", canonical_name="应交税费", row_index=5, sheet_name="Sheet1", source_value="ZT_024 应交税费"),
            TemplateSubject(code="ZT_006", canonical_name="应收票据及应收账款", row_index=6, sheet_name="Sheet1", source_value="ZT_006 应收票据及应收账款"),
        ]
        aliases = [
            AliasRecord(canonical_code="ZT_024", canonical_name="应交税费", alias="应交税金", alias_type="legacy_alias", enabled=True),
        ]
        relations = [
            RelationRecord(
                canonical_code="ZT_006",
                canonical_name="应收票据及应收账款",
                relation_type="aggregate_relation",
                related_codes=[],
                related_names=["应收票据和应收账款"],
                enabled=True,
                review_required=True,
            )
        ]
        facts = [
            self.make_fact(row_label_raw="应交税金", row_label_std="应交税金"),
            self.make_fact(row_label_raw="应收票据和应收账款", row_label_std="应收票据和应收账款"),
        ]

        facts, mapping_review, mapping_candidates, unmapped_summary, mapping_stats = apply_subject_mapping(
            facts,
            subjects,
            aliases,
            relations,
            {"max_candidates": 3},
        )

        self.assertEqual(facts[0].mapping_code, "ZT_024")
        self.assertEqual(facts[0].mapping_method, "legacy_alias")
        self.assertEqual(facts[1].mapping_code, "")
        self.assertTrue(any(candidate.relation_type == "aggregate_relation" for candidate in mapping_candidates))
        self.assertTrue(any(row.normalized_label for row in unmapped_summary))
        self.assertEqual(mapping_stats["mapped_by_alias"], 1)

    def test_validation_aware_conflict_resolution(self):
        facts = assign_fact_ids(
            [
                self.make_fact(
                    doc_id="demo",
                    page_no=1,
                    provider="aliyun_table",
                    statement_type="balance_sheet",
                    statement_name_raw="balance_sheet",
                    table_semantic_key="balance_sheet|h:item,end",
                    row_label_raw="assets_total",
                    row_label_std="assets_total",
                    mapping_code="ZT_A",
                    mapping_name="assets_total",
                    column_semantic_key="end",
                    col_header_raw="end",
                    col_header_path=["end"],
                    period_key="2022-12-31__end",
                    period_role_raw="end",
                    period_role_norm="end",
                    report_date_norm="2022-12-31",
                    value_raw="301",
                    value_num=301.0,
                ),
                self.make_fact(
                    doc_id="demo",
                    page_no=1,
                    provider="tencent_table_v3",
                    statement_type="balance_sheet",
                    statement_name_raw="balance_sheet",
                    table_semantic_key="balance_sheet|h:item,end",
                    row_label_raw="assets_total",
                    row_label_std="assets_total",
                    mapping_code="ZT_A",
                    mapping_name="assets_total",
                    column_semantic_key="end",
                    col_header_raw="end",
                    col_header_path=["end"],
                    period_key="2022-12-31__end",
                    period_role_raw="end",
                    period_role_norm="end",
                    report_date_norm="2022-12-31",
                    value_raw="300",
                    value_num=300.0,
                ),
                self.make_fact(
                    doc_id="demo",
                    page_no=1,
                    provider="aliyun_table",
                    statement_type="balance_sheet",
                    statement_name_raw="balance_sheet",
                    table_semantic_key="balance_sheet|h:item,end",
                    row_label_raw="liabilities_total",
                    row_label_std="liabilities_total",
                    mapping_code="ZT_B",
                    mapping_name="liabilities_total",
                    column_semantic_key="end",
                    col_header_raw="end",
                    col_header_path=["end"],
                    period_key="2022-12-31__end",
                    period_role_raw="end",
                    period_role_norm="end",
                    report_date_norm="2022-12-31",
                    value_raw="100",
                    value_num=100.0,
                ),
                self.make_fact(
                    doc_id="demo",
                    page_no=1,
                    provider="aliyun_table",
                    statement_type="balance_sheet",
                    statement_name_raw="balance_sheet",
                    table_semantic_key="balance_sheet|h:item,end",
                    row_label_raw="equity_total",
                    row_label_std="equity_total",
                    mapping_code="ZT_C",
                    mapping_name="equity_total",
                    column_semantic_key="end",
                    col_header_raw="end",
                    col_header_path=["end"],
                    period_key="2022-12-31__end",
                    period_role_raw="end",
                    period_role_norm="end",
                    report_date_norm="2022-12-31",
                    value_raw="200",
                    value_num=200.0,
                ),
            ]
        )
        resolved, conflicts, _ = resolve_conflicts(facts, ["aliyun_table", "tencent_table_v3"], enabled=False)
        resolved, enriched, audits, impacts = enrich_conflicts(
            facts=resolved,
            conflicts=conflicts,
            provider_priority=["aliyun_table", "tencent_table_v3"],
            validation_config={
                "balance_equation": {
                    "aliases": {
                        "assets_total": ["assets_total"],
                        "liabilities_total": ["liabilities_total"],
                        "equity_total": ["equity_total"],
                    },
                    "tolerance": {"absolute": 0.001, "relative": 0.000001},
                },
                "subtotal_rules": {"min_detail_rows": 2, "tolerance": {"absolute": 1, "relative": 0.0001}},
                "ratio_rules": {"expected_total": 1.0, "tolerance": 0.02},
                "amount_legality": {"status_when_noise_detected": "review"},
            },
            conflict_config={"magnitude_ratio_review_threshold": 10, "magnitude_ratio_force_review_threshold": 100},
            merge_enabled=True,
            validation_aware_enabled=True,
        )

        self.assertEqual(enriched[0].decision, "accepted_with_validation_support")
        self.assertEqual(enriched[0].accepted_provider, "tencent_table_v3")
        self.assertTrue(audits)
        self.assertTrue(impacts)

    def test_large_magnitude_conflict_requires_review(self):
        facts = assign_fact_ids(
            [
                self.make_fact(
                    doc_id="demo",
                    page_no=17,
                    provider="aliyun_table",
                    statement_type="note",
                    table_semantic_key="note|h:项目,本期发生额",
                    row_label_raw="财产保险费",
                    row_label_std="财产保险费",
                    column_semantic_key="本期发生额",
                    col_header_raw="本期发生额",
                    col_header_path=["本期发生额"],
                    period_key="2022年度__本期",
                    period_role_raw="本期",
                    period_role_norm="本期",
                    report_date_norm="2022年度",
                    value_raw="85533955",
                    value_num=85533955.0,
                ),
                self.make_fact(
                    doc_id="demo",
                    page_no=17,
                    provider="tencent_table_v3",
                    statement_type="note",
                    table_semantic_key="note|h:项目,本期发生额",
                    row_label_raw="财产保险费",
                    row_label_std="财产保险费",
                    column_semantic_key="本期发生额",
                    col_header_raw="本期发生额",
                    col_header_path=["本期发生额"],
                    period_key="2022年度__本期",
                    period_role_raw="本期",
                    period_role_norm="本期",
                    report_date_norm="2022年度",
                    value_raw="85,334.55",
                    value_num=85334.55,
                ),
            ]
        )
        resolved, conflicts, _ = resolve_conflicts(facts, ["aliyun_table", "tencent_table_v3"], enabled=False)
        resolved, enriched, audits, impacts = enrich_conflicts(
            facts=resolved,
            conflicts=conflicts,
            provider_priority=["aliyun_table", "tencent_table_v3"],
            validation_config={"balance_equation": {}, "subtotal_rules": {}, "ratio_rules": {}, "amount_legality": {"status_when_noise_detected": "review"}},
            conflict_config={"magnitude_ratio_review_threshold": 10, "magnitude_ratio_force_review_threshold": 100},
            merge_enabled=True,
            validation_aware_enabled=True,
        )
        self.assertEqual(enriched[0].decision, "review_required")
        self.assertGreaterEqual(enriched[0].magnitude_ratio, 100)

    def test_export_and_integrity_contract(self):
        template_path = Path(__file__).resolve().parent.parent / "会计报表.xlsx"
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            facts = assign_fact_ids(
                [
                    self.make_fact(
                        mapping_code="ZT_001",
                        mapping_name="货币资金",
                        row_label_std="货币资金",
                        period_key="2022-12-31__期末数",
                        report_date_norm="2022-12-31",
                        period_role_norm="期末数",
                        value_raw="100",
                        value_num=100.0,
                    ),
                    self.make_fact(
                        mapping_code="ZT_001",
                        mapping_name="货币资金",
                        row_label_std="货币资金",
                        period_key="unknown_date__期末数",
                        report_date_norm="unknown_date",
                        period_role_norm="期末数",
                        value_raw="100",
                        value_num=100.0,
                    ),
                ]
            )
            export_stats = export_template(
                template_path=template_path,
                output_path=output_dir / "会计报表_填充结果.xlsx",
                facts=facts,
                run_summary={"unknown_date_total": 0},
                issues=[],
                validations=[],
                duplicates=[],
                conflicts=[],
                review_queue=[],
                export_rules={"allowed_statuses": ["observed", "repaired"]},
            )
            write_rows = [
                {
                    "fact_id": fact.fact_id,
                    "mapping_code": fact.mapping_code,
                    "period_key": fact.period_key,
                    "status": fact.status,
                    "conflict_decision": fact.conflict_decision,
                    "report_date_norm": fact.report_date_norm,
                    "period_role_norm": fact.period_role_norm,
                    "value_num": fact.value_num,
                    "mapping_review_required": fact.mapping_review_required,
                }
                for fact in facts
            ]
            with (output_dir / "facts_deduped.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(write_rows[0].keys()))
                writer.writeheader()
                writer.writerows(write_rows)
            with (output_dir / "conflicts_enriched.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["conflict_id", "decision", "accepted_fact_id", "provider_values_json"])
                writer.writeheader()

            integrity = run_artifact_integrity(
                output_dir=output_dir,
                workbook_path=output_dir / "会计报表_填充结果.xlsx",
                run_summary={"unknown_date_total": 0},
                export_stats=export_stats,
                export_rules={"required_helper_sheets": ["_meta_summary", "_issues", "_validation", "_duplicates", "_conflicts", "_unplaced_facts", "_review_queue"]},
            )

            workbook = load_workbook(output_dir / "会计报表_填充结果.xlsx")
            headers = [workbook[workbook.sheetnames[0]].cell(row=3, column=idx).value for idx in range(1, workbook[workbook.sheetnames[0]].max_column + 1)]
            self.assertIn("2022-12-31__期末数", headers)
            self.assertNotIn("unknown_date__期末数", headers)
            for sheet_name in ["_meta_summary", "_validation", "_duplicates", "_conflicts", "_unplaced_facts", "_review_queue"]:
                self.assertIn(sheet_name, workbook.sheetnames)
            self.assertEqual(integrity["summary"]["integrity_fail_total"], 0)

    def test_review_queue_generates_crops(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            source_dir = base / "data"
            source_dir.mkdir(parents=True, exist_ok=True)
            image = Image.new("RGB", (300, 300), "white")
            draw = ImageDraw.Draw(image)
            draw.rectangle((50, 50, 200, 120), outline="black", width=3)
            pdf_path = source_dir / "demo.pdf"
            image.save(pdf_path, "PDF")

            bbox = json.dumps([{"x": 50, "y": 50}, {"x": 200, "y": 50}, {"x": 200, "y": 120}, {"x": 50, "y": 120}], ensure_ascii=False)
            cell = CellRecord(
                doc_id="demo",
                page_no=1,
                provider="aliyun_table",
                source_file="page_0001.json",
                table_id="1",
                logical_subtable_id="1_sub1",
                row_start=1,
                row_end=1,
                col_start=1,
                col_end=1,
                bbox_json=bbox,
                text_raw="务屏20,000,000.00",
                text_clean="务屏20,000,000.00",
                ocr_conf=None,
                is_empty=False,
                is_header=False,
                is_suspicious=True,
                suspicious_reason="contains_chinese_noise",
                repair_status="raw",
                meta_json="",
            )
            fact = assign_fact_ids(
                [
                    self.make_fact(
                        doc_id="demo",
                        page_no=1,
                        provider="aliyun_table",
                        source_cell_ref="demo:1:aliyun_table:1:1-1:1-1",
                        row_label_raw="异常金额",
                        row_label_std="异常金额",
                        mapping_code="",
                        value_raw="务屏20,000,000.00",
                        value_num=None,
                        issue_flags=["numeric_parse_failed", "contains_chinese_noise"],
                        status="review",
                    )
                ]
            )
            validation = [
                self.make_validation(
                    validation_id="VAL000001",
                    doc_id="demo",
                    period_key="2022年度__本期",
                    rule_name="amount_legality",
                    status="review",
                    evidence_fact_refs=["demo:1:aliyun_table:1:1-1:1-1"],
                )
            ]

            review_items, summary = build_review_queue(
                facts=fact,
                cells=[cell],
                issues=[],
                conflicts=[],
                validations=validation,
                mapping_candidates=[],
                source_image_dir=source_dir,
                output_dir=base / "normalized",
                review_config={"crop_padding": 8, "reason_weights": {"validation": 3.0, "mapping": 2.0, "quality": 2.0}},
                generate_evidence=True,
            )

            self.assertTrue(review_items)
            self.assertTrue((base / "normalized" / "review_pack" / "index.csv").exists())
            self.assertTrue(any(item.evidence_cell_path for item in review_items))

    def test_page_selector(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            image_dir = base / "images"
            input_dir = base / "outputs"
            image_dir.mkdir(parents=True, exist_ok=True)
            input_dir.mkdir(parents=True, exist_ok=True)

            table_image = Image.new("RGB", (800, 1000), "white")
            draw = ImageDraw.Draw(table_image)
            for x in range(50, 750, 100):
                draw.line((x, 100, x, 900), fill="black", width=3)
            for y in range(100, 900, 80):
                draw.line((50, y, 750, y), fill="black", width=3)
            table_path = image_dir / "page_0001.png"
            table_image.save(table_path)

            text_image = Image.new("RGB", (800, 1000), "white")
            draw = ImageDraw.Draw(text_image)
            for idx in range(15):
                draw.text((80, 80 + idx * 45), f"plain text paragraph line {idx}", fill="black")
            text_path = image_dir / "page_0002.png"
            text_image.save(text_path)

            records, plan = build_page_selection(
                source_image_dir=image_dir,
                input_dir=input_dir,
                routing_config={
                    "pre_ocr": {
                        "selection_threshold": 0.45,
                        "strong_horizontal_ratio": 0.35,
                        "strong_vertical_ratio": 0.18,
                        "table_likelihood_trigger": 0.45,
                        "line_density_trigger": 0.45,
                        "numeric_density_trigger": 0.35,
                        "hard_keyword_hits": 2,
                        "weights": {
                            "table_likelihood_score": 0.4,
                            "numeric_density_score": 0.2,
                            "line_density_score": 0.3,
                            "keyword_score": 0.1,
                        },
                        "keywords": [],
                    }
                },
            )

            by_page = {record.page_no: record for record in records}
            self.assertTrue(by_page[1].is_candidate_table_page)
            self.assertFalse(by_page[2].is_candidate_table_page)
            self.assertEqual(plan["pages_total"], 2)

    def test_review_action_parsing_and_application(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            config_dir = base / "config"
            ensure_override_store(config_dir)
            action_csv = base / "review_actions.csv"
            rows = [
                {
                    "review_id": "REV_demo_1",
                    "action_type": "accept_mapping_alias",
                    "candidate_mapping_code": "ZT_001",
                    "candidate_mapping_name": "货币资金",
                    "row_label_std": "货币资金",
                    "row_label_raw": "货币资金",
                    "action_value": "",
                    "reviewer_note": "accepted",
                    "reviewer_name": "tester",
                },
                {
                    "review_id": "REV_demo_1",
                    "action_type": "unsupported_action",
                    "candidate_mapping_code": "",
                    "candidate_mapping_name": "",
                    "row_label_std": "货币资金",
                    "row_label_raw": "货币资金",
                    "action_value": "",
                    "reviewer_note": "",
                    "reviewer_name": "",
                },
            ]
            with action_csv.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

            parsed = parse_review_actions_file(action_csv)
            applied, rejected, audit_rows, summary = apply_review_actions(parsed, ["REV_demo_1"], config_dir)

            self.assertEqual(len(applied), 1)
            self.assertEqual(len(rejected), 1)
            self.assertEqual(summary["applied_total"], 1)
            self.assertTrue((config_dir / "manual_overrides" / "mapping_overrides.yml").exists())
            self.assertEqual(audit_rows[0]["action_type"], "accept_mapping_alias")

    def test_period_override_and_suppression_affect_fact_state(self):
        facts = assign_fact_ids(
            [
                self.make_fact(
                    period_key="2022年度__本期",
                    report_date_norm="2022年度",
                    period_role_norm="本期",
                    value_raw="100",
                    value_num=100.0,
                )
            ]
        )
        fact_id = facts[0].fact_id
        facts = apply_period_overrides(
            facts,
            [{"fact_id": fact_id, "period_key": "2022-12-31__期末数", "report_date_norm": "2022-12-31", "period_role_norm": "期末数", "note": "manual"}],
        )
        facts = apply_suppression_overrides(
            facts,
            [{"fact_id": fact_id, "action_type": "suppress_false_positive", "note": "false_positive"}],
        )
        self.assertEqual(facts[0].period_key, "2022-12-31__期末数")
        self.assertEqual(facts[0].report_date_norm, "2022-12-31")
        self.assertEqual(facts[0].status, "suppressed")

    def test_delta_reporting(self):
        before = {
            "run_summary": {"mapped_facts_ratio": 0.1, "amount_coverage_ratio": 0.2, "review_total": 10, "validation_fail_total": 3, "provider_conflict_pairs": 2},
            "review_rows": [{"review_id": "REV1", "priority_score": "5", "row_label_std": "货币资金", "period_key": "2022-12-31__期末数"}],
            "unmapped_rows": [{"row_label_std": "未知科目", "occurrences": "3", "amount_abs_total": "100"}],
            "reocr_rows": [{"task_id": "REOCR1"}],
            "conflict_rows": [{"decision": "review_required"}],
            "facts_rows": [{"mapping_code": "ZT_001", "value_num": "1", "report_date_norm": "2022-12-31", "period_role_norm": "期末数", "status": "observed", "conflict_decision": "", "unplaced_reason": ""}],
            "unplaced_rows": [{"fact_id": "F1"}],
        }
        after = {
            "run_summary": {"mapped_facts_ratio": 0.2, "amount_coverage_ratio": 0.25, "review_total": 4, "validation_fail_total": 1, "provider_conflict_pairs": 1},
            "review_rows": [],
            "unmapped_rows": [{"row_label_std": "未知科目", "occurrences": "1", "amount_abs_total": "10"}],
            "reocr_rows": [],
            "conflict_rows": [],
            "facts_rows": [{"mapping_code": "ZT_001", "value_num": "1", "report_date_norm": "2022-12-31", "period_role_norm": "期末数", "status": "observed", "conflict_decision": "", "unplaced_reason": ""}],
            "unplaced_rows": [],
        }
        payload = build_delta_reports(before, after)
        metric_map = {row["metric"]: row for row in payload["coverage_rows"]}
        self.assertEqual(metric_map["mapped_facts_ratio"]["delta"], 0.1)
        self.assertEqual(metric_map["review_total"]["delta"], -6.0)
        self.assertTrue(any(row["status"] == "resolved" for row in payload["review_delta_rows"]))

    def test_materialize_reocr_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            source_dir = base / "data"
            output_dir = base / "normalized"
            source_dir.mkdir(parents=True, exist_ok=True)
            image = Image.new("RGB", (300, 300), "white")
            draw = ImageDraw.Draw(image)
            draw.rectangle((50, 50, 200, 120), outline="black", width=3)
            (source_dir / "demo.pdf").parent.mkdir(parents=True, exist_ok=True)
            image.save(source_dir / "demo.pdf", "PDF")
            task = ReOCRTaskRecord(
                task_id="REOCR_demo",
                granularity="cell",
                doc_id="demo",
                page_no=1,
                table_id="1",
                logical_subtable_id="1_sub1",
                bbox=json.dumps([50, 50, 200, 120], ensure_ascii=False),
                reason_codes=["quality:suspicious_numeric"],
                suggested_provider="tencent_table_v3",
                priority_score=5.0,
                expected_benefit="improve_numeric_legibility",
                source_review_id="REV_demo",
                meta_json="",
            )

            manifest_rows, summary = materialize_reocr_inputs([task], source_dir, output_dir)

            self.assertEqual(summary["materialized_total"], 1)
            self.assertTrue(Path(manifest_rows[0]["crop_path"]).exists())

    def test_export_writes_applied_actions_sheet(self):
        repo_root = Path(__file__).resolve().parent
        template_path = repo_root.parent / "会计报表.xlsx"
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            facts = assign_fact_ids(
                [
                    self.make_fact(
                        mapping_code="ZT_001",
                        mapping_name="货币资金",
                        row_label_std="货币资金",
                        period_key="2022-12-31__期末数",
                        report_date_norm="2022-12-31",
                        period_role_norm="期末数",
                        value_raw="100",
                        value_num=100.0,
                    )
                ]
            )
            export_template(
                template_path=template_path,
                output_path=output_dir / "会计报表_填充结果.xlsx",
                facts=facts,
                run_summary={"unknown_date_total": 0},
                issues=[],
                validations=[],
                duplicates=[],
                conflicts=[],
                review_queue=[],
                applied_actions=[{"action_id": "ACT_1", "review_id": "REV_1", "action_type": "accept_mapping_alias"}],
                export_rules={"allowed_statuses": ["observed", "repaired"], "required_helper_sheets": ["_applied_actions"]},
            )
            workbook = load_workbook(output_dir / "会计报表_填充结果.xlsx")
            self.assertIn("_applied_actions", workbook.sheetnames)
            self.assertEqual(workbook["_applied_actions"].cell(row=1, column=1).value, "action_id")

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
                    "--source-image-dir",
                    str(repo_root / "data"),
                    "--provider-priority",
                    "aliyun,tencent",
                    "--enable-conflict-merge",
                    "--enable-period-normalization",
                    "--enable-dedupe",
                    "--enable-validation",
                    "--enable-integrity-check",
                    "--enable-mapping-suggestions",
                    "--enable-review-pack",
                    "--enable-validation-aware-conflicts",
                    "--emit-routing-plan",
                    "--emit-reocr-tasks",
                    "--emit-review-actions-template",
                    "--materialize-reocr-inputs",
                    "--emit-delta-report",
                ]
            )

            self.assertEqual(exit_code, 0)
            for filename in [
                "cells.csv",
                "facts_raw.csv",
                "facts_deduped.csv",
                "facts.csv",
                "duplicates.csv",
                "provider_comparison_summary.csv",
                "validation_results.csv",
                "mapping_candidates.csv",
                "unmapped_labels_summary.csv",
                "conflicts_enriched.csv",
                "conflict_decision_audit.csv",
                "validation_impact_of_conflicts.csv",
                "review_queue.csv",
                "review_summary.json",
                "review_workbook.xlsx",
                "reocr_tasks.csv",
                "reocr_task_summary.json",
                "review_actions_template.xlsx",
                "review_actions_template.csv",
                "reocr_input_manifest.csv",
                "reocr_input_manifest.json",
                "artifact_integrity.json",
                "run_summary.json",
                "会计报表_填充结果.xlsx",
            ]:
                self.assertTrue((output_dir / filename).exists(), filename)

            with (output_dir / "run_summary.json").open("r", encoding="utf-8") as handle:
                summary = json.load(handle)
            self.assertLessEqual(summary["facts_deduped_total"], summary["facts_raw_total"])
            self.assertGreater(summary["provider_compared_pairs"], 0)

            with (output_dir / "facts_deduped.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(any(row["row_label_raw"] == "货币资金" for row in rows))

            workbook = load_workbook(output_dir / "会计报表_填充结果.xlsx")
            worksheet = workbook[workbook.sheetnames[0]]
            headers = [worksheet.cell(row=3, column=idx).value for idx in range(1, worksheet.max_column + 1)]
            self.assertIn("2022-12-31__期初数", headers)
            self.assertIn("2022-12-31__期末数", headers)
            self.assertFalse(any(str(header).startswith("unknown_date__") for header in headers if header))
            self.assertFalse(any(str(header).endswith("__unknown") for header in headers if header))
            self.assertIn("_meta_summary", workbook.sheetnames)
            self.assertIn("_validation", workbook.sheetnames)
            self.assertIn("_duplicates", workbook.sheetnames)
            self.assertIn("_conflicts", workbook.sheetnames)
            self.assertIn("_unplaced_facts", workbook.sheetnames)
            self.assertIn("_review_queue", workbook.sheetnames)

            with (output_dir / "review_actions_template.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                action_rows = list(csv.DictReader(handle))
            self.assertTrue(action_rows)
            sample_action_file = output_dir / "review_actions_filled.csv"
            sample_row = action_rows[0]
            sample_row["action_type"] = "ignore"
            sample_row["reviewer_name"] = "tester"
            sample_row["review_status"] = "done"
            with sample_action_file.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=action_rows[0].keys())
                writer.writeheader()
                writer.writerow(sample_row)

            second_exit_code = cli.main(
                [
                    "--input-dir",
                    str(input_dir),
                    "--template",
                    str(template_path),
                    "--output-dir",
                    str(output_dir),
                    "--source-image-dir",
                    str(repo_root / "data"),
                    "--provider-priority",
                    "aliyun,tencent",
                    "--enable-conflict-merge",
                    "--enable-period-normalization",
                    "--enable-dedupe",
                    "--enable-validation",
                    "--enable-integrity-check",
                    "--enable-mapping-suggestions",
                    "--enable-review-pack",
                    "--enable-validation-aware-conflicts",
                    "--emit-routing-plan",
                    "--emit-reocr-tasks",
                    "--emit-review-actions-template",
                    "--review-actions-file",
                    str(sample_action_file),
                    "--apply-review-actions",
                    "--materialize-reocr-inputs",
                    "--emit-delta-report",
                ]
            )
            self.assertEqual(second_exit_code, 0)
            for filename in [
                "applied_review_actions.csv",
                "override_audit.csv",
                "review_decision_summary.json",
                "coverage_delta.json",
                "coverage_delta.csv",
                "review_delta.csv",
                "export_delta_summary.json",
                "top_resolved_items.csv",
                "review_priority_backlog.csv",
                "mapping_opportunities.csv",
            ]:
                self.assertTrue((output_dir / filename).exists(), filename)
            workbook = load_workbook(output_dir / "会计报表_填充结果.xlsx")
            self.assertIn("_applied_actions", workbook.sheetnames)

    def copy_sample_page_0004(self, repo_root: Path, input_dir: Path) -> None:
        doc_name = "债务人审计报告-2022年"

        aliyun_raw_dir = input_dir / "aliyun_table" / doc_name / "raw"
        aliyun_raw_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            next((repo_root / "outputs" / "aliyun_table").glob(f"*/raw/page_0004.json")),
            aliyun_raw_dir / "page_0004.json",
        )
        aliyun_result = {
            "provider": "aliyun_table",
            "pages": [
                {
                    "page_number": 4,
                    "text": "资产负债表 编制单位：泰兴市泰泽实业有限公司 2022年12月31日 单位：元",
                    "raw_file": "raw/page_0004.json",
                    "artifact_files": [],
                }
            ],
        }
        (input_dir / "aliyun_table" / doc_name / "result.json").write_text(
            json.dumps(aliyun_result, ensure_ascii=False, indent=2),
            encoding="utf-8",
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

        # Copy text-provider result.json so period normalization and routing can use cheap text hints.
        for provider in ["aliyun_text", "tencent_text"]:
            provider_doc_dir = input_dir / provider / doc_name
            provider_doc_dir.mkdir(parents=True, exist_ok=True)
            provider_result = {
                "provider": provider,
                "pages": [
                    {
                        "page_number": 4,
                        "text": "资产负债表\n编制单位：泰兴市泰泽实业有限公司\n2022年12月31日\n单位：元",
                    }
                ],
            }
            (provider_doc_dir / "result.json").write_text(
                json.dumps(provider_result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def make_fact(self, **overrides):
        base = {
            "doc_id": "demo",
            "page_no": 1,
            "provider": "aliyun_table",
            "statement_type": "note",
            "statement_name_raw": "2022年度财务报表附注",
            "logical_subtable_id": "1_sub1",
            "table_semantic_key": "note|h:项目,上期发生额,本期发生额",
            "row_label_raw": "项目",
            "row_label_std": "项目",
            "col_header_raw": "本期发生额",
            "col_header_path": ["本期发生额"],
            "column_semantic_key": "本期发生额",
            "period_role_raw": "本期",
            "report_date_raw": "",
            "period_key": "2022年度__本期",
            "value_raw": "1",
            "value_num": 1.0,
            "value_type": "amount",
            "unit_raw": "元",
            "unit_multiplier": 1.0,
            "source_cell_ref": "demo:1:aliyun_table:1:1-1:1-1",
            "status": "observed",
            "mapping_code": "",
            "mapping_name": "",
            "mapping_method": "",
            "mapping_confidence": None,
            "issue_flags": [],
            "fact_id": "",
            "report_date_norm": "2022年度",
            "period_role_norm": "本期",
            "period_source_level": "page",
            "period_reason": "",
            "duplicate_group_id": "",
            "kept_fact_id": "",
            "comparison_status": "single_provider",
            "comparison_reason": "single_provider_only",
            "source_kind": "json",
            "statement_group_key": "note|2022年度财务报表附注",
            "source_row_start": 1,
            "source_row_end": 1,
            "source_col_start": 1,
            "source_col_end": 1,
        }
        base.update(overrides)
        return FactRecord(**base)

    def make_validation(self, **overrides):
        base = {
            "validation_id": "VAL000001",
            "doc_id": "demo",
            "statement_type": "note",
            "period_key": "2022年度__本期",
            "rule_name": "amount_legality",
            "rule_type": "quality",
            "lhs_value": None,
            "rhs_value": None,
            "diff_value": None,
            "tolerance": None,
            "status": "review",
            "evidence_fact_refs": [],
            "message": "review",
            "meta_json": "",
        }
        base.update(overrides)
        return ValidationResultRecord(**base)

    def statement_config(self):
        return {
            "statement_titles": {
                "balance_sheet": ["资产负债表"],
                "income_statement": ["利润表", "损益表"],
                "cash_flow": ["现金流量表"],
                "equity_statement": ["所有者权益变动表", "股东权益变动表"],
                "note": ["附注", "财务报表附注"],
            },
            "period_roles": {
                "期初数": ["期初数"],
                "期末数": ["期末数"],
                "本期": ["本期", "本年累计"],
                "上期": ["上期", "上年同期"],
            },
        }


if __name__ == "__main__":
    unittest.main()
