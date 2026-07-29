from __future__ import annotations

import argparse
import csv
import tempfile
import unittest
from pathlib import Path

from standard_map.cli import main as standard_map_main
from standard_map.confidence import apply_confidence_bulk_accept, build_confidence_bulk_accept_preview, eligible_for_accept_once_by_confidence
from standard_map.decisions import append_mapping_decision_file, apply_mapping_decision_to_output
from standard_map.llm import (
    MockDeepSeekClient,
    build_llm_cache_key,
    build_llm_context,
    build_llm_mapping_prompts,
    is_valid_deepseek_api_key,
    load_deepseek_config,
    parse_llm_mapping_response,
)
from standard_map.models import STANDARD_OUTPUT_COLUMNS
from standard_map.mapper import run_standard_mapping
from standard_map.normalizer import normalize_metric_name
from standard_map.policy import default_confidence_threshold
from standard_map.registry import load_standard_registry
from standard_map.store import LocalMappingStore


class StandardMapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.tempdir.name)
        self.raw_metrics_root = self.temp_path / "raw_metrics"
        self.standard_metrics_root = self.temp_path / "standard_metrics"
        self.mapping_store_root = self.temp_path / "mapping_store"
        self.raw_metrics_root.mkdir(parents=True, exist_ok=True)
        self.standard_metrics_root.mkdir(parents=True, exist_ok=True)
        self.mapping_store_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_raw_metrics(self, rows: list[dict[str, object]]) -> tuple[tempfile.TemporaryDirectory, Path]:
        tempdir = tempfile.TemporaryDirectory(dir=self.raw_metrics_root)
        run_dir = Path(tempdir.name) / "RUN_TEST"
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "raw_metrics.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["填表日期", "当前条目日期", "期间类型", "公司名", "指标名", "指标数值"])
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in writer.fieldnames})
        detailed = run_dir / "raw_metrics_detailed.csv"
        with detailed.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["source_cell_ref", "page_no", "bbox_json", "evidence_path", "source_file", "provider", "doc_id", "statement_type"],
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
                        "statement_type": rows[index - 1].get("_statement_type", "balance_sheet"),
                    }
                )
        return tempdir, path

    def _new_store_path(self) -> Path:
        tempdir = tempfile.TemporaryDirectory(dir=self.mapping_store_root)
        self.addCleanup(tempdir.cleanup)
        return Path(tempdir.name) / "local_mappings.sqlite"

    def _run_mapping(
        self,
        rows: list[dict[str, object]],
        *,
        mapping_store_path: Path | None = None,
        llm_mock: bool = False,
        enable_llm_mapping: bool = False,
        disable_llm_mapping: bool = True,
        disable_llm_cache: bool = False,
    ):
        raw_temp, input_path = self._write_raw_metrics(rows)
        output_temp = tempfile.TemporaryDirectory(dir=self.standard_metrics_root)
        mapping_store_path = mapping_store_path or self._new_store_path()
        args = argparse.Namespace(
            input=str(input_path),
            output_dir=output_temp.name,
            mapping_registry="config/standard_terms.yml",
            mapping_store_path=str(mapping_store_path),
            doc_id="DTEST",
            company_name="",
            enable_llm_mapping=enable_llm_mapping,
            disable_llm_mapping=disable_llm_mapping and not llm_mock and not enable_llm_mapping,
            llm_mock=llm_mock,
            disable_llm_cache=disable_llm_cache,
            debug=False,
            raw_metrics_root=str(self.raw_metrics_root),
            standard_metrics_root=str(self.standard_metrics_root),
        )
        result = run_standard_mapping(args=args, cli_args=["--input", str(input_path), "--output-dir", output_temp.name])
        return raw_temp, output_temp, result

    def _read_output_row(self, output_dir: str, filename: str = "standardized_metrics_detailed.csv") -> dict[str, str]:
        with (Path(output_dir) / filename).open("r", encoding="utf-8-sig", newline="") as handle:
            return next(csv.DictReader(handle))

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

    def test_template_registry_uses_canonical_194_codes(self):
        registry = load_standard_registry()
        template_terms = [term for term in registry.terms if term.code.startswith("ZT_")]
        self.assertEqual(len(template_terms), 194)
        self.assertEqual(registry.term_by_code["ZT_002"].name, "结算备付金")
        self.assertEqual(registry.term_by_code["ZT_068"].name, "短期借款")
        self.assertEqual(registry.term_by_code["ZT_138"].name, "营业收入")
        self.assertEqual(registry.term_by_code["ZT_182"].name, "净利润")

    def test_store_migrates_conflicting_legacy_codes_by_standard_name(self):
        store = LocalMappingStore(self._new_store_path())
        store.add_alias(
            alias="旧短借名称",
            standard_code="ZT_002",
            standard_name="短期借款",
            approved_by="tester",
        )
        store.record_decision(
            job_id="JOB_OLD_NAMESPACE",
            raw_metric_name="旧短借名称",
            suggested_code="ZT_002",
            suggested_name="短期借款",
            decision="accept_once",
            final_code="ZT_002",
            final_name="短期借款",
            decided_by="tester",
        )

        registry = load_standard_registry()
        store.sync_registry(registry)

        alias = next(row for row in store.alias_rows(include_base=False) if row["alias"] == "旧短借名称")
        decision = next(row for row in store.decision_rows() if row["job_id"] == "JOB_OLD_NAMESPACE")
        self.assertEqual(alias["standard_code"], "ZT_068")
        self.assertEqual(alias["standard_name"], "短期借款")
        self.assertEqual(decision["suggested_code"], "ZT_068")
        self.assertEqual(decision["final_code"], "ZT_068")
        self.assertGreaterEqual(store.count("namespace_migrations"), 3)

    def test_exact_standard_term_mapping(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("货币资金", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "mapped")
            self.assertEqual(mapped.mapping_method, "exact")
            self.assertEqual(mapped.confidence, 1.0)
            self.assertEqual(mapped.standard_code, "ZT_001")
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_exact_mapping_without_temporal_key_requires_review(self):
        row = self._row("货币资金", "100")
        row["当前条目日期"] = ""
        row["期间类型"] = "金额"
        raw_temp, output_temp, result = self._run_mapping([row])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.standard_code, "ZT_001")
            self.assertEqual(mapped.mapping_status, "review_required")
            self.assertTrue(mapped.review_required)
            self.assertIn("missing_temporal_key", mapped.issue_reason)
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

    def test_human_approved_local_alias_auto_maps_future_runs(self):
        store_path = self._new_store_path()
        store = LocalMappingStore(store_path)
        store.add_alias(alias="本地记忆货币", standard_code="ZT_001", standard_name="货币资金", approved_by="tester")
        raw_temp, output_temp, result = self._run_mapping([self._row("本地记忆货币", "100")], mapping_store_path=store_path)
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "mapped")
            self.assertEqual(mapped.mapping_method, "local_alias")
            self.assertEqual(mapped.standard_code, "ZT_001")
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_remembered_alias_is_scoped_by_company_and_statement_type(self):
        store_path = self._new_store_path()
        store = LocalMappingStore(store_path)
        store.record_decision(
            job_id="JOB_SCOPE",
            raw_metric_name="本公司专用货币",
            decision="accept_and_remember",
            final_code="ZT_001",
            final_name="货币资金",
            relation_type="exact_alias",
            company_name="AAA有限公司",
            statement_type="balance_sheet",
        )

        matching = self._row("本公司专用货币", "100")
        wrong_company = self._row("本公司专用货币", "100")
        wrong_company["公司名"] = "BBB有限公司"
        wrong_statement = self._row("本公司专用货币", "100")
        wrong_statement["_statement_type"] = "income_statement"

        runs = [
            self._run_mapping([matching], mapping_store_path=store_path),
            self._run_mapping([wrong_company], mapping_store_path=store_path),
            self._run_mapping([wrong_statement], mapping_store_path=store_path),
        ]
        try:
            self.assertEqual(runs[0][2].rows[0].mapping_method, "local_alias")
            self.assertNotEqual(runs[1][2].rows[0].mapping_method, "local_alias")
            self.assertNotEqual(runs[2][2].rows[0].mapping_method, "local_alias")
            alias = next(row for row in store.alias_rows(include_base=False) if row["alias"] == "本公司专用货币")
            self.assertEqual(alias["scope_company"], "AAA有限公司")
            self.assertEqual(alias["scope_statement_type"], "balance_sheet")
        finally:
            for raw_temp, output_temp, _ in runs:
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

    def test_split_relation_requires_review(self):
        raw_temp, output_temp, result = self._run_mapping([self._row("原材料", "100")])
        try:
            mapped = result.rows[0]
            self.assertEqual(mapped.mapping_status, "review_required")
            self.assertEqual(mapped.mapping_method, "relation_review")
            self.assertEqual(mapped.relation_type, "split")
            self.assertIn("split", mapped.issue_reason)
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
            self.assertEqual(rows[0]["action_default"], "accept_once")
            self.assertIn("accept_and_remember", rows[0]["action_options"])
            self.assertIn("映射置信度", STANDARD_OUTPUT_COLUMNS)
            self.assertIn("口径关系", STANDARD_OUTPUT_COLUMNS)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_human_decisions_store_export_and_current_output(self):
        store_path = self._new_store_path()
        raw_temp, output_temp, result = self._run_mapping([self._row("测试本次采用术语", "100")], mapping_store_path=store_path)
        try:
            store = LocalMappingStore(store_path)
            raw = result.rows[0].raw

            reject = store.record_decision(
                job_id="JOB1",
                doc_id="DTEST",
                raw_metric_id=raw.raw_metric_id,
                raw_metric_name=raw.metric_name,
                suggested_code="ZT_001",
                suggested_name="货币资金",
                decision="reject",
                decided_by="tester",
            )
            apply_mapping_decision_to_output(result.output_dir, reject)
            append_mapping_decision_file(Path(result.output_dir), reject)
            self.assertEqual(store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"), 0)
            row = self._read_output_row(result.output_dir)
            self.assertEqual(row["映射状态"], "unmapped")
            self.assertEqual(row["标准指标编码"], "")

            accept_once = store.record_decision(
                job_id="JOB1",
                doc_id="DTEST",
                raw_metric_id=raw.raw_metric_id,
                raw_metric_name=raw.metric_name,
                suggested_code="ZT_001",
                suggested_name="货币资金",
                decision="accept_once",
                final_code="ZT_001",
                final_name="货币资金",
                relation_type="exact_alias",
                confidence=1.0,
                decided_by="tester",
            )
            apply_mapping_decision_to_output(result.output_dir, accept_once)
            append_mapping_decision_file(Path(result.output_dir), accept_once)
            row = self._read_output_row(result.output_dir)
            self.assertEqual(row["映射方法"], "manual_once")
            self.assertEqual(row["标准指标编码"], "ZT_001")
            self.assertEqual(store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"), 0)

            audit_path = store.export_decision_audit(Path(store_path).parent / "mapping_decisions_audit.csv")
            self.assertTrue(audit_path.exists())
            self.assertTrue((Path(result.output_dir) / "mapping_decisions.csv").exists())
            with (Path(result.output_dir) / "mapping_decisions.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                decisions = list(csv.DictReader(handle))
            self.assertEqual([row["decision"] for row in decisions], ["reject", "accept_once"])
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_store_default_exports_follow_sqlite_directory(self):
        store_path = self._new_store_path()
        store = LocalMappingStore(store_path)
        aliases_path = store.export_aliases()
        decisions_path = store.export_decision_audit()
        self.assertEqual(aliases_path, store_path.parent / "local_aliases_export.yml")
        self.assertEqual(decisions_path, store_path.parent / "mapping_decisions_audit.csv")
        self.assertTrue(aliases_path.exists())
        self.assertTrue(decisions_path.exists())

    def test_accept_and_remember_writes_local_alias_and_reuses_it(self):
        store_path = self._new_store_path()
        raw_temp, output_temp, result = self._run_mapping([self._row("测试记忆术语", "100")], mapping_store_path=store_path)
        try:
            store = LocalMappingStore(store_path)
            raw = result.rows[0].raw
            decision = store.record_decision(
                job_id="JOB2",
                doc_id="DTEST",
                raw_metric_id=raw.raw_metric_id,
                raw_metric_name=raw.metric_name,
                suggested_code="ZT_068",
                suggested_name="短期借款",
                decision="accept_and_remember",
                final_code="ZT_068",
                final_name="短期借款",
                relation_type="exact_alias",
                confidence=1.0,
                decided_by="tester",
            )
            apply_mapping_decision_to_output(result.output_dir, decision)
            export_path = store.export_aliases(Path(store_path).parent / "local_aliases_export.yml")
            snapshot_path = store.write_snapshot(Path(result.output_dir) / "mapping_store_snapshot.yml")
            self.assertTrue(export_path.exists())
            self.assertTrue(snapshot_path.exists())
            self.assertEqual(store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"), 1)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

        raw_temp2, output_temp2, result2 = self._run_mapping([self._row("测试记忆术语", "200")], mapping_store_path=store_path)
        try:
            mapped = result2.rows[0]
            self.assertEqual(mapped.mapping_status, "mapped")
            self.assertEqual(mapped.mapping_method, "local_alias")
            self.assertEqual(mapped.standard_code, "ZT_068")
        finally:
            raw_temp2.cleanup()
            output_temp2.cleanup()

    def test_confidence_threshold_preview_does_not_mutate_store(self):
        store_path = self._new_store_path()
        raw_temp, output_temp, result = self._run_mapping([self._row("总收入", "100")], mapping_store_path=store_path, llm_mock=True, disable_llm_mapping=False)
        try:
            self.assertEqual(default_confidence_threshold(), 0.90)
            store = LocalMappingStore(store_path)
            before = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
            eligible = eligible_for_accept_once_by_confidence(result.output_dir, 0.90)
            preview = build_confidence_bulk_accept_preview(
                result.output_dir,
                threshold=0.90,
                before_alias_count=before,
                after_alias_count=before,
            )
            after = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
            self.assertGreaterEqual(len(eligible), 1)
            self.assertEqual(preview["future_decision"], "accept_once")
            self.assertEqual(preview["would_apply_decision"], "accept_once")
            self.assertFalse(preview["mutated_mappings"])
            self.assertEqual(before, after)
            self.assertTrue((Path(result.output_dir) / "confidence_bulk_accept_preview.json").exists())
            self.assertFalse((Path.cwd() / "local_mappings.sqlite").exists())
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_deepseek_config_and_mock_do_not_require_real_key(self):
        config = load_deepseek_config(env={"DEEPSEEK_API_KEY": "[请输入你的api]"})
        self.assertFalse(config.enabled)
        self.assertFalse(is_valid_deepseek_api_key("[请输入你的api]"))
        mock_config = load_deepseek_config(env={"DEEPSEEK_API_KEY": ""}, mock_mode=True)
        self.assertTrue(mock_config.enabled)
        response = MockDeepSeekClient().complete_json(
            system_prompt="",
            user_prompt='{"raw_metric_name":"总收入","candidates":[{"rank":1,"code":"ST_001","name":"总营业额"}]}',
            config=mock_config,
        )
        self.assertIn("ST_001", response)
        self.assertNotIn("[请输入你的api]", repr(config))

    def test_live_deepseek_requires_explicit_opt_in(self):
        key_only = load_deepseek_config(env={"DEEPSEEK_API_KEY": "sk-valid-test-key"})
        env_enabled = load_deepseek_config(
            env={"DEEPSEEK_API_KEY": "sk-valid-test-key", "DEEPSEEK_ENABLED": "true"}
        )
        env_disabled = load_deepseek_config(
            env={"DEEPSEEK_API_KEY": "sk-valid-test-key", "DEEPSEEK_ENABLED": "false"}
        )
        cli_enabled = load_deepseek_config(
            env={"DEEPSEEK_API_KEY": "sk-valid-test-key", "DEEPSEEK_ENABLED": "false"},
            enabled_override=True,
        )

        self.assertFalse(key_only.enabled)
        self.assertEqual(key_only.disabled_reason, "not_explicitly_enabled")
        self.assertTrue(env_enabled.enabled)
        self.assertFalse(env_disabled.enabled)
        self.assertEqual(env_disabled.disabled_reason, "explicitly_disabled")
        self.assertTrue(cli_enabled.enabled)

    def test_llm_prompt_response_validation(self):
        registry = load_standard_registry()
        raw = self._raw_row("总收入")
        candidates = [
            candidate
            for candidate in [
                # The mapper will pass MappingCandidate objects; reuse a real run's candidate shape.
                self._llm_candidate("ST_001", "总营业额", 1),
                self._llm_candidate("ZT_138", "营业收入", 2),
            ]
        ]
        system_prompt, user_prompt = build_llm_mapping_prompts(raw, candidates, registry)
        self.assertIn("只能", system_prompt)
        self.assertIn("ST_001", user_prompt)
        valid = parse_llm_mapping_response(
            '{"decision":"candidate","standard_code":"ST_001","standard_name":"总营业额","relation_type":"same_as","confidence":0.93,"review_required":true,"reason":"接近","candidate_rank":1}',
            candidates=candidates,
        )
        self.assertEqual(valid["validation_status"], "valid")
        invented = parse_llm_mapping_response(
            '{"decision":"candidate","standard_code":"BAD_999","standard_name":"坏编码","relation_type":"same_as","confidence":0.93,"review_required":true,"reason":"x","candidate_rank":1}',
            candidates=candidates,
        )
        self.assertEqual(invented["validation_status"], "invalid_code")
        malformed = parse_llm_mapping_response("not-json", candidates=candidates)
        self.assertEqual(malformed["validation_status"], "invalid_json")
        bad_confidence = parse_llm_mapping_response(
            '{"decision":"candidate","standard_code":"ST_001","standard_name":"总营业额","relation_type":"same_as","confidence":1.2,"review_required":true,"reason":"x","candidate_rank":1}',
            candidates=candidates,
        )
        self.assertEqual(bad_confidence["validation_status"], "invalid_confidence")
        aggregate = parse_llm_mapping_response(
            '{"decision":"candidate","standard_code":"ST_001","standard_name":"总营业额","relation_type":"aggregate","confidence":0.88,"review_required":false,"reason":"期间汇总","candidate_rank":1}',
            candidates=candidates,
        )
        self.assertTrue(aggregate["review_required"])

    def test_llm_suggestion_cache_and_outputs_are_written(self):
        store_path = self._new_store_path()
        raw_temp, output_temp, result = self._run_mapping([self._row("总收入", "100")], mapping_store_path=store_path, llm_mock=True, disable_llm_mapping=False)
        try:
            store = LocalMappingStore(store_path)
            self.assertEqual(store.count("llm_suggestions"), 1)
            self.assertTrue((Path(result.output_dir) / "llm_suggestions.csv").exists())
            self.assertTrue((Path(result.output_dir) / "llm_suggestion_audit.csv").exists())
            self.assertTrue((Path(result.output_dir) / "llm_mapping_summary.json").exists())
            first_summary = result.summary["llm_mapping"]
            self.assertEqual(first_summary["suggestions_total"], 1)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

        raw_temp2, output_temp2, result2 = self._run_mapping([self._row("总收入", "200")], mapping_store_path=store_path, llm_mock=True, disable_llm_mapping=False)
        try:
            self.assertEqual(result2.summary["llm_mapping"]["cached_suggestions_total"], 1)
            raw = result2.rows[0].raw
            context = build_llm_context(raw)
            key1 = build_llm_cache_key(
                raw_metric_name=raw.metric_name,
                context=context,
                candidate_codes=["ST_001", "ZT_138"],
                policy_version="stage15_2_candidate_constrained_v1",
                model_name="deepseek-v4-flash",
            )
            key2 = build_llm_cache_key(
                raw_metric_name=raw.metric_name,
                context=context,
                candidate_codes=["ZT_138", "ST_001"],
                policy_version="stage15_2_candidate_constrained_v1",
                model_name="deepseek-v4-flash",
            )
            self.assertNotEqual(key1, key2)
        finally:
            raw_temp2.cleanup()
            output_temp2.cleanup()

    def test_llm_mapping_integration_and_human_decisions(self):
        store_path = self._new_store_path()
        rows = [self._row("货币资金", "100"), self._row("总收入", "200"), self._row("上半年营收", "300"), self._row("奇怪项目XYZ", "400")]
        raw_temp, output_temp, result = self._run_mapping(rows, mapping_store_path=store_path, llm_mock=True, disable_llm_mapping=False)
        try:
            exact, llm, aggregate, unknown = result.rows
            self.assertEqual(exact.mapping_method, "exact")
            self.assertEqual(llm.mapping_method, "llm_suggested")
            self.assertEqual(llm.standard_code, "ST_001")
            self.assertTrue(llm.review_required)
            self.assertEqual(aggregate.relation_type, "aggregate")
            self.assertTrue(aggregate.review_required)
            self.assertEqual(unknown.mapping_status, "unmapped")

            store = LocalMappingStore(store_path)
            reject = store.record_decision(
                job_id="JOBLLM",
                doc_id="DTEST",
                raw_metric_id=llm.raw.raw_metric_id,
                raw_metric_name=llm.raw.metric_name,
                suggested_code=llm.standard_code,
                suggested_name=llm.standard_name,
                decision="reject",
                decided_by="tester",
            )
            apply_mapping_decision_to_output(result.output_dir, reject)
            append_mapping_decision_file(Path(result.output_dir), reject)

            once = store.record_decision(
                job_id="JOBLLM",
                doc_id="DTEST",
                raw_metric_id=llm.raw.raw_metric_id,
                raw_metric_name=llm.raw.metric_name,
                suggested_code=llm.standard_code,
                suggested_name=llm.standard_name,
                decision="accept_once",
                final_code=llm.standard_code,
                final_name=llm.standard_name,
                relation_type="same_as",
                confidence=0.93,
                decided_by="tester",
            )
            apply_mapping_decision_to_output(result.output_dir, once)
            append_mapping_decision_file(Path(result.output_dir), once)
            self.assertEqual(store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"), 0)

            remember = store.record_decision(
                job_id="JOBLLM",
                doc_id="DTEST",
                raw_metric_id=llm.raw.raw_metric_id,
                raw_metric_name=llm.raw.metric_name,
                suggested_code=llm.standard_code,
                suggested_name=llm.standard_name,
                decision="accept_and_remember",
                final_code=llm.standard_code,
                final_name=llm.standard_name,
                relation_type="same_as",
                confidence=0.93,
                decided_by="tester",
            )
            apply_mapping_decision_to_output(result.output_dir, remember)
            self.assertEqual(store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"), 1)
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

        raw_temp2, output_temp2, result2 = self._run_mapping([self._row("总收入", "500")], mapping_store_path=store_path, disable_llm_mapping=True)
        try:
            self.assertEqual(result2.rows[0].mapping_method, "local_alias")
            self.assertEqual(result2.rows[0].standard_code, "ST_001")
        finally:
            raw_temp2.cleanup()
            output_temp2.cleanup()

    def test_confidence_bulk_apply_accept_once_only_and_excludes_unsafe(self):
        store_path = self._new_store_path()
        rows = [self._row("总收入", "100"), self._row("营业额", "200"), self._row("上半年营收", "300")]
        raw_temp, output_temp, result = self._run_mapping(rows, mapping_store_path=store_path, llm_mock=True, disable_llm_mapping=False)
        try:
            store = LocalMappingStore(store_path)
            before_aliases = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
            preview_default = build_confidence_bulk_accept_preview(result.output_dir)
            self.assertEqual(preview_default["threshold"], 0.90)
            self.assertEqual(preview_default["eligible_total"], 2)
            self.assertGreaterEqual(preview_default["excluded_reasons"].get("unsafe_relation_type", 0), 1)
            preview_high = build_confidence_bulk_accept_preview(result.output_dir, threshold=0.94)
            self.assertEqual(preview_high["eligible_total"], 1)
            preview_low = build_confidence_bulk_accept_preview(result.output_dir, threshold=0.80)
            self.assertEqual(preview_low["eligible_total"], 2)
            summary = apply_confidence_bulk_accept(result.output_dir, store=store, job_id="JOBBULK", doc_id="DTEST", threshold=0.90)
            self.assertEqual(summary["applied_total"], 2)
            self.assertFalse(summary["mutated_local_alias_store"])
            self.assertEqual(before_aliases, store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"))
            with (Path(result.output_dir) / "mapping_decisions.csv").open("r", encoding="utf-8-sig", newline="") as handle:
                decisions = list(csv.DictReader(handle))
            self.assertEqual({row["decision"] for row in decisions}, {"accept_once"})
            self.assertTrue((Path(result.output_dir) / "confidence_bulk_accept_apply_summary.json").exists())
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
            self.assertEqual(mapped["期间类型"], "期末数")
            self.assertEqual(mapped["指标数值"], "00123.4500")
        finally:
            raw_temp.cleanup()
            output_temp.cleanup()

    def test_cli_runs(self):
        raw_temp, input_path = self._write_raw_metrics([self._row("货币资金", "100")])
        output_temp = tempfile.TemporaryDirectory(dir=self.standard_metrics_root)
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
                    "--raw-metrics-root",
                    str(self.raw_metrics_root),
                    "--standard-metrics-root",
                    str(self.standard_metrics_root),
                    "--disable-llm-mapping",
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
            "期间类型": "期末数",
            "公司名": "AAA有限公司",
            "指标名": metric_name,
            "指标数值": metric_value,
        }

    def _raw_row(self, metric_name: str):
        from standard_map.models import RawMetricRow

        return RawMetricRow(
            row_number=1,
            review_item_id="maprev_000001",
            raw_metric_id="raw_001",
            fill_date="2022-12-31",
            item_date="2022-12-31",
            company_name="AAA有限公司",
            metric_name=metric_name,
            metric_value="100",
        )

    def _llm_candidate(self, code: str, name: str, rank: int):
        from standard_map.models import MappingCandidate

        return MappingCandidate(
            raw_metric_id="raw_001",
            raw_metric_name="总收入",
            candidate_rank=rank,
            candidate_code=code,
            candidate_name=name,
            candidate_score=0.8,
            candidate_method="candidate",
        )


if __name__ == "__main__":
    unittest.main()
