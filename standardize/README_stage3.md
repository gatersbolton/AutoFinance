# Stage 3

阶段3在阶段2流水线基础上补了 5 类增强：

- export contract + artifact integrity
- mapping masterdata + candidate mining
- validation-aware conflicts
- review queue + evidence pack
- targeted re-OCR task generation

主 sheet 现在只写 resolved、mapped、period-resolved、deduped facts。其余事实会进入 `_unplaced_facts`、`_conflicts`、`_review_queue` 等辅助 sheet 和独立 CSV。

新增关键输出：

- `artifact_integrity.json/csv`
- `mapping_candidates.csv`
- `unmapped_labels_summary.csv`
- `conflicts_enriched.csv`
- `conflict_decision_audit.csv`
- `validation_impact_of_conflicts.csv`
- `review_queue.csv`
- `review_summary.json`
- `review_workbook.xlsx`
- `review_pack/`
- `reocr_tasks.csv`
- `reocr_task_summary.json`

配置入口：

- `config/export_rules.yml`
- `config/mapping_rules.yml`
- `config/subject_aliases.yml`
- `config/subject_relations.yml`
- `config/conflict_rules.yml`
- `config/review_rules.yml`
- `config/reocr_rules.yml`

扩展原则：

- alias 与 relation 分开维护
- aggregate/split 默认只做 suggestion，不自动落主 sheet
- conflict 先 compare，再做 validation-aware decision
- review/reOCR 都必须带追溯信息，不做 silent fix
