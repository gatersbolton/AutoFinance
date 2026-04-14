# standardize Stage2

阶段2在阶段1 OCR 标准化中间层之上补了五类增强：

- 期间归一：多源日期提取、statement/doc 级继承、`unknown_date` 原因留痕
- 去重：输出 `facts_raw.csv`、`facts_deduped.csv`、`duplicates.csv`
- 质量报告：输出 `run_summary.*`、provider compare 覆盖率、unknown/suspicious TOP
- 审计校验：输出 `validation_results.csv` 和 `validation_summary.json`
- 成本控制：输出 pre/post OCR routing plan

## 主要新增输出

- `facts_raw.csv`
- `facts_deduped.csv`
- `duplicates.csv`
- `provider_comparison_summary.csv`
- `validation_results.csv`
- `validation_summary.json`
- `run_summary.json`
- `run_summary.csv`
- `top_unknown_labels.csv`
- `top_suspicious_values.csv`
- `page_selection.csv`
- `pre_ocr_routing_plan.json`
- `secondary_ocr_candidates.csv`
- `post_ocr_routing_plan.json`

## CLI 示例

```bash
python -m standardize.cli \
  --input-dir data/corpus/inbox/ocr_outputs \
  --template data/templates/会计报表.xlsx \
  --output-dir data/generated/standardize/archive \
  --source-image-dir data/corpus/inbox/input \
  --provider-priority aliyun,tencent \
  --enable-conflict-merge \
  --enable-period-normalization \
  --enable-dedupe \
  --enable-validation \
  --emit-routing-plan
```

## 当前边界

- 期间归一仍是规则驱动，不做激进日期换算
- validation 只报告风险，不改值
- routing 只做本地打分和候选推荐，不触发 OCR API
- 双 provider 比较现在会输出 compared/equal/conflict 覆盖率，但复杂跨表语义错位仍依赖 table semantic key

## 后续扩展点

- 更强的主表/附注 statement 继承
- validation 引入更多勾稽规则
- routing 引入区域级别触发
- LLM 只用于长尾别名建议和疑难冲突排序，不替代主规则链路
