# Standard Metrics Mapping

Step 2 of the current product flow is:

```text
raw normalized metrics table
-> standardized mapped metrics table
```

This step consumes Stage 12 `raw_metrics.csv` or `raw_metrics.xlsx` and maps each raw metric name to a standard metric code and name. It preserves dates and numeric values exactly as received from Step 1.

## CLI

```powershell
python -m standard_map.cli ^
  --input data/generated/raw_metrics/D01/<run_id>/raw_metrics.csv ^
  --output-dir data/generated/standard_metrics/D01 ^
  --mapping-registry config/standard_terms.yml
```

Optional flags:

```text
--doc-id
--company-name
--debug
```

This CLI does not require `会计报表.xlsx`.

## Registry

The default registry files are:

```text
config/standard_terms.yml
config/standard_term_aliases.yml
config/standard_term_relations.yml
```

Supported term fields:

```text
code
name
aliases
statement_scope
metric_type
legacy_aliases
notes
```

Exact aliases and safe legacy aliases can auto-map. Aggregate, split, broader/narrower, and ambiguous relations produce review items by default.

## Outputs

Each run writes under:

```text
data/generated/standard_metrics/<doc_id>/<run_id>/
```

Required files:

```text
standardized_metrics.csv
standardized_metrics.xlsx
standardized_metrics.jsonl
standardized_metrics_detailed.csv
mapping_candidates.csv
mapping_issues.csv
standard_mapping_summary.json
mapping_review_items.csv
standard_mapping_run_manifest.json
```

The main CSV/XLSX columns are:

```text
填表日期
当前条目日期
公司名
原始指标名
标准指标编码
标准指标名称
指标数值
映射方法
映射状态
口径说明
是否需要人工校对
```

## Review

The web app exposes a simple term-mapping proofreading page. Review actions are stored under:

```text
data/generated/web/jobs/<job_id>/mapping_review/
```

The web page can approve, skip, or change a mapping. It does not edit the base registry files in this stage.
