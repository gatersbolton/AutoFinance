# Raw Metrics Table

Step 1 of the current product flow is:

```text
PDF / existing OCR outputs
-> raw normalized metrics table
```

The raw metrics table is intentionally not mapped to ZT or any other standard term registry. It preserves what the financial statement says in a simple long table:

```text
填表日期
当前条目日期
期间类型
公司名
指标名
指标数值
```

## CLI

```powershell
python -m raw_extract.cli ^
  --input-dir data/corpus/D01/ocr_outputs ^
  --source-image-dir data/corpus/D01/input ^
  --output-dir data/generated/raw_metrics/D01 ^
  --provider-priority aliyun_table,tencent_table_v3,paddle_table_local
```

This step reads existing OCR outputs. It does not call OCR APIs, does not run PaddleOCR, does not fill `会计报表.xlsx`, and does not do standard-term mapping.

## Outputs

Each run writes under:

```text
data/generated/raw_metrics/<doc_id>/<run_id>/
```

Business users usually download:

```text
raw_metrics.xlsx
raw_metrics.csv
```

The detailed files preserve evidence and troubleshooting data:

```text
raw_metrics_detailed.csv
raw_metric_candidates.csv
raw_metrics_issues.csv
raw_metrics_summary.json
date_resolution_audit.csv
company_resolution_audit.csv
extraction_run_manifest.json
```

## Review

The web app exposes a simple raw-data proofreading page. It shows the original PDF when available and only the useful extracted fields:

```text
公司名
填表日期
当前条目日期
期间类型
指标名
指标数值
```

Actions are stored under `data/generated/web/jobs/<job_id>/raw_review/` and do not mutate the raw extraction artifacts.
