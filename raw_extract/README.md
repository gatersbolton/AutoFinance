# Stage 12 Raw Metrics Extraction

Stage 12 is Step 1 of the revised pipeline:

```text
existing financial statement OCR outputs
-> raw normalized long-format metrics table
```

It reads already-produced OCR artifacts from providers such as `aliyun_table`, `tencent_table_v3`, `paddle_table_local`, or workbook fallbacks. It does not call cloud OCR APIs, does not run PaddleOCR, does not fill `会计报表.xlsx`, and does not perform ZT code mapping.

## Main Output Schema

`raw_metrics.csv` and the first sheet of `raw_metrics.xlsx` contain exactly these columns:

```text
填表日期
当前条目日期
期间类型
公司名
指标名
指标数值
```

`raw_metrics_detailed.csv` preserves provenance and diagnostics, including provider, page, table, row/column indexes, raw value text, unit, header path, period role, cell reference, bbox JSON, confidence, evidence path, and issue flags.

## Date Semantics

`填表日期` is the report or statement reference date for the table/document. A balance sheet dated `2022年12月31日` resolves to `2022-12-31`. An annual statement such as `2022年度` resolves to year end, `2022-12-31`, with an `annual_to_year_end` method.

`当前条目日期` is the date represented by the metric value cell. For point-in-time tables, `期末数` maps to the fill date, `期初数`/`年初数` maps to January 1 of the fill-date year, and `上年年末` maps to the previous year end. For annual flow tables, `本期`/`本年累计` maps to January 1 of the fill-date year, while `上期`/`上年同期` maps to January 1 of the previous year.

`期间类型` is the normalized business role of the value column, such as `期初数`, `期末数`, `本期`, or `上期`. It is intended to be used together with `填表日期` when importing rows into downstream databases.

## Number Semantics

Amounts are normalized to Python numeric values. Chinese and English commas are accepted. Parentheses indicate negative numbers. Percentages are emitted as ratios, so `98.26%` becomes `0.9826`; use `--include-ratios false` to exclude ratio rows from accepted raw metrics.

Suspicious formats such as abnormal comma grouping are parsed when safe and flagged. Ambiguous values such as multiple decimal points are not silently parsed.

## What Stage 12 Does Not Do

Stage 12 does not map accounting terms to ZT codes, does not normalize old/new accounting term names, does not deduplicate against the accounting workbook template, and does not write generated artifacts into the repo root.

Step 2 will consume this raw long-format output and perform term mapping, ZT code assignment, and accounting-term normalization.

## CLI

```powershell
python -m raw_extract.cli ^
  --input-dir data/corpus/D01/ocr_outputs ^
  --source-image-dir data/corpus/D01/input ^
  --output-dir data/generated/raw_metrics/D01 ^
  --provider-priority aliyun_table,tencent_table_v3,paddle_table_local
```

Optional flags:

```text
--doc-id
--company-name
--default-fill-date
--include-ratios true|false
--include-blank true|false
--debug
```

## Output Location

Each run writes to a run subdirectory under the requested base output directory:

```text
data/generated/raw_metrics/<doc_id>/<run_id>/
```

The run emits:

```text
raw_metrics.csv
raw_metrics.xlsx
raw_metrics.jsonl
raw_metrics_detailed.csv
raw_metric_candidates.csv
raw_metrics_issues.csv
raw_metrics_summary.json
date_resolution_audit.csv
company_resolution_audit.csv
extraction_run_manifest.json
raw_metrics_smoke_summary.json
```
