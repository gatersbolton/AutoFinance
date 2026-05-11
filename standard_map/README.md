# Stage 13 Standard Metrics Mapping

This package implements Step 2 of the revised pipeline:

```text
raw normalized metrics table
-> standardized mapped metrics table
```

It consumes Stage 12 `raw_metrics.csv` or `raw_metrics.xlsx` only. It does not call OCR APIs, does not run PaddleOCR, and does not fill `会计报表.xlsx`.

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

The main CSV uses Chinese business headers and preserves raw metric dates, company names, metric names, and values.
