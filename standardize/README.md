# standardize

`standardize` consumes OCR provider outputs and exports a deterministic, auditable accounting workbook.

## Typical Usage

```bash
python -m standardize.cli ^
  --input-dir data/corpus/inbox/ocr_outputs ^
  --template data/templates/会计报表.xlsx ^
  --output-dir data/generated/standardize/archive ^
  --source-image-dir data/corpus/inbox/input ^
  --provider-priority aliyun,tencent ^
  --enable-conflict-merge
```

## Input Rules

- Primary table evidence comes from `data/corpus/*/ocr_outputs/<provider>/<doc>/raw/*.json`
- `result.json` is used for page-level indexing, text hints, and artifact references
- Only `aliyun_table`, `tencent_table_v3`, and `xlsx_fallback` reconstruct tables
- `xlsx` is fallback-only and is marked as missing bbox/confidence evidence

## Main Outputs

- `cells.csv`
- `facts.csv`
- `issues.csv`
- `conflicts.csv`
- `mapping_review.csv`
- `summary.json`
- `run_manifest.json` / `artifact_manifest_core.csv`
- `pipeline_stage_timings.json` / `pipeline_stage_status.json` / `pipeline_completion_summary.json`
- `会计报表_填充结果.xlsx`

Default run outputs now live under `data/generated/standardize/archive/`.

## Boundaries

- Deterministic and config-driven; no LLM in the core pipeline
- `OCR.py` is upstream and remains separate
- Main statements are the first-class export target; note tables are preserved for review and follow-up
