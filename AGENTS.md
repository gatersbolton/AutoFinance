# AutoFinance Agent Guide

## Project Goal

This repo turns OCR output from scanned financial statements into structured facts and a filled accounting workbook.

Primary flow:

1. `OCR.py` reads PDFs from a corpus input directory and writes provider outputs.
2. `standardize.cli` reads provider outputs, resolves facts, validates them, and exports a workbook plus audit artifacts.
3. `standardize.batch` runs `standardize.cli` across the document registry in `benchmarks/registry.yml`.

## Top-Level Repo Contract

- `OCR.py`: OCR entrypoint
- `standardize/`: deterministic normalization, validation, benchmark, review, and export pipeline
- `benchmarks/registry.yml`: batch registry for corpus docs
- `tools/`: utility scripts such as workbook comparison builders
- `tests/`: automated tests and tracked fixtures
- `data/`: local-only workspace root; ignored by Git

The root should not accumulate run outputs, secrets, vendor clones, or corpus files.

## `data/` Layout

```text
data/
  corpus/
    inbox/
      input/
      ocr_outputs/
    D01/..D08/
      input/
      ocr_outputs/
      benchmarks/
  templates/
    会计报表.xlsx
  secrets/
    secret
  vendor/
    PaddleOCR/
    generated/
      standardize/
        archive/
        control_runs/
          paddle_provider_pilot/
          paddle_provider_eval/
        batches/
    audits/
    experiments/
      paddle_provider_pilot/
      paddle_provider_eval/
    comparisons/
    legacy/
    web/
      uploads/
      jobs/
      results/
      logs/
```

Rules:

- `data/corpus/...` holds reusable inputs and OCR output corpora.
- `data/templates/` holds local workbook templates.
- `data/secrets/` holds credential files.
- `data/vendor/` holds local third-party source checkouts.
- `data/generated/` holds all run outputs, audits, experiments, and historical leftovers.
- Stage 8 Paddle pilot outputs belong under `data/generated/experiments/paddle_provider_pilot/` and `data/generated/standardize/control_runs/paddle_provider_pilot/`.
- Stage 8.1 Paddle quality-eval outputs belong under `data/generated/experiments/paddle_provider_eval/` and `data/generated/standardize/control_runs/paddle_provider_eval/`.
- Stage 9 web runtime state belongs under `data/generated/web/`, including uploads, job workspaces, result summaries, logs, and the local SQLite database.

## Standard Commands

OCR:

```bash
python OCR.py --method aliyun_table

python OCR.py --method paddle_table_local ^
  --paddle-runtime-python .venv_paddlegpu\Scripts\python.exe
```

Single document:

```bash
python -m standardize.cli ^
  --input-dir data/corpus/inbox/ocr_outputs ^
  --template data/templates/会计报表.xlsx ^
  --output-dir data/generated/standardize/archive ^
  --source-image-dir data/corpus/inbox/input
```

Batch:

```bash
python -m standardize.batch ^
  --template data/templates/会计报表.xlsx ^
  --output-dir data/generated/standardize/batches/default ^
  --registry benchmarks/registry.yml ^
  --batch-mode
```

## Registry Semantics

`benchmarks/registry.yml` is the source of truth for batch corpus selection.

Each entry defines:

- `doc_id`: stable corpus id such as `D01`
- `input_dir`: OCR provider output root
- `source_image_dir`: original PDF/image root for routing and evidence
- `benchmark_path`: optional reference workbook
- `benchmark_enabled` / `target_gap_enabled` / `batch_enabled`: feature gates

## Never Commit These Paths

- Anything under `data/`
- Local virtual environments such as `.venv*`
- Python caches such as `__pycache__/` and `.pytest_cache/`

If a path change is needed later, update these first:

1. `project_paths.py`
2. `benchmarks/registry.yml`
3. `README.md` and this file
4. Tests that assert default paths or fixture locations
