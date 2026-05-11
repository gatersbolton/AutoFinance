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
- `tmp/`: local-only scratch space for browser captures, rendered previews, and one-off diagnostics; ignored by Git

The root should not accumulate run outputs, secrets, vendor clones, or corpus files.
Temporary tool directories such as `.playwright-cli/` should not stay in the repo root; move or write browser automation captures under `tmp/playwright-cli/`.

## `data/` Layout

```text
data/
  corpus/
    library/
      <doc_id>/
        input/
        ocr_outputs/
        metadata.json
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
      deletions/
```

Rules:

- `data/corpus/...` holds reusable inputs and OCR output corpora.
- `data/corpus/library/<doc_id>/` holds the reusable local PDF library used by the web home page.
- `data/templates/` holds local workbook templates.
- `data/secrets/` holds credential files.
- `data/vendor/` holds local third-party source checkouts.
- `data/generated/` holds all run outputs, audits, experiments, and historical leftovers.
- Stage 8 Paddle pilot outputs belong under `data/generated/experiments/paddle_provider_pilot/` and `data/generated/standardize/control_runs/paddle_provider_pilot/`.
- Stage 8.1 Paddle quality-eval outputs belong under `data/generated/experiments/paddle_provider_eval/` and `data/generated/standardize/control_runs/paddle_provider_eval/`.
- Stage 9+ web runtime state belongs under `data/generated/web/`, including uploads, job workspaces, result summaries, logs, deletion summaries, and the local SQLite database.

## `tmp/` Layout

```text
tmp/
  playwright-cli/
  pdfs/
  screenshots/
```

Rules:

- `tmp/` is for disposable local debugging artifacts only.
- Playwright CLI snapshots, screenshots, console logs, and videos belong under `tmp/playwright-cli/` when they need to be kept briefly for inspection.
- Do not reference `tmp/` artifacts from committed docs, tests, or runtime configuration.
- Summaries that are part of a requested smoke run should stay under `data/generated/web/`, not `tmp/`.

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
- Anything under `tmp/`
- `.playwright-cli/`
- Real `.env` files; commit only sanitized examples such as `.env.example` and `.env.aliyun.example`
- Local virtual environments such as `.venv*`
- Python caches such as `__pycache__/` and `.pytest_cache/`

## Docker / Server Upload Notes

- Use `Dockerfile`, `docker-compose.yml`, and optionally `docker-compose.aliyun.yml` for server deployment.
- For sub-path deployment at `http://ip/AutoFinance/`, set `WEBAPP_BASE_PATH=/AutoFinance` and keep the Nginx proxy under `/AutoFinance/`.
- Runtime state is mounted through `./data:/app/data`; do not bake corpus PDFs, OCR outputs, secrets, generated files, or Playwright captures into the image.
- To reuse local OCR and review results without calling OCR again, upload the relevant ignored `data/` directories to the server's repo root before starting containers.
- Keep `.dockerignore` aligned with `.gitignore` for local-only paths, especially `data/generated/`, `data/corpus/`, `data/secrets/`, `tmp/`, and `.playwright-cli/`.
- Production OCR defaults remain cloud-first (`WEBAPP_UPLOAD_OCR_METHOD=cloud_first`, `WEBAPP_PROVIDER_PRIORITY=aliyun,tencent`). PaddleOCR remains pilot-only and should not be promoted by deployment cleanup.

If a path change is needed later, update these first:

1. `project_paths.py`
2. `benchmarks/registry.yml`
3. `README.md` and this file
4. Tests that assert default paths or fixture locations
