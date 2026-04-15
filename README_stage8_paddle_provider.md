# Stage 8: Paddle Local Provider Pilot

`paddle_table_local` is an optional third OCR/table provider for small-scale local experiments. It does not replace Tencent or Aliyun, and the production default provider priority remains cloud-first.

## Status

- Pilot only
- Optional/fallback provider
- Intended for targeted evaluation on selected pages before any broader rollout

## Runtime Expectations

- Paddle is not required in the default repo Python environment.
- The provider can point at a separate ready runtime with `--paddle-runtime-python`, for example `.venv_paddlegpu\Scripts\python.exe`.
- `--paddle-device auto` prefers GPU when the selected Paddle runtime can use it; otherwise it falls back to CPU unless `--paddle-skip-if-no-gpu` is set.
- `--paddle-layout-detection on|off` controls whether Paddle layout detection is used for the page. `auto` currently defaults to `on`; the pilot harness overrides this per sample page.

## OCR Usage

```bash
python OCR.py --method paddle_table_local ^
  --input data/corpus/inbox/input ^
  --output data/corpus/inbox/ocr_outputs ^
  --paddle-runtime-python .venv_paddlegpu\Scripts\python.exe ^
  --paddle-device auto ^
  --paddle-layout-detection auto
```

The provider writes outputs under the same corpus contract as the cloud providers:

- `data/corpus/<DOC>/ocr_outputs/paddle_table_local/<pdf_stem>/result.json`
- `data/corpus/<DOC>/ocr_outputs/paddle_table_local/<pdf_stem>/raw/page_XXXX.json`
- `data/corpus/<DOC>/ocr_outputs/paddle_table_local/<pdf_stem>/artifacts/page_XXXX_table_YY.xlsx`
- `data/corpus/<DOC>/ocr_outputs/paddle_table_local/<pdf_stem>/artifacts/page_XXXX_table_YY.html`

## Pilot Harness

```bash
python tools/paddle_provider_pilot.py ^
  --registry benchmarks/registry.yml ^
  --paddle-runtime-python .venv_paddlegpu\Scripts\python.exe ^
  --paddle-device auto
```

Pilot outputs go to:

- `data/generated/experiments/paddle_provider_pilot/<run_id>/paddle_environment_summary.json`
- `data/generated/experiments/paddle_provider_pilot/<run_id>/paddle_provider_contract_summary.json`
- `data/generated/experiments/paddle_provider_pilot/<run_id>/paddle_runtime_summary.json`
- `data/generated/experiments/paddle_provider_pilot/<run_id>/paddle_pilot_pages.csv`
- `data/generated/experiments/paddle_provider_pilot/<run_id>/paddle_vs_cloud_compare.csv`
- `data/generated/experiments/paddle_provider_pilot/<run_id>/paddle_pilot_summary.json`

Interpretation for this stage is structural:

- table count emitted
- raw/json/html/xlsx artifact presence
- runtime by page
- whether the current standardize pipeline can ingest the output without crashing

This is not a final semantic quality bakeoff.

## Standardize Compatibility

```bash
python tools/paddle_standardize_compatibility.py ^
  --registry benchmarks/registry.yml ^
  --doc-id D01 ^
  --run-id <pilot_run_id>
```

Compatibility outputs go to:

- `data/generated/standardize/control_runs/paddle_provider_pilot/<run_id>/`

The compatibility summary file is:

- `paddle_standardize_compatibility.json`

## Current Recommendation

Keep Paddle as pilot-only until the local provider shows stable structural coverage and acceptable note-page behavior across a wider sample. Cloud providers remain the default priority for production runs.
