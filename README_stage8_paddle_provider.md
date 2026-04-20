# Stage 8 / 8.1 / 8.2: Paddle Local Provider Pilot, Quality Eval, And Parity Gate

`paddle_table_local` remains an optional third OCR/table provider for targeted local experiments. It does not replace Tencent or Aliyun, and the production default provider priority remains cloud-first.

## Current Status

- Stage 8 provider contract exists and is working.
- Stage 8 standardize compatibility exists and is working.
- Stage 8.1 expands evaluation breadth and adds deterministic quality gates.
- Stage 8.2 expands role-level parity evidence and hardens the fallback gate so thin evidence stays `pilot_only`.
- Current routing remains cloud-first even when Paddle clears limited fallback-readiness checks for specific page roles.

## Runtime Expectations

- Paddle is not required in the default repo Python environment.
- The provider can point at a separate ready runtime with `--paddle-runtime-python`, for example `.venv_paddlegpu\Scripts\python.exe`.
- `--paddle-device auto` prefers GPU when the selected Paddle runtime can use it; otherwise it falls back to CPU unless `--paddle-skip-if-no-gpu` is set.
- `--paddle-layout-detection on|off` controls whether Paddle layout detection is used for a page. `auto` currently defaults to `on`.

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

## Stage 8 Pilot Harness

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

This stage is structural only. It checks whether the provider emits consumable artifacts and whether the existing standardize pipeline can ingest them without crashing.

## Stage 8.1 / 8.2 Expanded Eval

The repo-visible evaluation sample is:

- `benchmarks/paddle_pilot_registry.yml`

The registry is intentionally small enough to stay fast, but it now covers multiple page roles with more than one doc/page for:

- `main_statement`
- `note_multi_table`
- `cross_doc_main_statement`
- `note_noise`

Run the expanded evaluation:

```bash
python tools/paddle_quality_eval.py ^
  --registry benchmarks/paddle_pilot_registry.yml ^
  --main-registry benchmarks/registry.yml ^
  --paddle-runtime-python .venv_paddlegpu\Scripts\python.exe ^
  --paddle-device auto
```

Expanded evaluation outputs go to:

- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_eval_summary.json`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_eval_runtime.json`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_eval_pages.csv`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_vs_cloud_compare.csv`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_parity_summary.json`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_parity_by_role.csv`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_zero_fact_pages.csv`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_failure_analysis.csv`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_quality_gate.json`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_route_recommendation.json`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_role_summary.json`
- `data/generated/experiments/paddle_provider_eval/<run_id>/paddle_role_compare.csv`

Additional support artifacts are also written:

- `paddle_environment_summary.json`
- `paddle_provider_contract_summary.json`

## Expanded Compatibility

Run compatibility directly against the expanded sample docs:

```bash
python tools/paddle_standardize_compatibility.py ^
  --registry benchmarks/registry.yml ^
  --sample-registry benchmarks/paddle_pilot_registry.yml ^
  --run-id <eval_run_id>
```

Compatibility outputs go to:

- `data/generated/standardize/control_runs/paddle_provider_eval/<run_id>/paddle_standardize_compatibility.json`
- `data/generated/standardize/control_runs/paddle_provider_eval/<run_id>/paddle_standardize_compatibility_by_doc.csv`

Per-doc standardize outputs are written under that same run directory in doc-specific subfolders. The compatibility export now flags `zero_fact_output` and `weak_output`, and records the sampled page roles for each doc.

## How To Interpret The Quality Gate

`paddle_quality_gate.json` returns one of:

- `not_ready`
- `pilot_only`
- `fallback_candidate_for_specific_roles`

The gate considers:

- environment readiness
- provider contract pass
- role-aware parity versus existing cloud outputs
- runtime reasonableness
- expanded standardize compatibility
- lightweight cloud non-regression
- breadth of sampled evidence
- page-role-aware structural sufficiency
- explicit evidence thresholds such as minimum pages/docs per role and maximum zero-fact / parity-deficit allowance

`paddle_route_recommendation.json` preserves cloud-first routing and only recommends explicit Paddle fallback for roles that have sufficient evidence. Thin evidence is intentionally forced to remain `pilot_only`.

## Why Production Remains Cloud-First

- Tencent and Aliyun remain the accepted stable production path.
- Paddle evidence is still intentionally sampled rather than batch-wide.
- The current parity benchmark is conservative and compares Paddle against existing cloud structural outputs, including table-count deficits and zero-fact outcomes.
- Note-heavy pages remain structurally diverse and noisier than the current fallback gate should tolerate globally.
- Stage 8.2 remains an evaluation and gating round, not a cutover round.

## What Must Happen Before Any Fallback Promotion

- Broader evidence across more docs and more note-page variants
- Stable compatibility across multiple non-D01 corpora and role types
- Repeated cloud non-regression checks
- Role-specific quality that consistently stays within acceptable review and validation pressure
- Role-level parity that no longer shows repeated zero-fact pages or large deficits versus cloud structure
- Explicit routing rules that remain cloud-first by default
