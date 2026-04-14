# Stage 5

Stage 5 adds deterministic coverage-lift tooling on top of the Stage 4 review loop.

Main additions:

- `run_manifest.json`, `artifact_manifest_core.csv`, and legacy alias `artifact_manifest.csv` with run-level provenance, hashes, and snapshot packaging
- optional benchmark comparison against a reference workbook
- statement-aware row label normalization before subject mapping
- deterministic formula / relationship-derived facts
- benchmark-assisted gap explanations and suggestion outputs
- workbook helper sheets for `_derived_facts`, `_benchmark_summary`, and `_gap_explanations`

Normal full runs now default to manifest `core` mode, which hashes top-level run artifacts and avoids recursively scanning heavy generated directories such as `review_pack/` and `reocr_inputs/`. Use `--artifact-manifest-mode full` only when a full nested manifest is explicitly needed.

The benchmark workbook is comparison-only. It never overwrites the export workbook automatically.

Derived facts are conservative by default:

- they are emitted separately to `derived_facts.csv`
- they keep provenance (`source_kind=derived_formula`)
- they do not overwrite stronger observed facts
- conflicts are written to `derived_conflicts.csv`

Hardening / completion diagnostics also emit:

- `pipeline_stage_timings.json`
- `pipeline_stage_status.json`
- `pipeline_completion_summary.json`

Snapshot packaging writes run-specific copies under each run directory's `_snapshots/<run_id>/`.
