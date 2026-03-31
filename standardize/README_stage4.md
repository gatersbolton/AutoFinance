# Stage 4

Stage 4 adds a deterministic human-in-the-loop loop on top of the existing OCR standardization pipeline.

Main additions:

- stable IDs for facts, conflicts, review items, and re-OCR tasks
- editable `review_actions_template.xlsx/csv`
- manual override storage under `standardize/config/manual_overrides/`
- reviewer action parsing, validation, application, and audit trails
- before/after delta reports
- review priority backlog and mapping opportunity views
- re-OCR input manifests and crop materialization
- re-OCR result merge audit
- `_applied_actions` helper sheet in the export workbook

The base deterministic pipeline remains the source of truth. Reviewer decisions are additive overrides and are not written back into the base alias/relation masterdata by default.
