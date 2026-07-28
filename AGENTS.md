# AutoFinance Agent Guide

## Project Goal

This repo turns OCR output from scanned financial statements into reviewable structured facts and a filled accounting workbook. Both deliverables belong to the same product: finance staff need to be able to inspect and correct the structured facts before relying on the workbook.

Two implementation paths currently coexist:

1. Legacy workbook path: `OCR.py` -> `standardize.cli` / `standardize.batch` -> filled accounting workbook and audit artifacts.
2. Reviewable-data path: `OCR.py` -> `raw_extract` -> `standard_map` -> web proofreading -> Excel/CSV dataset.

The target integration between these paths is still being designed. Do not remove either path or assume that one of the two deliverables is optional without an explicit product decision.

## Confirmed Product Requirements

- The structured dataset and the filled accounting workbook are both formal outputs of one project.
- The structured dataset is also the finance-user review surface for extraction accuracy.
- A single reviewed structured-fact layer must feed both Excel/CSV exports and workbook filling. Do not build two independently corrected sources of truth.
- Store canonical facts in long form: one metric, period, reporting scope, and value per fact. Finance-facing Excel may pivot those facts into beginning/end or current/prior columns.
- The current default workbook layout is fixed at `data/templates/会计报表.xlsx`. Its 194 `ZT_*` subjects are the canonical template codes and names. Keep template-specific cell placement behind an adapter boundary for future layouts.
- Current workbook filling covers the balance sheet and income statement. Cash-flow workbook support is deferred, not part of the current acceptance scope.
- Preserve every valid extracted metric in the structured dataset. Classify it as template-mapped, known outside the template, unresolved mapping, or uncertain extraction/period; do not report every non-template metric as an error.
- Only safely template-mapped facts fill workbook cells.
- Support arbitrary numbers of periods in the canonical layer and dynamic period columns in finance-facing Excel. Do not hard-code the data model to exactly two periods.
- If a metric name and value are valid but OCR did not capture a date, do not silently discard the row solely because the date is missing.
- Infer a date or period only when the available evidence reaches an explicit confidence standard; otherwise route the row to human review. Same-table headers and current-page statement titles are the strongest evidence. Cover/report periods, adjacent pages, column context, and filenames require corroboration.
- Persist whether a date or period was observed, inferred, or manually corrected, together with its evidence and confidence.
- Downstream business amounts are normalized RMB-yuan values and should use precise decimal arithmetic. Foreign-currency conversion is out of the current scope, but raw OCR values and units remain audit provenance.
- Only genuine one-to-one synonyms may auto-map. Aggregate, split, and ambiguous relationships must not be persisted as exact aliases; use an explicit computation/relation or human review.
- Learned mapping decisions are scoped by company and statement type by default; they must not silently become global aliases.
- Primary downstream interchange formats are Excel and CSV.
- A risk-marked draft dataset may be downloaded before review is complete. A workbook with unresolved high-risk facts remains incomplete unless the user explicitly requests a risk-bearing export, and that override must be audited.
- When both consolidated and parent-company statements exist, preserve both scopes and use consolidated statements as the default workbook scope. Fall back to parent-company statements only when the consolidated scope is absent or the user explicitly selects it.
- Primary statement values take precedence for workbook filling. Notes may supplement a missing primary-statement value only when metric, period, unit, and reporting scope align; conflicts require review and must not silently overwrite the primary statement.
- Preserve reported totals and independently calculate consistency checks. A computed total may flag a discrepancy but must not silently replace the reported fact.
- Treat an explicit numeric zero as zero, a blank as missing, and a dash as missing/not-presented unless the table's convention reliably establishes that the dash means zero. Parenthesized amounts are negative.
- The user manually confirms generation of the final workbook. Automatically produced workbooks are drafts.
- An uploaded PDF and each processing run are versioned. Company-and-statement mapping memory may carry forward, but manually corrected amounts and dates must not silently carry into a new OCR/source version.
- The expected deployment environment is a domestic enterprise server.
- Public Aliyun/Tencent OCR and DeepSeek use is currently permitted, including sending the financial context needed for extraction and mapping. Provider selection, enablement, and outbound use must still be explicit, configurable, and auditable.
- Corpus documents D01-D08 are representative of expected production inputs.
- Expected usage is one or two regular users and no more than roughly five concurrent users, using one shared application account.
- The server may be low-specification. Load balancing, horizontal scaling, and high-throughput architecture are out of scope; bounded resource use, serialized or tightly limited background work, recoverable failures, and preserving completed results are still required.
- The production target is Linux with Docker. Batch upload of multiple PDFs is required.
- Current accepted document scope is annual reports. Keep the period model extensible, but do not claim that monthly, quarterly, or half-year reports are tested or supported.
- A normal batch contains two to five PDFs. The desktop browser target is Chrome; mobile-specific UI and email/SMS/IM completion notifications are out of scope.
- Prefer a durable SQLite-backed queue with one worker and one actively processing document at a time for the current deployment profile. Do not retain OCR/extraction/mapping execution inside a long-lived HTTP request.
- Production OCR is cloud-based. Select the primary provider using D01-D08 quality results and use the other permitted cloud provider as fallback; local PaddleOCR is not a production dependency on the low-specification server.
- Initial configurable safety limits may use 50 MB and 300 pages per PDF and five PDFs per upload batch. D01-D08 currently range from 3 to 69 pages and under 1 MB to about 26 MB.
- Implement browser batch upload as sequential single-file requests grouped into one logical batch. Do not send all two-to-five PDFs in one large multipart request.
- Retain original PDFs, processing versions, and review history until explicit user deletion.
- Automatic backup is out of the current scope. Never claim that server-local retained data is backed up or protected against disk/server loss.

## Observed Production Baseline

Read-only SSH inspection on 2026-07-23 found:

- Ubuntu 24.04, Docker 29 / Compose 2.40, 2 vCPU, about 1.6 GB RAM, no swap, and a 40 GB root disk with about 17 GB free.
- Four healthy containers: web, worker, Redis, and Nginx. None has a Docker CPU or memory limit.
- The deployed data directory is about 1.7 GB: generated output about 880 MB, vendor files about 450 MB, and corpus data about 342 MB.
- Docker reported about 6.1 GB of reclaimable image data and about 2.2 GB of reclaimable build cache. Do not prune automatically; inspect exact targets before a user-authorized cleanup.
- Production currently sets `WEBAPP_QUEUE_BACKEND=local`, runs a separate worker, and also runs Redis. Reconcile this topology rather than assuming Redis is providing queue durability.
- The web and worker containers use different image ids/build dates; inspected `webapp/main.py` hashes differ. Build both services from one immutable release/version and verify it at startup.
- Nginx listens publicly on ports 80 and 443. HTTPS has an enabled renewal timer, but the HTTP AutoFinance endpoint currently challenges for Basic Auth instead of redirecting to HTTPS.
- Public SSH currently permits root password authentication; UFW is active and fail2ban is inactive.
- The deployed Git worktree contains local modifications and backup files. Do not overwrite it or infer that a checkout/rebuild is reproducible without first inventorying and preserving those changes.

## Web Batch Queue Implementation Contract

- The normal new-upload path is a logical document batch, even when it contains one PDF. The expected business batch remains two to five PDFs.
- `POST /api/document-batches` creates the logical batch. Each PDF is then sent separately to `POST /api/document-batches/{batch_id}/files`, and `POST /api/document-batches/{batch_id}/queue` activates processing only after every expected position is present.
- `document_batches` and `document_batch_items` in the web SQLite database are durable batch state. The existing `jobs` table is the durable processing queue.
- New library documents use job mode `document_pipeline`. Its worker stages are cloud OCR, raw fact extraction, standard-term mapping, and combined download generation.
- Batch job upserts and the transition from uploading to queued must remain one `BEGIN IMMEDIATE` SQLite transaction. Do not replace `queue_jobs_atomically` with a check-then-update sequence; that can turn an already running job back into a queued job or partially enqueue a batch.
- SQLite connections opened by `webapp.db` must be explicitly closed on both success and exception paths. This is required for predictable Windows tests and bounded long-running server resources.
- `claim_next_queued_job` enforces at most one globally running document job. Keep document processing serialized unless the product and server constraints explicitly change.
- A document's queued/running metadata must not be automatically rewritten to completed merely because outputs from an older run still exist. Reprocessing preserves older files but the current run state remains authoritative.
- The browser's batch upload state permits idempotent retry of an already accepted upload position. Keep the one-file-per-request behavior and do not replace it with one large multipart body.
- Unfinished browser uploads are recoverable after refresh or browser restart. The durable batch record is authoritative; `localStorage` may retain only recovery hints such as batch id, expected count, and file signature. Derive upload, queue, status, and detail URLs from the current application base path rather than trusting stored URLs.
- Recovery requires the user to reselect the original complete file set in the same order. Skip server-confirmed positions and upload only missing ones before atomically queueing the batch. Clearing the browser recovery pointer must not silently delete the durable unfinished batch.
- Per-document raw extraction and standard mapping actions must use the durable `document_pipeline` queue, not execute inside the HTTP request. Persist `requested_stage`: `raw_metrics` reuses completed OCR and continues through standard mapping; `standard_metrics` reuses both OCR and raw facts and reruns mapping only.
- Queue and progress displays must reflect the requested resume stage. While a document job is queued or running, hide or disable mutating actions that conflict with it and reject deletion server-side.
- Batch status polling uses a server-generated state token that changes with job status and processing stage, so long OCR/extraction runs remain visible without high-frequency server work.
- A stale running job is failed after its own `timeout_seconds` plus `WEBAPP_WORKER_STALE_JOB_GRACE_SECONDS`. Recovery must release the serialized queue and mark the corresponding document stage failed without deleting completed artifacts.
- Current safety defaults are 50 MB, 300 pages, and five PDFs per batch. Validate both browser input and server-side content; browser validation alone is not a security or resource boundary.
- Primary regression coverage is in `tests/test_webapp.py`. Before changing this flow, run its batch/queue tests and then `python -m pytest -q`.

## Terminology Namespace Contract

- `config/standard_terms.yml` is synchronized from the 194 `ZT_*` subjects in `data/templates/会计报表.xlsx`; non-template terms use a separate non-`ZT_*` namespace.
- Regenerate and verify it with `python tools/sync_standard_terms_from_template.py` and `python tools/sync_standard_terms_from_template.py --check` after an approved template-subject change.
- Legacy local aliases and decisions from the former conflicting 14-term registry are migrated by canonical name and recorded in the SQLite `namespace_migrations` audit table.
- Never reinterpret a historical mapping by a conflicting code alone. For example, legacy `ZT_002 短期借款` migrates by name to `ZT_068 短期借款`; current `ZT_002` means `结算备付金`.

## Template Export Safety Contract

- `data/templates/会计报表.xlsx` contains demonstration amounts, so exporters must treat it as a layout source rather than trusted output data.
- `standardize/normalize/export.py` clears every result column from column B onward before writing real dynamic-period values. It must not leave unused `金额`, `期初`, or `期末` columns beside actual-period columns.
- Final-workbook validation is fail-closed when demonstration values, ambiguous placeholder headers, numeric values under blank result headers, or inconsistent period columns remain.
- Keep the `template_placeholder_values_removed` integrity check and its regression tests when changing workbook export behavior. Never weaken this contract to accommodate a new template; adapt and sanitize the template explicitly.

## Benchmark Label Policy

- Agent-produced reference labels are permitted for D01-D08 and future benchmark samples.
- Record the source document/page, labeling method, label version, confidence, and unresolved ambiguity for every reference set.
- Call agent-produced labels `reference labels` or `silver labels`; do not describe them as independently human-verified gold labels.
- Structural and accounting consistency checks should validate labels where possible, but they do not erase uncertainty in ambiguous source documents.
- On the representative corpus, prioritize retaining valid facts over minimizing the number of review rows.

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

- The production Nginx also carries the `/codex/` relay used to control this
  project. A task launched from that relay must never synchronously stop or
  force-recreate the Nginx service that carries its own connection.
- Never include `nginx` in the same `docker compose up --force-recreate`
  command as `web` or `worker`. Deploy only the application services that
  changed, wait for them to become healthy, and verify them before touching
  Nginx.
- Prefer `nginx -t` followed by a reload for Nginx-only configuration changes.
  If a real Nginx recreate is unavoidable, submit the complete validate,
  recreate, and health-check operation to a detached server-side systemd
  service before the old Nginx is stopped. Do not leave that operation owned by
  the browser, Codex process tree, or its SSH session.
- The server watchdog can recover a stopped/`Created` Nginx entry after a grace
  period, but it is a last-resort availability guard, not a deployment method.
  Docker's restart policy does not start a replacement container that never
  reached the running state.
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
