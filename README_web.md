# AutoFinance Web MVP

This Stage 9 web layer is a thin orchestration wrapper around the existing OCR and `standardize.cli` commands. It does not replace the deterministic standardization backend, and it does not promote PaddleOCR to a production fallback.

Current production recommendation: cloud OCR first. Paddle remains pilot-only.

## Runtime Paths

All web runtime artifacts stay under `data/generated/web/`:

```text
data/generated/web/
  uploads/
  jobs/
  results/
  logs/
  webapp.sqlite3
  web_mvp_summary.json
  web_mvp_hardening_summary.json
```

`jobs/<job_id>/standardize/` contains standardization outputs. `results/<job_id>/` contains web-facing summary JSON, `job_quality_summary.json`, and log bundle JSON.

Stage 10 review UI exports are written under:

```text
data/generated/web/jobs/<job_id>/review/
  review_actions_filled.csv
  review_actions_filled.xlsx
  review_action_export_summary.json
```

## Local Development

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Recommended local env:

```bash
$env:WEBAPP_ENV="dev"
$env:WEBAPP_AUTH_REQUIRED="0"
```

Start the web app with the in-process local worker enabled:

```bash
uvicorn webapp.main:app --reload
```

Or run the worker separately:

```bash
$env:WEBAPP_ENABLE_LOCAL_WORKER="0"
uvicorn webapp.main:app --reload

python -m webapp.runner run-worker
```

Run a single queued job once:

```bash
python -m webapp.runner run-worker --once
```

## Deployment Startup

Use deployment mode for trial environments:

```bash
$env:WEBAPP_ENV="prod"
$env:WEBAPP_AUTH_REQUIRED="1"
$env:WEBAPP_ADMIN_PASSWORD="change-this"
docker compose up --build -d
```

If `WEBAPP_AUTH_REQUIRED=1` but `WEBAPP_ADMIN_PASSWORD` is missing, the app now fails startup with a clear configuration error.

## Creating Jobs

### Standardize-only job

Use the web form and select `existing_ocr_outputs`, then point it at a path such as:

```text
data/corpus/D01/ocr_outputs
```

The worker will run:

```bash
python -m standardize.cli --input-dir ... --template ... --output-dir data/generated/web/jobs/<job_id>/standardize --output-run-subdir none
```

### Upload PDF job

Upload one or more PDFs. Files are stored under `data/generated/web/uploads/<job_id>/`.

- If `WEBAPP_AUTO_RUN_UPLOAD_OCR=1` and cloud OCR credentials appear configured, the worker will run `OCR.py` and then `standardize.cli`.
- If auto OCR is disabled, the upload is stored and the job remains in a created / OCR-pending state until re-queued under a configured environment.

## Job Status Meanings

- `created`: 任务已创建，但尚未进入处理队列。
- `queued`: 任务已入队，等待 worker。
- `running`: 任务正在执行。
- `succeeded`: 已完成，当前没有关键警告信号。
- `succeeded_with_warnings`: 已生成结果，但存在完整性或合同类警告。
- `needs_review`: 已生成结果，但存在人工复核或校验失败信号，建议先复核再交付。
- `failed`: 后台命令未成功完成，当前结果不建议直接使用。
- `cancelled`: 任务已取消。

“完成但建议复核”通常意味着：

- 生成了会计报表；
- 但 `review_total`、`validation_fail_total` 或类似质量信号不为 0；
- 建议先下载人工复核表、问题清单和日志后再交付。

## Docker Compose / Trial Deployment

1. Copy `.env.example` to `.env`.
2. Adjust `WEBAPP_ENV`, `WEBAPP_AUTH_REQUIRED`, `WEBAPP_ADMIN_PASSWORD`, and OCR credentials as needed.
3. Start the skeleton deployment:

```bash
docker compose up --build
```

The compose file includes:

- `web`: FastAPI + Jinja app
- `worker`: local polling worker process
- `redis`: optional future RQ profile
- `postgres`: optional future database profile

Redis and Postgres are still optional future components. The current web app continues to use SQLite by default and does not require Redis/Postgres unless a later stage adopts them.

## Outputs Exposed In The UI

If present, the job detail page exposes download links for:

- `会计报表_填充结果.xlsx`
- `run_summary.json`
- `artifact_integrity.json`
- `review_workbook.xlsx`
- `review_queue.csv`
- `issues.csv`
- `validation_results.csv`
- `job_summary.json`
- `job_quality_summary.json`
- `job_log_bundle.json`
- `conflicts_enriched.csv`
- `conflict_decision_audit.csv`
- `unplaced_facts.csv`
- `mapping_candidates.csv`
- `benchmark_gap_explanations.csv`
- `source_backed_gap_closure.csv`
- `review_actions_filled.csv`
- `review_actions_filled.xlsx`
- `review_action_export_summary.json`

If an expected artifact is missing, the UI shows `未生成`.

## Review Dashboard

Open a completed or `needs_review` job, then use:

- `/jobs/<job_id>/review` for the audit-facing review dashboard
- `/jobs/<job_id>/review/items` for the filterable review-item list
- `/jobs/<job_id>/review/export-actions` to export saved actions

The review dashboard is a server-rendered triage layer over existing `standardize` artifacts. It reads review-oriented files such as `review_queue.csv`, `issues.csv`, `validation_results.csv`, `conflicts_enriched.csv`, `unplaced_facts.csv`, `mapping_candidates.csv`, and related review workbook / gap files when available. Missing files are shown as unavailable instead of crashing the page.

## Review Statuses

- `unresolved`: 尚未在 Web 复核界面中保存动作。
- `resolved`: 已保存一个处理动作，通常表示该条目已被人工判定。
- `ignored`: 已标记忽略，不再作为当前待办。
- `deferred`: 已暂缓，后续仍应进入复核积压。
- `reocr_requested`: 已记录需要二次 OCR 的请求，但本阶段不会自动 rerun。

## Review Action Types

- `ignore`: 关闭当前条目，不直接改动底层事实。
- `defer`: 记录暂缓，保留到后续复核积压。
- `mark_not_financial_fact`: 标记为非财务事实，供后续 apply 阶段使用。
- `request_reocr`: 记录需要二次 OCR 的请求。
- `accept_mapping_candidate`: 接受当前候选映射；导出时会尽量映射到现有 backend 可识别动作。
- `set_mapping_override`: 指定局部映射覆盖值。
- `set_conflict_winner`: 指定供应商冲突的胜出结果。
- `suppress_false_positive`: 标记 OCR 误报，供后续 apply 阶段使用。

## Exporting Review Actions

1. 在 `/jobs/<job_id>/review/items` 页面逐条保存动作。
2. 打开 `/jobs/<job_id>/review/export-actions`。
3. 点击“生成导出文件”。
4. 从任务详情页或导出页下载 `review_actions_filled.csv` / `review_actions_filled.xlsx` / `review_action_export_summary.json`。

导出文件会尽量复用现有 `standardize.feedback` 的动作模板字段，例如 `review_id`, `action_type`, `action_value`, `reviewer_note`, `reviewer_name`, `review_status`, `source_type`, `source_cell_ref`, `candidate_mapping_code`, `candidate_conflict_fact_id` 等。这样后续可以尽量对接既有 backend `--apply-review-actions` 工作流。

当前限制：

- Web UI 会保存并导出动作，但不会直接改写基础 standardize 配置。
- 对 `review_queue.csv` 原生条目以外的 Web 复核项，现有 backend apply 流程可能仍需要补齐 `review_id` 或其他字段。
- `accept_mapping_candidate` 会在导出层尽量映射到现有 backend 支持动作；兼容性缺口会写入 `review_action_export_summary.json`。
- 自动 apply + rerun 计划放到 Stage 10.1，不在当前 MVP 范围内。

## System Status

The system page and `/api/system-status` endpoint show:

- app version
- Python version
- template path existence
- web runtime directory existence
- available provider modes
- whether Redis is configured
- whether OCR credentials appear configured without showing secret values
- whether authentication is enabled / required
- worker mode

The page does not display secret values. TODO: Stage 10/11 should restrict system-status visibility by role.

## Known Limitations

- Authentication is still single-password and not multi-user RBAC.
- Queueing uses a local SQLite-backed polling worker, not Redis/RQ.
- Running jobs cannot be force-killed from the web UI in this MVP.
- Upload jobs are safe but conservative; if cloud OCR is not configured they remain pending or fail with a clear message rather than falling back to Paddle.
- The web app does not browse arbitrary filesystem locations. Existing OCR paths are limited to `data/corpus/` or `data/generated/web/`.
- Production OCR recommendation remains cloud-first. Paddle remains pilot-only and is not used as a production fallback in the web path.
