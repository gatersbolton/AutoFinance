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

If an expected artifact is missing, the UI shows `未生成`.

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
