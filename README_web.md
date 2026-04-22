# AutoFinance Web Demo

This web app is a thin orchestration and review layer for the existing `OCR.py` and `standardize.cli` pipeline.

Stage 11 goals:

- user opens the web app in a browser
- user uploads PDF financial statements
- backend runs cloud OCR through the existing `OCR.py`
- backend runs `standardize.cli`
- user downloads the filled workbook
- user enters the review workbench when the job becomes `needs_review`
- user can apply review actions and rerun

Important constraints remain unchanged:

- no LLM
- no redesign of `OCR.py`
- no redesign of `standardize.cli`
- cloud OCR stays the production path
- PaddleOCR remains pilot-only
- this is a customer demo deployment, not a final enterprise production release

## Runtime Paths

All web runtime state stays under `data/generated/web/`:

```text
data/generated/web/
  uploads/
  jobs/
  results/
  logs/
  webapp.sqlite3
  worker_heartbeat.json
  deployment_check_summary.json
  backup_summary.json
  cleanup_summary.json
  aliyun_demo_deploy_summary.json
```

Uploads and job outputs never go into the repo root.

## Main Web Flows

### 1. 上传 PDF 开始处理

- Upload one or more PDFs.
- Choose OCR provider:
  - `cloud_first`
  - `aliyun_table`
  - `tencent_table_v3`
- PDFs are stored under `data/generated/web/uploads/<job_id>/`.
- OCR outputs are written under `data/generated/web/jobs/<job_id>/ocr_outputs/`.
- Standardize outputs are written under `data/generated/web/jobs/<job_id>/standardize/`.
- Result summaries are written under `data/generated/web/results/<job_id>/`.
- OCR and standardize logs are captured separately in `data/generated/web/logs/<job_id>/`.

### 2. 使用已有 OCR 结果

- Reuse an existing OCR output directory under `data/corpus/` or `data/generated/web/`.
- Skip OCR and run `standardize.cli` directly.
- This is useful for smoke testing and issue isolation.

### 3. 复核与重跑

- Review workbench entry remains under `/jobs/{job_id}/review`.
- Supported background operations remain:
  - `apply_review_actions`
  - `apply_and_rerun`
  - `rerun_only`
- Duplicate `apply_and_rerun` requests are still blocked.
- Operation status polling and log tails still work.

## Queue Modes

`WEBAPP_QUEUE_BACKEND` controls review-operation dispatch:

- `local`: default for the demo. A polling worker handles jobs and review operations.
- `rq`: optional Redis/RQ mode. The deployment worker service runs a job poller plus an RQ worker for review operations.

Demo recommendation:

- start with `WEBAPP_QUEUE_BACKEND=local`
- switch to `rq` only when you need Redis-backed review-operation dispatch

## System Status Page

`/system` now shows a demo-operator view in Chinese:

- 系统是否可用
- 模板是否存在
- OCR 密钥是否配置
- 当前默认 OCR 方式
- 队列是否可用
- Worker 是否可用
- 存储目录是否可写
- 最近任务
- 常见故障提示

The page does not display secret values.

## Deployment And Ops Scripts

### Deployment check

```bash
python scripts/deployment_check.py
```

Writes:

- `data/generated/web/deployment_check_summary.json`

Checks:

- Python version
- runtime directories
- template existence
- write permissions
- `WEBAPP_ADMIN_PASSWORD` in prod
- Redis reachability when `WEBAPP_QUEUE_BACKEND=rq`
- OCR credential file existence and parse status
- active OCR provider readiness
- free disk space
- nginx config presence for the aliyun profile

### Backup

```bash
python scripts/backup_data.py
python scripts/backup_data.py --include-corpus
```

Writes:

- archive under `data/generated/audits/backups/`
- `data/generated/web/backup_summary.json`

### Cleanup

Preview only:

```bash
python scripts/cleanup_old_jobs.py
```

Actually delete old terminal jobs:

```bash
python scripts/cleanup_old_jobs.py --age-days 14 --apply
```

Writes:

- `data/generated/web/cleanup_summary.json`

The cleanup script only touches old job data under `data/generated/web/` and never deletes secrets.

## Local Run

Install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Start web app:

```bash
$env:WEBAPP_ENV="dev"
$env:WEBAPP_AUTH_REQUIRED="0"
$env:WEBAPP_ENABLE_LOCAL_WORKER="1"
$env:WEBAPP_AUTO_RUN_UPLOAD_OCR="1"
uvicorn webapp.main:app --reload
```

Or start a separate worker:

```bash
$env:WEBAPP_ENABLE_LOCAL_WORKER="0"
uvicorn webapp.main:app --reload

python -m webapp.runner run-service
```

## Docker Demo Run

Copy an env file first:

```bash
Copy-Item .env.example .env
```

Then run:

```bash
docker compose up --build -d
```

Or for the Alibaba Cloud profile:

```bash
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml up --build -d
```

## Logs

Container logs:

```bash
docker compose logs -f web
docker compose logs -f worker
docker compose logs -f nginx
```

Per-job logs:

- OCR stdout: `data/generated/web/logs/<job_id>/ocr_stdout.txt`
- OCR stderr: `data/generated/web/logs/<job_id>/ocr_stderr.txt`
- standardize stdout: `data/generated/web/logs/<job_id>/standardize_stdout.txt`
- standardize stderr: `data/generated/web/logs/<job_id>/standardize_stderr.txt`

## Known Limitations

- Authentication is still single-password basic auth, not RBAC.
- SQLite is acceptable for the demo, but not the final multi-user production database story.
- `rq` is optional and currently primarily targets review-operation dispatch; the deployment worker still keeps the persistent job poller.
- OCR smoke/mock mode exists for CI and smoke tests only. Do not rely on it for customer delivery.
- Cloud OCR remains the production path.
- PaddleOCR remains pilot-only and is not installed into the web deployment image.
- This Stage 11 deployment is for customer demos on a small Alibaba Cloud CPU server, not a final enterprise deployment blueprint.
