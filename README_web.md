# AutoFinance Web Review Workbench

The web app remains a thin orchestration and review layer over existing OCR and `standardize` artifacts.

It does not redesign the OCR pipeline, does not redesign the standardize pipeline, does not change cloud-first defaults, does not change batch or benchmark semantics, and does not promote PaddleOCR beyond pilot-only.

Current production recommendation remains cloud OCR first. Paddle stays pilot-only.

## Runtime Paths

All runtime state stays under `data/generated/web/`:

```text
data/generated/web/
  uploads/
  jobs/
  results/
  logs/
  webapp.sqlite3
  web_operation_queue_summary.json
```

Per-job web artifacts use these paths:

```text
data/generated/web/jobs/<job_id>/
  standardize/
  review/
    review_actions_filled.csv
    review_actions_filled.xlsx
    review_action_export_summary.json
    review_action_compatibility_summary.json
    review_dashboard_counts_summary.json
    review_workbench_summary.json
    review_evidence_preview_summary.json
    review_apply_preview_summary.json
    review_operation_summary.json
    operation_stage_timeline.json
    operation_lock_summary.json
    operation_retry_summary.json
    operations/
      <operation_id>/
        review_operation_summary.json
        operation_stage_timeline.json
        operation.log
        operation_retry_summary.json
  reruns/
    <rerun_id>/
      standardize/
      standardize_stdout.txt
      standardize_stderr.txt

data/generated/web/results/<job_id>/
  job_summary.json
  job_quality_summary.json
  job_log_bundle.json
  reruns/
    <rerun_id>/
      job_quality_summary.json
      review_rerun_summary.json
      review_rerun_delta.json
      review_rerun_delta_explained.json
      review_rerun_only_summary.json
      review_apply_and_rerun_summary.json
```

The web app should not write operation or rerun artifacts into the repo root.

## Queue Backend Modes

`WEBAPP_QUEUE_BACKEND` controls how long review operations are dispatched:

- `local`: default local queue-backed mode. Routes create an operation record and return quickly; an in-process or external polling worker executes queued operations.
- `rq`: optional Redis/RQ mode. Routes still create the same persisted operation record, but dispatch execution to an RQ worker.

Common environment variables:

```text
WEBAPP_QUEUE_BACKEND=local|rq
REDIS_URL=redis://redis:6379/0
WEBAPP_OPERATION_TIMEOUT_SECONDS=3600
WEBAPP_AUTH_REQUIRED=0|1
WEBAPP_ADMIN_PASSWORD=...
```

The operation abstraction is the same in both modes: queueing, status polling, logs, cancel, retry, and duplicate protection all use the persisted web operation record under the job review directory.

## Operation Types

Supported operation types:

- `apply_review_actions`: 应用复核动作
- `apply_and_rerun`: 应用复核并重新生成
- `rerun_only`: 仅重新生成

## Operation States

Each review operation has:

- `operation_id`
- `job_id`
- `operation_type`
- `status`
- `created_at`
- `started_at`
- `finished_at`
- `duration_seconds`
- `progress_stage`
- `progress_message_zh`
- `log_paths`
- `result_paths`
- `error_message`
- `user_friendly_error_zh`

Operation status values:

- `created`: 已创建
- `queued`: 排队中
- `running`: 运行中
- `succeeded`: 已完成
- `failed`: 失败
- `cancelled`: 已取消

The latest summary is copied to `review_operation_summary.json`; the full event trail is copied to `operation_stage_timeline.json`.

## Review Operation Behavior

These routes now return quickly and do not block the browser until standardization finishes:

- `POST /jobs/{job_id}/review/apply`
- `POST /jobs/{job_id}/review/apply-and-rerun`
- `POST /jobs/{job_id}/review/rerun`

Polling and inspection routes:

- `GET /jobs/{job_id}/review/operation-status`
- `GET /jobs/{job_id}/operations`
- `GET /jobs/{job_id}/operations/{operation_id}`
- `GET /jobs/{job_id}/operations/{operation_id}/logs`

Operation control routes:

- `POST /jobs/{job_id}/operations/{operation_id}/cancel`
- `POST /jobs/{job_id}/operations/{operation_id}/retry`

## Duplicate Protection And Lock Policy

The web app enforces one active review operation per job for these mutating paths:

- `apply_review_actions`
- `apply_and_rerun`
- `rerun_only`

Current lock policy is `reject`.

If an operation for the same job is already `created`, `queued`, or `running`, a second request is blocked and `operation_lock_summary.json` records the blocker.

## Cancel And Retry

Cancel behavior:

- Queued operations can be cancelled immediately.
- Running operations are marked `cancel_requested` and stop at the next safe checkpoint.
- The rerun subprocess supports best-effort cancellation in local execution by terminating the child process.
- Some apply work still cannot be interrupted mid-function; the UI and summaries remain explicit about that limitation.

Retry behavior:

- Failed or cancelled operations can be retried.
- Retry creates a new operation id.
- `operation_retry_summary.json` records the source operation and the new retry operation.

## Why Apply + Rerun Can Take Minutes

`apply_and_rerun` does two kinds of work:

1. Exported review actions are applied to a job-specific config snapshot.
2. `standardize.cli` is rerun against the original job input using that snapshot.

The web app does not rewrite base configs directly from the UI. That is why apply outputs are first written under the job review directory, and reruns are then written under the job-specific rerun directories. The browser returns immediately, but the underlying standardization pass can still take minutes depending on fixture size and source complexity.

## Inspecting Logs

The job detail page and export/apply page show:

- latest operation status
- operation type
- current stage
- elapsed time
- last log lines
- result paths
- cancel/retry buttons when appropriate

The log-tail helper is restricted to allowed job directories only. It does not expose arbitrary filesystem files.

## Chinese Labels

User-facing dropdowns and operation UI use centralized Chinese labels for:

- review `source_type`
- review `reason_code`
- review `status`
- apply compatibility
- operation status
- operation type
- provider mode

Examples:

- `backend_ready` -> `可自动应用`
- `partial` -> `部分支持`
- `suggestion_only` -> `仅作为建议`
- `unsupported` -> `暂不支持`
- `validation:*` -> `校验相关`
- `mapping:*` -> `科目映射问题`
- `apply_and_rerun` -> `应用复核并重新生成`
- `running` -> `运行中`
- `succeeded` -> `已完成`
- `failed` -> `失败`
- `cancelled` -> `已取消`

## Local Development

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Recommended local dev mode:

```bash
$env:WEBAPP_ENV="dev"
$env:WEBAPP_AUTH_REQUIRED="0"
$env:WEBAPP_QUEUE_BACKEND="local"
$env:WEBAPP_ENABLE_LOCAL_WORKER="1"
uvicorn webapp.main:app --reload
```

If you prefer an external worker:

```bash
$env:WEBAPP_ENABLE_LOCAL_WORKER="0"
uvicorn webapp.main:app --reload

python -m webapp.runner run-worker
```

Run one queued item only:

```bash
python -m webapp.runner run-worker --once
```

## Trial Deployment Mode

Example trial deployment env:

```bash
$env:WEBAPP_ENV="prod"
$env:WEBAPP_AUTH_REQUIRED="1"
$env:WEBAPP_ADMIN_PASSWORD="change-this"
$env:WEBAPP_QUEUE_BACKEND="local"
$env:WEBAPP_ENABLE_LOCAL_WORKER="0"
docker compose up --build -d
```

The compose skeleton includes:

- `web`: FastAPI + Jinja server with `/healthz`
- `worker`: queue worker with its own healthcheck
- `redis`: optional Redis service for `rq` mode

`./data:/app/data` is mounted so runtime outputs remain outside the image and continue to follow the repo path contract.

## Queue Mode With Redis/RQ

To use Redis-backed queue dispatch:

```bash
$env:WEBAPP_QUEUE_BACKEND="rq"
$env:REDIS_URL="redis://127.0.0.1:6379/0"
$env:WEBAPP_ENABLE_LOCAL_WORKER="0"
```

Then run either:

```bash
docker compose --profile rq up --build
```

or locally:

```bash
python -m webapp.operations run-rq-worker
```

## Known Limitations

- Authentication is still single-password and not RBAC.
- The web app remains an orchestration/review layer; it does not replace deterministic backend logic.
- `cancel` is honest but still best-effort for some in-process apply steps.
- Long reruns can still take minutes; queueing removes browser blocking but not backend compute cost.
- `rerun_only` uses the latest available job-safe config snapshot, or a fresh snapshot copy when none exists.
- The web app does not expose arbitrary files; log tails and operation downloads are restricted to allowed job directories.
- Cloud-first defaults remain unchanged.
- Paddle remains pilot-only and is not promoted as a production fallback in the web path.
