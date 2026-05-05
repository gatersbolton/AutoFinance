# Alibaba Cloud Demo Deployment MVP

This document is for a customer-demo deployment on a small Alibaba Cloud CPU server.

It is intentionally pragmatic:

- production default remains cloud OCR
- PaddleOCR remains pilot-only
- SQLite is acceptable for the demo
- this is not the final enterprise deployment architecture

## 1. Prepare the server

Recommended minimum server spec:

- 2 vCPU
- 4 GB RAM
- 40 GB system disk
- Ubuntu 22.04 LTS or another recent Linux distribution with Docker support

Open ports:

- `80` for nginx
- `22` for SSH

## 2. Install Docker

Install Docker Engine and the Docker Compose plugin by following Alibaba Cloud or Docker official instructions for your Linux distribution.

After installation, verify:

```bash
docker --version
docker compose version
```

## 3. Copy the project

Upload or clone the repository to the server, for example:

```bash
git clone <your-repo-url> AutoFinance
cd AutoFinance
```

Or upload the prepared project directory with SFTP / SCP.

## 4. Prepare the `.env` file

Copy the Alibaba Cloud example:

```bash
cp .env.aliyun.example .env.aliyun
```

Edit `.env.aliyun` and set at least:

- `WEBAPP_ADMIN_PASSWORD`
- `WEBAPP_QUEUE_BACKEND`
- `WEBAPP_UPLOAD_OCR_METHOD`
- `REDIS_URL`

Recommended demo values:

```text
WEBAPP_ENV=prod
WEBAPP_AUTH_REQUIRED=1
WEBAPP_ENABLE_LOCAL_WORKER=0
WEBAPP_QUEUE_BACKEND=local
WEBAPP_AUTO_RUN_UPLOAD_OCR=1
WEBAPP_UPLOAD_OCR_METHOD=cloud_first
WEBAPP_PROVIDER_PRIORITY=aliyun,tencent
PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
```

`PIP_INDEX_URL` is used only while building the Docker image. Keep the Alibaba Cloud mirror on small mainland China servers; override it only if your server has reliable access to another PyPI index.

## 5. Configure OCR credentials

Choose one of these methods:

### Method A: secret file

Create:

```text
data/secrets/secret
```

Format:

```text
aliyun:
  AccessKeyId: your-id
  AccessKeySecret: your-secret

tencent:
  SecretId: your-id
  SecretKey: your-secret
```

You only need the provider you actually use.

### Method B: environment variables

For Aliyun:

- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`

For Tencent:

- `TENCENTCLOUD_SECRET_ID`
- `TENCENTCLOUD_SECRET_KEY`

Do not bake secrets into images.

## 6. Run the deployment preflight check

Before starting containers:

```bash
python scripts/deployment_check.py
```

Check:

- `data/generated/web/deployment_check_summary.json`

If `pass=false`, fix the reported issue before continuing.

## 7. Start services

```bash
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml up --build -d
```

Services started:

- `web`
- `worker`
- `redis`
- `nginx`

## 8. Open the browser

Visit:

```text
http://<your-server-ip>/
```

If `WEBAPP_AUTH_REQUIRED=1`, the browser will ask for the admin password.

## 9. Verify the app after startup

Check health:

```bash
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml ps
curl http://127.0.0.1/healthz
```

Open the web UI and confirm:

- home page loads
- `/system` shows the deployment status panel
- upload PDF page is available
- recent tasks can be viewed

## 10. View logs

Container logs:

```bash
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml logs -f web
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml logs -f worker
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml logs -f nginx
```

Per-job logs in the mounted data directory:

- `data/generated/web/logs/<job_id>/ocr_stdout.txt`
- `data/generated/web/logs/<job_id>/ocr_stderr.txt`
- `data/generated/web/logs/<job_id>/standardize_stdout.txt`
- `data/generated/web/logs/<job_id>/standardize_stderr.txt`

## 11. Backup data

Backup the demo runtime:

```bash
python scripts/backup_data.py
```

If you also want corpus data:

```bash
python scripts/backup_data.py --include-corpus
```

Generated files:

- archive under `data/generated/audits/backups/`
- `data/generated/web/backup_summary.json`

## 12. Clean up old jobs

Preview first:

```bash
python scripts/cleanup_old_jobs.py
```

Apply deletion:

```bash
python scripts/cleanup_old_jobs.py --age-days 14 --apply
```

Generated file:

- `data/generated/web/cleanup_summary.json`

## 13. Known limitations

- SQLite is still used for the demo deployment.
- This deployment targets a small Alibaba Cloud CPU server and is optimized for usability, not for enterprise-grade HA.
- `rq` is optional; the recommended first deployment is still `WEBAPP_QUEUE_BACKEND=local`.
- Real upload processing depends on cloud OCR credentials being available.
- OCR smoke/mock mode exists only for testing and smoke runs.
- PaddleOCR remains pilot-only and is not part of the deployment default.

## 14. Build troubleshooting

If Docker build fails at `pip install` with `Could not find a version that satisfies the requirement ...`, first verify the active pip index can see the package:

```bash
docker run --rm python:3.10-slim python -m pip index versions tencentcloud-sdk-python-ocr -i https://mirrors.aliyun.com/pypi/simple
```

Then rebuild without stale cache:

```bash
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml build --no-cache web worker
docker compose --env-file .env.aliyun -f docker-compose.yml -f docker-compose.aliyun.yml up -d
```

## 15. Recommended next step after MVP

After the demo deployment is stable, the next practical step is:

- keep the same UI and job contract
- move from SQLite demo persistence toward a managed database
- decide whether to standardize on Redis/RQ for review operations in the hosted environment
- harden authentication beyond single shared password
