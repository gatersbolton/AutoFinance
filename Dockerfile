FROM python:3.10-slim

ARG PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_DEFAULT_TIMEOUT=120
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV WEBAPP_ENV=prod
ENV WEBAPP_ENABLE_LOCAL_WORKER=0
ENV WEBAPP_QUEUE_BACKEND=local
ENV WEBAPP_UPLOAD_OCR_METHOD=cloud_first
ENV WEBAPP_AUTO_RUN_UPLOAD_OCR=1
ENV WEBAPP_PROVIDER_PRIORITY=aliyun,tencent
ENV WEBAPP_DEEPSEEK_ENV_PATH=data/secrets/deepseek.env
ENV DEEPSEEK_MODEL=deepseek-v4-flash
ENV DEEPSEEK_BASE_URL=https://api.deepseek.com
ENV DEEPSEEK_TIMEOUT_SECONDS=60
ENV DEEPSEEK_ENABLED=false
ENV LLM_MAPPING_MAX_CANDIDATES=20
ENV LLM_MAPPING_CACHE_ENABLED=true
ENV LLM_MAPPING_MOCK=false

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz').read()"

CMD ["uvicorn", "webapp.main:app", "--host", "0.0.0.0", "--port", "8000"]
