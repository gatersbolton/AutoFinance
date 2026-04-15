# AutoFinance

AutoFinance is a local OCR-to-standardization workflow for scanned financial statements. It keeps code in the repo and keeps corpus files, credentials, vendor checkouts, and generated artifacts under `data/`, which is intentionally ignored by Git.

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Recommended local layout:

```text
data/
  corpus/
    inbox/
      input/
      ocr_outputs/
    D01/..D08/
  templates/
    会计报表.xlsx
  secrets/
    secret
  vendor/
    PaddleOCR/
  generated/
    experiments/
      paddle_provider_pilot/
    standardize/
      control_runs/
        paddle_provider_pilot/
```

## Main Commands

Run OCR into the default inbox output root:

```bash
python OCR.py --method tencent_table_v3

python OCR.py --method paddle_table_local ^
  --paddle-runtime-python .venv_paddlegpu\Scripts\python.exe
```

Run single-document standardization:

```bash
python -m standardize.cli ^
  --input-dir data/corpus/inbox/ocr_outputs ^
  --template data/templates/会计报表.xlsx ^
  --output-dir data/generated/standardize/archive ^
  --source-image-dir data/corpus/inbox/input ^
  --provider-priority aliyun,tencent ^
  --enable-conflict-merge
```

Run multi-document batch standardization:

```bash
python -m standardize.batch ^
  --template data/templates/会计报表.xlsx ^
  --output-dir data/generated/standardize/batches/default ^
  --registry benchmarks/registry.yml ^
  --batch-mode
```

## Credentials

By default `OCR.py` reads credentials from `data/secrets/secret`:

```text
Tencent:
SecretId:YOUR_TENCENT_SECRET_ID
SecretKey:YOUR_TENCENT_SECRET_KEY

Aliyun:
AccessKey ID:YOUR_ALIYUN_ACCESS_KEY_ID
AccessKey Secret:YOUR_ALIYUN_ACCESS_KEY_SECRET
```

Environment variables still override the file:

- `TENCENTCLOUD_SECRET_ID`
- `TENCENTCLOUD_SECRET_KEY`
- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`

## Tests

```bash
python -m unittest discover -s tests
```

## Docs

- `AGENTS.md`: repo map and path contract for Codex and other agents
- `benchmarks/registry.yml`: document registry for batch runs
- `standardize/README*.md`: stage-specific notes for the standardization pipeline
- `README_stage8_paddle_provider.md`: optional Paddle local provider pilot notes
