# AutoFinance OCR

This project batch-processes scanned PDF audit reports in `data/` with multiple OCR methods from Tencent Cloud and Aliyun.
The current methods are:

- `tencent_text`: Tencent `GeneralBasicOCR`
- `aliyun_text`: Aliyun `RecognizeAllText` with `Type=Advanced`
- `tencent_table_v3`: Tencent `RecognizeTableAccurateOCR`
- `aliyun_table`: Aliyun `RecognizeTableOcr`

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Secrets

By default the tool reads credentials from the root-level `secret` file:

```text
Tencent:
SecretId:YOUR_TENCENT_SECRET_ID
SecretKey:YOUR_TENCENT_SECRET_KEY

Aliyun:
AccessKey ID:YOUR_ALIYUN_ACCESS_KEY_ID
AccessKey Secret:YOUR_ALIYUN_ACCESS_KEY_SECRET
```

Environment variables are also supported and override the file:

- `TENCENTCLOUD_SECRET_ID`
- `TENCENTCLOUD_SECRET_KEY`
- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`

## Usage

Run Tencent text OCR:

```bash
python3 OCR.py --method tencent_text
```

Run Aliyun text OCR:

```bash
python3 OCR.py --method aliyun_text
```

Run Tencent table OCR V3:

```bash
python3 OCR.py --method tencent_table_v3
```

Run Aliyun table OCR:

```bash
python3 OCR.py --method aliyun_table
```

Optional flags:

```bash
python3 OCR.py --method tencent_table_v3 --input data --output outputs --secret secret
```

## Output Layout

Each provider writes results to:

```text
outputs/<provider>/<pdf_stem>/
  result.txt
  result.json
  raw/page_0001.json
  artifacts/page_0001.xlsx  # table methods when the provider returns a workbook
```

- `result.txt`: readable text grouped by page
- `result.json`: normalized page-level OCR result
- `raw/*.json`: original provider responses for each page
- `artifacts/*`: provider-specific exported files, currently used by `tencent_table_v3` for per-page Excel workbooks

If any page fails, the tool still writes outputs and returns a non-zero exit code.

## Test

The automated tests mock PDF rendering and cloud OCR calls, so they do not need the SDKs or real credentials:

```bash
python3 -m unittest testOCR.py
```
