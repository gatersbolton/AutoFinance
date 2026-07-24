# AutoFinance

AutoFinance is an OCR-to-standardization product for scanned financial statements. It keeps code in the repo and keeps corpus files, credentials, vendor checkouts, and generated artifacts under `data/`, which is intentionally ignored by Git.

## Product Scope

The intended product has two connected deliverables in the same project:

1. A reviewable structured financial dataset, primarily exported as Excel or CSV, so finance staff can inspect and correct extraction, period, value, and terminology results.
2. A filled accounting workbook/template produced from the reviewed financial facts.

The target business flow is:

```text
PDF -> OCR evidence -> structured facts -> confidence-based automation and human review
    -> Excel/CSV dataset + filled accounting workbook
```

The reviewed structured-fact layer is the shared source for both deliverables. Internally, facts should use a long form in which one metric, period, and reporting scope identify one value. Finance-facing Excel exports may pivot the same facts into familiar beginning/end or current/prior columns.

The current default accounting template is `data/templates/会计报表.xlsx`. Its 194 `ZT_*` subjects are the canonical template terminology. Template filling currently covers the balance sheet and income statement; cash-flow workbook support is deferred. Template-specific placement should remain behind an adapter boundary so additional layouts can be supported later without changing the canonical facts.

The structured dataset should retain every valid extracted metric, including metrics that do not belong in the current template. Each fact is classified as template-mapped, known but outside the template, unresolved mapping, or uncertain extraction/period. Only safely template-mapped facts fill workbook cells. Arbitrary numbers of periods are supported internally and may be rendered as dynamic period columns in Excel.

A missing OCR date is not, by itself, a reason to discard an otherwise valid metric and value. The system should infer the date or period when the available evidence is sufficiently reliable; otherwise it should preserve the row for human review.

The current amount scope uses normalized RMB-yuan values with precise decimal arithmetic and does not require foreign-currency conversion. OCR text, raw values, units, and inference evidence should still be retained as audit provenance rather than used as the downstream business value. Only genuine one-to-one synonyms may map automatically; aggregate, split, or otherwise ambiguous relationships require an explicit rule or review.

Users may download a draft dataset with visible risk markers at any time. A workbook with unresolved high-risk facts remains marked incomplete unless the user explicitly requests a risk-bearing export; that decision must be recorded.

The expected deployment environment is a domestic enterprise server. Auditability, data-boundary controls, recoverable failures, and reproducible exports therefore need to be treated as product requirements. The current product policy permits calls to public Aliyun/Tencent OCR and DeepSeek, including the financial context required for extraction and mapping, while provider use must remain explicit, configurable, and auditable.

The expected installation has one or two regular users and at most roughly five concurrent users, using one shared application account. It may run on a low-specification server. Horizontal scaling and load balancing are not current requirements; simple resource-bounded execution, failure recovery, and preservation of completed work are.

The production target is Linux with Docker. The current business scope is annual reports. Users typically upload batches of two to five PDFs. Processing should use a durable, serialized background queue rather than keeping OCR inside an HTTP request; SQLite and one worker are sufficient for the current scale. Cloud OCR is the production default, with the primary provider selected from representative-corpus quality and the other permitted provider used as fallback.

Representative D01-D08 inputs range from 3 to 69 pages and from under 1 MB to about 26 MB. Initial configurable safety limits may therefore use 50 MB and 300 pages per PDF, five PDFs per upload batch, and one actively processing document at a time. A browser batch should upload one file per request and group the resulting document ids, avoiding one oversized multipart request on the small server.

Original PDFs, result versions, and review history are retained until a user explicitly deletes them. Automatic backups are not part of the current deployment scope, so the application and deployment documentation must not imply that retained data is protected from server or disk loss.

The current users work in desktop Chrome. Responsive mobile workflows and external completion notifications are out of scope; durable status and progress are displayed in the web application.

The observed production server baseline on 2026-07-23 is 2 vCPU, about 1.6 GB RAM with no swap, and a 40 GB root disk with about 17 GB free. This is a binding design constraint, not a performance-test target.

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
    raw_metrics/
    standard_metrics/
    experiments/
      paddle_provider_pilot/
      paddle_provider_eval/
    standardize/
      control_runs/
        paddle_provider_pilot/
        paddle_provider_eval/
    web/
      uploads/
      jobs/
      results/
      logs/
```

The workbook exporter treats the template as layout only: it clears demonstration/result columns before writing actual dynamic periods and rejects outputs that retain placeholder headers or example amounts.

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
python -m pytest -q
```

`pytest.ini` limits collection to the repository test suite and excludes local corpus, vendor, virtual-environment, generated-data, and scratch directories.

## Docs

- `AGENTS.md`: repo map and path contract for Codex and other agents
- `benchmarks/registry.yml`: document registry for batch runs
- `benchmarks/paddle_pilot_registry.yml`: small Stage 8.1 evaluation sample for Paddle quality gating
- `standardize/README*.md`: stage-specific notes for the standardization pipeline
- `README_stage8_paddle_provider.md`: Paddle pilot and Stage 8.1 quality-eval notes
- `README_web.md`: Stage 9 web MVP local-dev and deployment notes
