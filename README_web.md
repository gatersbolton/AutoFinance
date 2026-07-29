# AutoFinance Web

The ordinary web flow starts with a logical upload batch and finishes in the
reviewable local PDF library.

```text
1. 打开首页看到已保存 PDF
2. 选择 1–5 份 PDF（通常 2–5 份）
3. 浏览器逐份上传并自动加入队列
4. 单 Worker 依次完成 OCR、原始数据提取和标准化映射
5. 在批次状态页查看进度
6. 打开文件进行校对并下载 Excel/CSV
7. 按需重新处理或删除文件及相关结果
```

The normal home page shows:

```text
财务报表数据提取
上传新的 PDF
已保存 PDF
```

Each PDF shows a simple status and actions:

- 未识别: 开始识别, 删除
- 已排队或处理中: 查看进度, 处理中；删除入口隐藏且服务端也会拒绝删除
- 已识别或已有结果: 重新识别, 继续处理, 删除

The upload page automatically starts the full background pipeline. The
per-document buttons and advanced/admin pages remain available for previously
saved documents and diagnostics, but they are not the normal new-upload path.
The continue page refreshes every three seconds while a queued or running
document is changing stages.

## Batch Upload and Durable Queue

The browser never sends all selected PDFs in one large multipart request. It
creates one logical batch, uploads each file sequentially, and queues the batch
only after every expected upload position is present:

```text
POST /api/document-batches
POST /api/document-batches/{batch_id}/files
POST /api/document-batches/{batch_id}/queue
GET  /api/document-batches/{batch_id}
GET  /document-batches/{batch_id}
```

An individual file upload is retry-safe for the same batch position and
filename. The browser stores only the unfinished batch id, expected count, and
selected-file signature in `localStorage`; API URLs are always derived from the
current page rather than trusted from browser storage.

An unfinished upload survives a page refresh or browser restart. Recovery can
start automatically from the saved browser pointer or explicitly from
`/documents/upload?resume_batch_id=<batch_id>` via the batch detail page's
`继续上传` button. The user must reselect the original complete file set in the
same order because browsers do not restore file-input contents. The page reads
the durable batch state, skips positions already accepted by the server, uploads
only missing positions, and then queues the complete batch. `开始新批次` clears
the browser recovery pointer; it does not delete the unfinished server batch.

Batch and queue state is durable in:

```text
data/generated/web/webapp.sqlite3

document_batches
document_batch_items
jobs
```

All jobs in a completed upload batch are enqueued in one SQLite transaction.
The same transaction refuses the entire batch when one of its documents is
already running, preventing partial queuing and duplicate processing.

The worker claims no more than one document globally. A `document_pipeline` job
runs:

```text
云 OCR
  -> 原始结构化指标
  -> 标准科目映射
  -> 数据表.xlsx / CSV
  -> 浏览器校对
```

The upload HTTP request performs no OCR or mapping. A job that remains running
longer than `WEBAPP_JOB_TIMEOUT_SECONDS` plus
`WEBAPP_WORKER_STALE_JOB_GRACE_SECONDS` is marked failed on the next worker
cycle, allowing later queued documents to proceed. It can then be explicitly
re-enqueued from the file list.

Default limits are 50 MB and 300 pages per PDF, with five PDFs per batch. They
are configured by:

```text
WEBAPP_MAX_UPLOAD_BYTES
WEBAPP_MAX_UPLOAD_BATCH_FILES
WEBAPP_MAX_PDF_PAGES
WEBAPP_JOB_TIMEOUT_SECONDS
WEBAPP_WORKER_STALE_JOB_GRACE_SECONDS
```

## Runtime Paths

Reusable uploaded PDFs and their OCR outputs are stored in the local corpus library:

```text
data/corpus/library/<doc_id>/
  input/<original_filename>.pdf
  ocr_outputs/
  metadata.json
```

Generated runtime outputs stay under generated directories:

```text
data/generated/web/jobs/
data/generated/web/results/
data/generated/web/logs/
data/generated/web/deletions/
data/generated/web/mapping_store/
data/generated/web/webapp.sqlite3
data/generated/raw_metrics/<doc_id>/<run_id>/
data/generated/standard_metrics/<doc_id>/<run_id>/
```

No generated artifacts should be written into the repo root.

Unified download workbooks are written under:

```text
data/generated/web/results/<job_id>/downloads/数据表.xlsx
data/generated/web/jobs/<job_id>/combined_download_summary.json
data/generated/web/combined_download_summary.json
```

## OCR

Real OCR requires cloud OCR credentials. If credentials are missing, the user sees:

```text
未配置 OCR 密钥，无法识别 PDF。请联系管理员。
```

Existing OCR outputs can be copied into `data/corpus/library/<doc_id>/ocr_outputs/` for demos or tests. Re-OCR archives previous OCR outputs under `ocr_outputs_archive/<timestamp>/` before writing new outputs.

## Manual Compatibility Flow

The following resume actions remain available for existing documents, testing,
and recovery. A normal batch upload already runs both stages in the background
worker. Both actions only enqueue durable work and return immediately; the
single worker performs the actual processing.

### Resume from raw extraction

The document page button is:

```text
生成结构化数据
```

This queues `requested_stage=raw_metrics`. The worker requires completed OCR,
reuses those OCR outputs, runs raw extraction, and then continues through
standard mapping and combined-download generation:

```text
input-dir = data/corpus/library/<doc_id>/ocr_outputs/
source-image-dir = data/corpus/library/<doc_id>/input/
raw-output-dir = data/generated/raw_metrics/<doc_id>/
standard-output-dir = data/generated/standard_metrics/<doc_id>/
```

After completion, the page shows:

```text
下载数据表
校对数据和映射
重新生成标准指标 / 标准映射
```

Raw and standardized CSV files remain available under `高级下载`.

### Resume from standard mapping

When raw facts already exist, the document page button is:

```text
生成标准指标 / 标准映射
重新生成标准指标 / 标准映射
```

This queues `requested_stage=standard_metrics`. The worker reuses OCR and the
latest raw facts, runs only deterministic standard mapping (including the Stage
15 local mapping store), and regenerates the combined download. It writes:

```text
data/generated/standard_metrics/<doc_id>/
```

After Step 2, the page shows:

```text
下载数据表
高级下载
校对数据和映射
```

The main workbook contains raw and standardized information in one file. Typical
sheets are:

```text
数据总表
标准化数据
原始数据
术语映射校对
说明
```

The Excel workbook is formatted for ordinary finance users: metric value cells are
numeric Excel cells with thousands separators, date columns display as
`yyyy-mm-dd`, the header row is frozen, filters are enabled, and column widths are
set for direct review. `数据表.xlsx` and `数据表.csv` are the two ordinary structured
data downloads. Detailed JSON and logs remain advanced files.

## Proofreading Pages

The ordinary proofreading page is now the unified workbench:

```text
/documents/{doc_id}/proofread
```

It combines date, amount, source-unit, and standard-term proofreading in one screen. The left side shows the source PDF page image and highlights the selected source term or value when a bbox is available. The right side is a continuous spreadsheet-like table:

```text
原始术语 | 表格日期 | 指标数值及原单位 | 标准术语 | 状态
```

OCR confidence is hidden by default. Use the slider-style `显示置信度` switch to show confidence inline next to each original term; missing confidence is shown as `未记录`.

Value editing:

- Numeric cells display thousands separators, for example `396149420.62` becomes `396,149,420.62`.
- The editor accepts plain numbers and comma-formatted numbers.
- On blur, parseable values are formatted again with thousands separators.
- Invalid numeric input is marked in the cell and is rejected by the save endpoint.
- Changed value cells are highlighted and show `重置`; reset restores the original extracted value.

Date and unit editing:

- A table date can be corrected directly. A changed row shows `日期已修改`, and `重置` restores the extracted or inferred date.
- Amount source units support `元`, `千元`, `万元`, and `亿元`. Changing the source unit immediately recalculates the displayed RMB-yuan amount.
- Unit corrections show `单位已修改`; `重置单位` restores both the original unit and normalized amount.
- Rows whose dates cannot be inferred safely remain visible as `日期待校对`.

Mapping editing:

- The standard term cell is a direct autocomplete input.
- It supports standard code (`ZT_068`), numeric code (`68` or `068`), Chinese substring (`短期` / `借款`), aliases, and pinyin initials such as `dqjk` when the term index supports them.
- Empty input does not show `没有找到标准术语`; no-results appears only after a non-empty query returns no matches.
- Changed mapping cells are highlighted and show `重置`; reset restores the original system mapping.
- The status column remains visible with Chinese labels such as `精确匹配`, `别名匹配`, `建议校对`, `未映射`, and `已修改`.
- Each row exposes Stage 15 mapping decisions: `不采纳`, `仅本次采用`, and `采用并记住`.

Unified review actions are written under:

```text
data/generated/web/jobs/<job_id>/unified_review/
data/generated/web/results/<job_id>/unified_review/
```

The main files are:

```text
unified_review_actions.csv
unified_review_actions.json
unified_review_summary.json
```

Compatibility raw-review and mapping-review action files may also be written under their existing review folders.

Saving the unified page immediately refreshes the current `数据表.xlsx` and `数据表.csv`. There is no separate fact-version or export-history subsystem. Use `保存并生成会计报表` only after the current rows have been checked. The accounting workbook writes only safely mapped balance-sheet and income-statement rows with clear dates; skipped or conflicting rows are listed in its `生成说明` sheet. If proofreading changes afterward, the old accounting workbook is no longer downloadable until it is regenerated.

Known limitations:

- Some terms or numeric cells may not have a recorded bbox; the page reports `当前项目未记录位置` instead of failing.
- Pinyin search depends on the available standard-term search index and fallback pinyin coverage.
- OCR confidence may be missing from some providers or fallback inputs.

The legacy proofreading pages remain available for direct access and compatibility:

```text
/documents/{doc_id}/raw-review
/documents/{doc_id}/mapping-review
```

The older job routes remain available for administrators:

```text
/jobs/{job_id}/raw-review
/jobs/{job_id}/mapping-review
```

`/documents/{doc_id}/mapping-review` is a table-style mapping workbench. The left side shows the original page image, and the right side focuses only on term mapping:

```text
原始术语 | 标准术语 | 状态 | 置信度 | 口径说明 | 操作
```

Selecting a row highlights the original metric term on the source page when a term bbox is available. The page does not use numeric values as the primary review target.

The standard term field is an autocomplete search box. It supports:

- standard code, for example `ZT_068`
- numeric code, for example `002` or `2`
- Chinese name or alias substring, for example `短期` or `借款`
- pinyin initials, for example `dqjk`

After choosing a term, use:

- `不采纳`: reject the suggestion for this job.
- `仅本次采用`: apply only to the current job output.
- `采用并记住`: apply now and save the raw term to the local mapping store.

Remembered aliases are scoped by company and statement type, so a decision for one company/report does not silently become a global alias.

Actions are written under the existing web review path, for example:

```text
data/generated/web/jobs/<job_id>/mapping_review/mapping_review_actions.csv
data/generated/web/jobs/<job_id>/mapping_review/mapping_review_actions.json
data/generated/web/jobs/<job_id>/mapping_review/mapping_decisions.csv
data/generated/web/jobs/<job_id>/mapping_review/mapping_decisions.json
data/generated/web/jobs/<job_id>/mapping_review/mapping_decision_summary.json
```

Remembered aliases are stored only in runtime data:

```text
data/generated/web/mapping_store/local_mappings.sqlite
data/generated/web/mapping_store/local_aliases_export.yml
data/generated/web/mapping_store/mapping_decisions_audit.csv
data/generated/web/mapping_store/llm_suggestions.csv
data/generated/web/mapping_store/llm_suggestion_audit.csv
```

If DeepSeek mapping suggestions are enabled, each row can show `AI建议`, confidence as a percentage, and a short reason. The LLM is candidate-constrained: it receives only local standard terms and any returned code outside that candidate list is rejected. It cannot mutate `config/*.yml` or save aliases by itself.

Configure DeepSeek through environment variables or `data/secrets/deepseek.env`:

```text
DEEPSEEK_API_KEY=[请输入你的api]
DEEPSEEK_MODEL=deepseek-v4-flash
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_ENABLED=false
LLM_MAPPING_CACHE_ENABLED=true
```

The bulk confidence control defaults to `90%`. `先预览` writes `confidence_bulk_accept_preview.json`; `确认本次采纳` writes `accept_once` decisions and `confidence_bulk_accept_apply_summary.json`. The UI text states that this only affects the current file and does not write to the local mapping store.

The web app does not edit tracked base config files. `采用并记住` is the only mapping action that writes a local alias; after that, the same raw term can map automatically in future runs.

Limitations:

- Not every source term has a recorded bbox; those rows report `当前术语未记录位置`.
- Pinyin uses `pypinyin` when available. Without it, the web app falls back to built-in initials/full-pinyin coverage for the configured standard terms.

## Delete

Deletion requires a confirmation page and deletes:

- original PDF
- OCR outputs
- raw metrics outputs
- standard metrics outputs
- related web job/result/log files

Deletion is irreversible in demo mode. A summary is written to:

```text
data/generated/web/deletions/<doc_id>_<timestamp>_delete_summary.json
```

Deletion refuses to run if any computed path escapes the allowed roots.

## Local Run

```powershell
$env:WEBAPP_ENV="dev"
$env:WEBAPP_AUTH_REQUIRED="0"
$env:WEBAPP_ENABLE_LOCAL_WORKER="1"
uvicorn webapp.main:app --reload
```

`WEBAPP_ENABLE_LOCAL_WORKER=1` starts one in-process development worker. In a
Docker deployment, the web process should use `WEBAPP_ENABLE_LOCAL_WORKER=0`
and the separate `worker` service should be the only document worker.

For demos without real OCR credentials, use existing OCR outputs or the test-only mock OCR environment variables. PaddleOCR remains pilot-only.

## Docker Deployment

For a server deployment, copy `.env.aliyun.example` to `.env` and set at least:

```text
WEBAPP_BASE_PATH=/AutoFinance
WEBAPP_ADMIN_PASSWORD=...
WEBAPP_SECRET_PATH=data/secrets/secret
NGINX_PORT=80
```

Then run:

```bash
docker compose --env-file .env -f docker-compose.yml -f docker-compose.aliyun.yml up -d --build
```

The default Aliyun Nginx config serves the app at:

```text
http://<server-ip>/AutoFinance/
```

Runtime state is mounted at `./data:/app/data`. The image build excludes local-only paths such as:

```text
data/corpus/
data/generated/
data/secrets/
tmp/
.playwright-cli/
```

Keep the accounting template at `data/templates/会计报表.xlsx` on the server, and place reusable PDFs/OCR outputs under `data/corpus/library/<doc_id>/` when testing without re-running OCR.
If copied `metadata.json` files still contain absolute paths from another machine, the web app rebases missing document paths to the current `data/corpus/library/<doc_id>/input` and `ocr_outputs` directories.

To reuse the existing local results by manual upload, copy these ignored paths into the server repo root with the same relative paths:

```text
data/templates/
data/corpus/library/
data/generated/raw_metrics/
data/generated/standard_metrics/
data/generated/web/jobs/
```

Optional uploads:

```text
data/secrets/secret        # only if the server should run cloud OCR
data/generated/web/results/
data/generated/web/logs/
data/generated/web/webapp.sqlite3
data/corpus/D01/..D08/     # only if you also need batch/benchmark corpus data
```
