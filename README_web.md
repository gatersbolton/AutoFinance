# AutoFinance Web

The ordinary web flow starts from a local PDF library.

```text
1. 打开首页看到已保存 PDF
2. 上传新的 PDF
3. 对未识别文件点击开始识别
4. 对已识别文件点击继续处理
5. 提取原始数据
6. 生成标准化数据
7. 下载结果
8. 删除文件及相关结果
```

The normal home page shows:

```text
财务报表数据提取
上传新的 PDF
已保存 PDF
```

Each PDF shows a simple status and actions:

- 未识别: 开始识别, 删除
- 已识别或已有结果: 重新识别, 继续处理, 删除

Advanced/admin pages still exist, but they are not the normal workflow.

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
data/generated/raw_metrics/<doc_id>/<run_id>/
data/generated/standard_metrics/<doc_id>/<run_id>/
```

No generated artifacts should be written into the repo root.

## OCR

Real OCR requires cloud OCR credentials. If credentials are missing, the user sees:

```text
未配置 OCR 密钥，无法识别 PDF。请联系管理员。
```

Existing OCR outputs can be copied into `data/corpus/library/<doc_id>/ocr_outputs/` for demos or tests. Re-OCR archives previous OCR outputs under `ocr_outputs_archive/<timestamp>/` before writing new outputs.

## Step 1: 原始数据

The document page button is:

```text
一键提取原始数据
```

It runs Stage 12 raw extraction with:

```text
input-dir = data/corpus/library/<doc_id>/ocr_outputs/
source-image-dir = data/corpus/library/<doc_id>/input/
output-dir = data/generated/raw_metrics/<doc_id>/
```

After Step 1, the page shows:

```text
下载原始数据
校对数据和映射
一键生成标准化数据
```

## Step 2: 标准化数据

The document page button is:

```text
一键生成标准化数据
```

It runs Stage 13 standard mapping with the latest `raw_metrics.csv` and writes:

```text
data/generated/standard_metrics/<doc_id>/
```

After Step 2, the page shows:

```text
下载原始数据
下载标准化数据
校对数据和映射
```

## Proofreading Pages

The ordinary proofreading page is now the unified workbench:

```text
/documents/{doc_id}/proofread
```

It combines raw value proofreading and standard term mapping in one screen. The left side shows the source PDF page image and highlights the selected source term or value when a bbox is available. The right side is a continuous spreadsheet-like table:

```text
原始术语 | 当前条目日期 | 指标数值 | 标准术语 | 状态
```

OCR confidence is hidden by default. Use the slider-style `显示置信度` switch to show confidence inline next to each original term; missing confidence is shown as `未记录`.

Value editing:

- Numeric cells display thousands separators, for example `396149420.62` becomes `396,149,420.62`.
- The editor accepts plain numbers and comma-formatted numbers.
- On blur, parseable values are formatted again with thousands separators.
- Invalid numeric input is marked in the cell and is rejected by the save endpoint.
- Changed value cells are highlighted and show `重置`; reset restores the original extracted value.

Mapping editing:

- The standard term cell is a direct autocomplete input.
- It supports standard code (`ZT_002`), numeric code (`2` or `002`), Chinese substring (`短期` / `借款`), aliases, and pinyin initials such as `dqjk` when the term index supports them.
- Empty input does not show `没有找到标准术语`; no-results appears only after a non-empty query returns no matches.
- Changed mapping cells are highlighted and show `重置`; reset restores the original system mapping.
- The status column remains visible with Chinese labels such as `精确匹配`, `别名匹配`, `建议校对`, `未映射`, and `已修改`.

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
原始术语 | 标准术语 | 状态 | 操作
```

Selecting a row highlights the original metric term on the source page when a term bbox is available. The page does not use numeric values as the primary review target.

The standard term field is an autocomplete search box. It supports:

- standard code, for example `ZT_002`
- numeric code, for example `002` or `2`
- Chinese name or alias substring, for example `短期` or `借款`
- pinyin initials, for example `dqjk`

After choosing a term, use `通过`, `跳过`, or `保存修改`. Actions are written under the existing web review path, for example:

```text
data/generated/web/jobs/<job_id>/mapping_review/mapping_review_actions.csv
data/generated/web/jobs/<job_id>/mapping_review/mapping_review_actions.json
```

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
