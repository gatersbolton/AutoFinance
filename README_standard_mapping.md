# Standard Metrics Mapping

Step 2 of the current product flow is:

```text
raw normalized metrics table
-> standardized mapped metrics table
```

This step consumes Stage 12 `raw_metrics.csv` or `raw_metrics.xlsx` and maps each raw metric name to a standard metric code and name. It preserves dates and numeric values exactly as received from Step 1.

## CLI

```powershell
python -m standard_map.cli ^
  --input data/generated/raw_metrics/D01/<run_id>/raw_metrics.csv ^
  --output-dir data/generated/standard_metrics/D01 ^
  --mapping-registry config/standard_terms.yml ^
  --mapping-store-path data/generated/web/mapping_store/local_mappings.sqlite
```

Optional flags:

```text
--doc-id
--company-name
--mapping-store-path
--debug
```

This CLI does not require `会计报表.xlsx`.

## Registry

The default registry files are:

```text
config/standard_terms.yml
config/standard_term_aliases.yml
config/standard_term_relations.yml
config/mapping_policy.yml
```

Supported term fields:

```text
code
name
category
aliases
statement_scope
metric_type
period_type
legacy_aliases
description
enabled
notes
```

Exact aliases and safe legacy aliases can auto-map. `same_as`, `exact_alias`, and configured-safe `legacy_alias` relations are safe mapping relations. `broader_than`, `narrower_than`, `aggregate`, `split`, `formula`, and `ambiguous` relations produce review items by default.

Formula and aggregate/split relations are not aliases. For example, `上半年营收 + 下半年营收 = 营业收入` is a derived relationship and must remain review-required unless a later reviewed workflow explicitly applies the formula.

## DeepSeek Suggestions

Stage 15.2 can ask DeepSeek for a suggestion only after local exact, alias, remembered alias, and safe relation mapping fail. The prompt is constrained to the local candidate list from the standard-term registry; DeepSeek must choose one provided candidate or return `unknown`. Responses are strictly parsed as JSON and rejected if the returned code is not in the candidate list, so the LLM cannot invent `ST` or `ZT` codes.

Configure live suggestions with environment variables or `data/secrets/deepseek.env`:

```text
DEEPSEEK_API_KEY=[请输入你的api]
DEEPSEEK_MODEL=deepseek-v4-flash
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_TIMEOUT_SECONDS=60
DEEPSEEK_ENABLED=false
LLM_MAPPING_MAX_CANDIDATES=20
LLM_MAPPING_CACHE_ENABLED=true
```

Missing keys and placeholder keys disable live calls gracefully. Tests use `LLM_MAPPING_MOCK=true` or `--llm-mock` and never require network access or a real API key. API keys are not written to logs or audit files.

## Local Mapping Store

Human-created mappings are runtime data, not tracked config. The default local store lives under:

```text
data/generated/web/mapping_store/local_mappings.sqlite
data/generated/web/mapping_store/local_aliases_export.yml
data/generated/web/mapping_store/mapping_decisions_audit.csv
```

The SQLite store keeps standard terms, local aliases, relation metadata, mapping decisions, and cached LLM suggestions. LLM rows are cached by normalized raw metric, scoped context, candidate-code list, mapping policy version, and model name.

Back up the local mapping memory by copying `data/generated/web/mapping_store/`. The YAML export is human-readable, but SQLite remains the source of truth for runtime decisions.

## Outputs

Each run writes under:

```text
data/generated/standard_metrics/<doc_id>/<run_id>/
```

Required files:

```text
standardized_metrics.csv
standardized_metrics.xlsx
standardized_metrics.jsonl
standardized_metrics_detailed.csv
mapping_candidates.csv
mapping_issues.csv
llm_suggestions.csv
llm_suggestion_audit.csv
llm_mapping_summary.json
standard_mapping_summary.json
mapping_review_items.csv
mapping_decisions.csv
mapping_store_snapshot.yml
confidence_bulk_accept_preview.json
confidence_bulk_accept_apply_summary.json
standard_mapping_run_manifest.json
```

When the web two-step flow runs, it also refreshes the ordinary user download:

```text
data/generated/web/results/<job_id>/downloads/数据表.xlsx
```

`数据表.xlsx` combines the Step 1 raw metrics and Step 2 standardized mapping
results. Raw CSV, standardized CSV, JSON summaries, and other detailed files are
advanced downloads in the web UI.

The main CSV/XLSX columns are:

```text
填表日期
当前条目日期
公司名
原始指标名
标准指标编码
标准指标名称
指标数值
映射方法
映射状态
映射置信度
口径关系
口径说明
是否需要人工校对
```

In the web-generated `数据表.xlsx`, numeric metric values are written as numeric
Excel cells and displayed with thousands separators (`#,##0.00`). Date columns use
`yyyy-mm-dd`. If only Step 1 has completed, the workbook may contain only raw-data
sheets; after Step 2 it includes the standardized-data and mapping-review sheets.

## Review Decisions

The web app exposes a simple term-mapping proofreading page. Review actions are stored under:

```text
data/generated/web/jobs/<job_id>/mapping_review/
```

The user-facing mapping decisions are:

- `不采纳`: records `reject`, does not save an alias, and removes the suggestion from the current standardized output.
- `仅本次采用`: records `accept_once`, applies the selected mapping only to the current job output, and does not add local mapping memory.
- `采用并记住`: records `accept_and_remember`, applies the mapping to the current job output, and saves the raw term as a local alias for future automatic runs.

The web UI never mutates `config/*.yml`. Runtime decisions write only under `data/generated/web/...` and the current run directory under `data/generated/standard_metrics/...`.

`config/mapping_policy.yml` controls the confidence bulk-accept threshold:

```text
auto_accept_once_confidence_threshold: 0.90
future_bulk_accept_default_decision: accept_once
```

The web action `采纳所有置信度大于 90% 的术语` first emits `confidence_bulk_accept_preview.json`. Applying the preview writes `accept_once` decisions and `confidence_bulk_accept_apply_summary.json`. It never writes to `term_aliases`, even for high-confidence AI suggestions. Unsafe relation types such as `aggregate`, `split`, `formula`, and `ambiguous` are excluded.

Audit files are stored beside each standard-mapping run and in the web mapping store:

```text
data/generated/standard_metrics/<doc_id>/<run_id>/
data/generated/web/mapping_store/llm_suggestions.csv
data/generated/web/mapping_store/llm_suggestion_audit.csv
data/generated/web/mapping_store/mapping_decisions_audit.csv
```

Back up the complete mapping memory, including LLM cache and local aliases, by copying `data/generated/web/mapping_store/`.
