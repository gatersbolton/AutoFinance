# standardize

`standardize` 是 `OCR.py` 的消费层，用于把 `outputs/` 下的 OCR 结果转成稳定、可追溯、可校验的中间数据，再导出到标准化会计报表模板。

## 用法

```bash
python -m standardize.cli \
  --input-dir outputs \
  --template ..\会计报表.xlsx \
  --output-dir normalized \
  --provider-priority aliyun,tencent \
  --enable-conflict-merge
```

## 输入规则

- 真值优先读取 `outputs/<provider>/<doc>/raw/*.json`
- `result.json` 仅用于补充页级索引、文本和 artifact 提示
- 仅 `aliyun_table`、`tencent_table_v3`、`xlsx_fallback` 参与表格重建
- `xlsx` 仅作为缺失 json 时的 fallback，并会显式标记缺少 bbox / confidence

## 输出文件

- `cells.csv`: dense cell 级结构
- `facts.csv`: long format 财务事实表
- `issues.csv`: 可疑值、修复和输入问题
- `conflicts.csv`: 多 provider 冲突与裁决
- `mapping_review.csv`: 无法自动落模板的映射候选
- `summary.json`: 本次运行摘要
- `run_manifest.json` / `artifact_manifest_core.csv`: 运行 provenance 与核心 artifact 清单
- `pipeline_stage_timings.json` / `pipeline_stage_status.json` / `pipeline_completion_summary.json`: 阶段耗时与完成诊断
- `会计报表_填充结果.xlsx`: 模板副本和动态期间列

## 设计边界

- 规则和配置驱动，不接 LLM
- 不重写 `OCR.py`
- 当前 MVP 重点覆盖财务报表主表，附注类页面优先保留结构和 review
- 腾讯 raw json 的行列坐标存在起止索引不一致问题，适配层会按样例做修正

## 后续扩展点

- 更强的 logical subtable 切分
- 更细的勾稽校验
- 用 LLM 仅处理长尾别名候选，不替代规则主流程
