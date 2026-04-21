from __future__ import annotations


REVIEW_STATUS_LABELS_ZH = {
    "unresolved": "未处理",
    "resolved": "已处理",
    "deferred": "暂缓",
    "ignored": "已忽略",
    "reocr_requested": "已请求重新 OCR",
}

REVIEW_SOURCE_TYPE_LABELS_ZH = {
    "review_queue": "复核队列",
    "issue": "问题清单",
    "validation": "校验结果",
    "conflict": "冲突明细",
    "unplaced_fact": "未落位事实",
    "mapping_candidate": "科目映射候选",
}

REVIEW_COMPATIBILITY_LABELS_ZH = {
    "backend_ready": "可自动应用",
    "partial": "部分支持",
    "suggestion_only": "仅作为建议",
    "unsupported": "暂不支持",
}

OPERATION_STATUS_LABELS_ZH = {
    "created": "已创建",
    "queued": "排队中",
    "running": "运行中",
    "succeeded": "已完成",
    "failed": "失败",
    "cancelled": "已取消",
}

OPERATION_TYPE_LABELS_ZH = {
    "apply_review_actions": "应用复核动作",
    "apply_and_rerun": "应用复核并重新生成",
    "rerun_only": "仅重新生成",
}

PROVIDER_MODE_LABELS_ZH = {
    "cloud_first": "云优先",
    "aliyun_table": "阿里云表格 OCR",
    "tencent_table_v3": "腾讯表格 OCR",
    "paddle_table_local": "Paddle 表格 OCR（试点）",
    "paddle_table_local (pilot_only)": "Paddle 表格 OCR（试点）",
}

REASON_CODE_EXACT_LABELS_ZH = {
    "quality:suspicious_numeric": "金额异常",
    "issue:suspicious_value": "OCR 可疑",
    "mapping:candidate_review": "科目映射问题",
    "mapping:candidate": "科目映射问题",
}

REASON_CODE_PREFIX_LABELS_ZH = (
    ("validation:", "校验相关"),
    ("mapping:", "科目映射问题"),
    ("conflict:", "供应商冲突"),
    ("source:", "OCR 可疑"),
    ("unplaced:", "待定位事实"),
    ("issue:", "问题项"),
)


def review_status_label_zh(status: str) -> str:
    return REVIEW_STATUS_LABELS_ZH.get(status, status or "未处理")


def review_source_type_label_zh(source_type: str) -> str:
    return REVIEW_SOURCE_TYPE_LABELS_ZH.get(source_type, source_type or "未分类")


def review_compatibility_label_zh(status: str) -> str:
    return REVIEW_COMPATIBILITY_LABELS_ZH.get(status, status or "未评估")


def operation_status_label_zh(status: str) -> str:
    return OPERATION_STATUS_LABELS_ZH.get(status, status or "未记录")


def operation_type_label_zh(operation_type: str) -> str:
    return OPERATION_TYPE_LABELS_ZH.get(operation_type, operation_type or "未记录")


def provider_mode_label_zh(provider_mode: str) -> str:
    return PROVIDER_MODE_LABELS_ZH.get(provider_mode, provider_mode or "未记录")


def reason_code_label_zh(reason_code: str) -> str:
    normalized = (reason_code or "").strip()
    if not normalized:
        return "待复核"
    if normalized in REASON_CODE_EXACT_LABELS_ZH:
        return REASON_CODE_EXACT_LABELS_ZH[normalized]
    for prefix, label in REASON_CODE_PREFIX_LABELS_ZH:
        if normalized.startswith(prefix):
            return label
    return "待复核"


def option_item(value: str, label_zh: str) -> dict[str, str]:
    return {"value": value, "label_zh": label_zh}
