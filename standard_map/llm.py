from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

import httpx

from project_paths import MAPPING_POLICY_PATH, SECRETS_ROOT

from .models import MappingCandidate, RawMetricRow, StandardRegistry
from .normalizer import normalize_metric_name
from .relations import normalize_relation_type


PLACEHOLDER_API_KEYS = {
    "",
    "[请输入你的api]",
    "请输入你的api",
    "your_api_key",
    "your_deepseek_api_key",
    "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
}
DEFAULT_MODEL = "deepseek-v4-flash"
DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_CANDIDATES = 20
MAPPING_POLICY_VERSION = "stage15_2_candidate_constrained_v1"
REVIEW_REQUIRED_RELATION_TYPES = {"aggregate", "split", "formula", "ambiguous"}
SAFE_LLM_RELATION_TYPES = {
    "same_as",
    "exact_alias",
    "legacy_alias",
    "broader_than",
    "narrower_than",
    "aggregate",
    "split",
    "formula",
    "ambiguous",
    "unknown",
}


@dataclass(frozen=True)
class DeepSeekConfig:
    api_key: str = ""
    model: str = DEFAULT_MODEL
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    enabled: bool = False
    max_candidates: int = DEFAULT_MAX_CANDIDATES
    cache_enabled: bool = True
    mock_mode: bool = False
    disabled_reason: str = ""

    @property
    def has_valid_api_key(self) -> bool:
        return is_valid_deepseek_api_key(self.api_key)

    def __repr__(self) -> str:
        return (
            "DeepSeekConfig(api_key=<redacted>, "
            f"model={self.model!r}, base_url={self.base_url!r}, timeout_seconds={self.timeout_seconds!r}, "
            f"enabled={self.enabled!r}, max_candidates={self.max_candidates!r}, cache_enabled={self.cache_enabled!r}, "
            f"mock_mode={self.mock_mode!r}, disabled_reason={self.disabled_reason!r})"
        )


class LLMClient(Protocol):
    def complete_json(self, *, system_prompt: str, user_prompt: str, config: DeepSeekConfig) -> str:
        ...


class DeepSeekClient:
    def complete_json(self, *, system_prompt: str, user_prompt: str, config: DeepSeekConfig) -> str:
        if not config.has_valid_api_key:
            raise RuntimeError("DeepSeek API key is missing or placeholder.")
        base_url = config.base_url.rstrip("/")
        payload = {
            "model": config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "authorization": f"Bearer {config.api_key}",
            "content-type": "application/json",
        }
        with httpx.Client(timeout=config.timeout_seconds) as client:
            response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            response.raise_for_status()
            body = response.json()
        choices = body.get("choices") if isinstance(body, dict) else None
        if not choices:
            raise RuntimeError("DeepSeek response did not include choices.")
        message = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
        content = message.get("content", "")
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("DeepSeek response content is empty.")
        return content


class MockDeepSeekClient:
    def complete_json(self, *, system_prompt: str, user_prompt: str, config: DeepSeekConfig) -> str:
        payload = json.loads(user_prompt)
        raw_metric_name = str(payload.get("raw_metric_name", "") or "")
        candidates = payload.get("candidates", [])
        candidate_by_code = {str(item.get("code", "")): item for item in candidates if isinstance(item, dict)}

        def choose(preferred_code: str, fallback_name_contains: str = "") -> dict[str, Any] | None:
            if preferred_code in candidate_by_code:
                return candidate_by_code[preferred_code]
            if fallback_name_contains:
                for item in candidates:
                    if isinstance(item, dict) and fallback_name_contains in str(item.get("name", "")):
                        return item
            return candidates[0] if candidates else None

        if raw_metric_name == "奇怪项目XYZ":
            result = {
                "decision": "unknown",
                "standard_code": None,
                "standard_name": None,
                "relation_type": "unknown",
                "confidence": 0.2,
                "review_required": True,
                "reason": "候选列表中没有足够接近的标准术语。",
                "candidate_rank": None,
            }
        elif raw_metric_name == "上半年营收":
            item = choose("ST_001", "营业")
            result = _candidate_response(item, relation_type="aggregate", confidence=0.88, reason="该指标像期间汇总口径，不能直接作为安全别名。")
        elif raw_metric_name == "营业额":
            item = choose("ST_001", "营业")
            result = _candidate_response(item, relation_type="exact_alias", confidence=0.95, reason="营业额通常可作为总营业额的精确别名。")
        elif raw_metric_name == "总收入":
            item = choose("ST_001", "营业")
            result = _candidate_response(item, relation_type="same_as", confidence=0.93, reason="总收入与总营业额语义接近，需人工确认后采用。")
        else:
            item = candidates[0] if candidates else None
            result = _candidate_response(item, relation_type="same_as", confidence=0.91, reason="测试模式返回首个候选。") if item else {
                "decision": "unknown",
                "standard_code": None,
                "standard_name": None,
                "relation_type": "unknown",
                "confidence": 0.1,
                "review_required": True,
                "reason": "无候选。",
                "candidate_rank": None,
            }
        return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


def load_deepseek_config(
    *,
    env: dict[str, str] | None = None,
    env_file: str | Path | None = None,
    enabled_override: bool | None = None,
    model_override: str | None = None,
    mock_mode: bool | None = None,
    cache_enabled_override: bool | None = None,
) -> DeepSeekConfig:
    env_values = _load_env_file(Path(env_file) if env_file else SECRETS_ROOT / "deepseek.env")
    process_env = dict(os.environ if env is None else env)
    values = {**env_values, **process_env}

    api_key = str(values.get("DEEPSEEK_API_KEY", "") or "").strip()
    model = str(model_override or values.get("DEEPSEEK_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip()
    base_url = str(values.get("DEEPSEEK_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL).strip()
    timeout_seconds = _int_value(values.get("DEEPSEEK_TIMEOUT_SECONDS"), DEFAULT_TIMEOUT_SECONDS)
    max_candidates = max(1, min(_int_value(values.get("LLM_MAPPING_MAX_CANDIDATES"), DEFAULT_MAX_CANDIDATES), 50))
    cache_enabled = _bool_value(values.get("LLM_MAPPING_CACHE_ENABLED"), True)
    if cache_enabled_override is not None:
        cache_enabled = cache_enabled_override
    resolved_mock = _bool_value(values.get("LLM_MAPPING_MOCK"), False) if mock_mode is None else bool(mock_mode)
    has_key = is_valid_deepseek_api_key(api_key)
    enabled_env = _optional_bool(values.get("DEEPSEEK_ENABLED"))
    if enabled_override is not None:
        enabled = bool(enabled_override)
    elif resolved_mock:
        enabled = True
    elif enabled_env is None:
        enabled = has_key
    else:
        enabled = enabled_env and has_key
    disabled_reason = ""
    if not enabled:
        disabled_reason = "mock_disabled" if resolved_mock is False and not has_key else "explicitly_disabled"
        if not has_key and not resolved_mock:
            disabled_reason = "missing_or_placeholder_api_key"
    return DeepSeekConfig(
        api_key=api_key,
        model=model,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        enabled=enabled,
        max_candidates=max_candidates,
        cache_enabled=cache_enabled,
        mock_mode=resolved_mock,
        disabled_reason=disabled_reason,
    )


def is_valid_deepseek_api_key(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return lowered not in PLACEHOLDER_API_KEYS and "请输入" not in text and "your_" not in lowered


def build_llm_mapping_prompts(raw: RawMetricRow, candidates: list[MappingCandidate], registry: StandardRegistry | None = None) -> tuple[str, str]:
    system_prompt = (
        "你是财务会计标准术语映射助手。你只能从用户提供的本地候选标准术语列表中选择，"
        "不能发明标准编码或标准名称。如果没有足够合适的候选，返回 unknown。"
        "请区分 exact_alias、legacy_alias、broader_than、narrower_than、aggregate、split、formula、ambiguous 等关系。"
        "不确定时 review_required 必须为 true。只返回 JSON。"
    )
    user_payload = {
        "task": "map_raw_financial_metric_to_fixed_local_registry",
        "raw_metric_name": raw.metric_name,
        "company_name": raw.company_name,
        "fill_date": raw.fill_date,
        "item_date": raw.item_date,
        "report_type": raw.provenance.get("statement_type", ""),
        "header_path": raw.provenance.get("header_path", ""),
        "same_table_context": raw.provenance.get("row_context_path", ""),
        "value_type": raw.provenance.get("value_type", ""),
        "instructions": [
            "只能选择 candidates 中的 code。",
            "如果没有足够合适的候选，decision 返回 unknown 且 standard_code 为 null。",
            "不要输出候选列表之外的标准编码。",
            "只返回符合 schema 的 JSON，不要 Markdown。",
        ],
        "schema": {
            "decision": "candidate | unknown",
            "standard_code": "candidate code or null",
            "standard_name": "candidate name or null",
            "relation_type": "same_as | exact_alias | legacy_alias | broader_than | narrower_than | aggregate | split | formula | ambiguous | unknown",
            "confidence": "number from 0 to 1",
            "review_required": "boolean",
            "reason": "简短中文理由",
            "candidate_rank": "integer candidate rank or null",
        },
        "candidates": [_candidate_prompt_row(candidate, registry) for candidate in candidates],
    }
    return system_prompt, json.dumps(user_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def parse_llm_mapping_response(
    response_text: str,
    *,
    candidates: list[MappingCandidate],
) -> dict[str, Any]:
    candidate_by_code = {candidate.candidate_code: candidate for candidate in candidates}
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError:
        return _invalid_suggestion("invalid_json", response_text=response_text, issue_reason="JSON parse failure")
    if not isinstance(payload, dict):
        return _invalid_suggestion("invalid_schema", response_text=response_text, issue_reason="Response is not a JSON object")

    decision = str(payload.get("decision", "") or "").strip()
    relation_type = normalize_relation_type(payload.get("relation_type", "")) or "unknown"
    if relation_type not in SAFE_LLM_RELATION_TYPES:
        relation_type = "unknown"
    confidence_raw = payload.get("confidence")
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        return _invalid_suggestion("invalid_confidence", response_json=payload, issue_reason="confidence is not numeric")
    if confidence < 0 or confidence > 1:
        return _invalid_suggestion("invalid_confidence", response_json=payload, issue_reason="confidence is outside 0..1")
    review_required = bool(payload.get("review_required", True))
    if relation_type in REVIEW_REQUIRED_RELATION_TYPES:
        review_required = True

    if decision == "unknown":
        if payload.get("standard_code") not in (None, ""):
            return _invalid_suggestion("invalid_unknown_code", response_json=payload, issue_reason="unknown decision returned a code")
        return {
            "decision": "unknown",
            "candidate_code": "",
            "candidate_name": "",
            "relation_type": "unknown",
            "confidence": confidence,
            "review_required": True,
            "reason": str(payload.get("reason", "") or ""),
            "candidate_rank": "",
            "validation_status": "valid_unknown",
            "response_json": payload,
            "issue_reason": "",
        }

    if decision != "candidate":
        return _invalid_suggestion("invalid_decision", response_json=payload, issue_reason="decision must be candidate or unknown")

    standard_code = str(payload.get("standard_code", "") or "").strip()
    if standard_code not in candidate_by_code:
        return _invalid_suggestion("invalid_code", response_json=payload, issue_reason="standard_code is not in provided candidates")
    candidate = candidate_by_code[standard_code]
    return {
        "decision": "candidate",
        "candidate_code": candidate.candidate_code,
        "candidate_name": candidate.candidate_name,
        "relation_type": relation_type,
        "confidence": confidence,
        "review_required": review_required,
        "reason": str(payload.get("reason", "") or ""),
        "candidate_rank": _candidate_rank(payload.get("candidate_rank"), candidate.candidate_rank),
        "validation_status": "valid",
        "response_json": payload,
        "issue_reason": "",
    }


class LLMSuggestionService:
    def __init__(
        self,
        *,
        config: DeepSeekConfig,
        store: Any,
        registry: StandardRegistry,
        client: LLMClient | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.registry = registry
        self.client = client or (MockDeepSeekClient() if config.mock_mode else DeepSeekClient())
        self.records: list[dict[str, Any]] = []
        self.live_llm_executed = False
        self.invalid_responses_total = 0
        self.cached_suggestions_total = 0

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def suggest(self, raw: RawMetricRow, candidates: list[MappingCandidate]) -> dict[str, Any] | None:
        if not self.enabled or not candidates:
            return None
        candidates = candidates[: self.config.max_candidates]
        context = build_llm_context(raw)
        candidate_codes = [candidate.candidate_code for candidate in candidates]
        cache_key = build_llm_cache_key(
            raw_metric_name=raw.metric_name,
            context=context,
            candidate_codes=candidate_codes,
            policy_version=MAPPING_POLICY_VERSION,
            model_name=self.config.model,
        )
        system_prompt, user_prompt = build_llm_mapping_prompts(raw, candidates, self.registry)
        prompt_hash = _sha256_text(system_prompt + "\n" + user_prompt)
        if self.config.cache_enabled:
            cached = self.store.get_llm_suggestion(cache_key)
            if cached:
                cached = dict(cached)
                cached["from_cache"] = True
                self.cached_suggestions_total += 1
                self.records.append(cached)
                return cached

        try:
            response_text = self.client.complete_json(system_prompt=system_prompt, user_prompt=user_prompt, config=self.config)
            if not self.config.mock_mode:
                self.live_llm_executed = True
            parsed = parse_llm_mapping_response(response_text, candidates=candidates)
        except Exception as exc:
            response_text = ""
            parsed = _invalid_suggestion("client_error", response_json={"error": str(exc)}, issue_reason="LLM client error")

        if parsed.get("validation_status") not in {"valid", "valid_unknown"}:
            self.invalid_responses_total += 1

        record = {
            "suggestion_id": f"llmsug_{uuid.uuid4().hex}",
            "cache_key": cache_key,
            "raw_metric_name": raw.metric_name,
            "context_json": json.dumps(context, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            "candidate_codes_json": json.dumps(candidate_codes, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            "candidate_code": parsed.get("candidate_code", ""),
            "candidate_name": parsed.get("candidate_name", ""),
            "relation_type": parsed.get("relation_type", ""),
            "confidence": parsed.get("confidence", ""),
            "review_required": bool(parsed.get("review_required", True)),
            "reason": parsed.get("reason", ""),
            "model_name": self.config.model,
            "prompt_hash": prompt_hash,
            "response_json": json.dumps(parsed.get("response_json", {"raw": response_text}), ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            "validation_status": parsed.get("validation_status", "invalid_schema"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "from_cache": False,
            "decision": parsed.get("decision", ""),
            "standard_code_valid": parsed.get("validation_status") in {"valid", "valid_unknown"},
            "candidate_rank": parsed.get("candidate_rank", ""),
            "issue_reason": parsed.get("issue_reason", ""),
        }
        self.store.record_llm_suggestion(record)
        self.records.append(record)
        return record

    def summary(self) -> dict[str, Any]:
        return {
            "pass": True,
            "llm_enabled": self.enabled,
            "mock_llm_used": self.config.mock_mode,
            "live_llm_executed": self.live_llm_executed,
            "model_name": self.config.model,
            "cache_enabled": self.config.cache_enabled,
            "suggestions_total": len(self.records),
            "cached_suggestions_total": self.cached_suggestions_total,
            "invalid_responses_total": self.invalid_responses_total,
            "valid_candidate_total": sum(1 for row in self.records if row.get("validation_status") == "valid"),
            "valid_unknown_total": sum(1 for row in self.records if row.get("validation_status") == "valid_unknown"),
            "disabled_reason": "" if self.enabled else self.config.disabled_reason,
        }


def build_llm_context(raw: RawMetricRow) -> dict[str, Any]:
    return {
        "normalized_raw_metric_name": normalize_metric_name(raw.metric_name),
        "company_name": raw.company_name,
        "fill_date": raw.fill_date,
        "item_date": raw.item_date,
        "statement_type": raw.provenance.get("statement_type", ""),
        "header_path": raw.provenance.get("header_path", ""),
        "row_context_path": raw.provenance.get("row_context_path", ""),
        "value_type": raw.provenance.get("value_type", ""),
        "doc_id": raw.provenance.get("doc_id", ""),
    }


def build_llm_cache_key(
    *,
    raw_metric_name: str,
    context: dict[str, Any],
    candidate_codes: Iterable[str],
    policy_version: str,
    model_name: str,
) -> str:
    payload = {
        "normalized_raw_metric_name": normalize_metric_name(raw_metric_name),
        "company_name": context.get("company_name", ""),
        "statement_type": context.get("statement_type", ""),
        "header_path": context.get("header_path", ""),
        "row_context_path": context.get("row_context_path", ""),
        "candidate_codes": list(candidate_codes),
        "mapping_policy_version": policy_version,
        "model_name": model_name,
    }
    return "llmcache_" + _sha256_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True))


def _candidate_prompt_row(candidate: MappingCandidate, registry: StandardRegistry | None = None) -> dict[str, Any]:
    term = registry.term_by_code.get(candidate.candidate_code) if registry is not None else None
    return {
        "rank": candidate.candidate_rank,
        "code": candidate.candidate_code,
        "name": candidate.candidate_name,
        "aliases": list(term.aliases) if term is not None else [],
        "legacy_aliases": list(term.legacy_aliases) if term is not None else [],
        "relation_type": candidate.relation_type,
        "candidate_method": candidate.candidate_method,
        "score": candidate.candidate_score,
        "description": term.description if term is not None else candidate.issue_reason,
    }


def _candidate_response(item: dict[str, Any] | None, *, relation_type: str, confidence: float, reason: str) -> dict[str, Any]:
    if not item:
        return {
            "decision": "unknown",
            "standard_code": None,
            "standard_name": None,
            "relation_type": "unknown",
            "confidence": 0.1,
            "review_required": True,
            "reason": "无候选。",
            "candidate_rank": None,
        }
    return {
        "decision": "candidate",
        "standard_code": item.get("code"),
        "standard_name": item.get("name"),
        "relation_type": relation_type,
        "confidence": confidence,
        "review_required": relation_type in REVIEW_REQUIRED_RELATION_TYPES,
        "reason": reason,
        "candidate_rank": item.get("rank"),
    }


def _invalid_suggestion(
    validation_status: str,
    *,
    response_text: str = "",
    response_json: dict[str, Any] | None = None,
    issue_reason: str,
) -> dict[str, Any]:
    return {
        "decision": "invalid",
        "candidate_code": "",
        "candidate_name": "",
        "relation_type": "unknown",
        "confidence": "",
        "review_required": True,
        "reason": "",
        "candidate_rank": "",
        "validation_status": validation_status,
        "response_json": response_json if response_json is not None else {"raw": response_text},
        "issue_reason": issue_reason,
    }


def _candidate_rank(value: Any, fallback: int) -> int:
    try:
        rank = int(value)
        return rank if rank > 0 else fallback
    except (TypeError, ValueError):
        return fallback


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip().lstrip("\ufeff")] = value.strip().strip('"').strip("'")
    return values


def _optional_bool(value: Any) -> bool | None:
    if value is None or str(value).strip() == "":
        return None
    return _bool_value(value, False)


def _bool_value(value: Any, default: bool) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _int_value(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
