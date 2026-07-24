from __future__ import annotations

import csv
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from project_paths import WEB_MAPPING_STORE_PATH

from .models import AliasEntry, StandardRegistry, StandardTerm
from .models import LLM_SUGGESTION_AUDIT_COLUMNS, LLM_SUGGESTION_COLUMNS
from .normalizer import normalize_metric_name
from .relations import SAFE_RELATION_TYPES, normalize_relation_type


DECISION_FIELDNAMES = [
    "decision_id",
    "job_id",
    "doc_id",
    "raw_metric_id",
    "raw_metric_name",
    "suggested_code",
    "suggested_name",
    "decision",
    "final_code",
    "final_name",
    "relation_type",
    "confidence",
    "decided_by",
    "decided_at",
    "note",
]


class LocalMappingStore:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or WEB_MAPPING_STORE_PATH).resolve()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS standard_terms (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT,
                    metric_type TEXT,
                    period_type TEXT,
                    description TEXT,
                    enabled INTEGER,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS term_aliases (
                    id TEXT PRIMARY KEY,
                    alias TEXT NOT NULL,
                    alias_norm TEXT NOT NULL,
                    standard_code TEXT NOT NULL,
                    standard_name TEXT,
                    relation_type TEXT NOT NULL,
                    scope_company TEXT DEFAULT '*',
                    scope_statement_type TEXT DEFAULT '*',
                    confidence REAL,
                    source TEXT,
                    approved_by TEXT,
                    approved_at TEXT,
                    enabled INTEGER DEFAULT 1,
                    note TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_term_aliases_alias_norm
                    ON term_aliases(alias_norm, enabled);
                CREATE INDEX IF NOT EXISTS idx_term_aliases_standard_code
                    ON term_aliases(standard_code, enabled);

                CREATE TABLE IF NOT EXISTS term_relations (
                    relation_id TEXT PRIMARY KEY,
                    target_code TEXT NOT NULL,
                    target_name TEXT,
                    relation_type TEXT NOT NULL,
                    formula_json TEXT,
                    auto_apply INTEGER DEFAULT 0,
                    review_required INTEGER DEFAULT 1,
                    enabled INTEGER DEFAULT 1,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS mapping_decisions (
                    decision_id TEXT PRIMARY KEY,
                    job_id TEXT,
                    doc_id TEXT,
                    raw_metric_id TEXT,
                    raw_metric_name TEXT,
                    suggested_code TEXT,
                    suggested_name TEXT,
                    decision TEXT,
                    final_code TEXT,
                    final_name TEXT,
                    relation_type TEXT,
                    confidence REAL,
                    decided_by TEXT,
                    decided_at TEXT,
                    note TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_mapping_decisions_job
                    ON mapping_decisions(job_id, decided_at);

                CREATE TABLE IF NOT EXISTS llm_suggestions (
                    suggestion_id TEXT PRIMARY KEY,
                    cache_key TEXT UNIQUE,
                    raw_metric_name TEXT,
                    context_json TEXT,
                    candidate_codes_json TEXT,
                    candidate_code TEXT,
                    candidate_name TEXT,
                    relation_type TEXT,
                    confidence REAL,
                    review_required INTEGER,
                    reason TEXT,
                    model_name TEXT,
                    prompt_hash TEXT,
                    response_json TEXT,
                    validation_status TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS namespace_migrations (
                    migration_id TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    old_code TEXT,
                    old_name TEXT,
                    new_code TEXT,
                    new_name TEXT,
                    migrated_at TEXT,
                    note TEXT
                );
                """
            )
            self._migrate_llm_suggestions(conn)

    def sync_registry(self, registry: StandardRegistry) -> None:
        self.initialize()
        now = _utc_now()
        with self._connect() as conn:
            self._migrate_non_base_namespace(conn, registry, now)
            # Base aliases and relations are config-owned. Rebuild them so old
            # code ids cannot survive a namespace change as active records.
            conn.execute("DELETE FROM term_aliases WHERE COALESCE(source, '') = 'base'")
            conn.execute("DELETE FROM term_relations")
            for term in registry.terms:
                conn.execute(
                    """
                    INSERT INTO standard_terms (
                        code, name, category, metric_type, period_type, description, enabled, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                        name=excluded.name,
                        category=excluded.category,
                        metric_type=excluded.metric_type,
                        period_type=excluded.period_type,
                        description=excluded.description,
                        enabled=excluded.enabled,
                        updated_at=excluded.updated_at
                    """,
                    (
                        term.code,
                        term.name,
                        term.category,
                        term.metric_type,
                        term.period_type,
                        term.description or term.notes,
                        1 if term.enabled else 0,
                        now,
                        now,
                    ),
                )
            registry_codes = [term.code for term in registry.terms]
            if registry_codes:
                placeholders = ",".join("?" for _ in registry_codes)
                conn.execute(f"DELETE FROM standard_terms WHERE code NOT IN ({placeholders})", registry_codes)
            for alias in registry.aliases:
                self._upsert_alias(
                    conn,
                    alias=alias.alias,
                    standard_code=alias.term.code,
                    standard_name=alias.term.name,
                    relation_type=alias.alias_type,
                    confidence=0.98,
                    source="base",
                    approved_by="config",
                    approved_at=now,
                    enabled=alias.safe_auto_map,
                    note=alias.note,
                )
            for relation in registry.relations:
                relation_id = relation.relation_id or _stable_id(
                    "rel",
                    relation.relation_type,
                    relation.canonical_code,
                    relation.canonical_name,
                    *relation.raw_names,
                    *relation.related_names,
                )
                conn.execute(
                    """
                    INSERT INTO term_relations (
                        relation_id, target_code, target_name, relation_type, formula_json,
                        auto_apply, review_required, enabled, note
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(relation_id) DO UPDATE SET
                        target_code=excluded.target_code,
                        target_name=excluded.target_name,
                        relation_type=excluded.relation_type,
                        formula_json=excluded.formula_json,
                        auto_apply=excluded.auto_apply,
                        review_required=excluded.review_required,
                        enabled=excluded.enabled,
                        note=excluded.note
                    """,
                    (
                        relation_id,
                        relation.canonical_code,
                        relation.canonical_name,
                        relation.relation_type,
                        relation.formula_json,
                        1 if relation.auto_apply else 0,
                        1 if relation.review_required else 0,
                        1 if relation.enabled else 0,
                        relation.note,
                    ),
                )

    def load_local_alias_entries(self, registry: StandardRegistry) -> list[AliasEntry]:
        self.initialize()
        term_by_code = registry.term_by_code
        entries: list[AliasEntry] = []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT alias, standard_code, standard_name, relation_type, confidence, source, note, enabled
                FROM term_aliases
                WHERE enabled = 1 AND COALESCE(source, '') != 'base'
                ORDER BY approved_at, alias
                """
            ).fetchall()
        for row in rows:
            term = term_by_code.get(str(row["standard_code"]))
            if term is None:
                term = StandardTerm(code=str(row["standard_code"]), name=str(row["standard_name"] or ""))
            relation_type = normalize_relation_type(row["relation_type"]) or "exact_alias"
            source = str(row["source"] or "human_approved")
            entries.append(
                AliasEntry(
                    term=term,
                    alias=str(row["alias"] or ""),
                    alias_type=relation_type,
                    safe_auto_map=relation_type in SAFE_RELATION_TYPES or source == "human_approved",
                    source=source,
                    note=str(row["note"] or ""),
                )
            )
        return entries

    def add_alias(
        self,
        *,
        alias: str,
        standard_code: str,
        standard_name: str,
        relation_type: str = "exact_alias",
        confidence: float | None = 1.0,
        source: str = "human_approved",
        approved_by: str = "web",
        note: str = "",
    ) -> str:
        self.initialize()
        approved_at = _utc_now()
        relation_type = normalize_relation_type(relation_type) or "exact_alias"
        with self._connect() as conn:
            return self._upsert_alias(
                conn,
                alias=alias,
                standard_code=standard_code,
                standard_name=standard_name,
                relation_type=relation_type,
                confidence=confidence,
                source=source,
                approved_by=approved_by,
                approved_at=approved_at,
                enabled=True,
                note=note,
            )

    def record_decision(
        self,
        *,
        job_id: str = "",
        doc_id: str = "",
        raw_metric_id: str = "",
        raw_metric_name: str = "",
        suggested_code: str = "",
        suggested_name: str = "",
        decision: str,
        final_code: str = "",
        final_name: str = "",
        relation_type: str = "",
        confidence: float | None = None,
        decided_by: str = "web",
        note: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        decided_at = _utc_now()
        payload = {
            "decision_id": f"mapdec_{uuid.uuid4().hex}",
            "job_id": job_id,
            "doc_id": doc_id,
            "raw_metric_id": raw_metric_id,
            "raw_metric_name": raw_metric_name,
            "suggested_code": suggested_code,
            "suggested_name": suggested_name,
            "decision": decision,
            "final_code": final_code,
            "final_name": final_name,
            "relation_type": normalize_relation_type(relation_type) or "",
            "confidence": confidence,
            "decided_by": decided_by,
            "decided_at": decided_at,
            "note": note,
        }
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO mapping_decisions (
                    decision_id, job_id, doc_id, raw_metric_id, raw_metric_name,
                    suggested_code, suggested_name, decision, final_code, final_name,
                    relation_type, confidence, decided_by, decided_at, note
                )
                VALUES (
                    :decision_id, :job_id, :doc_id, :raw_metric_id, :raw_metric_name,
                    :suggested_code, :suggested_name, :decision, :final_code, :final_name,
                    :relation_type, :confidence, :decided_by, :decided_at, :note
                )
                """,
                payload,
            )
        if decision == "accept_and_remember" and raw_metric_name and final_code:
            self.add_alias(
                alias=raw_metric_name,
                standard_code=final_code,
                standard_name=final_name,
                relation_type=payload["relation_type"] or "exact_alias",
                confidence=confidence if confidence is not None else 1.0,
                source="human_approved",
                approved_by=decided_by,
                note=note or f"由任务 {job_id} 人工采用并记住。",
            )
        return payload

    def export_aliases(self, path: str | Path | None = None) -> Path:
        self.initialize()
        path = Path(path or self.path.parent / "local_aliases_export.yml").resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.alias_rows(include_base=False)
        payload = {
            "generated_at": _utc_now(),
            "store_path": str(self.path),
            "aliases": rows,
        }
        path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path

    def export_decision_audit(self, path: str | Path | None = None) -> Path:
        self.initialize()
        path = Path(path or self.path.parent / "mapping_decisions_audit.csv").resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.decision_rows()
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=DECISION_FIELDNAMES)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: "" if row.get(field) is None else row.get(field, "") for field in DECISION_FIELDNAMES})
        return path

    def record_llm_suggestion(self, suggestion: dict[str, Any]) -> dict[str, Any]:
        self.initialize()
        payload = dict(suggestion)
        payload.setdefault("suggestion_id", f"llmsug_{uuid.uuid4().hex}")
        payload.setdefault("created_at", _utc_now())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO llm_suggestions (
                    suggestion_id, cache_key, raw_metric_name, context_json, candidate_codes_json,
                    candidate_code, candidate_name, relation_type, confidence, review_required,
                    reason, model_name, prompt_hash, response_json, validation_status, created_at
                )
                VALUES (
                    :suggestion_id, :cache_key, :raw_metric_name, :context_json, :candidate_codes_json,
                    :candidate_code, :candidate_name, :relation_type, :confidence, :review_required,
                    :reason, :model_name, :prompt_hash, :response_json, :validation_status, :created_at
                )
                ON CONFLICT(cache_key) DO UPDATE SET
                    raw_metric_name=excluded.raw_metric_name,
                    context_json=excluded.context_json,
                    candidate_codes_json=excluded.candidate_codes_json,
                    candidate_code=excluded.candidate_code,
                    candidate_name=excluded.candidate_name,
                    relation_type=excluded.relation_type,
                    confidence=excluded.confidence,
                    review_required=excluded.review_required,
                    reason=excluded.reason,
                    model_name=excluded.model_name,
                    prompt_hash=excluded.prompt_hash,
                    response_json=excluded.response_json,
                    validation_status=excluded.validation_status,
                    created_at=excluded.created_at
                """,
                {
                    "suggestion_id": payload.get("suggestion_id", ""),
                    "cache_key": payload.get("cache_key", ""),
                    "raw_metric_name": payload.get("raw_metric_name", ""),
                    "context_json": payload.get("context_json", ""),
                    "candidate_codes_json": payload.get("candidate_codes_json", ""),
                    "candidate_code": payload.get("candidate_code", ""),
                    "candidate_name": payload.get("candidate_name", ""),
                    "relation_type": payload.get("relation_type", ""),
                    "confidence": _nullable_float(payload.get("confidence")),
                    "review_required": 1 if payload.get("review_required", True) in {True, 1, "1", "true", "True", "是"} else 0,
                    "reason": payload.get("reason", ""),
                    "model_name": payload.get("model_name", ""),
                    "prompt_hash": payload.get("prompt_hash", ""),
                    "response_json": payload.get("response_json", ""),
                    "validation_status": payload.get("validation_status", ""),
                    "created_at": payload.get("created_at", ""),
                },
            )
        return payload

    def get_llm_suggestion(self, cache_key: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM llm_suggestions WHERE cache_key = ?", (cache_key,)).fetchone()
        return _row_dict(row) if row else None

    def llm_suggestion_rows(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM llm_suggestions ORDER BY created_at, suggestion_id").fetchall()
        return [_row_dict(row) for row in rows]

    def export_llm_suggestions(self, path: str | Path | None = None, rows: list[dict[str, Any]] | None = None) -> Path:
        self.initialize()
        path = Path(path or self.path.parent / "llm_suggestions.csv").resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.llm_suggestion_rows() if rows is None else rows
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=LLM_SUGGESTION_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: "" if row.get(field) is None else row.get(field, "") for field in LLM_SUGGESTION_COLUMNS})
        return path

    def export_llm_suggestion_audit(self, path: str | Path | None = None, rows: list[dict[str, Any]] | None = None) -> Path:
        self.initialize()
        path = Path(path or self.path.parent / "llm_suggestion_audit.csv").resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.llm_suggestion_rows() if rows is None else rows
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=LLM_SUGGESTION_AUDIT_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: "" if row.get(field) is None else row.get(field, "") for field in LLM_SUGGESTION_AUDIT_COLUMNS})
        return path

    def write_snapshot(self, path: str | Path) -> Path:
        self.initialize()
        path = Path(path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": _utc_now(),
            "store_path": str(self.path),
            "standard_terms_total": self.count("standard_terms"),
            "term_aliases_total": self.count("term_aliases", where="enabled = 1"),
            "local_aliases_total": self.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'"),
            "term_relations_total": self.count("term_relations", where="enabled = 1"),
            "mapping_decisions_total": self.count("mapping_decisions"),
            "llm_suggestions_total": self.count("llm_suggestions"),
            "namespace_migrations_total": self.count("namespace_migrations"),
            "aliases": self.alias_rows(include_base=False),
        }
        path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path

    def alias_rows(self, *, include_base: bool = True) -> list[dict[str, Any]]:
        self.initialize()
        where = "enabled = 1"
        if not include_base:
            where += " AND COALESCE(source, '') != 'base'"
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT alias, alias_norm, standard_code, standard_name, relation_type,
                       scope_company, scope_statement_type, confidence, source,
                       approved_by, approved_at, note
                FROM term_aliases
                WHERE {where}
                ORDER BY source, standard_code, alias_norm
                """
            ).fetchall()
        return [_row_dict(row) for row in rows]

    def decision_rows(self, *, job_id: str = "") -> list[dict[str, Any]]:
        self.initialize()
        query = "SELECT * FROM mapping_decisions"
        params: tuple[str, ...] = ()
        if job_id:
            query += " WHERE job_id = ?"
            params = (job_id,)
        query += " ORDER BY decided_at, decision_id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_row_dict(row) for row in rows]

    def count(self, table: str, *, where: str = "") -> int:
        self.initialize()
        allowed = {"standard_terms", "term_aliases", "term_relations", "mapping_decisions", "llm_suggestions", "namespace_migrations"}
        if table not in allowed:
            raise ValueError(f"Unsupported table: {table}")
        query = f"SELECT COUNT(*) AS total FROM {table}"
        if where:
            query += f" WHERE {where}"
        with self._connect() as conn:
            row = conn.execute(query).fetchone()
        return int(row["total"] if row else 0)

    def _upsert_alias(
        self,
        conn: sqlite3.Connection,
        *,
        alias: str,
        standard_code: str,
        standard_name: str,
        relation_type: str,
        confidence: float | None,
        source: str,
        approved_by: str,
        approved_at: str,
        enabled: bool,
        note: str,
    ) -> str:
        alias_text = str(alias or "").strip()
        alias_norm = normalize_metric_name(alias_text)
        relation_type = normalize_relation_type(relation_type) or "exact_alias"
        alias_id = _stable_id("alias", alias_norm, standard_code, relation_type, source)
        conn.execute(
            """
            INSERT INTO term_aliases (
                id, alias, alias_norm, standard_code, standard_name, relation_type,
                scope_company, scope_statement_type, confidence, source, approved_by,
                approved_at, enabled, note
            )
            VALUES (?, ?, ?, ?, ?, ?, '*', '*', ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                alias=excluded.alias,
                alias_norm=excluded.alias_norm,
                standard_name=excluded.standard_name,
                relation_type=excluded.relation_type,
                confidence=excluded.confidence,
                source=excluded.source,
                approved_by=excluded.approved_by,
                approved_at=excluded.approved_at,
                enabled=excluded.enabled,
                note=excluded.note
            """,
            (
                alias_id,
                alias_text,
                alias_norm,
                standard_code,
                standard_name,
                relation_type,
                confidence,
                source,
                approved_by,
                approved_at,
                1 if enabled else 0,
                note,
            ),
        )
        return alias_id

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _migrate_llm_suggestions(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(llm_suggestions)").fetchall()
        existing = {str(row["name"]) for row in rows}
        migrations = {
            "cache_key": "TEXT",
            "candidate_codes_json": "TEXT",
            "relation_type": "TEXT",
            "review_required": "INTEGER",
            "validation_status": "TEXT",
        }
        for column, column_type in migrations.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE llm_suggestions ADD COLUMN {column} {column_type}")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_suggestions_cache_key ON llm_suggestions(cache_key)")

    def _migrate_non_base_namespace(self, conn: sqlite3.Connection, registry: StandardRegistry, migrated_at: str) -> None:
        terms_by_name: dict[str, list[StandardTerm]] = {}
        for term in registry.terms:
            terms_by_name.setdefault(normalize_metric_name(term.name), []).append(term)
        old_names = {
            str(row["code"]): str(row["name"] or "")
            for row in conn.execute("SELECT code, name FROM standard_terms").fetchall()
        }

        alias_rows = conn.execute(
            """
            SELECT id, alias_norm, standard_code, standard_name, relation_type, source, note
            FROM term_aliases
            WHERE COALESCE(source, '') != 'base'
            """
        ).fetchall()
        for row in alias_rows:
            old_code = str(row["standard_code"] or "")
            old_name = str(row["standard_name"] or old_names.get(old_code, "") or "")
            target = _unique_term_by_name(terms_by_name, old_name)
            if target is None or (target.code == old_code and target.name == old_name):
                continue
            old_id = str(row["id"])
            new_id = _stable_id(
                "alias",
                str(row["alias_norm"] or ""),
                target.code,
                str(row["relation_type"] or ""),
                str(row["source"] or ""),
            )
            existing = conn.execute("SELECT id FROM term_aliases WHERE id = ?", (new_id,)).fetchone()
            if existing is not None and new_id != old_id:
                conn.execute("DELETE FROM term_aliases WHERE id = ?", (old_id,))
            else:
                note = _append_migration_note(str(row["note"] or ""), old_code, target.code)
                conn.execute(
                    "UPDATE term_aliases SET id = ?, standard_code = ?, standard_name = ?, note = ? WHERE id = ?",
                    (new_id, target.code, target.name, note, old_id),
                )
            self._record_namespace_migration(
                conn,
                entity_type="term_alias",
                entity_id=old_id,
                field_name="standard_code",
                old_code=old_code,
                old_name=old_name,
                new_code=target.code,
                new_name=target.name,
                migrated_at=migrated_at,
            )

        decision_rows = conn.execute(
            """
            SELECT decision_id, suggested_code, suggested_name, final_code, final_name, note
            FROM mapping_decisions
            """
        ).fetchall()
        for row in decision_rows:
            updates: dict[str, str] = {}
            migration_notes: list[str] = []
            for prefix in ("suggested", "final"):
                code_field = f"{prefix}_code"
                name_field = f"{prefix}_name"
                old_code = str(row[code_field] or "")
                old_name = str(row[name_field] or old_names.get(old_code, "") or "")
                target = _unique_term_by_name(terms_by_name, old_name)
                if target is None or (target.code == old_code and target.name == old_name):
                    continue
                updates[code_field] = target.code
                updates[name_field] = target.name
                migration_notes.append(f"{prefix}:{old_code}->{target.code}")
                self._record_namespace_migration(
                    conn,
                    entity_type="mapping_decision",
                    entity_id=str(row["decision_id"]),
                    field_name=code_field,
                    old_code=old_code,
                    old_name=old_name,
                    new_code=target.code,
                    new_name=target.name,
                    migrated_at=migrated_at,
                )
            if updates:
                updates["note"] = _append_text_note(str(row["note"] or ""), "命名空间迁移 " + ", ".join(migration_notes))
                assignments = ", ".join(f"{field} = ?" for field in updates)
                conn.execute(
                    f"UPDATE mapping_decisions SET {assignments} WHERE decision_id = ?",
                    (*updates.values(), str(row["decision_id"])),
                )

    def _record_namespace_migration(
        self,
        conn: sqlite3.Connection,
        *,
        entity_type: str,
        entity_id: str,
        field_name: str,
        old_code: str,
        old_name: str,
        new_code: str,
        new_name: str,
        migrated_at: str,
    ) -> None:
        migration_id = _stable_id("nsmig", entity_type, entity_id, field_name, old_code, new_code)
        conn.execute(
            """
            INSERT OR IGNORE INTO namespace_migrations (
                migration_id, entity_type, entity_id, field_name, old_code,
                old_name, new_code, new_name, migrated_at, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                migration_id,
                entity_type,
                entity_id,
                field_name,
                old_code,
                old_name,
                new_code,
                new_name,
                migrated_at,
                "按标准科目名称迁移；未按冲突代码直接重解释。",
            ),
        )


def initialize_mapping_store(path: str | Path | None, registry: StandardRegistry | None = None) -> LocalMappingStore:
    store = LocalMappingStore(path)
    store.initialize()
    if registry is not None:
        store.sync_registry(registry)
    return store


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _stable_id(prefix: str, *parts: object) -> str:
    import hashlib

    payload = json.dumps([str(part or "") for part in parts], ensure_ascii=False, separators=(",", ":"))
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _nullable_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _unique_term_by_name(terms_by_name: dict[str, list[StandardTerm]], name: str) -> StandardTerm | None:
    matches = terms_by_name.get(normalize_metric_name(name), [])
    return matches[0] if len(matches) == 1 else None


def _append_migration_note(note: str, old_code: str, new_code: str) -> str:
    return _append_text_note(note, f"标准编码按名称迁移：{old_code}->{new_code}")


def _append_text_note(note: str, suffix: str) -> str:
    text = str(note or "").strip()
    if suffix in text:
        return text
    return f"{text}；{suffix}" if text else suffix
