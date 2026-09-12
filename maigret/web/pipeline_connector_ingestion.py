"""Fenced page checkpoints and durable machine-feed delivery.

Machine identities collect evidence; they can never curate or approve a Persona.
No network transport is performed here. Collectors checkpoint normalized records
only after their governed transport has consumed the shared provider budget.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from maigret.web.case_store import investigation_jobs
from maigret.web.pipeline_store import _digest, _id, _json, _now, _text

COMPLETENESS = frozenset({"complete", "partial", "truncated", "unknown"})
MAX_BATCH_RECORDS = 500
MAX_BATCH_BYTES = 1_000_000
_SECRET_FIELDS = frozenset(
    {
        "authorization",
        "password",
        "secret",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "token",
    }
)
_VOLATILE = frozenset(
    {
        "id",
        "case_id",
        "subject_id",
        "persona_id",
        "request_id",
        "task_id",
        "attempt_id",
        "observed_at",
    }
)


def _safe_document(value):
    """Reject credentials instead of copying or silently redacting source content."""
    if isinstance(value, dict):
        if any(
            str(key).casefold().replace("-", "_") in _SECRET_FIELDS for key in value
        ):
            raise ValueError("Credential fields cannot enter connector evidence")
        for item in value.values():
            _safe_document(item)
    elif isinstance(value, list):
        for item in value:
            _safe_document(item)


def _stable_record(record):
    # Source content is stable across execution leases. Normalization introduces
    # attempt timestamps in account projections, which are not source changes.
    if isinstance(record.get("payload"), dict) and record.get("schema_version"):
        return {
            "payload": record["payload"],
            "retention": record.get("retention"),
            "engine": record.get("engine"),
        }
    return {key: value for key, value in record.items() if key not in _VOLATILE}


def _source_version(spec):
    if not isinstance(spec, dict):
        raise ValueError("Every source record requires a version descriptor")
    operation = spec.get("operation", "upsert")
    if operation not in {"upsert", "withdraw"}:
        raise ValueError("Source operation must be upsert or withdraw")
    return {
        "record_id": _text(spec.get("record_id"), "record_id", 200),
        "source_version": _text(spec.get("source_version"), "source_version", 200),
        "supersedes_version": (
            _text(spec["supersedes_version"], "supersedes_version", 200)
            if spec.get("supersedes_version") is not None
            else None
        ),
        "operation": operation,
    }


def _validate_durable_retention(document, identity):
    """A durable inbox must not become a bypass around source retention rules."""

    def mode(policy):
        if isinstance(policy, dict):
            if policy.get("mode") in {
                "prohibited",
                "transient",
                "live_only",
                "transient_display_only",
            }:
                return policy["mode"]
            if policy.get("retained") is False or policy.get("retainable") is False:
                return "metadata_only"
            policy = policy.get("mode", "retained")
        return {
            "bounded_source_evidence": "retained",
            "permitted_place_ids_and_status": "metadata_only",
        }.get(policy, policy)

    declared = mode(identity.get("retention", "retained"))
    if declared not in {"retained", "metadata_only"}:
        raise ValueError("This source policy does not permit a durable feed inbox")
    for item in document["records"]:
        data = item["data"]
        record_mode = mode(data.get("retention", "retained"))
        if record_mode == "retained" and (
            data.get("retained") is False or data.get("retainable") is False
        ):
            record_mode = "metadata_only"
        if record_mode not in {"retained", "metadata_only"}:
            raise ValueError(
                "Source record policy does not permit a durable feed inbox"
            )
        if declared == "metadata_only" or record_mode == "metadata_only":
            if set(data) - {
                "source_url",
                "status",
                "source_name",
                "published_at",
                "retention",
            }:
                raise ValueError(
                    "Metadata-only feeds require a metadata-only record envelope"
                )
            for name, maximum in (
                ("source_url", 4096),
                ("source_name", 300),
                ("status", 32),
                ("published_at", 64),
            ):
                value = data.get(name)
                if value is not None and (
                    not isinstance(value, str) or len(value) > maximum
                ):
                    raise ValueError("Metadata-only fields must be bounded strings")
            from maigret.web.pipeline_evidence import (
                canonical_source_url,
                normalize_status,
            )

            if data.get("source_url") and not canonical_source_url(data["source_url"]):
                raise ValueError(
                    "Metadata-only source_url must be a public HTTP locator"
                )
            if data.get("published_at"):
                try:
                    datetime.fromisoformat(data["published_at"].replace("Z", "+00:00"))
                except ValueError as exc:
                    raise ValueError(
                        "Metadata-only published_at must be an ISO timestamp"
                    ) from exc
            data["status"] = normalize_status(data.get("status", "inconclusive"))[0]
            data["retention"] = {"mode": "metadata_only", "final_eligible": False}


class ConnectorIngestionStore:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.engine = pipeline.engine

    def table(self, name):
        return self.pipeline._table("connector_" + name)

    def get_checkpoint(self, task_id):
        with self.engine.connect() as connection:
            table = self.table("checkpoints")
            row = (
                connection.execute(select(table).where(table.c.task_id == task_id))
                .mappings()
                .first()
            )
            return (
                _json(dict(row))
                if row
                else {
                    "task_id": task_id,
                    "cursor": None,
                    "watermark": None,
                    "completeness": "unknown",
                    "page_count": 0,
                    "record_count": 0,
                }
            )

    def checkpoint_page(
        self,
        attempt_id,
        records,
        *,
        cursor,
        watermark=None,
        completeness="partial",
        source_versions=None,
        worker_id=None,
        expected_cursor=None,
    ):
        """Commit evidence, source versions and continuation under one lease.

        `expected_cursor` identifies the consumed page, not the next page. A
        duplicate delivery has exactly the same page content. Cursor changes
        without committed evidence and stale worker commits are impossible.
        """
        records = _json(list(records))
        if len(records) > MAX_BATCH_RECORDS:
            raise ValueError("Page exceeds the bounded record limit")
        if completeness not in COMPLETENESS:
            raise ValueError("Explicit supported completeness is required")
        if completeness == "complete" and cursor is not None:
            raise ValueError("Complete collection cannot retain a continuation cursor")
        if completeness == "partial" and cursor is None:
            raise ValueError("Partial collection requires a continuation cursor")
        if completeness == "partial" and cursor == expected_cursor:
            raise ValueError("A partial page must advance its continuation cursor")
        for value in (cursor, watermark, expected_cursor):
            _safe_document(value)
            if len(json.dumps(value).encode()) > 16_384:
                raise ValueError("Checkpoint metadata is too large")
        if records and source_versions is None:
            raise ValueError("Each checkpoint record requires stable source_versions")
        versions = (
            [_source_version(item) for item in source_versions]
            if source_versions is not None
            else [None] * len(records)
        )
        if len(versions) != len(records):
            raise ValueError("Each source descriptor must match one normalized record")
        stable = [_stable_record(item) for item in records]
        _safe_document(stable)
        page_key = _digest(expected_cursor)
        page_hash = _digest([stable, versions, cursor, watermark, completeness])
        p = self.pipeline
        with self.engine.begin() as connection:
            attempt, task = p._attempt_context(connection, attempt_id, worker_id)
            cp, pages = self.table("checkpoints"), self.table("pages")
            previous = (
                connection.execute(select(cp).where(cp.c.task_id == task["id"]))
                .mappings()
                .first()
            )
            replay = (
                connection.execute(
                    select(pages).where(
                        pages.c.task_id == task["id"], pages.c.page_key == page_key
                    )
                )
                .mappings()
                .first()
            )
            if replay:
                if replay["content_hash"] != page_hash:
                    raise ValueError("Page replay changed immutable content")
                return _json(
                    {
                        **dict(previous),
                        "observation_ids": replay["observation_ids"],
                        "replayed": True,
                        "added_count": 0,
                    }
                )
            if previous and previous["completeness"] == "complete":
                raise ValueError("A complete checkpoint cannot accept another page")
            if (previous["cursor"] if previous else None) != expected_cursor:
                raise ValueError("Stale cursor: reload the committed continuation")
            connector_id = str(
                (task.get("spec") or {}).get("connector_id") or task["engine"]
            )
            stored, added = [], 0
            for record, descriptor in zip(records, versions):
                if descriptor is None:
                    row = p._append_observations(connection, attempt, task, [record])[0]
                    added += 1
                else:
                    row, fresh = self._record_version(
                        connection, attempt, task, connector_id, record, descriptor
                    )
                    added += int(fresh)
                stored.append(row["id"])
            now = _now()
            checkpoint = dict(
                task_id=task["id"],
                case_id=task["case_id"],
                persona_id=task["persona_id"],
                attempt_id=attempt_id,
                cursor=cursor,
                watermark=watermark,
                completeness=completeness,
                page_count=(previous["page_count"] if previous else 0) + 1,
                record_count=(previous["record_count"] if previous else 0) + added,
                updated_at=now,
            )
            if previous:
                connection.execute(
                    update(cp).where(cp.c.task_id == task["id"]).values(**checkpoint)
                )
            else:
                connection.execute(insert(cp).values(**checkpoint))
            connection.execute(
                insert(pages).values(
                    id=_id(),
                    case_id=task["case_id"],
                    persona_id=task["persona_id"],
                    task_id=task["id"],
                    attempt_id=attempt_id,
                    page_key=page_key,
                    content_hash=page_hash,
                    observation_ids=stored,
                    created_at=now,
                )
            )
            return _json(
                {
                    **checkpoint,
                    "observation_ids": stored,
                    "replayed": False,
                    "added_count": added,
                }
            )

    def _record_version(
        self, connection, attempt, task, connector_id, record, descriptor
    ):
        versions, heads = self.table("record_versions"), self.table("record_heads")
        identity = dict(
            case_id=task["case_id"],
            persona_id=task["persona_id"],
            connector_id=connector_id,
            record_id=descriptor["record_id"],
        )
        clauses = lambda table: [
            table.c[key] == value for key, value in identity.items()
        ]
        digest = _digest([_stable_record(record), descriptor])
        existing = (
            connection.execute(
                select(versions).where(
                    *clauses(versions),
                    versions.c.source_version == descriptor["source_version"],
                )
            )
            .mappings()
            .first()
        )
        if existing:
            if existing["content_hash"] != digest:
                raise ValueError("Source version replay changed immutable content")
            return {"id": existing["observation_id"]}, False
        head = (
            connection.execute(select(heads).where(*clauses(heads))).mappings().first()
        )
        prior = (
            self.pipeline._row(connection, versions, head["version_id"])
            if head
            else None
        )
        if descriptor["supersedes_version"] != (
            prior["source_version"] if prior else None
        ):
            raise ValueError(
                "Source update must supersede the current committed version"
            )
        if descriptor["operation"] == "withdraw" and not prior:
            raise ValueError("Cannot withdraw an unknown source record")
        if descriptor["operation"] == "withdraw" and (
            record.get("claims") or record.get("account")
        ):
            raise ValueError(
                "A withdrawal cannot introduce supporting claims or accounts"
            )
        observation_key = "source:" + _digest(
            [connector_id, descriptor["record_id"], descriptor["source_version"]]
        )
        versioned_record = dict(
            record, id=observation_key, observation_key=observation_key
        )
        row = self.pipeline._append_observations(
            connection, attempt, task, [versioned_record]
        )[0]
        version_id, now = _id(), _now()
        connection.execute(
            insert(versions).values(
                id=version_id,
                **identity,
                **{k: v for k, v in descriptor.items() if k != "record_id"},
                content_hash=digest,
                observation_id=row["id"],
                created_at=now,
            )
        )
        values = dict(version_id=version_id, updated_at=now)
        if head:
            connection.execute(update(heads).where(*clauses(heads)).values(**values))
        else:
            connection.execute(insert(heads).values(**identity, **values))
        return row, True

    def accept_batch(self, connector_id, identity, payload, *, idempotency_key):
        try:
            return self._accept_batch(
                connector_id, identity, payload, idempotency_key=idempotency_key
            )
        except IntegrityError:
            # PostgreSQL serializes same-subject requests via _scope. SQLite's
            # uniqueness guard can instead win a concurrent delivery race.
            # Only an exact committed receipt can convert that race to replay.
            document = validate_batch(payload)
            _validate_durable_retention(document, identity)
            table = self.table("receipts")
            with self.engine.connect() as connection:
                receipt = (
                    connection.execute(
                        select(table).where(
                            table.c.connector_id == connector_id,
                            table.c.case_id == document["case_id"],
                            table.c.persona_id == document["persona_id"],
                            table.c.idempotency_key == idempotency_key,
                        )
                    )
                    .mappings()
                    .first()
                )
                if receipt is None:
                    raise
                if receipt["content_hash"] != _digest(document):
                    raise ValueError(
                        "Idempotency key already belongs to different content"
                    )
                return self._receipt_response(connection, receipt, replayed=True)

    def _accept_batch(self, connector_id, identity, payload, *, idempotency_key):
        """Authenticate first; transaction commits receipt + normal worker job."""
        connector_id = _text(connector_id, "connector identity", 100)
        key = _text(idempotency_key, "Idempotency-Key", 200)
        document = validate_batch(payload)
        _validate_durable_retention(document, identity)
        case_id, persona_id = document["case_id"], document["persona_id"]
        if {"case_id": case_id, "persona_id": persona_id} not in identity.get(
            "scopes", []
        ):
            raise PermissionError(
                "Connector is not authorized for this case and Persona"
            )
        digest, p = _digest(document), self.pipeline
        with self.engine.begin() as connection:
            p._scope(connection, case_id, persona_id, lock=True)
            table = self.table("receipts")
            previous = (
                connection.execute(
                    select(table).where(
                        table.c.connector_id == connector_id,
                        table.c.case_id == case_id,
                        table.c.persona_id == persona_id,
                        table.c.idempotency_key == key,
                    )
                )
                .mappings()
                .first()
            )
            if previous:
                if previous["content_hash"] != digest:
                    raise ValueError(
                        "Idempotency key already belongs to different content"
                    )
                return self._receipt_response(connection, previous, replayed=True)
            from maigret.web.execution_budget import execution_budget_spec
            from maigret.web.pipeline_contract import ENGINE_REGISTRY, PIPELINE_ID

            receipt_id, job_id, request_id, now = _id(), _id(), _id(), _now()
            engine = ENGINE_REGISTRY["connector_feed"]
            task = dict(
                engine.as_dict(),
                pipeline_id=PIPELINE_ID,
                task_id="receipt:" + receipt_id,
                input_id="receipt:" + receipt_id,
                input_type="receipt_reference",
                input_value=receipt_id,
                route_state="active",
                reason=None,
                connector_id=connector_id,
                source_config_revision=_digest(
                    {
                        "connector_id": connector_id,
                        "retention": identity.get("retention"),
                    }
                ),
                retention=identity.get("retention") or "bounded_source_evidence",
            )
            from maigret.web.connectors.registry import get_connector_registry

            connector = get_connector_registry().get("connector_feed")
            if connector.source.get("status") != "active":
                raise PermissionError("Connector feed is disabled in the source catalog")
            task.update(connector.task_metadata())
            plan = dict(
                pipeline_id=PIPELINE_ID,
                request_id=request_id,
                inputs=[{"type": "receipt_reference", "value": receipt_id}],
                tasks=[task],
                budgets={"max_requests": 0},
                origin={"kind": "machine_feed"},
            )
            budget = execution_budget_spec("focused")
            options = {
                "requested_by": "connector:" + connector_id,
                "execution_budget": budget,
                "connector_receipt_id": receipt_id,
            }
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind="connector_ingestion",
                    status="queued",
                    usernames=[],
                    options=options,
                    progress={"checked": 0, "total": 1, "found": 0},
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    budget_seconds=budget["total_seconds"],
                    budget_policy_version=budget["policy_version"],
                    deadline_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            p.create_request(
                case_id,
                persona_id,
                plan["inputs"],
                plan,
                actor="connector:" + connector_id,
                job_id=job_id,
                request_id=request_id,
                idempotency_key="receipt:" + receipt_id,
                connection=connection,
            )
            receipt = dict(
                id=receipt_id,
                connector_id=connector_id,
                case_id=case_id,
                persona_id=persona_id,
                idempotency_key=key,
                content_hash=digest,
                payload=document,
                job_id=job_id,
                request_id=request_id,
                status="queued",
                error_code=None,
                created_at=now,
                updated_at=now,
            )
            connection.execute(insert(table).values(**receipt))
            return self._receipt_response(connection, receipt, replayed=False)

    def _receipt_response(self, connection, receipt, *, replayed=False):
        job = self.pipeline._row(connection, investigation_jobs, receipt["job_id"])
        state = receipt["status"]
        if state != "completed" and job["status"] in {
            "interrupted",
            "cancelled",
            "failed",
        }:
            state = job["status"]
        return _json(
            {
                key: receipt[key]
                for key in ("id", "case_id", "persona_id", "job_id", "request_id")
            }
            | {
                "status": state,
                "error_code": receipt["error_code"],
                "replayed": replayed,
            }
        )

    def get_receipt(self, receipt_id, *, connector_id, identity):
        with self.engine.connect() as connection:
            receipt = self.pipeline._row(connection, self.table("receipts"), receipt_id)
            if receipt["connector_id"] != connector_id or {
                "case_id": receipt["case_id"],
                "persona_id": receipt["persona_id"],
            } not in identity.get("scopes", []):
                raise KeyError(receipt_id)
            return self._receipt_response(connection, receipt)


def validate_batch(payload):
    if not isinstance(payload, dict) or set(payload) != {
        "case_id",
        "persona_id",
        "records",
    }:
        raise ValueError("Batch requires only case_id, persona_id and records")
    if len(json.dumps(payload, allow_nan=False).encode()) > MAX_BATCH_BYTES:
        raise ValueError("Connector batch is too large")
    case_id = _text(payload.get("case_id"), "case_id", 36)
    persona_id = _text(payload.get("persona_id"), "persona_id", 36)
    records = payload.get("records")
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_BATCH_RECORDS:
        raise ValueError("Batch requires 1–500 records")
    clean, identities = [], set()
    for item in records:
        if not isinstance(item, dict) or set(item) - {
            "record_id",
            "source_version",
            "supersedes_version",
            "operation",
            "data",
        }:
            raise ValueError("Invalid source record envelope")
        descriptor = _source_version(item)
        record_key = descriptor["record_id"]
        if record_key in identities:
            raise ValueError(
                "Each batch must contain at most one version of each record"
            )
        identities.add(record_key)
        data = item.get("data", {})
        if not isinstance(data, dict):
            raise ValueError("Record data must be an object")
        if set(data) & {
            "case_id",
            "persona_id",
            "subject_id",
            "task_id",
            "attempt_id",
            "request_id",
            "source_engine",
            "engine",
            "source_record_id",
            "original_observation_id",
            "original_evidence_id",
            "id",
        }:
            raise ValueError("Source data cannot override pipeline identity or lineage")
        if descriptor["operation"] == "withdraw" and data.get("claims"):
            raise ValueError("Withdrawal data cannot contain new claims")
        _safe_document(data)
        clean.append({**descriptor, "data": data})
    return {"case_id": case_id, "persona_id": persona_id, "records": clean}


def load_connector_identities(config=None):
    source = (config or {}).get("PIPELINE_CONNECTOR_IDENTITIES")
    if source is None:
        source = json.loads(
            os.environ.get("OPENLEDGER_CONNECTOR_IDENTITIES_JSON", "{}")
        )
    if not isinstance(source, dict):
        raise ValueError("Connector identities configuration must be an object")
    if any(not isinstance(identity, dict) for identity in source.values()):
        raise ValueError("Every connector identity must be a configuration object")
    return source


def authenticate_connector(connector_id, authorization, config=None):
    identity = load_connector_identities(config).get(connector_id)
    token = (
        authorization.removeprefix("Bearer ")
        if authorization.startswith("Bearer ")
        else ""
    )
    expected = str((identity or {}).get("token_sha256", ""))
    supplied = hashlib.sha256(token.encode()).hexdigest()
    if (
        not identity
        or identity.get("enabled") is not True
        or len(expected) != 64
        or len(token) < 32
        or not hmac.compare_digest(supplied, expected)
    ):
        raise PermissionError("Connector authentication failed")
    return identity


async def process_feed(task, context):
    """Registered worker adapter: durable delivery is separate from curation."""
    service = ConnectorIngestionStore(context.pipeline)
    with service.engine.begin() as connection:
        context.pipeline._attempt_context(
            connection, context.attempt["id"], context.job.get("worker_id")
        )
        receipt = context.pipeline._row(
            connection, service.table("receipts"), task["input_value"]
        )
        context.pipeline._check_scope(
            receipt, context.job["case_id"], context.request["persona_id"]
        )
        if (
            receipt["request_id"] != context.request["id"]
            or receipt["connector_id"] != task["connector_id"]
        ):
            raise ValueError("Receipt does not belong to the leased task")
        from maigret.web.pipeline_execution import _app

        current = (
            load_connector_identities(_app().app.config).get(receipt["connector_id"])
            or {}
        )
        expected_revision = _digest(
            {
                "connector_id": receipt["connector_id"],
                "retention": current.get("retention"),
            }
        )
        if (
            current.get("enabled") is not True
            or {"case_id": receipt["case_id"], "persona_id": receipt["persona_id"]}
            not in current.get("scopes", [])
            or expected_revision != task["source_config_revision"]
        ):
            connection.execute(
                update(service.table("receipts"))
                .where(service.table("receipts").c.id == receipt["id"])
                .values(
                    status="failed",
                    error_code="connector_authorization_changed",
                    updated_at=_now(),
                )
            )
            return {
                "outcome": "blocked",
                "retryable": False,
                "completeness": "unknown",
                "error_code": "connector_authorization_changed",
                "error": "Connector authorization or retention changed before processing",
            }
        connection.execute(
            update(service.table("receipts"))
            .where(service.table("receipts").c.id == receipt["id"])
            .values(status="running", updated_at=_now())
        )
    rows, versions = [], []
    for item in receipt["payload"]["records"]:
        descriptor = {
            key: item[key]
            for key in (
                "record_id",
                "source_version",
                "supersedes_version",
                "operation",
            )
        }
        raw = dict(
            item["data"],
            source_engine="connector_feed",
            source_record_id="source:"
            + _digest(
                [receipt["connector_id"], item["record_id"], item["source_version"]]
            ),
            source_lifecycle={"connector_id": receipt["connector_id"], **descriptor},
        )
        if item["operation"] == "withdraw":
            raw.update(
                status="inconclusive",
                claims=[],
                account=None,
                reason="Source explicitly withdrew its prior record",
            )
        rows.append(raw)
        versions.append(descriptor)
    try:
        context.checkpoint_page(
            rows, next_cursor=None, completeness="complete", source_versions=versions
        )
    except ValueError:
        with service.engine.begin() as connection:
            context.pipeline._attempt_context(
                connection, context.attempt["id"], context.job.get("worker_id")
            )
            connection.execute(
                update(service.table("receipts"))
                .where(service.table("receipts").c.id == receipt["id"])
                .values(
                    status="failed",
                    error_code="source_contract_conflict",
                    updated_at=_now(),
                )
            )
        return {
            "outcome": "error",
            "retryable": False,
            "completeness": "unknown",
            "error_code": "source_contract_conflict",
            "error": "Connector source contract conflict; inspect and correct the delivery",
        }
    with service.engine.begin() as connection:
        context.pipeline._attempt_context(
            connection, context.attempt["id"], context.job.get("worker_id")
        )
        connection.execute(
            update(service.table("receipts"))
            .where(service.table("receipts").c.id == receipt["id"])
            .values(status="completed", error_code=None, updated_at=_now())
        )
    return {"outcome": "candidate", "completeness": "complete", "retryable": False}


def superseded_observation_ids(connection, case_id, persona_id):
    """Evidence changed upstream; existing finals remain immutable but need review."""
    from maigret.web.case_store import metadata

    versions = metadata.tables["pipeline_connector_record_versions"]
    heads = metadata.tables["pipeline_connector_record_heads"]
    join = versions.join(
        heads,
        (versions.c.case_id == heads.c.case_id)
        & (versions.c.persona_id == heads.c.persona_id)
        & (versions.c.connector_id == heads.c.connector_id)
        & (versions.c.record_id == heads.c.record_id),
    )
    rows = connection.execute(
        select(versions.c.observation_id)
        .select_from(join)
        .where(
            versions.c.case_id == case_id,
            versions.c.persona_id == persona_id,
            (versions.c.id != heads.c.version_id)
            | (versions.c.operation == "withdraw"),
        )
    )
    return set(rows.scalars())
