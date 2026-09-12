"""Durable bridges from legacy/AI/chat evidence and direct operator proposals.

These are collection/normalization operations. They never include a claim in a
curated version, assign probability, or approve a Persona. Request/task records
are restart checkpoints, with immutable native evidence and source independence.
"""

from __future__ import annotations

from typing import Mapping
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import or_, select

from maigret.web.pipeline_consolidation import (
    consolidate_observations,
    qualified_claim_identity,
)
from maigret.web.pipeline_evidence import (
    canonical_origin_url,
    fingerprint,
    iter_result_observations,
    normalize_observation,
)

IMPORT_ACTOR = "system:legacy-workspace-import"
_CLAIM_FIELDS = frozenset(
    {
        "predicate",
        "field_name",
        "value",
        "qualifiers",
        "valid_from",
        "valid_to",
        "role",
        "organization",
        "jurisdiction",
        "precision",
        "language",
        "account_key",
    }
)
_TERMINAL_JOBS = frozenset(
    {"completed", "failed", "cancelled", "interrupted", "budget_exhausted"}
)


def _pipeline(store):
    from maigret.web.pipeline_store import PipelineStore

    return PipelineStore(store)


def _scope(store, case_id, persona_id):
    case = store.get_case(case_id)
    if not case or not any(
        str(item["id"]) == str(persona_id) for item in case["personas"]
    ):
        raise KeyError("Persona does not belong to this case")
    return case


def _refresh(store, case_id, persona_id):
    # Avoid importing the Flask application while loading migration tools.
    try:
        from maigret.web.pipeline_execution import refresh_consolidation
    except ModuleNotFoundError as exc:
        if exc.name != "maigret.web.pipeline_execution":
            raise
        pipeline = _pipeline(store)
        return pipeline.upsert_groups(
            case_id,
            persona_id,
            consolidate_observations(pipeline.iter_observations(case_id, persona_id)),
        )
    return refresh_consolidation(store, case_id, persona_id)


def _checkpoint(pipeline, case_id, persona_id, *, key, kind, actor, source):
    """Recover an existing attempt rather than manufacture another on replay."""
    request_id = str(uuid5(NAMESPACE_URL, "openledger:p2:" + key))
    request = pipeline.create_request(
        case_id,
        persona_id,
        [{"type": kind, "value": source}],
        {
            "pipeline_id": "p2-e2e-v1",
            "request_id": request_id,
            "import_source": source,
            "tasks": [
                {
                    "task_id": key,
                    "engine_id": kind,
                    "route_state": "active",
                    "retry_ceiling": 0,
                }
            ],
        },
        actor=actor,
        request_id=request_id,
        idempotency_key=key,
    )
    task = request["tasks"][0]
    attempt = (
        task["attempts"][0]
        if task["attempts"]
        else pipeline.start_attempt(task["id"], "ingestion:" + fingerprint(key)[:32])
    )
    return request, task, attempt


def _import_records(
    pipeline,
    case_id,
    persona_id,
    *,
    key,
    kind,
    actor,
    source,
    records,
    source_time=None,
):
    request, task, attempt = _checkpoint(
        pipeline, case_id, persona_id, key=key, kind=kind, actor=actor, source=source
    )
    if attempt["status"] == "completed":
        return {
            "request_id": request["id"],
            "already_imported": True,
            "observation_count": 0,
        }
    scope = dict(
        case_id=case_id,
        subject_id=persona_id,
        request_id=request["id"],
        task_id=task["id"],
        attempt_id=attempt["id"],
        observed_at=source_time,
        legacy=True,
    )
    observations = (normalize_observation(record, **scope) for record in records)
    completed = pipeline.record_observations(
        attempt["id"], observations, outcome="candidate", worker_id=attempt["worker_id"]
    )
    return {
        "request_id": request["id"],
        "already_imported": False,
        "observation_count": len(completed["observations"]),
    }


def _legacy_job_result(store, case, persona_id, job):
    """Use persisted subject bindings; ambiguous evidence remains case-scoped."""
    spec = (job.get("options") or {}).get("investigation_spec") or {}
    grouped_id, by_username = store._job_persona_bindings(spec, case["personas"])
    whole_subject = grouped_id == persona_id or len(case["personas"]) == 1
    usernames = {name for name, ids in by_username.items() if persona_id in ids}
    if grouped_id and grouped_id != persona_id:
        return None
    if not whole_subject and not usernames:
        return None
    result = {}
    for name in (
        "individual_reports",
        "general_results",
        "collector_observations",
        "source_errors",
    ):
        records = job.get(name) or []
        if whole_subject or name == "source_errors":
            result[name] = records
        elif name == "individual_reports":
            result[name] = [
                row
                for row in records
                if str(row.get("username") or "").casefold() in usernames
            ]
        elif name == "general_results":
            result[name] = [
                row
                for row in records
                if isinstance(row, (tuple, list))
                and row
                and str(row[0]).casefold() in usernames
            ]
        else:
            result[name] = [
                row
                for row in records
                if str(
                    row.get("seed_username")
                    or row.get("username")
                    or row.get("subject_value")
                    or ""
                ).casefold()
                in usernames
            ]
    audit_method = getattr(store, "list_profile_search_audits", None)
    if whole_subject and audit_method:
        # Existing audit API is capped; page the underlying immutable rows so no
        # older source attempt silently disappears from the import.
        from maigret.web.case_store import profile_search_audits

        with store.engine.connect() as connection:
            result["profile_search_audits"] = [
                dict(row)
                for row in connection.execute(
                    select(profile_search_audits)
                    .where(profile_search_audits.c.job_id == job["job_id"])
                    .order_by(
                        profile_search_audits.c.created_at, profile_search_audits.c.id
                    )
                ).mappings()
            ]
    metadata = {
        key: job.get(key)
        for key in (
            "job_id",
            "kind",
            "status",
            "started_at",
            "completed_at",
            "attempts",
            "error",
            "source_coverage",
            "source_counts",
            "found_count",
            "checked_count",
            "source_errors",
        )
        if key in job
    }
    if not whole_subject:
        metadata["scope_note"] = (
            "Only evidence with a persisted username binding was assigned to this subject; other evidence remains in the original case job."
        )
    result["source_observations"] = [
        {
            "source_engine": "legacy_scan_metadata",
            "native_record_id": "job-metadata:" + job["job_id"],
            "status": "inconclusive",
            "legacy_job_id": job["job_id"],
            "claims": [],
            "metadata": metadata,
        }
    ]
    return result


def bootstrap_legacy_workspace(store, case_id, persona_id):
    """Import existing claims/reports/chat in restartable batches, then consolidate.

    A repeated call also discovers newly saved AI/chat candidate claims. The
    original review history remains metadata; no legacy item becomes Final.
    Checkpoints are persisted pipeline requests, not transient process counters.
    """
    case = _scope(store, case_id, persona_id)
    pipeline = _pipeline(store)
    report = {
        "case_id": case_id,
        "persona_id": persona_id,
        "claim_count": 0,
        "observation_count": 0,
        "missing_provenance_count": 0,
        "request_ids": [],
        "job_count": 0,
        "chat_count": 0,
        "auto_included": False,
        "auto_finalized": False,
    }
    after = None
    while True:
        batch = pipeline.backfill_legacy(
            case_id,
            persona_id,
            actor=IMPORT_ACTOR,
            dry_run=False,
            limit=250,
            after_claim_id=after,
            materialize=False,
        )
        for key in ("claim_count", "observation_count", "missing_provenance_count"):
            report[key] += batch.get(key, 0)
        report["request_ids"].extend(
            batch.get("request_ids")
            or ([batch["request_id"]] if batch.get("request_id") else [])
        )
        after = batch.get("next_after_claim_id")
        if not after:
            break
    for job in case["jobs"]:
        if job["status"] not in _TERMINAL_JOBS:
            continue
        # Enqueueing a request does not prove that its execution populated the
        # source ledger. Only an explicitly identified pipeline result may use
        # its presentation report as a projection of evidence already retained.
        # Historical/imported records must remain recoverable after a new plan
        # has been attached to the same job.
        if job.get("pipeline_id") == "p2-e2e-v1" and pipeline.requests_for_job(
            job["job_id"]
        ):
            continue
        result = _legacy_job_result(store, case, persona_id, job)
        if result is None:
            continue
        # Normalize date objects before computing the stable snapshot identity.
        from maigret.web.pipeline_store import _json

        result = _json(result)
        key = "legacy-job:" + fingerprint([case_id, persona_id, job["job_id"], result])
        request, task, attempt = _checkpoint(
            pipeline,
            case_id,
            persona_id,
            key=key,
            kind="legacy_scan_import",
            actor=IMPORT_ACTOR,
            source=job["job_id"],
        )
        report["request_ids"].append(request["id"])
        report["job_count"] += 1
        if attempt["status"] == "completed":
            continue
        documents = iter_result_observations(
            result,
            case_id=case_id,
            subject_id=persona_id,
            request_id=request["id"],
            task_id=task["id"],
            attempt_id=attempt["id"],
            observed_at=job.get("completed_at") or job.get("started_at"),
            legacy=True,
        )
        completed = pipeline.record_observations(
            attempt["id"],
            documents,
            outcome="candidate",
            worker_id=attempt["worker_id"],
        )
        report["observation_count"] += len(completed["observations"])

    from maigret.web.case_store import case_chat_messages

    with store.engine.connect() as connection:
        statement = select(case_chat_messages).where(
            case_chat_messages.c.case_id == case_id
        )
        if len(case["personas"]) == 1:
            statement = statement.where(
                or_(
                    case_chat_messages.c.persona_id == persona_id,
                    case_chat_messages.c.persona_id.is_(None),
                )
            )
        else:
            statement = statement.where(case_chat_messages.c.persona_id == persona_id)
        messages = (
            connection.execution_options(stream_results=True, yield_per=250)
            .execute(
                statement.order_by(
                    case_chat_messages.c.created_at, case_chat_messages.c.id
                )
            )
            .mappings()
        )
        # Do not keep a streaming database read open while writing on SQLite.
        message_ids = [str(row["id"]) for row in messages]
    for message_id in message_ids:
        with store.engine.connect() as connection:
            message = dict(
                connection.execute(
                    select(case_chat_messages).where(
                        case_chat_messages.c.id == message_id
                    )
                )
                .mappings()
                .one()
            )
        from maigret.web.pipeline_store import _json

        message = _json(message)
        raw = {
            "source_engine": "case_chat",
            "native_record_id": "chat:" + message_id,
            "status": "candidate",
            "claims": [],
            "legacy_chat_message_id": message_id,
            "observed_at": message["created_at"],
            "message": message,
            "evidence_type": (
                "model_summary"
                if message["role"] == "assistant"
                else "operator_statement"
            ),
            "independence": "derivative",
            "reason": "Original chat preserved; separately validated candidate claims are imported through their claim lineage.",
        }
        key = "legacy-chat:" + fingerprint([case_id, persona_id, message_id, message])
        imported = _import_records(
            pipeline,
            case_id,
            persona_id,
            key=key,
            kind="legacy_chat_import",
            actor=IMPORT_ACTOR,
            source=message_id,
            records=[raw],
            source_time=message["created_at"],
        )
        report["request_ids"].append(imported["request_id"])
        report["observation_count"] += imported["observation_count"]
        report["chat_count"] += 1
    report["group_count"] = len(_refresh(store, case_id, persona_id))
    pipeline.mark_legacy_imported(case_id, persona_id)
    report["request_ids"] = list(dict.fromkeys(report["request_ids"]))
    return report


def ingest_legacy_claim_updates(store, case_id, persona_id):
    """Bridge committed AI/chat candidate updates without waiting for a page GET.

    Call after the existing candidate/review transaction commits, never from
    inside that transaction. This imports only pending legacy claim revisions;
    it does not revisit complete reports/chat or trigger another collection job.
    """
    _scope(store, case_id, persona_id)
    pipeline = _pipeline(store)
    after = None
    request_ids = []
    pending_claim_count = 0
    while True:
        batch = pipeline.backfill_legacy(
            case_id,
            persona_id,
            actor=IMPORT_ACTOR,
            dry_run=False,
            limit=250,
            after_claim_id=after,
            materialize=False,
        )
        pending_claim_count += batch.get("pending_claim_count", 0)
        request_ids.extend(batch.get("request_ids") or [])
        after = batch.get("next_after_claim_id")
        if not after:
            break
    groups = _refresh(store, case_id, persona_id)
    return {
        "case_id": case_id,
        "persona_id": persona_id,
        "pending_claim_count": pending_claim_count,
        "request_ids": list(dict.fromkeys(request_ids)),
        "group_count": len(groups),
        "auto_included": False,
        "auto_finalized": False,
    }


def submit_manual_evidence(
    store, case_id, persona_id, actor, claim, source_url, reason
):
    """Append a typed, source-linked proposal for later operator/QC decisions."""
    from maigret.web.pipeline_store import _actor, _text

    actor, reason = _actor(actor), _text(reason, "Research proposal reason", 10000)
    _scope(store, case_id, persona_id)
    if not isinstance(claim, Mapping):
        raise ValueError("Manual claim must be an object")
    unknown = set(claim) - _CLAIM_FIELDS - {"observation_ids"}
    if unknown:
        raise ValueError(
            "Unsupported manual claim fields: " + ", ".join(sorted(unknown))
        )
    normalized_claim = {
        key: value for key, value in claim.items() if key in _CLAIM_FIELDS
    }
    qualified_claim_identity(normalized_claim, case_id=case_id, subject_id=persona_id)
    references = claim.get("observation_ids") or []
    if (
        not isinstance(references, (tuple, list))
        or len(references) > 500
        or not all(isinstance(item, str) and item for item in references)
    ):
        raise ValueError("Select at most 500 source observations")
    references = list(dict.fromkeys(references))
    url = canonical_origin_url(source_url) if source_url else None
    if source_url and not url:
        raise ValueError("Source must be a valid public HTTP(S) URL")
    if not url and not references:
        raise ValueError("A source URL or existing source observations are required")
    pipeline = _pipeline(store)
    table = pipeline._table("observations")
    with store.engine.connect() as connection:
        selected = (
            list(
                connection.execute(
                    select(table).where(
                        table.c.case_id == case_id,
                        table.c.persona_id == persona_id,
                        or_(
                            table.c.id.in_(references),
                            table.c.observation_key.in_(references),
                        ),
                    )
                ).mappings()
            )
            if references
            else []
        )
        matched = {
            value for row in selected for value in (row["id"], row["observation_key"])
        }
        if not set(references).issubset(matched):
            raise ValueError(
                "Source observation does not belong to this case and Persona"
            )
        account_key = normalized_claim.get("account_key")
        if account_key:
            groups = pipeline._table("groups")
            accounts = list(
                connection.execute(
                    select(groups).where(
                        groups.c.case_id == case_id,
                        groups.c.persona_id == persona_id,
                        groups.c.kind == "account",
                    )
                ).mappings()
            )
            account = next(
                (
                    row
                    for row in accounts
                    if account_key
                    in {
                        row["id"],
                        row["canonical_key"],
                        row["normalized"].get("id"),
                        row["normalized"].get("canonical_key"),
                    }
                ),
                None,
            )
            if account is None:
                raise ValueError(
                    "Account hypothesis does not belong to this case and Persona"
                )
            normalized_claim["account_key"] = account["normalized"].get(
                "canonical_key"
            ) or account["normalized"].get("id")
    normalized_ids = sorted(
        {str(row["payload"].get("id") or row["observation_key"]) for row in selected}
    )
    request_key = "manual:" + fingerprint(
        [case_id, persona_id, actor, normalized_claim, url, normalized_ids, reason]
    )
    request, task, attempt = _checkpoint(
        pipeline,
        case_id,
        persona_id,
        key=request_key,
        kind="manual_evidence",
        actor=actor,
        source={
            "claim": normalized_claim,
            "source_url": url,
            "observation_ids": normalized_ids,
            "reason": reason,
        },
    )
    original_ids = sorted(str(row["id"]) for row in selected)
    raw = {
        "source_engine": "manual_evidence",
        "native_record_id": request_key,
        "status": "candidate",
        "source_url": url,
        "original_url": url,
        "claims": [normalized_claim],
        "derived_from": [],
        "original_observation_ids": original_ids,
        "submitted_by": actor,
        "reason": reason,
        "evidence_type": "operator_manual_proposal",
        "independence": "derivative",
        "evidence_signals": {},
        "ownership_status": "unverified",
    }
    records = []
    for row in sorted(selected, key=lambda item: item["id"]):
        native_id = str(row["payload"].get("id") or row["observation_key"])
        # One extraction per cited observation preserves each source's root;
        # the operator note itself never contributes another corroboration.
        records.append(
            {
                **raw,
                "native_record_id": request_key + ":" + native_id,
                "source_url": row["source_url"],
                "original_url": None,
                "derived_from": [native_id],
                "artifact_ref": row.get("artifact_ref"),
                "source_retained": bool(row["retained"]),
            }
        )
    source_urls = {canonical_origin_url(row["source_url"]) for row in selected}
    if not selected or (url and url not in source_urls):
        records.append(raw)
    observations = []
    for record in records:
        observation = normalize_observation(
            record,
            case_id=case_id,
            subject_id=persona_id,
            request_id=request["id"],
            task_id=task["id"],
            attempt_id=attempt["id"],
            observed_at=attempt["created_at"],
        )
        if record.get("source_retained") is False:
            observation["retention"] = {
                "mode": "metadata_only",
                "final_eligible": False,
                "reason": "Referenced source is not retainable; this proposal needs retainable evidence.",
            }
        observations.append(observation)
    completed = pipeline.record_observations(
        attempt["id"], observations, outcome="candidate", worker_id=attempt["worker_id"]
    )
    groups = _refresh(store, case_id, persona_id)
    proposal_key = qualified_claim_identity(
        normalized_claim, case_id=case_id, subject_id=persona_id
    )["key"]
    proposal_groups = [
        group
        for group in groups
        if group["kind"] == "claim"
        and (group.get("normalized") or {}).get("key") == proposal_key
    ]
    # A cited native observation is itself version evidence, not merely a URL
    # string in an operator note. Memberships append; originals stay immutable.
    for group in proposal_groups:
        detailed = pipeline.get_group(case_id, persona_id, group["id"], limit=1)
        source = dict(detailed["normalized"])
        source["observation_ids"] = [
            *[item["id"] for item in observations],
            *original_ids,
        ]
        source["operator_evidence_references"] = original_ids
        pipeline.upsert_groups(case_id, persona_id, {"claims": [source]})
    return {
        "request_id": request["id"],
        "observation_id": completed["observations"][0]["id"],
        "normalized_observation_id": observations[0]["id"],
        "observation_ids": [item["id"] for item in completed["observations"]],
        "group_ids": [group["id"] for group in proposal_groups],
        "status": "candidate",
        "auto_included": False,
        "auto_finalized": False,
    }
