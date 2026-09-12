"""Bind P2 assessments to durable groups and actual collection configuration."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from maigret.web.pipeline_assessment import assess_group
from maigret.web.pipeline_probability import (
    abstention,
    digest,
    load_artifact,
    timestamp,
)


def _known(value: Any) -> str | None:
    if not isinstance(value, str) or value.strip().lower() in {
        "",
        "unknown",
        "und",
        "auto",
        "default",
    }:
        return None
    return value.strip()


def _combination(values: Iterable[str]) -> str | None:
    values = sorted(set(values))
    return "+".join(values) if values else None


def configured_artifact(
    environ: Mapping[str, str] | None = None, *, event: str | None = None
):
    """Read an optional externally pinned file; never activate an embedded hash."""
    environ = os.environ if environ is None else environ
    prefix = "OPENLEDGER_PROBABILITY"
    if event in {"account_attribution", "claim_correctness"}:
        event_prefix = prefix + "_" + event.upper()
        if environ.get(event_prefix + "_ARTIFACT") or environ.get(
            event_prefix + "_ARTIFACT_SHA256"
        ):
            prefix = event_prefix
    path = environ.get(prefix + "_ARTIFACT")
    expected = environ.get(prefix + "_ARTIFACT_SHA256")
    if not path and not expected:
        return None, None, None
    if not path or not expected:
        return None, None, "incomplete_probability_configuration"
    try:
        return load_artifact(path), expected, None
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        # User-visible errors must not expose host paths or arbitrary JSON content.
        return None, None, "probability_artifact_unavailable_or_invalid"


def derive_assessment_scope(
    group: Mapping[str, Any],
    observations: Iterable[Mapping[str, Any]],
    *,
    tasks_by_id: Mapping[str, Any] | None = None,
    accounts_by_id: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Use source/plan facts; missing language or versions remain unknown.

    Composite input/platform/language values are sorted actual values, not an
    assumed primary value. Version scope hashes the complete observed collector
    and parser/configuration revision set, so a changed engine invalidates scope.
    """
    observations = list(observations)
    tasks_by_id = tasks_by_id or {}
    accounts_by_id = accounts_by_id or {}
    account = (
        group
        if group.get("kind") == "account"
        else accounts_by_id.get(group.get("account_key"), {})
    )
    platform = _known(account.get("platform"))
    platforms, inputs, languages, revisions = set(), set(), set(), []
    missing_input, missing_revision = not observations, not observations
    missing_language = not observations
    for observation in observations:
        task = tasks_by_id.get(str(observation.get("task_id")), {})
        if task and (
            str(task.get("case_id")) != str(group.get("case_id"))
            or str(task.get("persona_id", task.get("subject_id")))
            != str(group.get("subject_id"))
        ):
            raise ValueError("Assessment task is outside its case and subject")
        spec = task.get("spec") or task
        actual_input = _known(
            (task.get("input") or {}).get("type") or spec.get("input_type")
        )
        if actual_input:
            inputs.add(actual_input)
        else:
            missing_input = True
        payload = observation.get("payload") or {}
        observed_language = _known(
            payload.get("language") or observation.get("language")
        )
        if observed_language:
            languages.add(observed_language)
        else:
            missing_language = True
        observed_platform = _known(
            (observation.get("account") or {}).get("platform")
            or payload.get("platform")
            or task.get("platform")
        )
        if observed_platform:
            platforms.add(observed_platform)
        elif observation.get("source_url"):
            host = urlsplit(observation["source_url"]).hostname
            if host:
                platforms.add("web:" + host.lower())
        revision = {
            "engine": _known(observation.get("engine")),
            "engine_version": _known(observation.get("engine_version")),
            "parser_version": _known(observation.get("parser_version")),
            "configuration_revision": _known(spec.get("source_config_revision")),
        }
        if all(revision.values()):
            revisions.append(revision)
        else:
            missing_revision = True
    # Qualifier language describes this exact claim when source documents did not
    # identify a language. No browser/account locale or Indonesia default is used.
    qualified_language = _known((group.get("qualifiers") or {}).get("language"))
    if qualified_language:
        languages.add(qualified_language)
    unique_revisions = {digest(item): item for item in revisions}
    return {
        "platform": platform or _combination(platforms),
        "input_type": None if missing_input else _combination(inputs),
        "language": qualified_language if missing_language else _combination(languages),
        "claim_family": (
            "account"
            if group.get("kind") == "account"
            else _known(group.get("predicate"))
        ),
        "source_revision": (
            None
            if missing_revision
            else "sources:"
            + digest([unique_revisions[key] for key in sorted(unique_revisions)])
        ),
    }


def assess_consolidated(
    consolidated: Mapping[str, Any],
    observations: Iterable[Mapping[str, Any]],
    *,
    tasks_by_id: Mapping[str, Any] | None = None,
    previous_assessments: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
    as_of: datetime | str | None = None,
) -> dict[str, Any]:
    """Return assessments keyed by the exact canonical source IDs storage expects."""
    by_id = {}
    for observation in observations:
        oid = str(observation.get("id") or "")
        if not oid:
            raise ValueError("Normalized assessment observations require an ID")
        if oid in by_id and by_id[oid] != observation:
            raise ValueError("Conflicting normalized observation ID")
        by_id[oid] = observation
    artifacts = {
        event: configured_artifact(environ, event=event)
        for event in ("account_attribution", "claim_correctness")
    }
    accounts = {item["id"]: item for item in consolidated.get("accounts", [])}
    now = timestamp(as_of) if as_of is not None else datetime.now(timezone.utc)
    if now is None:
        raise ValueError("Assessment time requires timezone")
    assessments = {}
    for kind, collection in (
        ("account", consolidated.get("accounts", [])),
        ("claim", consolidated.get("claims", [])),
    ):
        for source in collection:
            group = dict(source, kind=kind)
            event = "account_attribution" if kind == "account" else "claim_correctness"
            artifact, expected, config_error = artifacts[event]
            ids = group.get("observation_ids")
            if not isinstance(ids, list) or any(
                not isinstance(oid, str) or oid not in by_id for oid in ids
            ):
                raise ValueError(
                    "Consolidated assessment membership is missing normalized source observations"
                )
            members = [by_id[oid] for oid in ids]
            scope = derive_assessment_scope(
                group, members, tasks_by_id=tasks_by_id, accounts_by_id=accounts
            )
            snapshot = assess_group(
                group,
                members,
                as_of=now,
                probability_artifact=artifact,
                artifact_sha256=expected,
                scope=scope,
            )
            snapshot["operating_scope"] = scope
            if config_error:
                snapshot["probability"] = abstention(config_error, snapshot["event"])
            key = source.get("canonical_key") or source.get("id")
            if not isinstance(key, str) or not key:
                raise ValueError(
                    "Consolidated assessment group requires its canonical source ID"
                )
            if key in assessments:
                raise ValueError("Duplicate canonical assessment group key")
            previous = (previous_assessments or {}).get(key)
            if isinstance(previous, Mapping) and all(
                previous.get(field) == snapshot.get(field)
                for field in (
                    "schema_version",
                    "case_id",
                    "subject_id",
                    "group_id",
                    "event",
                    "evidence_digest",
                    "feature_schema",
                    "features",
                    "operating_scope",
                    "probability",
                )
            ):
                # Rechecked validity is unchanged: retain the original snapshot
                # and its honest assessed_at rather than dirtying the workspace
                # merely because a worker replayed an already committed result.
                snapshot = dict(previous)
            assessments[key] = snapshot
    return assessments


def assess_consolidated_groups(store, case_id, persona_id, *, environ=None, as_of=None):
    """Consolidate normalized durable documents and persist correctly bound snapshots.

    Accepts CaseStore or PipelineStore. Does not start collectors, approve facts,
    create versions or change any runtime activation setting.
    """
    from maigret.web.pipeline_store import PipelineStore

    pipeline = store if isinstance(store, PipelineStore) else PipelineStore(store)
    # Every evidence writer takes this subject lock. Rebuilding and advancing the
    # watermark in the same transaction prevents a concurrent append from being
    # marked processed without belonging to this snapshot. GET routes never call
    # this coordinator; committed ingestion and explicit operator POSTs do.
    with pipeline.engine.begin() as connection:
        if pipeline.engine.dialect.name == "sqlite":
            connection.exec_driver_sql("BEGIN IMMEDIATE")
        pipeline._scope(connection, case_id, persona_id, lock=True)
        pipeline._projection_state(connection, case_id, persona_id, create=True)
        return _rebuild_locked(
            pipeline, connection, case_id, persona_id, environ=environ, as_of=as_of
        )


def _rebuild_locked(pipeline, connection, case_id, persona_id, *, environ, as_of):
    from maigret.web.pipeline_consolidation import consolidate_observations
    from maigret.web.pipeline_store import _now
    from sqlalchemy import func, select, update

    observations_table = pipeline._table("observations")
    observations = list(
        connection.scalars(
            select(observations_table.c.payload)
            .where(
                observations_table.c.case_id == case_id,
                observations_table.c.persona_id == persona_id,
            )
            .order_by(observations_table.c.created_at, observations_table.c.id)
        )
    )
    consolidated = consolidate_observations(observations)
    task_table = pipeline._table("tasks")
    tasks = {
        str(row["id"]): dict(row)
        for row in connection.execute(
            select(task_table).where(
                task_table.c.case_id == case_id, task_table.c.persona_id == persona_id
            )
        ).mappings()
    }
    table = pipeline._table("assessments")
    latest = (
        select(
            table.c.document,
            func.row_number()
            .over(
                partition_by=table.c.group_id,
                order_by=(table.c.created_at.desc(), table.c.id.desc()),
            )
            .label("position"),
        )
        .where(table.c.case_id == case_id, table.c.persona_id == persona_id)
        .subquery()
    )
    previous = {}
    for document in connection.scalars(
        select(latest.c.document).where(latest.c.position == 1)
    ):
        group = document.get("consolidated") or {}
        previous[group.get("canonical_key") or group.get("id")] = document.get(
            "assessment"
        )
    assessments = assess_consolidated(
        consolidated,
        observations,
        tasks_by_id=tasks,
        previous_assessments=previous,
        environ=environ,
        as_of=as_of,
    )
    result = pipeline.materialize_groups(
        case_id,
        persona_id,
        consolidated,
        assessments=assessments,
        connection=connection,
    )
    state = pipeline._table("projection_state")
    connection.execute(
        update(state)
        .where(state.c.persona_id == persona_id)
        .values(projected_revision=state.c.evidence_revision, updated_at=_now())
    )
    return result
