"""Combined-case adapters under the mandatory P2 request/attempt lifecycle.

Snapshot construction and model calls return proposals/evidence. The coordinator
publishes the prepared snapshot or completes the synthesis run only after all
attempt records and consolidated evidence are committed. These adapters do not
call any former pipeline runner, approve a Persona or publish a final version.
"""

from __future__ import annotations

import asyncio
import copy
import hmac
import json
from datetime import datetime, timezone

from sqlalchemy import func, select

from maigret.web.case_store import personas, utcnow
from maigret.web.combined_intelligence import (
    bounded_combined_context,
    normalize_combined_insights,
)
from maigret.web.pipeline_contract import PIPELINE_ID, canonical_digest


def read_final_source_versions(store, pipeline, source_case_ids, *, connection=None):
    """Select all QC-final source versions in one read transaction.

    Referenced version manifests are immutable. A later final-pointer change
    cannot alter the versions captured by this operation.
    """
    states = pipeline._table("persona_state")
    versions = pipeline._table("persona_versions")

    def read(connection):
        rows = connection.execute(
            select(versions, personas.c.display_name.label("persona_name"))
            .join(states, states.c.final_version_id == versions.c.id)
            .join(personas, personas.c.id == versions.c.persona_id)
            .where(
                versions.c.case_id.in_(source_case_ids),
                states.c.final_status == "final",
            )
            .order_by(versions.c.case_id, versions.c.persona_id, versions.c.sequence)
        ).mappings()
        return [dict(row) for row in rows]

    if connection is not None:
        return read(connection)
    with store.engine.connect() as connection:
        return read(connection)


def _capture_source_snapshot(store, pipeline, job_id, source_ids):
    """Capture legacy evidence and final-version pointers in one DB snapshot."""
    dialect = store.engine.dialect.name
    connection = store.engine.connect()
    if dialect != "sqlite":
        connection = connection.execution_options(
            isolation_level=(
                "REPEATABLE READ" if dialect == "postgresql" else "SERIALIZABLE"
            )
        )
    with connection:
        if dialect == "sqlite":
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            generated_at = utcnow()
        else:
            connection.begin()
            if dialect == "postgresql":
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                generated_at = connection.scalar(select(func.statement_timestamp()))
            else:
                generated_at = utcnow()
        try:
            if not isinstance(generated_at, datetime):
                raise RuntimeError("Database did not provide a snapshot timestamp")
            generated_at = (
                generated_at.replace(tzinfo=timezone.utc)
                if generated_at.tzinfo is None
                else generated_at.astimezone(timezone.utc)
            )
            snapshot = store._build_case_fusion_snapshot_with_connection(
                connection, job_id, generated_at
            )
            versions = read_final_source_versions(
                store, pipeline, source_ids, connection=connection
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    return snapshot, versions


def _text(value):
    return (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, sort_keys=True)
    )


def prepare_case_fusion_snapshot(store, job, *, pipeline, version_reader=None):
    """Preserve legacy approved evidence and add exact QC-final version records."""
    if job.get("kind") != "case_fusion":
        raise ValueError("Snapshot adapter requires a combined-case job.")
    spec = (job.get("options") or {}).get("investigation_spec") or {}
    source_ids = list(spec.get("source_case_ids") or [])
    if len(set(source_ids)) < 2 or spec.get("evidence_scope") != "approved_only":
        raise ValueError(
            "Combined evidence requires an explicit approved-only case selection."
        )
    if version_reader is None:
        raw_result, versions = _capture_source_snapshot(
            store, pipeline, job["job_id"], source_ids
        )
    else:
        # Explicit dependency injection for fixtures; production captures both
        # source paths in the same database transaction above.
        raw_result = store.build_case_fusion_snapshot(job["job_id"])
        versions = version_reader(store, pipeline, source_ids)
    result = copy.deepcopy(raw_result)
    analysis = result.pop("analysis_context")
    manifest = result["snapshot"]
    source_titles = {
        row["id"]: row.get("title", "") for row in manifest["source_cases"]
    }
    if set(source_titles) != set(source_ids):
        raise ValueError("Combined snapshot sources differ from the selected cases.")
    manifest["pipeline_id"] = PIPELINE_ID
    manifest["pipeline_versions"] = []
    manifest["pipeline_claims"] = []
    manifest["legacy_evidence_scope"] = "operator_approved_claims_not_profile_qc"
    manifest["pipeline_evidence_scope"] = "exact_qc_approved_persona_versions"
    manifest["capture_policy"] = (
        "single_database_snapshot_of_legacy_evidence_and_immutable_qc_versions"
    )
    graph = result["relationship_graph"]
    graph.setdefault("nodes", [])
    graph.setdefault("edges", [])
    entity_refs = {row.get("reference_id") for row in analysis.get("entities", [])}
    node_ids = {row["id"] for row in graph["nodes"]}
    observation_rows = []
    for version in versions:
        if version["case_id"] not in source_ids:
            raise ValueError("A source Persona version is outside the selected cases.")
        version_id, persona_id, case_id = (
            version["id"],
            version["persona_id"],
            version["case_id"],
        )
        source_manifest = copy.deepcopy(version["manifest"])
        version_hash = version["content_hash"]
        source_digest = canonical_digest(source_manifest)
        if not hmac.compare_digest(source_digest.removeprefix("sha256:"), version_hash):
            raise ValueError(
                "A source Persona manifest does not match its immutable digest."
            )
        manifest["pipeline_versions"].append(
            {
                "version_id": version_id,
                "persona_id": persona_id,
                "case_id": case_id,
                "sequence": version["sequence"],
                "stored_content_hash": version_hash,
                "manifest_digest": source_digest,
                "manifest": source_manifest,
            }
        )
        entity_ref = f"persona:{persona_id}"
        if entity_ref not in entity_refs:
            analysis.setdefault("entities", []).append(
                {
                    "reference_id": entity_ref,
                    "entity_type": "persona",
                    "entity_id": persona_id,
                    "label": version.get("persona_name", "Source Persona"),
                    "case_id": case_id,
                    "case_title": source_titles[case_id],
                }
            )
            entity_refs.add(entity_ref)
        if entity_ref not in node_ids:
            graph["nodes"].append(
                {
                    "id": entity_ref,
                    "kind": "persona",
                    "label": version.get("persona_name", "Source Persona"),
                    "case_id": case_id,
                    "persona_id": persona_id,
                }
            )
            node_ids.add(entity_ref)
        for item in source_manifest.get("items", []):
            normalized = item.get("normalized") or {}
            group_id = item["group_id"]
            evidence = item.get("evidence", [])
            evidence_ids = [row["id"] for row in evidence]
            reference = f"pipeline:{version_id}:{group_id}"
            predicate = normalized.get(
                "predicate",
                normalized.get(
                    "field_name",
                    "social_account" if item.get("kind") == "account" else "claim",
                ),
            )
            value = normalized.get(
                "value",
                normalized.get(
                    "canonical_url",
                    normalized.get("profile_url", normalized.get("handle", "")),
                ),
            )
            if not value and item.get("kind") == "account":
                value = {
                    key: normalized.get(key)
                    for key in ("platform", "username", "account_id")
                    if normalized.get(key)
                }
            sources = [
                {
                    "name": row.get("engine", "Retained source evidence"),
                    "url": row.get("source_url"),
                    "type": "qc_version_evidence",
                    "observation_id": row["id"],
                }
                for row in evidence
            ]
            provenance = {
                "case_id": case_id,
                "persona_id": persona_id,
                "version_id": version_id,
                "group_id": group_id,
                "evidence_ids": evidence_ids,
                "operator_decision_id": (item.get("decision") or {}).get("id"),
            }
            manifest["pipeline_claims"].append(
                {
                    "reference_id": reference,
                    "kind": item.get("kind"),
                    "normalized": normalized,
                    **provenance,
                }
            )
            analysis.setdefault("approved_claims", []).append(
                {
                    "reference_id": reference,
                    "claim_id": group_id,
                    "entity_ref": entity_ref,
                    "persona_name": version.get("persona_name", "Source Persona"),
                    "case_title": source_titles[case_id],
                    "field_name": predicate,
                    "display_value": _text(value),
                    "sources": sources,
                    "confidence": 0,
                    "probability": None,
                    "assessment_label": "QC-approved source version; no inferred probability",
                    **provenance,
                }
            )
            node_id = "version-claim:" + version_id + ":" + group_id
            graph["nodes"].append(
                {
                    "id": node_id,
                    "kind": "attribute",
                    "label": _text(value),
                    "field_name": predicate,
                    "normalized": normalized,
                    **provenance,
                }
            )
            graph["edges"].append(
                {
                    "id": "source-version:" + version_id + ":" + group_id,
                    "from": entity_ref,
                    "to": node_id,
                    "kind": "approved_claim",
                    "label": predicate,
                    "sources": sources,
                    "identity_scope": "source_version_fact",
                    **provenance,
                }
            )
            observation_rows.append(
                {
                    "source_engine": "case_fusion_snapshot",
                    "source_record_id": reference,
                    "status": "candidate",
                    "source_url": next(
                        (row["url"] for row in sources if row.get("url")), None
                    ),
                    "source_name": "QC-approved source Persona version",
                    "payload": {
                        "source_version_provenance": provenance,
                        "source_item": item,
                        "attribution": "evidence_reuse_does_not_merge_source_personas",
                    },
                    "source_dependence": "copied_source_evidence",
                    "independence": "derivative",
                    "origin_family_id": canonical_digest(
                        {"source_evidence_ids": sorted(evidence_ids)}
                    ),
                }
            )
    # Include full factual membership in the hash, excluding only its timestamp
    # and previous digest. Model context can be bounded without truncating it.
    material = {
        key: value
        for key, value in manifest.items()
        if key not in {"sha256", "generated_at"}
    }
    digest = canonical_digest(material).removeprefix("sha256:")
    manifest["sha256"] = digest
    analysis["snapshot_sha256"] = digest
    graph.setdefault("stats", {}).update(
        pipeline_claim_count=len(manifest["pipeline_claims"]),
        pipeline_version_count=len(versions),
        connection_count=len(graph["edges"]),
    )
    result.update(
        pipeline_id=PIPELINE_ID,
        pipeline_claim_count=len(manifest["pipeline_claims"]),
        pipeline_version_count=len(versions),
        connection_count=len(graph["edges"]),
    )
    return {
        "snapshot_result": result,
        "analysis_context": analysis,
        "observations": observation_rows,
    }


async def collect_case_fusion_snapshot(task, context):
    if context.cancelled():
        return {"outcome": "cancelled"}
    prepared = prepare_case_fusion_snapshot(
        context.store, context.job, pipeline=context.pipeline
    )
    # Each source claim commits separately via the attempt sink, so evidence
    # remains inspectable even if publication or a subsequent source fails.
    for row in prepared["observations"]:
        if context.cancelled():
            return {"outcome": "cancelled"}
        context.emit_observations([row])
    if context.cancelled():
        return {"outcome": "cancelled"}
    result = prepared["snapshot_result"]
    publication = {
        "kind": "case_fusion",
        "snapshot_result": result,
        "analysis_context": prepared["analysis_context"],
    }
    marker = {
        "source_engine": "case_fusion_snapshot",
        "source_record_id": "snapshot-publication",
        "status": "candidate",
        "source_name": "Prepared combined-case snapshot",
        "payload": {
            "publication": publication,
            "publication_digest": canonical_digest(publication),
        },
        "source_dependence": "copied_source_evidence",
        "independence": "derivative",
    }
    context.emit_observations([marker])
    context.pending_publication = publication
    return {"outcome": "candidate", "snapshot_sha256": result["snapshot"]["sha256"]}


async def collect_case_fusion_synthesis(
    task, context, *, app_module=None, research_call=None, insights_call=None
):
    """Use existing async model primitives; leave finalization to coordinator."""
    if app_module is None:
        import importlib

        app_module = importlib.import_module("maigret.web.app")
    if research_call is None or insights_call is None:
        from maigret.ai import (
            get_case_chat_response,
            get_combined_investigation_insights,
        )

        research_call = research_call or get_case_chat_response
        insights_call = insights_call or get_combined_investigation_insights
    extra = context.context
    snapshot_id, digest = extra.get("snapshot_job_id"), str(
        extra.get("snapshot_sha256") or ""
    )
    snapshot = context.store.get_job(snapshot_id)
    if (
        not extra.get("validated_snapshot_reference")
        or not snapshot
        or snapshot.get("kind") != "case_fusion"
        or snapshot.get("status") != "completed"
        or snapshot.get("case_id") != context.job["case_id"]
        or not hmac.compare_digest(
            str((snapshot.get("snapshot") or {}).get("sha256") or ""), digest
        )
    ):
        raise ValueError(
            "Combined synthesis requires its exact published source snapshot."
        )
    api_key = app_module.get_openai_api_key()
    if not api_key:
        return {
            "outcome": "not_executed",
            "diagnostic": "The protected AI connection is not configured.",
        }
    settings = app_module.load_settings()
    model = settings.get("openai_model") or app_module.DEFAULT_SETTINGS["openai_model"]
    web_search_enabled = bool(settings.get("ai_web_enrichment", True))
    bounded = bounded_combined_context(extra["analysis_context"])
    for claim in bounded.get("approved_claims", []):
        if str(claim.get("reference_id", "")).startswith("pipeline:"):
            claim.pop("confidence", None)
            claim.update(
                probability=None, evidence_status="QC-approved source Persona version"
            )
    run_id = context.store.start_combined_analysis_run(
        snapshot_id, digest, model=model, web_search_enabled=web_search_enabled
    )
    if context.cancelled():
        context.store.stop_combined_analysis_run(
            run_id, status="cancelled", error="Collection stopped before synthesis."
        )
        return {"outcome": "cancelled"}
    try:
        research = await research_call(
            api_key=api_key,
            case_context=bounded,
            conversation=[],
            model=model,
            user_message=(
                "Compare the approved source evidence across the selected cases. Explain supported relationship hypotheses, "
                "contradictions and specific missing research. Cite every source. Treat shared attributes as hypotheses; "
                "do not infer common identity or wrongdoing. Do not use private or residential details as search terms."
            ),
            web_search_enabled=web_search_enabled,
            timeout_seconds=task["timeout_seconds"],
            **app_module.ai_endpoint_options(),
        )
        context.emit_observations(
            [
                {
                    "source_engine": "case_fusion_synthesis",
                    "source_record_id": "research:" + run_id,
                    "status": "candidate",
                    "source_name": "Combined-case cited assessment",
                    "payload": {
                        "snapshot_sha256": digest,
                        "analysis_run_id": run_id,
                        "model": model,
                        "research": research,
                        "source_version_refs": list(
                            (snapshot.get("snapshot") or {}).get(
                                "pipeline_versions", []
                            )
                        ),
                        "truncated_claim_count": bounded.get(
                            "truncated_claim_count", 0
                        ),
                    },
                    "source_dependence": "model_summary",
                    "independence": "derivative",
                }
            ]
        )
        if context.cancelled():
            raise asyncio.CancelledError()
        raw = await insights_call(
            api_key=api_key,
            case_context=bounded,
            research_answer=research.get("analysis", ""),
            sources=research.get("sources", []),
            model=model,
            timeout_seconds=task["timeout_seconds"],
            **app_module.ai_endpoint_options(),
        )
        insights = normalize_combined_insights(
            raw, context=bounded, web_sources=research.get("sources", [])
        )
        publication = {
            "kind": "case_fusion_ai",
            "run_id": run_id,
            "insights": insights,
            "snapshot_job_id": snapshot_id,
            "snapshot_sha256": digest,
            "model": model,
            "web_search_enabled": web_search_enabled,
            "web_search_completed": bool(research.get("web_search_completed")),
            "truncated_claim_count": bounded.get("truncated_claim_count", 0),
        }
        context.emit_observations(
            [
                {
                    "source_engine": "case_fusion_synthesis",
                    "source_record_id": "synthesis-publication",
                    "status": "candidate",
                    "source_name": "Pending combined-case proposals",
                    "payload": {
                        "publication": publication,
                        "publication_digest": canonical_digest(publication),
                    },
                    "source_dependence": "model_summary",
                    "independence": "derivative",
                }
            ]
        )
        if context.cancelled():
            raise asyncio.CancelledError()
        context.pending_publication = publication
        return {
            "outcome": "candidate",
            "analysis_run_id": run_id,
            "proposal_count": len(insights["proposals"]),
        }
    except asyncio.CancelledError:
        context.store.stop_combined_analysis_run(
            run_id,
            status="cancelled",
            error="Synthesis interrupted; committed evidence retained.",
        )
        raise
    except Exception:
        context.store.stop_combined_analysis_run(
            run_id,
            status="failed",
            error="Synthesis failed; inspect the attempt diagnostic.",
        )
        raise


def recover_pending_publication(
    observations, *, kind, request_id, completed_attempt_ids
):
    """Recover exact prepared output from only this request's completed attempts.

    The coordinator supplies completed attempt IDs after task/group persistence.
    A marker emitted by an interrupted, cancelled or failed attempt is not enough
    to publish, and evidence from an older request can never be replayed here.
    """
    markers = {
        "case_fusion": ("case_fusion_snapshot", "snapshot-publication"),
        "case_fusion_ai": ("case_fusion_synthesis", "synthesis-publication"),
    }
    if kind not in markers or not request_id:
        raise ValueError("A scoped combined publication kind and request are required.")
    allowed_attempts = set(completed_attempt_ids)
    expected_engine, expected_record = markers[kind]
    recovered = None
    for observation in observations:
        if (
            observation.get("request_id") != request_id
            or observation.get("attempt_id") not in allowed_attempts
        ):
            continue
        raw = observation.get("payload") or {}
        if (
            observation.get("engine") != expected_engine
            or raw.get("source_record_id") != expected_record
        ):
            continue
        payload = raw.get("payload") or {}
        publication = payload.get("publication")
        if not isinstance(publication, dict) or publication.get("kind") != kind:
            raise ValueError("The prepared publication has an invalid kind.")
        if not hmac.compare_digest(
            str(payload.get("publication_digest") or ""), canonical_digest(publication)
        ):
            raise ValueError(
                "The prepared publication no longer matches its retained digest."
            )
        if recovered is not None and recovered != publication:
            raise ValueError(
                "Completed attempts contain different prepared publications."
            )
        recovered = copy.deepcopy(publication)
    return recovered
