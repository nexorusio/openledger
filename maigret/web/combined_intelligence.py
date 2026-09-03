"""Validation and graph projection for governed cross-case AI insights."""

from __future__ import annotations

from copy import deepcopy
import json
from typing import Any, Dict, Iterable
from urllib.parse import urlsplit

RELATIONSHIP_TYPES = {
    "affiliation",
    "identity_overlap",
    "shared_infrastructure",
    "coordinated_activity",
    "temporal_connection",
    "publication_connection",
    "other",
}
ORGANIZATION_RELATIONSHIP_FIELDS = {"company", "company_ownership"}
MAX_PROPOSALS = 100


def bounded_combined_context(
    context: Dict[str, Any], *, maximum_chars: int = 60_000
) -> Dict[str, Any]:
    """Round-robin approved evidence into a deterministic model-input budget."""
    source_cases = list(context.get("source_cases") or [])[:10]
    source_case_ids = [str(item.get("id") or "") for item in source_cases]
    by_case = {case_id: [] for case_id in source_case_ids if case_id}
    for claim in list(context.get("approved_claims") or [])[:500]:
        if not isinstance(claim, dict):
            continue
        case_id = str(claim.get("case_id") or "")
        if case_id in by_case:
            by_case[case_id].append(claim)
    interleaved = []
    while any(by_case.values()):
        for case_id in source_case_ids:
            if by_case.get(case_id):
                interleaved.append(by_case[case_id].pop(0))

    all_entities = {
        str(item.get("reference_id") or ""): dict(item)
        for item in list(context.get("entities") or [])[:1000]
        if isinstance(item, dict) and item.get("reference_id")
    }
    initial_entities = [
        entity
        for entity in all_entities.values()
        if entity.get("entity_type") in {"case", "organization"}
    ][:30]

    bounded = {
        "purpose": _text(context.get("purpose"), 4000),
        "snapshot_sha256": _text(context.get("snapshot_sha256"), 64),
        "source_cases": source_cases,
        "entities": initial_entities,
        "approved_organizations": [
            _organization_evidence(item)
            for item in list(context.get("approved_organizations") or [])[:10]
            if isinstance(item, dict)
        ],
        "approved_claims": [],
        "truncated_claim_count": int(context.get("truncated_claim_count") or 0),
    }
    for claim in interleaved:
        candidate = _claim_evidence(claim)
        candidate["entity_ref"] = _text(claim.get("entity_ref"), 100)
        candidate["display_value"] = _text(candidate.get("display_value"), 1000)
        candidate["sources"] = list(candidate.get("sources") or [])[:3]
        entity = all_entities.get(candidate["entity_ref"])
        added_entity = bool(entity and entity not in bounded["entities"])
        if added_entity:
            bounded["entities"].append(entity)
        bounded["approved_claims"].append(candidate)
        serialized = json.dumps(bounded, ensure_ascii=False, separators=(",", ":"))
        if len(serialized) > maximum_chars:
            bounded["approved_claims"].pop()
            if added_entity:
                bounded["entities"].pop()
            break
    original_count = len(list(context.get("approved_claims") or []))
    bounded["truncated_claim_count"] += max(
        0, original_count - len(bounded["approved_claims"])
    )
    return bounded


def _text(value: Any, maximum: int, *, collapse: bool = False) -> str:
    candidate = str(value or "").strip()
    if collapse:
        candidate = " ".join(candidate.split())
    return candidate[:maximum]


def _strings(value: Any, *, limit: int, maximum: int) -> list[str]:
    if not isinstance(value, list):
        return []
    output = []
    for item in value[:limit]:
        candidate = _text(item, maximum, collapse=True)
        if candidate and candidate not in output:
            output.append(candidate)
    return output


def _public_url(value: Any) -> str | None:
    candidate = _text(value, 2000)
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    return candidate


def _claim_evidence(record: Dict[str, Any]) -> Dict[str, Any]:
    sources = []
    for source in list(record.get("sources") or [])[:10]:
        if not isinstance(source, dict):
            continue
        url = _public_url(source.get("url"))
        sources.append(
            {
                "name": _text(source.get("name"), 300, collapse=True)
                or "Approved source record",
                "url": url,
                "type": _text(source.get("type"), 64, collapse=True),
                "observed_at": _text(source.get("observed_at"), 64),
            }
        )
    return {
        "reference_id": _text(record.get("reference_id"), 100),
        "reference_type": "approved_claim",
        "claim_id": _text(record.get("claim_id"), 36),
        "case_id": _text(record.get("case_id"), 36),
        "case_title": _text(record.get("case_title"), 500, collapse=True),
        "persona_id": _text(record.get("persona_id"), 36),
        "persona_name": _text(record.get("persona_name"), 500, collapse=True),
        "field_name": _text(record.get("field_name"), 64),
        "display_value": _text(record.get("display_value"), 4000),
        "confidence": int(record.get("confidence") or 0),
        "last_seen_at": _text(record.get("last_seen_at"), 64),
        "sources": sources,
    }


def _organization_evidence(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "reference_id": _text(record.get("reference_id"), 100),
        "reference_type": "approved_organization",
        "case_id": _text(record.get("case_id"), 36),
        "case_title": _text(record.get("case_title"), 500, collapse=True),
        "entity_ref": _text(record.get("entity_ref"), 100),
        "label": _text(record.get("label"), 500, collapse=True),
        "source_name": _text(record.get("source_name"), 300, collapse=True),
        "source_url": _public_url(record.get("source_url")),
        "reviewed_by": _text(record.get("reviewed_by"), 200, collapse=True),
        "reviewed_at": _text(record.get("reviewed_at"), 64),
    }


def _web_evidence(index: int, record: Dict[str, Any]) -> Dict[str, Any] | None:
    url = _public_url(record.get("url"))
    if not url:
        return None
    return {
        "reference_id": f"web:{index}",
        "reference_type": "public_web",
        "title": _text(record.get("title"), 300, collapse=True) or urlsplit(url).netloc,
        "url": url,
    }


def normalize_combined_insights(
    raw: Any,
    *,
    context: Dict[str, Any],
    web_sources: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    """Return bounded insights whose references are valid for this snapshot.

    Invalid model proposals are discarded rather than weakened or repaired.
    Every retained proposal is anchored by approved evidence from both cases.
    """
    if not isinstance(raw, dict):
        raise ValueError("Combined insight output must be an object")

    entity_catalogue = {
        _text(item.get("reference_id"), 100): {
            "reference_id": _text(item.get("reference_id"), 100),
            "entity_type": _text(item.get("entity_type"), 32),
            "entity_id": _text(item.get("entity_id"), 100),
            "label": _text(item.get("label"), 500, collapse=True),
            "case_id": _text(item.get("case_id"), 36),
            "case_title": _text(item.get("case_title"), 500, collapse=True),
        }
        for item in list(context.get("entities") or [])[:1000]
        if isinstance(item, dict) and _text(item.get("reference_id"), 100)
    }
    evidence_catalogue: Dict[str, Dict[str, Any]] = {}
    for item in list(context.get("approved_claims") or [])[:500]:
        if not isinstance(item, dict):
            continue
        normalized = _claim_evidence(item)
        if normalized["reference_id"]:
            evidence_catalogue[normalized["reference_id"]] = normalized
    for item in list(context.get("approved_organizations") or [])[:10]:
        if not isinstance(item, dict):
            continue
        normalized = _organization_evidence(item)
        if normalized["reference_id"]:
            evidence_catalogue[normalized["reference_id"]] = normalized
    normalized_web_sources = []
    for index, item in enumerate(list(web_sources or [])[:100], start=1):
        if not isinstance(item, dict):
            continue
        normalized = _web_evidence(index, item)
        if normalized:
            normalized_web_sources.append(normalized)
            evidence_catalogue[normalized["reference_id"]] = normalized

    def resolve_references(value: Any) -> list[Dict[str, Any]]:
        resolved = []
        seen = set()
        if not isinstance(value, list):
            return resolved
        for raw_reference in value[:100]:
            reference_id = _text(raw_reference, 100)
            if reference_id in seen or reference_id not in evidence_catalogue:
                continue
            seen.add(reference_id)
            resolved.append(deepcopy(evidence_catalogue[reference_id]))
        return resolved

    proposals = []
    seen_proposals = set()
    for item in list(raw.get("proposals") or [])[:MAX_PROPOSALS]:
        if not isinstance(item, dict):
            continue
        subject_ref = _text(item.get("subject_ref"), 100)
        object_ref = _text(item.get("object_ref"), 100)
        subject = entity_catalogue.get(subject_ref)
        obj = entity_catalogue.get(object_ref)
        relationship_type = _text(item.get("relationship_type"), 64)
        if (
            not subject
            or not obj
            or subject_ref == object_ref
            or not subject["case_id"]
            or subject["case_id"] == obj["case_id"]
            or relationship_type not in RELATIONSHIP_TYPES
        ):
            continue
        try:
            confidence = int(item.get("confidence"))
        except (TypeError, ValueError):
            continue
        if not 0 <= confidence <= 85:
            continue
        evidence = resolve_references(item.get("evidence_reference_ids"))
        contradictory_evidence = resolve_references(
            item.get("contradictory_reference_ids")
        )
        anchored_cases = {
            reference.get("case_id")
            for reference in evidence
            if reference.get("reference_type")
            in {"approved_claim", "approved_organization"}
        }
        if not {subject["case_id"], obj["case_id"]}.issubset(anchored_cases):
            continue
        if (
            subject["entity_type"] == "organization"
            or obj["entity_type"] == "organization"
        ):
            affiliation_claim_present = any(
                reference.get("reference_type") == "approved_claim"
                and reference.get("field_name") in ORGANIZATION_RELATIONSHIP_FIELDS
                for reference in evidence
            )
            approved_organization_present = any(
                reference.get("reference_type") == "approved_organization"
                for reference in evidence
            )
            if not affiliation_claim_present or not approved_organization_present:
                continue
        title = _text(item.get("title"), 500, collapse=True)
        explanation = _text(item.get("explanation"), 6000)
        if not title or not explanation:
            continue
        fingerprint = (
            relationship_type,
            tuple(sorted((subject_ref, object_ref))),
            title.casefold(),
        )
        if fingerprint in seen_proposals:
            continue
        seen_proposals.add(fingerprint)
        proposals.append(
            {
                "title": title,
                "relationship_type": relationship_type,
                "subject_ref": subject_ref,
                "subject_entity": deepcopy(subject),
                "object_ref": object_ref,
                "object_entity": deepcopy(obj),
                "explanation": explanation,
                "confidence": confidence,
                "evidence": evidence,
                "contradictory_evidence": contradictory_evidence,
                "limitations": _strings(
                    item.get("limitations"), limit=10, maximum=1000
                ),
            }
        )

    contradictions = []
    for item in list(raw.get("contradictions") or [])[:20]:
        if not isinstance(item, dict):
            continue
        summary = _text(item.get("summary"), 2000)
        evidence = resolve_references(item.get("reference_ids"))
        if summary and evidence:
            contradictions.append(
                {
                    "summary": summary,
                    "evidence": evidence,
                }
            )
    key_findings = []
    for item in list(raw.get("key_findings") or [])[:20]:
        if not isinstance(item, dict):
            continue
        summary = _text(item.get("summary"), 2000)
        evidence = resolve_references(item.get("reference_ids"))
        if summary and evidence:
            key_findings.append({"summary": summary, "evidence": evidence})
    return {
        "executive_summary": _text(raw.get("executive_summary"), 8000),
        "key_findings": key_findings,
        "contradictions": contradictions,
        "information_gaps": _strings(
            raw.get("information_gaps"), limit=20, maximum=2000
        ),
        "next_steps": _strings(raw.get("next_steps"), limit=20, maximum=2000),
        "sources": normalized_web_sources,
        "proposals": proposals,
    }


def overlay_relationship_proposals(
    graph: Dict[str, Any], proposals: Iterable[Dict[str, Any]]
) -> Dict[str, Any]:
    """Overlay non-rejected AI hypotheses without mutating the snapshot graph."""
    projected = deepcopy(graph)
    nodes = list(projected.get("nodes") or [])
    edges = list(projected.get("edges") or [])
    node_ids = {str(node.get("id")) for node in nodes if isinstance(node, dict)}
    proposal_counts = {"pending": 0, "approved": 0, "uncertain": 0}
    for proposal in proposals:
        status = str(proposal.get("review_status") or "pending")
        if status == "rejected" or status not in proposal_counts:
            continue
        proposal_counts[status] += 1
        endpoint_ids = []
        for entity in (proposal.get("subject_entity"), proposal.get("object_entity")):
            if not isinstance(entity, dict):
                endpoint_ids = []
                break
            reference_id = str(entity.get("reference_id") or "")
            node_id = reference_id
            endpoint_ids.append(node_id)
            if node_id in node_ids:
                continue
            node_ids.add(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "label": entity.get("label") or reference_id,
                    "kind": "ai_entity",
                    "entity_type": entity.get("entity_type"),
                    "case_id": entity.get("case_id"),
                    "case_title": entity.get("case_title"),
                    "review_status": status,
                    "proposal_id": proposal.get("id"),
                }
            )
        if len(endpoint_ids) != 2:
            continue
        edges.append(
            {
                "id": f"ai-proposal:{proposal.get('id')}",
                "from": endpoint_ids[0],
                "to": endpoint_ids[1],
                "label": str(proposal.get("relationship_type") or "hypothesis").replace(
                    "_", " "
                ),
                "field_name": "ai_hypothesis",
                "confidence": int(proposal.get("confidence") or 0),
                "relationship_rule": "AI-proposed cross-case relationship requiring analyst review",
                "review_status": status,
                "proposal_id": proposal.get("id"),
                "sources": [
                    {
                        "name": source.get("title")
                        or source.get("source_name")
                        or source.get("case_title")
                        or "Evidence",
                        "url": source.get("url") or source.get("source_url"),
                        "type": source.get("reference_type"),
                    }
                    for source in list(proposal.get("evidence") or [])
                ],
            }
        )
    projected["nodes"] = nodes
    projected["edges"] = edges
    stats = dict(projected.get("stats") or {})
    stats["ai_proposal_counts"] = proposal_counts
    stats["ai_proposal_count"] = sum(proposal_counts.values())
    projected["stats"] = stats
    return projected
