"""Operator curation and immutable report snapshots for the complete pipeline."""

from __future__ import annotations

import io
import json
import math
import os
import re
from datetime import datetime, timezone
from functools import wraps
from typing import Any
from urllib.parse import urlsplit

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

PIPELINE_ID = "p2-e2e-v1"


def public_url(value):
    """Only create clickable public-web links, never script/file URLs."""
    value = str(value or '')
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme in {'http', 'https'}
            and parsed.hostname
            and not parsed.username
            and not parsed.password
        ):
            return value
    except ValueError:
        pass
    return ''


def persona_report_filename(projection):
    """Create a useful, filesystem-safe report name for investigators."""
    def slug(value):
        value = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip())
        return value.strip("-")[:72] or "untitled"

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        f"OpenLedger-Investigation-{slug(projection.get('case_title'))}-"
        f"{slug(projection.get('subject_name'))}-{date}.pdf"
    )


def version_projection(version, *, subject_name="", case_title=""):
    """One manifest contract shared by HTML, graph, JSON and PDF."""
    manifest = version['manifest']
    items = manifest.get('items', [])
    publication = version.get('publication_status')
    status = (
        publication if publication in {'withdrawn', 'superseded'} else version['status']
    )
    label = {
        'approved': 'Final Persona',
        'withdrawn': 'Withdrawn Persona',
        'superseded': 'Superseded final Persona',
    }.get(status, 'Reviewed Investigation Snapshot')
    scope = manifest.get('scope', '')
    return {
        'pipeline_id': manifest.get('pipeline_id', PIPELINE_ID),
        'case_id': manifest['case_id'],
        'persona_id': manifest['persona_id'],
        'version_id': version['id'],
        'sequence': version['sequence'],
        'content_hash': version['content_hash'],
        'status': status,
        'qc_status': version['status'],
        'label': label,
        'publication_status': publication,
        'review_needed': version.get('review_needed', False),
        'scope': scope,
        'subject_name': (
            scope.get('subject_name') if isinstance(scope, dict) else ''
        ) or subject_name or manifest['persona_id'],
        'case_title': (
            scope.get('case_title') if isinstance(scope, dict) else ''
        ) or case_title or manifest['case_id'],
        'limitations': manifest.get('limitations', []),
        'items': items,
        'exclusions': manifest.get('exclusions', []),
        'requirements': manifest.get('requirements', []),
        'evidence_set_hash': manifest.get('evidence_set_hash'),
        'created_at': version.get('created_at'),
        'created_by': version.get('created_by', version.get('actor')),
        'qc': version.get('qc', version.get('qc_decisions', [])),
    }


def version_graph(projection):
    """Untruncated factual topology: every item and observation is addressable."""
    from maigret.web.pipeline_evidence import observation_evidence_role

    subject_id = 'subject:' + projection['persona_id']
    nodes = [{'id': subject_id, 'kind': 'subject', 'label': projection['persona_id']}]
    edges, evidence_seen = [], set()
    for item in projection['items']:
        group_id = item['group_id']
        node_id = 'group:' + group_id
        normalized = item.get('normalized', {})
        label = (
            normalized.get('display_value')
            or normalized.get('value')
            or normalized.get('canonical_url')
            or normalized.get('url')
            or normalized.get('handle')
            or normalized
        )
        nodes.append(
            {'id': node_id, 'kind': item['kind'], 'label': label, 'group_id': group_id}
        )
        evidence_ids = [str(entry['id']) for entry in item.get('evidence', [])]
        edges.append(
            {
                'from': subject_id,
                'to': node_id,
                'kind': 'curated_fact',
                'group_id': group_id,
                'version_id': projection['version_id'],
                'evidence_ids': evidence_ids,
            }
        )
        for evidence in item.get('evidence', []):
            evidence_id = str(evidence['id'])
            source_id = 'observation:' + evidence_id
            if evidence_id not in evidence_seen:
                nodes.append(
                    {
                        'id': source_id,
                        'kind': 'observation',
                        'label': evidence.get('engine_id')
                        or evidence.get('engine')
                        or evidence_id,
                        'observation_id': evidence_id,
                        'outcome': evidence.get('outcome') or evidence.get('status'),
                        'source_url': public_url(
                            evidence.get('source_url') or evidence.get('original_url')
                        ),
                    }
                )
                evidence_seen.add(evidence_id)
            edges.append(
                {
                    'from': source_id,
                    'to': node_id,
                    'kind': 'provenance',
                    'group_id': group_id,
                    'version_id': projection['version_id'],
                    'observation_id': evidence_id,
                }
            )
            edges.append(
                {
                    'from': source_id,
                    'to': node_id,
                    'kind': observation_evidence_role(evidence, normalized),
                    'group_id': group_id,
                    'version_id': projection['version_id'],
                    'observation_id': evidence_id,
                }
            )
    return {
        'version_id': projection['version_id'],
        'content_hash': projection['content_hash'],
        'status': projection['status'],
        'label': projection['label'],
        'nodes': nodes,
        'edges': edges,
        'item_count': len(projection['items']),
        'observation_count': len(evidence_seen),
        'truncated': False,
    }


def register_pipeline_routes(
    app,
    get_case_store,
    current_auth_role,
    is_valid_csrf,
    launch_research=None,
    prepare_workspace=None,
    submit_manual_evidence=None,
    launch_approved_discovery=None,
    launch_approved_source_fetch=None,
    geocode_approved_location=None,
    affiliation_public_web_enabled=None,
    google_places_enabled=None,
):
    from maigret.web.pipeline_store import PipelineStore

    bp = Blueprint("pipeline", __name__)
    bp.add_app_template_filter(public_url, "pipeline_public_url")

    def store():
        current = get_case_store()
        if current is None:
            abort(503, description='Persistent case storage is required.')
        return PipelineStore(current)

    def scoped_persona(case_id, persona_id):
        current = get_case_store()
        if current is None:
            abort(503, description="Persistent case storage is required.")
        persona = store().get_subject(case_id, persona_id)
        if not persona or persona.get("case_id") != case_id:
            abort(404, description="Persona does not belong to this case.")
        return persona

    def actor():
        return session.get('username') or 'local-operator'

    def approved_persona(case_id, persona_id):
        from maigret.web.pipeline_store import (
            SHORTLIST_SECTIONS,
            _shortlist_section,
            presentation_predicate,
        )

        # This is the same information architecture as the original Persona
        # workspace: a subject area contains named fields, rather than a flat
        # stream of arbitrary approved rows. Keep the P2 section keys stable so
        # collection coverage and empty-state logic remain unchanged.
        persona_fields = {
            "identity": (
                ("summary", "Summary of the target"),
                ("full_name", "Full name"),
                ("alias", "Known aliases"),
                ("date_of_birth", "Date of birth"),
                ("photograph", "Photograph"),
            ),
            "contact": (
                ("email", "Email address"),
                ("phone", "Phone number"),
                ("address", "Address"),
                ("current_location", "Current location"),
            ),
            "digital": (
                ("social_account", "Social media and public accounts"),
                ("username", "Known usernames"),
                ("platform_identifier", "Stable platform identifiers"),
                ("linked_profile_lead", "Linked profile leads"),
                ("account_registration", "Email registration evidence"),
                ("website", "Website"),
            ),
            "affiliations": (
                ("occupation", "Role or occupation"),
                ("company", "Organization, institution or company"),
                ("organization_location", "Organization location"),
                ("company_ownership", "Ownership or leadership"),
            ),
            "public_exposure": (
                ("news_mention", "News and media coverage"),
                ("event_appearance", "Events and public appearances"),
                ("speaking_engagement", "Speaking engagements"),
                ("interview", "Interviews and podcasts"),
                ("publication", "Publications and authored work"),
                ("award", "Awards and recognition"),
            ),
            "records": (
                ("offshore_database_match", "Offshore Leaks record match"),
                ("financial_profile", "Financial profile"),
                ("vehicle_ownership", "Vehicle ownership"),
                ("criminal_record", "Criminal record"),
            ),
        }
        predicate_aliases = {
            "about": "summary",
            "bio": "summary",
            "biography": "summary",
            "description": "summary",
            "display_name": "full_name",
            "name": "full_name",
            "employer": "company",
            "organization": "company",
            "organisation": "company",
            "affiliation": "company",
            "job_title": "occupation",
            "role": "occupation",
            "location": "current_location",
            "city": "current_location",
            "news": "news_mention",
            "media_mention": "news_mention",
            "news_article": "news_mention",
            "event": "event_appearance",
            "public_event": "event_appearance",
            "public_appearance": "event_appearance",
            "conference_appearance": "event_appearance",
            "panel_appearance": "event_appearance",
            "talk": "speaking_engagement",
            "speaker": "speaking_engagement",
            "podcast_appearance": "interview",
            "authored_article": "publication",
            "article": "publication",
        }

        current = store()
        included_rows = list(
            current.iter_included_groups(
                case_id, persona_id, include_observations=True
            )
        )
        included_account_keys = {
            str(value)
            for row in included_rows
            if row.get("kind") == "account"
            for value in (
                (row.get("normalized") or {}).get("key"),
                (row.get("normalized") or {}).get("canonical_key"),
            )
            if value
        }
        raw_items = []
        for row in included_rows:
            normalized = dict(row.get("normalized") or {})
            predicate = str(
                normalized.get("predicate")
                or normalized.get("field_name")
                or ""
            ).casefold()
            if (
                row.get("kind") == "claim"
                and predicate == "social_account"
                and str(normalized.get("account_key") or "")
                in included_account_keys
            ):
                continue
            value = normalized.get("value")
            label = (
                normalized.get("display_value")
                or (value.get("url") if isinstance(value, dict) else value)
                or normalized.get("canonical_url")
                or normalized.get("url")
                or normalized.get("handle")
                or normalized.get("predicate")
                or "Approved finding"
            )
            item_url = public_url(
                normalized.get("canonical_url")
                or normalized.get("url")
                or (
                    value.get("url")
                    if isinstance(value, dict)
                    else value if isinstance(value, str) else ""
                )
            )
            raw_items.append(
                {
                    "id": row["id"],
                    "kind": row["kind"],
                    "normalized": normalized,
                    "label": str(label),
                    "url": item_url,
                    "section": _shortlist_section(row["kind"], normalized),
                    "decision_actor": row.get("decision_actor"),
                    "decision_reason": row.get("decision_reason"),
                    "observations": list(row.get("observations") or []),
                }
            )

        def field_key(item):
            if item["kind"] == "account":
                return "social_account"
            predicate = presentation_predicate(item["kind"], item["normalized"])
            return predicate_aliases.get(predicate, predicate or "other")

        def field_label(key):
            for fields in persona_fields.values():
                for candidate_key, candidate_label in fields:
                    if candidate_key == key:
                        return candidate_label
            return key.replace("_", " ").title() if key != "other" else "Other approved findings"
        # A group is the review/audit unit, but the Persona is a reader-facing
        # projection. Merge exact field/value duplicates here while retaining
        # every group and source in the record's evidence modal.
        merged = {}
        for item in raw_items:
            key = field_key(item)
            identity = (key, str(item["label"]).strip().casefold())
            record = merged.setdefault(
                identity,
                {
                    **item,
                    "field_key": key,
                    "group_ids": [item["id"]],
                    "evidence": [],
                },
            )
            if item["id"] not in record["group_ids"]:
                record["group_ids"].append(item["id"])
            seen_observations = {entry["id"] for entry in record["evidence"]}
            for observation in item["observations"]:
                observation_id = str(observation.get("id") or "")
                if not observation_id or observation_id in seen_observations:
                    continue
                seen_observations.add(observation_id)
                record["evidence"].append(
                    {
                        "id": observation_id,
                        "label": str(
                            observation.get("engine")
                            or observation.get("engine_id")
                            or "Retained source"
                        ),
                        "url": public_url(
                            observation.get("source_url")
                            or observation.get("original_url")
                        ),
                        "outcome": str(
                            observation.get("outcome")
                            or observation.get("status")
                            or "observed"
                        ).replace("_", " "),
                    }
                )
        items = list(merged.values())

        source_urls = []
        for item in raw_items:
            url = str(item.get("url") or "").strip()
            if url and url.casefold() not in {value.casefold() for value in source_urls}:
                source_urls.append(url)
        latest_source_fetches = {}
        for observation in current.list_observations(
            case_id, persona_id, limit=500
        ):
            if observation.get("engine") != "approved_public_source_fetch":
                continue
            payload = observation.get("payload") or {}
            target_url = str(
                payload.get("subject_value")
                or observation.get("source_url")
                or ""
            ).strip()
            if not target_url:
                continue
            latest_source_fetches[target_url.casefold()] = {
                "status": str(payload.get("status") or observation.get("status") or ""),
                "reason": str(payload.get("reason") or "").strip(),
                "http_status": payload.get("http_status"),
                "claim_candidate_count": (
                    (payload.get("extra") or {}).get("claim_candidate_count", 0)
                ),
            }
        for item in items:
            item["source_fetch"] = latest_source_fetches.get(
                str(item.get("url") or "").casefold()
            )
        photographs = [
            item["url"]
            for item in items
            if str(item["normalized"].get("predicate") or "").casefold()
            == "photograph"
            and item["url"]
        ]

        def approved_hero_value(*predicates):
            wanted = {str(predicate).casefold() for predicate in predicates}
            for item in items:
                predicate = str(
                    item["normalized"].get("predicate") or ""
                ).casefold()
                if predicate in wanted and str(item.get("label") or "").strip():
                    return str(item["label"]).strip()[:700]
            return ""

        hero = {
            "summary": "",
            "location": approved_hero_value("current_location"),
            "affiliation": approved_hero_value(
                "affiliation", "company", "organization", "organisation", "employer"
            ),
            "occupation": approved_hero_value("occupation", "job_title", "role"),
        }
        name = approved_hero_value("full_name", "display_name", "name")
        subject = name or "This person"
        clauses = []
        if hero["occupation"] and hero["affiliation"]:
            clauses.append(
                f"{subject} is {hero['occupation']} at {hero['affiliation']}"
            )
        elif hero["occupation"]:
            clauses.append(f"{subject} is {hero['occupation']}")
        elif hero["affiliation"]:
            clauses.append(f"{subject} is associated with {hero['affiliation']}")
        if hero["location"]:
            clauses.append(f"Based in {hero['location']}")
        raw_summary = approved_hero_value(
            "summary", "biography", "bio", "about", "description"
        )
        if not clauses and raw_summary:
            clauses.append(raw_summary)
        hero["summary"] = ". ".join(clauses).rstrip(".") + "." if clauses else ""
        # A narrative introduces the profile; the evidence register remains
        # the source of record. Do not repeat raw or derived summaries as a
        # separate Persona field.
        items = [item for item in items if field_key(item) != "summary"]
        for index, item in enumerate(items, start=1):
            item["modal_id"] = f"persona-evidence-{index}"
            item["evidence_count"] = len(item.get("evidence") or [])
        map_points = []
        for item in items:
            normalized = item["normalized"]
            predicate = str(normalized.get("predicate") or "").casefold()
            if predicate not in {
                "address",
                "current_location",
                "organization_location",
            }:
                continue
            qualifiers = (
                normalized.get("qualifiers")
                if isinstance(normalized.get("qualifiers"), dict)
                else {}
            )
            try:
                latitude = float(qualifiers.get("latitude"))
                longitude = float(qualifiers.get("longitude"))
            except (TypeError, ValueError):
                continue
            if (
                math.isfinite(latitude)
                and math.isfinite(longitude)
                and -90 <= latitude <= 90
                and -180 <= longitude <= 180
            ):
                map_points.append(
                    {
                        "id": item["id"],
                        "label": item["label"],
                        "latitude": latitude,
                        "longitude": longitude,
                        "predicate": predicate,
                        "precision": qualifiers.get("coordinate_precision")
                        or "place",
                    }
                )
        return {
            "items": items,
            "photograph": photographs[0] if photographs else "",
            "hero": hero,
            "map_points": map_points,
            "source_urls": source_urls,
            "source_fetches": latest_source_fetches,
            "sections": [
                {
                    "key": key,
                    "title": title,
                    "items": [item for item in items if item["section"] == key],
                    "fields": [
                        {
                            "key": field_name,
                            "label": field_label(field_name),
                            "items": [
                                item
                                for item in items
                                if item["section"] == key and field_key(item) == field_name
                            ],
                        }
                        for field_name, _field_label in persona_fields[key]
                        if any(
                            item["section"] == key and field_key(item) == field_name
                            for item in items
                        )
                    ]
                    + [
                        {
                            "key": extra_key,
                            "label": field_label(extra_key),
                            "items": [
                                item
                                for item in items
                                if item["section"] == key and field_key(item) == extra_key
                            ],
                        }
                        for extra_key in sorted(
                            {
                                field_key(item)
                                for item in items
                                if item["section"] == key
                                and field_key(item)
                                not in {name for name, _label in persona_fields[key]}
                            }
                        )
                    ],
                }
                for key, title in SHORTLIST_SECTIONS
            ],
        }

    def approved_relationship_graph(case_id, persona_id, subject):
        """Project every approved P2 group and supporting observation."""
        current = store()
        rows = list(
            current.iter_included_groups(
                case_id, persona_id, include_observations=True
            )
        )
        subject_node = 'persona:' + persona_id
        nodes = [
            {
                'id': subject_node,
                'kind': 'persona',
                'label': subject.get('display_name') or persona_id,
                'persona_id': persona_id,
                'case_id': case_id,
                'case_title': subject.get('case_title') or '',
            }
        ]
        edges = []
        field_counts = {}
        evidence_seen = set()
        for row in rows:
            normalized = dict(row.get('normalized') or {})
            predicate = str(
                normalized.get('predicate')
                or normalized.get('field_name')
                or row.get('kind')
                or 'finding'
            ).casefold()
            value = normalized.get('value')
            label = (
                normalized.get('display_value')
                or (value.get('url') if isinstance(value, dict) else value)
                or normalized.get('canonical_url')
                or normalized.get('url')
                or normalized.get('handle')
                or predicate
            )
            group_node = 'claim:' + row['id']
            field_counts[predicate] = field_counts.get(predicate, 0) + 1
            probability = (
                ((row.get('assessment') or {}).get('probability') or {}).get('value')
            )
            confidence = (
                round(float(probability) * 100)
                if isinstance(probability, (int, float))
                else None
            )
            nodes.append(
                {
                    'id': group_node,
                    'kind': 'claim',
                    'label': str(label),
                    'claim_id': row['id'],
                    'field_name': predicate,
                    'confidence': confidence,
                    'review_status': 'approved',
                    'evidence_count': len(row.get('observations') or []),
                }
            )
            edges.append(
                {
                    'id': 'persona-group:' + row['id'],
                    'from': subject_node,
                    'to': group_node,
                    'label': 'approved ' + predicate.replace('_', ' '),
                    'field_name': predicate,
                }
            )
            for observation in row.get('observations') or []:
                evidence_id = str(observation['id'])
                source_node = 'source:' + evidence_id
                if evidence_id not in evidence_seen:
                    nodes.append(
                        {
                            'id': source_node,
                            'kind': 'source',
                            'label': observation.get('engine') or evidence_id,
                            'url': public_url(observation.get('source_url')),
                            'evidence_type': observation.get('status') or 'observation',
                        }
                    )
                    evidence_seen.add(evidence_id)
                edges.append(
                    {
                        'id': 'group-source:' + row['id'] + ':' + evidence_id,
                        'from': group_node,
                        'to': source_node,
                        'label': 'evidence from',
                        'field_name': predicate,
                    }
                )
        return {
            'mode': 'persona',
            'nodes': nodes,
            'edges': edges,
            'stats': {
                'persona_count': 1,
                'claim_count': len(rows),
                'source_count': len(evidence_seen),
                'pending_count': 0,
                'field_counts': field_counts,
                'truncated_count': 0,
            },
        }

    def payload():
        if request.is_json:
            value = request.get_json()
            if not isinstance(value, dict):
                abort(400, description='A JSON object is required.')
            return value
        return request.form.to_dict()

    def structured(data, key, default):
        value = data.get(key, default)
        if isinstance(value, str):
            if not value.strip():
                return default
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                abort(400, description=f'{key} must contain valid JSON.')
        if not isinstance(value, type(default)):
            abort(400, description=f'{key} has an invalid structure.')
        return value

    def access(*, mutate=False, qc=False):
        def decorate(function):
            @wraps(function)
            def guarded(*args, **kwargs):
                if (
                    app.config.get('AUTH_REQUIRED')
                    and session.get('authenticated') is not True
                ):
                    abort(401, description='Authentication required.')
                role = current_auth_role()
                if role not in {'admin', 'analyst'}:
                    abort(403, description='Operator access required.')
                if qc and role != 'admin':
                    abort(
                        403,
                        description='QC permission is required; an administrator can perform QC.',
                    )
                if mutate:
                    token = request.headers.get(
                        'X-OpenLedger-CSRF'
                    ) or request.form.get('csrf_token')
                    if not is_valid_csrf(token):
                        abort(
                            403,
                            description='Invalid CSRF token. Reload the workspace and retry.',
                        )
                return function(*args, **kwargs)

            return guarded

        return decorate

    def respond(case_id, persona_id, message, result, status=200):
        if request.is_json or request.accept_mimetypes.best == 'application/json':
            return jsonify(result), status
        flash(message, 'success')
        return redirect(
            url_for('pipeline.workspace', case_id=case_id, persona_id=persona_id),
            code=303,
        )

    def review_queue_redirect(case_id, persona_id, message):
        """Return form submissions to the actionable review queue."""
        flash(message, "warning")
        return redirect(
            url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id)
            + "#operator-review",
            code=303,
        )

    def collection_action_block_reason(workspace_data):
        if (
            workspace_data["unreconciled_input_count"]
            or workspace_data["projection"]["pending"]
            or workspace_data["projection"]["legacy_available"]
        ):
            return "Reconcile submitted and retained evidence in Step 1 before collecting more."
        if workspace_data["review_pending_count"]:
            return "Resolve the review queue in Step 1 before collecting more."
        return ""

    @bp.errorhandler(ValueError)
    def invalid_transition(error):
        body = {'error': str(error), 'findings': getattr(error, 'findings', [])}
        if request.is_json or request.path.startswith('/api/'):
            return jsonify(body), 409
        return (
            render_template(
                'pipeline_error.html', error=body, scope=request.view_args or {}
            ),
            409,
        )

    @bp.errorhandler(PermissionError)
    def forbidden_action(error):
        return jsonify({'error': str(error)}), 403

    @bp.errorhandler(LookupError)
    def missing_record(error):
        return jsonify({'error': str(error)}), 404

    @bp.route('/cases/<case_id>/pipeline')
    @access()
    def case_entry(case_id):
        current = get_case_store()
        case = store().get_case_shell(case_id, offset=request.args.get('offset', 0, type=int)) if current else None
        if not case:
            abort(404)
        personas = case.get('personas', [])
        if case['persona_count'] == 1:
            return redirect(
                url_for(
                    'pipeline.workspace', case_id=case_id, persona_id=personas[0]['id']
                )
            )
        return render_template('pipeline_subjects.html', case=case)

    @bp.route("/cases/<case_id>/pipeline/<persona_id>")
    @access()
    def workspace(case_id, persona_id):
        persona = scoped_persona(case_id, persona_id)
        page = max(1, request.args.get("page", 1, type=int))
        history_page = max(1, request.args.get("history_page", 1, type=int))
        review_sort = request.args.get("sort", "default", type=str)
        review_direction = request.args.get("direction", "ascending", type=str)
        review_filter = request.args.get("decision", "all", type=str)
        data = store().get_workspace(
            case_id,
            persona_id,
            limit=25,
            offset=(page - 1) * 25,
            history_offset=(history_page - 1) * 25,
            review_sort=review_sort,
            review_direction=review_direction,
            review_filter=review_filter,
        )
        return render_template(
            "pipeline_workspace.html",
            workspace=data,
            persona=persona,
            page=page,
            page_size=25,
            review_sort=review_sort,
            review_direction=review_direction,
            review_filter=review_filter,
            qc_allowed=current_auth_role() == "admin",
            research_available=launch_research is not None,
            preparation_available=prepare_workspace is not None,
            history_page=history_page,
        )

    @bp.route("/api/cases/<case_id>/pipeline/<persona_id>")
    @access()
    def workspace_api(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        limit = min(100, max(1, request.args.get("limit", 25, type=int)))
        offset = max(0, request.args.get("offset", 0, type=int))
        return jsonify(
            store().get_workspace(
                case_id,
                persona_id,
                limit=limit,
                offset=offset,
                history_offset=max(0, request.args.get("history_offset", 0, type=int)),
            )
        )

    @bp.route("/cases/<case_id>/pipeline/<persona_id>/proceed", methods=["POST"])
    @access(mutate=True)
    def proceed(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        data = store().get_workspace(case_id, persona_id, limit=1)
        if (
            data["unreconciled_input_count"]
            or data["projection"]["pending"]
            or data["projection"]["legacy_available"]
        ):
            flash(
                "Reconcile submitted and retained evidence before proceeding.",
                "warning",
            )
            return redirect(
                url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id),
                code=303,
            )
        if data["review_pending_count"]:
            flash(
                "Resolve every review-queue finding before proceeding to the Persona.",
                "warning",
            )
            return redirect(
                url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id)
                + "#operator-review",
                code=303,
            )
        if not data["approved_count"]:
            flash("Approve at least one finding before proceeding.", "warning")
            return redirect(
                url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id),
                code=303,
            )
        return redirect(
            url_for("pipeline.persona", case_id=case_id, persona_id=persona_id),
            code=303,
        )

    @bp.route("/cases/<case_id>/pipeline/<persona_id>/persona")
    @access()
    def persona(case_id, persona_id):
        subject = scoped_persona(case_id, persona_id)
        workspace_data = store().get_workspace(case_id, persona_id, limit=1)
        projection = approved_persona(case_id, persona_id)
        if not projection["items"]:
            if workspace_data["review_pending_count"]:
                flash(
                    "Approve at least one reviewed finding before opening the Persona.",
                    "warning",
                )
                return redirect(
                    url_for(
                        "pipeline.workspace", case_id=case_id, persona_id=persona_id
                    )
                    + "#operator-review",
                    code=303,
                )
            if (
                workspace_data["unreconciled_input_count"]
                or workspace_data["projection"]["pending"]
                or workspace_data["projection"]["legacy_available"]
            ):
                flash(
                    "Reconcile submitted and retained evidence before opening the Persona.",
                    "warning",
                )
                return redirect(
                    url_for(
                        "pipeline.workspace", case_id=case_id, persona_id=persona_id
                    ),
                    code=303,
                )
            flash("Approve at least one finding before opening the Persona.", "warning")
            return redirect(
                url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id)
            )
        collection_block_reason = collection_action_block_reason(workspace_data)
        return render_template(
            "pipeline_persona.html",
            persona=subject,
            approved=projection,
            workspace=workspace_data,
            collection_available=bool(projection["items"]) and bool(
                launch_approved_discovery or (
                    launch_approved_source_fetch and projection["source_urls"]
                )
            ),
            collection_actions_blocked=bool(collection_block_reason),
            collection_action_block_reason=collection_block_reason,
            affiliation_public_web_available=bool(
                affiliation_public_web_enabled
                and affiliation_public_web_enabled()
            ),
            google_places_available=bool(
                google_places_enabled and google_places_enabled()
            ),
            map_tile_url=os.getenv(
                "OPENLEDGER_MAP_TILE_URL",
                "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            ),
        )

    @bp.route("/cases/<case_id>/pipeline/<persona_id>/persona/relationships")
    @access()
    def relationships(case_id, persona_id):
        subject = scoped_persona(case_id, persona_id)
        case_store = get_case_store()
        cases = case_store.list_cases()
        available_personas = [
            {
                'id': item['id'],
                'display_name': item['display_name'],
                'case_id': case['id'],
                'case_title': case['title'],
            }
            for case in cases
            for item in case.get('personas', [])
        ]
        from maigret.web.persona_intelligence import field_display_label

        return render_template(
            'relationships.html',
            graph=approved_relationship_graph(case_id, persona_id, subject),
            cases=cases,
            mode='persona',
            selected_case_id=case_id,
            available_personas=available_personas,
            selected_persona_id=persona_id,
            field_display_label=field_display_label,
            combined_scope=False,
            approved_only=True,
        )

    @bp.route(
        "/cases/<case_id>/pipeline/<persona_id>/collect-approved-evidence",
        methods=["POST"],
    )
    @access(mutate=True)
    def collect_approved_evidence(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        workspace_data = store().get_workspace(case_id, persona_id, limit=1)
        if (
            workspace_data["unreconciled_input_count"]
            or workspace_data["projection"]["pending"]
            or workspace_data["projection"]["legacy_available"]
        ):
            return review_queue_redirect(
                case_id,
                persona_id,
                "Reconcile submitted and retained evidence before collecting new evidence.",
            )
        if workspace_data["review_pending_count"]:
            return review_queue_redirect(
                case_id,
                persona_id,
                "Resolve the review queue before collecting new evidence.",
            )
        projection = approved_persona(case_id, persona_id)
        if not projection["items"]:
            return review_queue_redirect(
                case_id,
                persona_id,
                "Approve evidence before collecting new evidence.",
            )
        results = []
        if launch_approved_source_fetch is not None and projection["source_urls"]:
            results.append(
                launch_approved_source_fetch(
                    case_id=case_id,
                    persona_id=persona_id,
                    approved_groups=projection["items"],
                    actor=actor(),
                )
            )
        if launch_approved_discovery is not None:
            results.append(
                launch_approved_discovery(
                    case_id=case_id,
                    persona_id=persona_id,
                    approved_groups=projection["items"],
                    actor=actor(),
                )
            )
        if not results:
            return review_queue_redirect(
                case_id,
                persona_id,
                "No approved source or identifier is available to collect from yet.",
            )
        flash(
            "New evidence collection was queued from the approved Persona. "
            "Every result returns to Step 1 for review.",
            "success",
        )
        return redirect(url_for("live_results", job_id=results[-1]["job_id"]), code=303)

    @bp.route(
        "/cases/<case_id>/pipeline/<persona_id>/discover-related",
        methods=["POST"],
    )
    @access(mutate=True)
    def discover_related(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        if launch_approved_discovery is None:
            abort(503, description="Related-evidence discovery is unavailable.")
        workspace_data = store().get_workspace(case_id, persona_id, limit=1)
        if (
            workspace_data["unreconciled_input_count"]
            or workspace_data["projection"]["pending"]
            or workspace_data["projection"]["legacy_available"]
        ):
            return review_queue_redirect(
                case_id,
                persona_id,
                "Reconcile submitted and retained evidence before launching related discovery.",
            )
        if workspace_data["review_pending_count"]:
            return review_queue_redirect(
                case_id,
                persona_id,
                "Resolve the review queue before launching related discovery.",
            )
        projection = approved_persona(case_id, persona_id)
        if not projection["items"]:
            return review_queue_redirect(
                case_id,
                persona_id,
                "Approve evidence before launching related discovery.",
            )
        result = launch_approved_discovery(
            case_id=case_id,
            persona_id=persona_id,
            approved_groups=projection["items"],
            actor=actor(),
        )
        flash(
            "Approved identifiers were queued for an AI-assisted cross-source check. New output will return to the review queue and is not auto-approved.",
            "success",
        )
        return redirect(url_for("live_results", job_id=result["job_id"]), code=303)

    @bp.route(
        "/cases/<case_id>/pipeline/<persona_id>/fetch-approved-sources",
        methods=["POST"],
    )
    @access(mutate=True)
    def fetch_approved_sources(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        if launch_approved_source_fetch is None:
            abort(503, description="Approved source fetching is unavailable.")
        workspace_data = store().get_workspace(case_id, persona_id, limit=1)
        if (
            workspace_data["unreconciled_input_count"]
            or workspace_data["projection"]["pending"]
            or workspace_data["projection"]["legacy_available"]
        ):
            return review_queue_redirect(
                case_id,
                persona_id,
                "Reconcile submitted and retained evidence before fetching approved sources.",
            )
        if workspace_data["review_pending_count"]:
            return review_queue_redirect(
                case_id,
                persona_id,
                "Resolve the review queue before fetching approved sources.",
            )
        projection = approved_persona(case_id, persona_id)
        if not projection["source_urls"]:
            return review_queue_redirect(
                case_id,
                persona_id,
                "Approve at least one public source URL before fetching approved sources.",
            )
        result = launch_approved_source_fetch(
            case_id=case_id,
            persona_id=persona_id,
            approved_groups=projection["items"],
            actor=actor(),
        )
        flash(
            f'Fetching {result["source_count"]} exact approved public source(s). '
            "Public page fields and bounded image-index candidates will return to the review queue; access blocks are recorded explicitly and nothing is auto-approved.",
            "success",
        )
        return redirect(url_for("live_results", job_id=result["job_id"]), code=303)

    @bp.route(
        "/cases/<case_id>/pipeline/<persona_id>/groups/<group_id>/branch-affiliation",
        methods=["POST"],
    )
    @access(mutate=True)
    def branch_affiliation(case_id, persona_id, group_id):
        scoped_persona(case_id, persona_id)
        current = store()
        workspace_data = current.get_workspace(case_id, persona_id, limit=1)
        if (
            workspace_data["unreconciled_input_count"]
            or workspace_data["projection"]["pending"]
            or workspace_data["projection"]["legacy_available"]
            or workspace_data["review_pending_count"]
        ):
            abort(
                409,
                description=(
                    "Complete evidence reconciliation and review before opening "
                    "an affiliation branch."
                ),
            )
        included = {
            row["id"]: row
            for row in current.iter_included_groups(case_id, persona_id)
        }
        row = included.get(group_id)
        if row is None or row.get("kind") != "claim":
            abort(409, description="Only an approved affiliation can open a branch.")
        normalized = dict(row.get("normalized") or {})
        predicate = str(normalized.get("predicate") or "").casefold()
        if predicate not in {"company", "organization", "affiliation"}:
            abort(409, description="Select an approved organization affiliation.")
        organization = normalized.get("value")
        if isinstance(organization, dict):
            organization = organization.get("name") or organization.get("label")
        current = get_case_store()
        try:
            job_id = current.create_affiliation_investigation(
                str(organization or ""),
                source_claim_id=group_id,
                source_claim_field="company",
                target_basis="approved_affiliation_claim",
                enable_public_web_research=bool(
                    affiliation_public_web_enabled
                    and affiliation_public_web_enabled()
                ),
                enable_google_places_search=bool(
                    google_places_enabled and google_places_enabled()
                ),
            )
        except ValueError as error:
            flash(str(error), "warning")
            return redirect(
                url_for("pipeline.persona", case_id=case_id, persona_id=persona_id)
            )
        flash(
            "A separate affiliation investigation was opened. Its findings require their own review.",
            "success",
        )
        return redirect(url_for("live_results", job_id=job_id), code=303)

    @bp.route('/cases/<case_id>/pipeline/<persona_id>/requests/<request_id>/resume', methods=['POST'])
    @access(mutate=True)
    def resume_request(case_id, persona_id, request_id):
        scoped_persona(case_id, persona_id)
        result = store().resume_interrupted_request(
            request_id, actor=actor(), case_id=case_id, persona_id=persona_id,
        )
        return respond(case_id, persona_id,
                       'Collection queued to resume in the same case. Existing evidence and original budgets are retained.',
                       result, 202)

    @bp.route("/cases/<case_id>/pipeline/<persona_id>/prepare", methods=["POST"])
    @access(mutate=True)
    def prepare(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        if prepare_workspace is None:
            abort(503, description="Evidence preparation is unavailable.")
        result = prepare_workspace(case_id=case_id, persona_id=persona_id)
        return respond(
            case_id, persona_id, "Working evidence is prepared for review.", result
        )

    @bp.route("/api/cases/<case_id>/pipeline/<persona_id>/history/<kind>")
    @bp.route("/cases/<case_id>/pipeline/<persona_id>/history/<kind>")
    @access()
    def history(case_id, persona_id, kind):
        persona = scoped_persona(case_id, persona_id)
        data = store().list_history(
            case_id,
            persona_id,
            kind,
            limit=request.args.get("limit", 25, type=int),
            offset=request.args.get("offset", 0, type=int),
            parent_id=request.args.get("parent_id"),
        )
        if request.path.startswith("/api/"):
            return jsonify(data)
        return render_template(
            "pipeline_history.html",
            persona=persona,
            data=data,
            kind=kind,
            parent_id=request.args.get("parent_id"),
        )

    @bp.route("/api/cases/<case_id>/pipeline/<persona_id>/requests/<request_id>")
    @access()
    def request_document(case_id, persona_id, request_id):
        scoped_persona(case_id, persona_id)
        return jsonify(store().get_request_document(case_id, persona_id, request_id))

    @bp.route('/api/cases/<case_id>/pipeline/<persona_id>/groups/<group_id>')
    @bp.route('/cases/<case_id>/pipeline/<persona_id>/groups/<group_id>')
    @access()
    def group_detail(case_id, persona_id, group_id):
        persona = scoped_persona(case_id, persona_id)
        page = max(1, request.args.get('page', 1, type=int))
        group = store().get_group(
            case_id, persona_id, group_id, limit=100, offset=(page - 1) * 100
        )
        if not group:
            abort(404)
        if request.path.startswith('/api/'):
            return jsonify(group)
        accounts = store().list_accounts(case_id, persona_id)
        return render_template(
            'pipeline_group.html',
            persona=persona,
            group=group,
            page=page,
            page_size=100,
            accounts=accounts,
            case_personas=(store().get_case_shell(case_id) or {}).get(
                'personas', []
            ),
        )

    @bp.route(
        '/cases/<case_id>/pipeline/<persona_id>/groups/<group_id>/decision',
        methods=['POST'],
    )
    @access(mutate=True)
    def group_decision(case_id, persona_id, group_id):
        scoped_persona(case_id, persona_id)
        data = payload()
        changes = structured(data, 'corrected_claim', {})
        allowed = {
            'predicate',
            'value',
            'qualifiers',
            'valid_from',
            'valid_to',
            'account_key',
        }
        if set(changes) - allowed:
            abort(
                400,
                description='Correct claim values and qualifiers only; use the grouping controls for identity changes.',
            )
        for field in ('value', 'valid_from', 'valid_to', 'account_key'):
            value = str(data.get('corrected_' + field, '')).strip()
            if value:
                changes[field] = value
        corrected = None
        if changes:
            group = store().get_group(case_id, persona_id, group_id, limit=1)
            if group['kind'] != 'claim':
                abort(400, description='Claim corrections require a claim group.')
            corrected = {**group['normalized'], **changes}
        geocoding_warning = None
        if data.get('decision') == 'include' and geocode_approved_location:
            group = store().get_group(case_id, persona_id, group_id, limit=1)
            candidate = dict(corrected or group.get('normalized') or {})
            predicate = str(
                candidate.get('predicate') or candidate.get('field_name') or ''
            ).casefold()
            qualifiers = (
                dict(candidate.get('qualifiers'))
                if isinstance(candidate.get('qualifiers'), dict)
                else {}
            )
            has_coordinates = all(
                qualifiers.get(key) is not None
                for key in ('latitude', 'longitude')
            )
            if predicate in {
                'address',
                'current_location',
                'organization_location',
            } and not has_coordinates:
                place_value = candidate.get('value')
                if not isinstance(place_value, (dict, list)):
                    try:
                        center = geocode_approved_location(str(place_value or ''))
                    except Exception:
                        center = None
                        geocoding_warning = (
                            'The location was approved, but its map point could not '
                            'be generated. The evidence remains approved.'
                        )
                    if center:
                        qualifiers.update(
                            latitude=center['latitude'],
                            longitude=center['longitude'],
                            coordinate_precision=center.get('precision') or 'place',
                            coordinate_role='approximate_map_center',
                            coordinate_source='approved_place_geocoder',
                        )
                        corrected = dict(candidate, qualifiers=qualifiers)
                    elif geocoding_warning is None:
                        geocoding_warning = (
                            'The location was approved, but no approximate map '
                            'point was found. The evidence remains approved.'
                        )
        dispositions = (
            structured(data, 'evidence_dispositions', [])
            if 'evidence_dispositions' in data
            else None
        )
        if not request.is_json and data.get('evidence_observation_id'):
            dispositions = [
                {
                    'observation_id': data.get('evidence_observation_id'),
                    'disposition': data.get('evidence_disposition'),
                    'reason': data.get('evidence_reason'),
                }
            ]
        result = store().decide(
            case_id,
            persona_id,
            group_id,
            data.get('decision'),
            actor=actor(),
            reason=str(data.get('reason', '')).strip(),
            corrected_claim=corrected,
            evidence_dispositions=dispositions,
        )
        if not (
            request.is_json
            or request.accept_mimetypes.best == 'application/json'
        ):
            flash(
                'Operator decision recorded. Previous evidence and decisions are retained.',
                'success',
            )
            if geocoding_warning:
                flash(geocoding_warning, 'warning')
            return_page = max(
                1, request.form.get('return_page', 1, type=int) or 1
            )
            return_sort = request.form.get('return_sort', 'default', type=str)
            return_direction = request.form.get(
                'return_direction', 'ascending', type=str
            )
            return_filter = request.form.get('return_filter', 'all', type=str)
            return redirect(
                url_for(
                    'pipeline.workspace',
                    case_id=case_id,
                    persona_id=persona_id,
                    page=return_page,
                    sort=return_sort,
                    direction=return_direction,
                    decision=return_filter,
                )
                + '#finding-'
                + group_id,
                code=303,
            )
        return respond(
            case_id,
            persona_id,
            'Operator decision recorded. Previous evidence and decisions are retained.',
            result,
            201,
        )

    @bp.route('/api/cases/<case_id>/pipeline/<persona_id>/observations')
    @bp.route('/cases/<case_id>/pipeline/<persona_id>/observations')
    @access()
    def observations(case_id, persona_id):
        persona = scoped_persona(case_id, persona_id)
        page = max(1, request.args.get('page', 1, type=int))
        rows = store().list_observations(
            case_id, persona_id, limit=101, offset=(page - 1) * 100
        )
        data = {'observations': rows[:100], 'page': page, 'has_next': len(rows) > 100}
        if request.path.startswith('/api/'):
            return jsonify(data)
        return render_template(
            'pipeline_observations.html',
            persona=persona,
            data=data,
            manual_available=submit_manual_evidence is not None,
        )

    @bp.route(
        '/cases/<case_id>/pipeline/<persona_id>/manual-evidence', methods=['POST']
    )
    @access(mutate=True)
    def manual_evidence(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        if submit_manual_evidence is None:
            abort(
                503,
                description='Manual evidence ingestion is unavailable; no evidence was changed.',
            )
        data = payload()
        claim = (
            structured(data, 'claim', {})
            if request.is_json
            else {
                'predicate': str(data.get('predicate', '')).strip(),
                'value': str(data.get('value', '')).strip(),
                'observation_ids': [
                    value.strip()
                    for value in str(data.get('observation_ids', '')).split(',')
                    if value.strip()
                ],
            }
        )
        if not claim.get('predicate') or not claim.get('value'):
            abort(400, description='A claim field and value are required.')
        source_url = public_url(data.get('source_url'))
        if not source_url and not claim.get('observation_ids'):
            abort(
                400,
                description='A public HTTP(S) source URL or existing observation is required.',
            )
        if data.get('source_url') and not source_url:
            abort(400, description='The source URL must use public HTTP(S).')
        reason = str(data.get('reason', '')).strip()
        if not reason:
            abort(400, description='Explain what the cited source supports.')
        result = submit_manual_evidence(
            case_id=case_id,
            persona_id=persona_id,
            actor=actor(),
            claim=claim,
            source_url=source_url,
            reason=reason,
        )
        return respond(
            case_id,
            persona_id,
            'Manual evidence recorded for operator assessment. It has not been included or finalized automatically.',
            result,
            201,
        )

    @bp.route(
        '/cases/<case_id>/pipeline/<persona_id>/groups/<group_id>/revision',
        methods=['POST'],
    )
    @access(mutate=True)
    def revise_group(case_id, persona_id, group_id):
        scoped_persona(case_id, persona_id)
        data = payload()
        observation_ids = (
            structured(data, 'observation_ids', [])
            if request.is_json
            else request.form.getlist('observation_ids')
        )
        target_persona_id = data.get('target_persona_id') or None
        if target_persona_id:
            scoped_persona(case_id, target_persona_id)
        result = store().revise_group(
            case_id,
            persona_id,
            group_id,
            actor=actor(),
            reason=str(data.get('reason', '')).strip(),
            action=data.get('action'),
            observation_ids=observation_ids,
            target_persona_id=target_persona_id,
        )
        return respond(
            case_id,
            persona_id,
            'Grouping revision recorded. Review affected groups before creating a successor version.',
            result,
            201,
        )

    @bp.route('/cases/<case_id>/pipeline/<persona_id>/versions', methods=['POST'])
    @access(mutate=True)
    def create_version(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        data = payload()
        limitations = (
            structured(data, 'limitations', [])
            if request.is_json
            else [
                line.strip()
                for line in str(data.get('limitations', '')).splitlines()
                if line.strip()
            ]
        )
        parent = data.get('parent_version_id') or None
        current = store()
        if parent:
            current.get_version(parent, case_id=case_id, persona_id=persona_id)
        result = current.create_version(
            case_id,
            persona_id,
            actor=actor(),
            scope=(
                data.get('scope', {})
                if isinstance(data.get('scope'), dict)
                else str(data.get('scope', '')).strip()
            ),
            limitations=limitations,
            parent_version_id=parent,
        )
        return respond(
            case_id,
            persona_id,
            'Immutable curated version submitted for QC. It remains a draft until explicit approval.',
            result,
            201,
        )

    @bp.route('/cases/<case_id>/pipeline/<persona_id>/report', methods=['POST'])
    @access(mutate=True)
    def create_report_snapshot(case_id, persona_id):
        """Freeze operator-approved findings and export them without a second QC UI.

        The immutable manifest and per-finding decision trail remain intact; this
        restores the single analyst-review flow used by the Persona outline.
        """
        subject = scoped_persona(case_id, persona_id)
        current = store()
        case = current.get_case_shell(case_id) or {}
        workspace_data = current.get_workspace(case_id, persona_id, limit=1)
        if (
            workspace_data["unreconciled_input_count"]
            or workspace_data["projection"]["pending"]
            or workspace_data["projection"]["legacy_available"]
        ):
            flash(
                "Reconcile submitted and retained evidence before exporting a report.",
                "warning",
            )
            return redirect(
                url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id)
            )
        if workspace_data["review_pending_count"]:
            flash(
                "Resolve every review-queue finding before exporting a report.",
                "warning",
            )
            return redirect(
                url_for("pipeline.workspace", case_id=case_id, persona_id=persona_id)
                + "#operator-review"
            )
        try:
            version = current.create_version(
                case_id,
                persona_id,
                actor=actor(),
                scope={
                    'report_type': 'approved_persona',
                    'decision_model': 'per_finding_operator_review',
                    'subject_name': subject.get('display_name') or persona_id,
                    'case_title': case.get('title') or case_id,
                },
                limitations=[
                    'Only findings explicitly approved by an analyst are included.',
                    'An absent category means no evidence was approved; it is not proof that no such information exists.',
                    'Assets, misconduct and risk are never inferred from a social profile, affiliation or AI summary.',
                ],
            )
        except ValueError as error:
            flash(str(error), 'warning')
            return redirect(url_for('pipeline.workspace', case_id=case_id, persona_id=persona_id))
        return redirect(
            url_for(
                'pipeline.export_pdf',
                case_id=case_id,
                persona_id=persona_id,
                version_id=version['id'],
            )
        )

    def scoped_version(case_id, persona_id, version_id):
        scoped_persona(case_id, persona_id)
        version = store().get_version(
            version_id, case_id=case_id, persona_id=persona_id
        )
        if not version:
            abort(404)
        if version.get('status') == 'approved':
            final = store().get_final_version(case_id, persona_id)
            if final and final['id'] == version_id:
                version.update(
                    {
                        key: final.get(key)
                        for key in (
                            'publication_status',
                            'review_needed',
                            'withdrawal_reason',
                        )
                    }
                )
            elif final:
                version['publication_status'] = 'superseded'
        return version

    @bp.route('/cases/<case_id>/pipeline/<persona_id>/versions/<version_id>')
    @access()
    def version_view(case_id, persona_id, version_id):
        version = scoped_version(case_id, persona_id, version_id)
        return render_template(
            'pipeline_version.html',
            projection=version_projection(version),
            version=version,
            qc_allowed=current_auth_role() == 'admin',
        )

    @bp.route('/api/cases/<case_id>/pipeline/<persona_id>/versions/<version_id>')
    @access()
    def version_api(case_id, persona_id, version_id):
        return jsonify(
            version_projection(scoped_version(case_id, persona_id, version_id))
        )

    @bp.route('/api/cases/<case_id>/pipeline/<persona_id>/final')
    @bp.route('/cases/<case_id>/pipeline/<persona_id>/final')
    @access()
    def final_version(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        version = store().get_final_version(case_id, persona_id)
        if not version:
            abort(
                404,
                description='No QC-approved final Persona exists. Review a draft and perform explicit QC.',
            )
        if request.path.startswith('/api/'):
            return jsonify(version_projection(version))
        return redirect(
            url_for(
                'pipeline.version_view',
                case_id=case_id,
                persona_id=persona_id,
                version_id=version['id'],
            )
        )

    @bp.route('/api/cases/<case_id>/pipeline/<persona_id>/versions/<version_id>/graph')
    @bp.route('/cases/<case_id>/pipeline/<persona_id>/versions/<version_id>/graph')
    @access()
    def graph(case_id, persona_id, version_id):
        projection = version_projection(scoped_version(case_id, persona_id, version_id))
        graph_data = version_graph(projection)
        if request.path.startswith('/api/'):
            return jsonify(graph_data)
        return redirect(
            url_for(
                'pipeline.version_view',
                case_id=case_id,
                persona_id=persona_id,
                version_id=version_id,
            )
        )

    @bp.route('/cases/<case_id>/pipeline/<persona_id>/versions/<version_id>/export.pdf')
    @access()
    def export_pdf(case_id, persona_id, version_id):
        from maigret.web.pipeline_pdf import generate_pipeline_pdf

        subject = scoped_persona(case_id, persona_id)
        case = store().get_case_shell(case_id) or {}
        projection = version_projection(
            scoped_version(case_id, persona_id, version_id),
            subject_name=subject.get('display_name') or persona_id,
            case_title=case.get('title') or case_id,
        )
        response = send_file(
            io.BytesIO(generate_pipeline_pdf(projection)),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=persona_report_filename(projection),
            max_age=0,
        )
        response.headers['X-OpenLedger-Version'] = projection['version_id']
        response.headers['X-OpenLedger-Manifest-Hash'] = projection['content_hash']
        return response

    @bp.route(
        '/cases/<case_id>/pipeline/<persona_id>/versions/<version_id>/qc',
        methods=['POST'],
    )
    @access(mutate=True, qc=True)
    def qc_decision(case_id, persona_id, version_id):
        scoped_version(case_id, persona_id, version_id)
        data = payload()
        findings = structured(data, 'findings', [])
        requirements = structured(data, 'requirements', [])
        if not request.is_json:
            if data.get('finding', '').strip():
                findings.append(
                    {
                        'reason': data['finding'].strip(),
                        'severity': data.get('severity', 'material'),
                    }
                )
            if data.get('question', '').strip():
                requirement = {
                    key: str(data.get(key, '')).strip()
                    for key in ('question', 'reason', 'completion_criteria')
                }
                requirement['inputs'] = structured(data, 'inputs', [])
                for kind in ('username', 'full_name', 'email', 'phone'):
                    value = str(data.get('research_' + kind, '')).strip()
                    if value:
                        requirement['inputs'].append({'type': kind, 'value': value})
                requirement['engines'] = structured(data, 'engines', [])
                requirement['target_group_id'] = data.get('target_group_id') or None
                requirements.append(requirement)
        result = store().qc(
            version_id,
            data.get('decision'),
            actor=actor(),
            permissions={'persona:qc'},
            expected_hash=str(data.get('expected_hash', '')),
            findings=findings,
            requirements=requirements,
            waivers=structured(data, 'waivers', []),
        )
        return respond(
            case_id,
            persona_id,
            'QC decision recorded against this exact version.',
            result,
        )

    def scoped_requirement(case_id, persona_id, requirement_id):
        scoped_persona(case_id, persona_id)
        result = store().get_requirement(
            requirement_id, case_id=case_id, persona_id=persona_id
        )
        if not result:
            abort(404)
        return result

    @bp.route(
        '/cases/<case_id>/pipeline/<persona_id>/requirements/<requirement_id>/launch',
        methods=['POST'],
    )
    @access(mutate=True)
    def launch_requirement(case_id, persona_id, requirement_id):
        requirement = scoped_requirement(case_id, persona_id, requirement_id)
        if requirement.get('status') in {'resolved', 'waived'}:
            abort(409, description='This requirement already has a final disposition.')
        if launch_research is None:
            abort(
                503,
                description='Research execution is unavailable; the requirement remains open.',
            )
        result = launch_research(
            case_id=case_id,
            persona_id=persona_id,
            requirement_id=requirement_id,
            actor=actor(),
        )
        return respond(
            case_id,
            persona_id,
            'Targeted research queued in this case. Review its evidence before resolving the requirement.',
            result,
            202,
        )

    @bp.route(
        '/cases/<case_id>/pipeline/<persona_id>/requirements/<requirement_id>/resolve',
        methods=['POST'],
    )
    @access(mutate=True)
    def resolve_requirement(case_id, persona_id, requirement_id):
        scoped_requirement(case_id, persona_id, requirement_id)
        data = payload()
        evidence_ids = (
            structured(data, 'evidence_ids', [])
            if request.is_json
            else [
                value.strip()
                for value in str(data.get('evidence_ids', '')).split(',')
                if value.strip()
            ]
        )
        result = store().resolve_requirement(
            requirement_id,
            actor=actor(),
            disposition=data.get('disposition'),
            reason=str(data.get('reason', '')).strip(),
            evidence_ids=evidence_ids,
        )
        return respond(
            case_id,
            persona_id,
            'Research disposition recorded. A successor version requires a new QC decision.',
            result,
        )

    @bp.route('/cases/<case_id>/pipeline/<persona_id>/final/withdraw', methods=['POST'])
    @access(mutate=True, qc=True)
    def withdraw_final(case_id, persona_id):
        scoped_persona(case_id, persona_id)
        data = payload()
        if not data.get('expected_version_id') or not data.get('expected_hash'):
            abort(
                400,
                description='Withdrawal requires the reviewed final version and hash.',
            )
        result = store().withdraw_final(
            case_id,
            persona_id,
            actor=actor(),
            permissions={'persona:qc'},
            reason=str(data.get('reason', '')).strip(),
            expected_version_id=data['expected_version_id'],
            expected_hash=data['expected_hash'],
        )
        return respond(
            case_id,
            persona_id,
            'Final designation withdrawn; the version and evidence remain available.',
            result,
        )

    @bp.after_request
    def private_response(response):
        response.headers['Cache-Control'] = 'private, no-store, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['X-OpenLedger-Pipeline'] = PIPELINE_ID
        return response

    app.register_blueprint(bp)
    return bp
