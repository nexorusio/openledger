"""Operator curation and explicit QC for the complete P2 pipeline.

Every presentation endpoint projects the stored version manifest. Neither a legacy
approved claim nor an export request may designate a Persona as final.
"""

from __future__ import annotations

import io
import json
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


def version_projection(version):
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
    }.get(status, 'Draft Persona')
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
        'scope': manifest.get('scope', ''),
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
        data = store().get_workspace(
            case_id,
            persona_id,
            limit=25,
            offset=(page - 1) * 25,
            history_offset=(history_page - 1) * 25,
        )
        return render_template(
            "pipeline_workspace.html",
            workspace=data,
            persona=persona,
            page=page,
            page_size=25,
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

        projection = version_projection(scoped_version(case_id, persona_id, version_id))
        response = send_file(
            io.BytesIO(generate_pipeline_pdf(projection)),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'OpenLedger-Persona-v{projection["sequence"]}-{projection["status"]}.pdf',
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
