"""The real Flask application, worker and durable QC feedback loop.

Only external collectors are substituted with deterministic synthetic adapters.
The HTTP launch, manual-evidence ingestion, storage, consolidation, review, QC,
research scheduling and final projections use their production implementations.
"""

from __future__ import annotations

import importlib
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from werkzeug.datastructures import MultiDict

from maigret.web.case_store import CaseStore
from maigret.web.pipeline_contract import ENGINE_REGISTRY
from maigret.web.pipeline_store import PipelineStore


@pytest.fixture
def application_journey(tmp_path, monkeypatch):
    web = importlib.import_module('maigret.web.app')
    execution = importlib.import_module('maigret.web.pipeline_execution')
    store = CaseStore(
        f'sqlite:///{tmp_path / "application-journey.db"}', create_schema=True
    )
    monkeypatch.setattr(web, 'case_store', store)
    for key, value in {
        'TESTING': True,
        'AUTH_REQUIRED': True,
        'SESSION_COOKIE_SECURE': False,
        'AUTH_FILE': str(tmp_path / 'auth.json'),
        'SETTINGS_FILE': str(tmp_path / 'settings.json'),
        'OPENAI_API_KEY_FILE': str(tmp_path / 'absent-openai-key'),
        'GOOGLE_MAPS_API_KEY_FILE': str(tmp_path / 'absent-maps-key'),
        'REPORTS_FOLDER': str(tmp_path / 'reports'),
        'MAIGRET_DB_FILE': str(Path(web.__file__).parents[2] / 'tests' / 'db.json'),
    }.items():
        monkeypatch.setitem(web.app.config, key, value)
    for name in ('PROFILE_DISCOVERY', 'FOCUSED_DISCOVERY', 'MAIGRET_DISCOVERY'):
        monkeypatch.setenv('OPENLEDGER_' + name + '_ENABLED', 'true')
    monkeypatch.setattr(
        web,
        '_stream_search',
        lambda *args, **kwargs: pytest.fail(
            'The previous pipeline must never execute.'
        ),
    )
    status = {
        'discovery_enabled': True,
        'focused_enabled': True,
        'exhaustive_enabled': True,
        'maigret_enabled': True,
        'scanner_enabled': False,
        'scanner_available': False,
        'enrichment_enabled': True,
        'native_search': {'enabled': False},
        'public_search': {'enabled': True},
        'engines': {
            key: {
                'enabled': key in {'maigret', 'public_exact_match'},
                'reason': 'Deterministic public source fixture.',
            }
            for key in ENGINE_REGISTRY
        },
    }
    monkeypatch.setattr(
        execution, 'source_configuration', lambda *args, **kwargs: status
    )
    web.save_auth_credentials('pipeline-reviewer', 'Synthetic fixture password 2026!')
    client = web.app.test_client()
    assert client.get('/login').status_code == 200
    with client.session_transaction() as current:
        login_csrf = current['csrf_token']
    logged_in = client.post(
        '/login',
        data={
            'csrf_token': login_csrf,
            'username': 'pipeline-reviewer',
            'password': 'Synthetic fixture password 2026!',
        },
    )
    assert logged_in.status_code == 302
    with client.session_transaction() as current:
        assert current['authenticated'] is True and current['role'] == 'admin'
        csrf = current['csrf_token']
    calls = []

    async def public_source(task, context):
        calls.append(
            {
                'job_id': context.job['job_id'],
                'engine_id': task['engine_id'],
                'input_type': task['input_type'],
                'input_value': task['input_value'],
            }
        )
        # This evidence intentionally has no extracted claim or platform account;
        # it must remain inspectable and usable by direct operator assessment.
        context.emit_observations(
            [
                {
                    'source_engine': task['engine_id'],
                    'source_record_id': task['input_type'] + ':' + task['input_value'],
                    'status': 'found',
                    'source_url': 'https://example.test/public-record/'
                    + task['input_type'],
                    'source_name': 'Synthetic public directory',
                    'excerpt': 'Synthetic Person is the named public contact for the reviewed record.',
                }
            ]
        )
        return {'outcome': 'found'}

    adapters = {'maigret_search': public_source, 'public_exact_match': public_source}
    yield {
        'web': web,
        'client': client,
        'store': store,
        'pipeline': PipelineStore(store),
        'execution': execution,
        'csrf': csrf,
        'calls': calls,
        'adapters': adapters,
    }
    store.dispose()


def form(journey, identifiers):
    return MultiDict(
        [
            ('csrf_token', journey['csrf']),
            ('mode', 'focused'),
            ('processing_mode', 'same_subject'),
            ('subject_label', 'Synthetic Person'),
        ]
        + [
            (key, value)
            for kind, identifier in identifiers
            for key, value in [
                ('identifier_type', kind),
                ('identifier_value', identifier),
            ]
        ]
    )


def post(journey, path, body):
    return journey['client'].post(
        path, json=body, headers={'X-OpenLedger-CSRF': journey['csrf']}
    )


def run_queued(journey, job_id):
    job = journey['store'].claim_next('worker:application-journey')
    assert job is not None and job['job_id'] == job_id
    result = journey['execution'].execute_pipeline_job(
        journey['store'], job, adapters=journey['adapters']
    )
    assert result['pipeline_id'] == 'p2-e2e-v1'
    assert result['status'] == 'completed', result
    assert journey['store'].get_job(job_id)['status'] == 'completed'
    return result


@pytest.mark.parametrize(
    'kind,value',
    [
        ('username', 'synthetic.person'),
        ('full_name', 'Synthetic Person'),
        ('email', 'synthetic@example.test'),
        ('phone', '+628123456789'),
    ],
)
def test_real_app_accepts_and_executes_each_input_without_username_dependency(
    application_journey, kind, value
):
    journey = application_journey
    preview = journey['client'].post(
        '/api/investigation-plan', data=form(journey, [(kind, value)])
    )
    assert preview.status_code == 200, preview.get_data(as_text=True)
    plan = preview.get_json()['plan']
    assert plan['pipeline_id'] == 'p2-e2e-v1'
    assert {task['route_state'] for task in plan['tasks']} <= {
        'active',
        'conditional',
        'unavailable',
        'excluded',
    }
    assert any(
        task['route_state'] == 'active' and task['input_type'] == kind
        for task in plan['tasks']
    )
    queued = journey['client'].post('/api/scan', data=form(journey, [(kind, value)]))
    assert queued.status_code == 200, queued.get_data(as_text=True)
    job_id = queued.get_json()['job_id']
    result = run_queued(journey, job_id)
    case = journey['store'].get_case(result['case_id'])
    assert len(case['personas']) == 1
    if kind != 'username':
        assert journey['store'].get_job(job_id)['usernames'] == []
    assert any(call['input_type'] == kind for call in journey['calls'])
    request = journey['pipeline'].requests_for_job(job_id)[0]
    assert request['persona_id'] == case['personas'][0]['id']
    assert (
        journey['pipeline'].get_final_version(case['id'], request['persona_id']) is None
    )


def test_real_app_manual_review_qc_research_worker_and_final_projection(
    application_journey,
):
    journey = application_journey
    client, store, pipeline = journey['client'], journey['store'], journey['pipeline']
    identifiers = [
        ('username', 'synthetic.person'),
        ('full_name', 'Synthetic Person'),
        ('email', 'synthetic@example.test'),
        ('phone', '+628123456789'),
    ]
    preview = client.post('/api/investigation-plan', data=form(journey, identifiers))
    assert preview.status_code == 200
    assert {item['type'] for item in preview.get_json()['plan']['inputs']} >= {
        item[0] for item in identifiers
    }
    queued = client.post('/api/scan', data=form(journey, identifiers))
    assert queued.status_code == 200, queued.get_data(as_text=True)
    initial_job_id = queued.get_json()['job_id']
    result = run_queued(journey, initial_job_id)
    case_id = result['case_id']
    case = store.get_case(case_id)
    assert len(case['personas']) == 1
    persona_id = case['personas'][0]['id']
    base = f'/cases/{case_id}/pipeline/{persona_id}'
    assert client.get('/personas/' + persona_id).headers['Location'].endswith(base)
    legacy_results = client.get('/results/search_' + initial_job_id)
    assert legacy_results.status_code == 302
    assert legacy_results.headers['Location'].endswith('/pipeline')
    legacy_export = client.get('/personas/' + persona_id + '/export.pdf')
    assert (
        legacy_export.status_code == 302 and legacy_export.mimetype != 'application/pdf'
    )
    assert client.get('/api' + base + '/final').status_code == 404
    workspace = client.get(base)
    assert workspace.status_code == 200, workspace.get_data(as_text=True)
    observations_response = client.get('/api' + base + '/observations')
    assert observations_response.status_code == 200
    originals = observations_response.get_json()['observations']
    assert originals
    source = next(
        row for row in originals if row.get('source_url') and row['outcome'] == 'found'
    )
    manual = post(
        journey,
        base + '/manual-evidence',
        {
            'claim': {
                'predicate': 'full_name',
                'value': 'Synthetic Person',
                'observation_ids': [source['id']],
            },
            'source_url': source['source_url'],
            'reason': 'The retained public directory explicitly names the subject.',
        },
    )
    assert manual.status_code == 201, manual.get_data(as_text=True)
    assert (
        manual.get_json()['auto_included'] is False
        and manual.get_json()['auto_finalized'] is False
    )
    groups = client.get('/api' + base).get_json()['groups']
    group = next(
        item
        for item in groups
        if item['kind'] == 'claim' and item['normalized']['predicate'] == 'full_name'
    )
    assert group['latest_decision'] is None
    included = post(
        journey,
        base + f'/groups/{group["id"]}/decision',
        {
            'decision': 'include',
            'reason': 'I reviewed the public name evidence and subject scope.',
        },
    )
    assert included.status_code == 201
    first = post(
        journey,
        base + '/versions',
        {
            'scope': 'Public full-name attribution as of the source observation date.',
            'limitations': ['No residential location claim.'],
        },
    )
    assert first.status_code == 201, first.get_data(as_text=True)
    first = first.get_json()
    assert client.get('/api' + base + '/final').status_code == 404
    draft_pdf = client.get(base + f'/versions/{first["id"]}/export.pdf')
    assert (
        draft_pdf.status_code == 200
        and 'submitted' in draft_pdf.headers['Content-Disposition']
    )
    rejected = post(
        journey,
        base + f'/versions/{first["id"]}/qc',
        {
            'decision': 'changes_required',
            'expected_hash': first['content_hash'],
            'requirements': [
                {
                    'question': 'Does the public email reference support this subject name?',
                    'reason': 'Email linkage requires an additional observed public source.',
                    'completion_criteria': 'Retrieve a public record for the supplied email and cite its observation ID.',
                    'target_group_id': group['id'],
                    'inputs': [{'type': 'email', 'value': 'followup@example.test'}],
                    'engines': ['public_exact_match'],
                    'request_budget': 2,
                }
            ],
        },
    )
    assert rejected.status_code == 200, rejected.get_data(as_text=True)
    requirement = rejected.get_json()['requirements'][0]
    before_ids = {
        row['id'] for row in pipeline.list_observations(case_id, persona_id, limit=500)
    }
    launched = post(journey, base + f'/requirements/{requirement["id"]}/launch', {})
    assert launched.status_code == 202, launched.get_data(as_text=True)
    launch = launched.get_json()
    assert launch['case_id'] == case_id and launch['persona_id'] == persona_id
    followup = pipeline.get_request(
        launch['request_id'], case_id=case_id, persona_id=persona_id
    )
    assert followup['parent_request_id']
    assert (
        launch['request_id']
        in pipeline.get_requirement(requirement['id'])['request_ids']
    )
    assert (
        followup['inputs'][0]['type'] == 'email'
        and followup['inputs'][0]['value'] == 'followup@example.test'
    )
    assert store.get_job(launch['job_id'])['case_id'] == case_id
    run_queued(journey, launch['job_id'])
    assert any(
        call['job_id'] == launch['job_id']
        and call['input_value'] == 'followup@example.test'
        for call in journey['calls']
    )
    after = pipeline.list_observations(case_id, persona_id, limit=500)
    assert before_ids < {row['id'] for row in after}
    assert len(store.get_case(case_id)['personas']) == 1
    assert pipeline.get_requirement(requirement['id'])['status'] == 'open'
    assert pipeline.get_version(first['id'])['manifest'] == first['manifest']
    new_evidence = next(
        row
        for row in after
        if row['request_id'] == launch['request_id']
        and row.get('source_url')
        and row['outcome'] == 'found'
    )
    resolved = post(
        journey,
        base + f'/requirements/{requirement["id"]}/resolve',
        {
            'disposition': 'resolved',
            'reason': 'The specific public email record was retrieved and inspected against the stated criterion.',
            'evidence_ids': [new_evidence['id']],
        },
    )
    assert resolved.status_code == 200, resolved.get_data(as_text=True)
    second = post(
        journey,
        base + '/versions',
        {
            'scope': 'Reviewed public full name with the targeted email research disposition.',
            'parent_version_id': first['id'],
            'limitations': [
                'The email remains research context, not an included ownership claim.'
            ],
        },
    )
    assert second.status_code == 201, second.get_data(as_text=True)
    second = second.get_json()
    approved = post(
        journey,
        base + f'/versions/{second["id"]}/qc',
        {
            'decision': 'approved',
            'expected_hash': second['content_hash'],
            'findings': [
                {
                    'severity': 'informational',
                    'reason': 'Reviewed provenance, subject attribution, limits and resolved research against this exact version.',
                }
            ],
        },
    )
    assert approved.status_code == 200, approved.get_data(as_text=True)
    final = client.get('/api' + base + '/final').get_json()
    assert final['version_id'] == second['id'] and final['label'] == 'Final Persona'
    exact = client.get('/api' + base + f'/versions/{second["id"]}').get_json()
    assert (
        final['items'] == exact['items']
        and final['content_hash'] == exact['content_hash'] == second['content_hash']
    )
    html = client.get(base + f'/versions/{second["id"]}')
    assert html.status_code == 200 and second['content_hash'].encode() in html.data
    assert b'Final Persona' in html.data and b'Synthetic Person' in html.data
    graph = client.get('/api' + base + f'/versions/{second["id"]}/graph').get_json()
    assert (
        graph['version_id'] == final['version_id']
        and graph['content_hash'] == final['content_hash']
    )
    assert {
        edge['group_id'] for edge in graph['edges'] if edge['kind'] == 'curated_fact'
    } == {item['group_id'] for item in final['items']}
    final_evidence_ids = {
        entry['id'] for item in final['items'] for entry in item['evidence']
    }
    assert {
        node['observation_id']
        for node in graph['nodes']
        if node['kind'] == 'observation'
    } == final_evidence_ids
    final_pdf = client.get(
        '/personas/' + persona_id + '/export.pdf', follow_redirects=True
    )
    assert final_pdf.status_code == 200 and final_pdf.data.startswith(b'%PDF-')
    assert final_pdf.headers['X-OpenLedger-Version'] == final['version_id']
    assert final_pdf.headers['X-OpenLedger-Manifest-Hash'] == final['content_hash']
    if shutil.which('pdftotext'):
        text = subprocess.run(
            ['pdftotext', '-', '-'],
            input=final_pdf.data,
            capture_output=True,
            check=True,
        ).stdout.decode()
        assert final['content_hash'] in re.sub(r'\s+', '', text)
        assert all(evidence_id in text for evidence_id in final_evidence_ids)
    assert pipeline.get_version(first['id'])['status'] == 'changes_required'
    assert pipeline.get_version(first['id'])['manifest'] == first['manifest']
