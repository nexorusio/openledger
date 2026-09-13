"""HTTP-level P2 operator → QC → research → final journey using real storage."""

from pathlib import Path

import pytest
from flask import Flask, session

from maigret.web.case_store import CaseStore
from maigret.web.pipeline_store import PipelineStore
from maigret.web.pipeline_routes import (
    register_pipeline_routes,
    version_graph,
    version_projection,
    public_url,
)


@pytest.fixture
def journey(tmp_path):
    case_store = CaseStore(f'sqlite:///{tmp_path / "pipeline.db"}', create_schema=True)
    pipeline = PipelineStore(case_store)
    job_id = case_store.create_investigation(["synthetic.person"], {})
    case_store.claim_next("fixture-worker")
    case_id = case_store.get_job(job_id)["case_id"]
    persona_id = case_store.get_case(case_id)["personas"][0]["id"]
    inputs = [{"kind": "username", "value": "synthetic.person"}]
    plan = {
        "pipeline_id": "p2-e2e-v1",
        "tasks": [{"engine": "fixture", "availability": "active", "input": inputs[0]}],
    }
    query = pipeline.create_request(
        case_id, persona_id, inputs, plan, actor="analyst", job_id=job_id
    )
    attempt = pipeline.start_attempt(query["tasks"][0]["id"], "fixture-worker")
    observations = pipeline.record_observations(
        attempt["id"],
        [
            {
                "id": "obs-fixture",
                "outcome": "found",
                "source_url": "https://example.test/public-profile",
                "value": "Synthetic Person",
                "origin_family": "public-profile",
            }
        ],
        outcome="found",
        worker_id="fixture-worker",
    )
    # record_observations returns the observation list / result according to durable API.
    retained = pipeline.list_observations(case_id, persona_id)
    rows = (
        retained
        if isinstance(retained, list)
        else retained.get("observations", retained.get("items", []))
    )
    observation_id = rows[0]["id"]
    groups = pipeline.upsert_groups(
        case_id,
        persona_id,
        {
            "claims": [
                {
                    "canonical_key": "synthetic-full-name",
                    "normalized": {
                        "predicate": "full_name",
                        "value": "Synthetic Person",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [observation_id],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(case_id, persona_id),
    )
    group_id = groups[0]["id"]
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parents[1] / "maigret/web/templates"),
        static_folder=str(Path(__file__).parents[1] / "maigret/web/static"),
    )
    app.config.update(TESTING=True, SECRET_KEY="test-only", AUTH_REQUIRED=True)
    for endpoint in (
        "index",
        "history",
        "cases_workspace",
        "combine_cases_workspace",
        "relationships_workspace",
        "configure_persona_investigation",
        "settings_update",
        "security_settings",
        "logout",
    ):
        app.add_url_rule("/stub/" + endpoint, endpoint, lambda: "stub")
    app.add_url_rule("/cases/<case_id>", "case_workspace", lambda case_id: case_id)
    app.add_url_rule(
        "/personas/<persona_id>", "persona_workspace", lambda persona_id: persona_id
    )
    app.add_url_rule("/live/<job_id>", "live_results", lambda job_id: job_id)
    app.context_processor(
        lambda: {
            "csrf_token": session.get("csrf_token"),
            "current_user": session.get("username"),
            "current_role": session.get("role"),
        }
    )
    launches = []
    discovery_launches = []

    def launch_research(**kwargs):
        launches.append(kwargs)
        return {
            'job_id': 'follow-up-job',
            'case_id': kwargs['case_id'],
            'persona_id': kwargs['persona_id'],
        }

    def launch_approved_discovery(**kwargs):
        discovery_launches.append(kwargs)
        return {
            "job_id": "approved-discovery-job",
            "case_id": kwargs["case_id"],
            "persona_id": kwargs["persona_id"],
        }

    register_pipeline_routes(
        app,
        lambda: case_store,
        lambda: session.get("role", ""),
        lambda token: token == session.get("csrf_token") == "test-csrf",
        launch_research=launch_research,
        launch_approved_discovery=launch_approved_discovery,
    )
    client = app.test_client()
    with client.session_transaction() as current:
        current.update(
            authenticated=True,
            username="human-reviewer",
            role="admin",
            csrf_token="test-csrf",
        )
    result = dict(
        client=client,
        app=app,
        store=case_store,
        pipeline=pipeline,
        case_id=case_id,
        persona_id=persona_id,
        group_id=group_id,
        observation_id=observation_id,
        launches=launches,
        discovery_launches=discovery_launches,
    )
    yield result
    case_store.dispose()


def base(journey):
    return f'/cases/{journey["case_id"]}/pipeline/{journey["persona_id"]}'


def post(journey, path, body):
    return journey['client'].post(
        base(journey) + path, json=body, headers={'X-OpenLedger-CSRF': 'test-csrf'}
    )


def curate(journey):
    result = post(
        journey,
        '/groups/' + journey['group_id'] + '/decision',
        {'decision': 'include', 'reason': 'Read the independent public record.'},
    )
    assert result.status_code == 201, result.get_data(as_text=True)
    result = post(
        journey,
        '/versions',
        {
            'scope': 'Establish the public full name at the reference date.',
            'limitations': ['Public sources only.'],
        },
    )
    assert result.status_code == 201, result.get_data(as_text=True)
    return result.get_json()


def test_ranked_review_reject_research_resolve_approve_same_case(journey):
    client = journey['client']
    workspace = client.get(base(journey))
    assert workspace.status_code == 200
    assert b'Step 1 confirms digital presence' in workspace.data
    assert b'Decision note (optional)' in workspace.data
    assert b'>Reject<' in workspace.data
    assert b'Record decision' not in workspace.data
    assert client.get(base(journey) + '/final').status_code == 404
    version = curate(journey)
    assert client.get(base(journey) + '/versions/' + version['id']).status_code == 200
    assert version['status'] == 'submitted'
    assert client.get(base(journey) + '/final').status_code == 404
    rejected = post(
        journey,
        f'/versions/{version["id"]}/qc',
        {
            'decision': 'changes_required',
            'expected_hash': version['content_hash'],
            'requirements': [
                {
                    'question': 'Does another public source link this full name to the same subject?',
                    'reason': 'Independent identity evidence is needed.',
                    'completion_criteria': 'Cite a public source connecting the name and subject.',
                    'inputs': [{'kind': 'full_name', 'value': 'Synthetic Person'}],
                }
            ],
        },
    )
    assert rejected.status_code == 200, rejected.get_data(as_text=True)
    requirement = rejected.get_json()['requirements'][0]
    # Requirements remain auditable and launchable, but are not part of the
    # streamlined per-finding assessment workspace.
    assert client.get(base(journey)).status_code == 200
    launched = post(journey, f'/requirements/{requirement["id"]}/launch', {})
    assert launched.status_code == 202
    assert journey['launches'][0]['case_id'] == journey['case_id']
    assert journey['launches'][0]['persona_id'] == journey['persona_id']
    assert journey['pipeline'].get_requirement(requirement['id'])['status'] == 'open'
    insufficient = post(
        journey,
        f'/requirements/{requirement["id"]}/resolve',
        {'disposition': 'resolved', 'reason': 'Job completed.', 'evidence_ids': []},
    )
    assert insufficient.status_code == 409
    resolved = post(
        journey,
        f'/requirements/{requirement["id"]}/resolve',
        {
            'disposition': 'resolved',
            'reason': 'The retained public observation directly connects the name.',
            'evidence_ids': [journey['observation_id']],
        },
    )
    assert resolved.status_code == 200
    successor = post(
        journey,
        '/versions',
        {
            'scope': 'Identity linkage after research.',
            'parent_version_id': version['id'],
        },
    ).get_json()
    approved = post(
        journey,
        f'/versions/{successor["id"]}/qc',
        {
            'decision': 'approved',
            'expected_hash': successor['content_hash'],
            'findings': [
                {
                    'severity': 'informational',
                    'reason': 'Checked retained provenance and research criteria.',
                }
            ],
        },
    )
    assert approved.status_code == 200, approved.get_data(as_text=True)
    final = client.get('/api' + base(journey) + '/final').get_json()
    assert final['version_id'] == successor['id'] and final['label'] == 'Final Persona'
    assert (
        final['case_id'] == journey['case_id']
        and final['persona_id'] == journey['persona_id']
    )
    assert (
        journey['pipeline'].get_version(version['id'])['status'] == 'changes_required'
    )
    graph = client.get(
        '/api' + base(journey) + f'/versions/{successor["id"]}/graph'
    ).get_json()
    assert (
        graph['version_id'] == final['version_id']
        and graph['content_hash'] == final['content_hash']
    )
    assert graph['item_count'] == len(final['items']) and graph['truncated'] is False
    assert journey['observation_id'] in {
        node.get('observation_id') for node in graph['nodes']
    }


def test_review_proceed_persona_and_approved_discovery_are_an_explicit_wizard(
    journey,
):
    client = journey["client"]
    blocked = client.post(
        base(journey) + "/proceed",
        data={"csrf_token": "test-csrf"},
        follow_redirects=False,
    )
    assert blocked.status_code == 303
    assert blocked.location.endswith(base(journey) + "#operator-review")
    blocked_persona = client.get(base(journey) + "/persona")
    assert blocked_persona.status_code == 303
    assert blocked_persona.location.endswith(base(journey) + "#operator-review")
    blocked_discovery = client.post(
        base(journey) + "/discover-related",
        data={"csrf_token": "test-csrf"},
    )
    assert blocked_discovery.status_code == 409

    decision = post(
        journey,
        "/groups/" + journey["group_id"] + "/decision",
        {"decision": "include", "reason": "Verified against retained evidence."},
    )
    assert decision.status_code == 201
    proceeded = client.post(
        base(journey) + "/proceed",
        data={"csrf_token": "test-csrf"},
        follow_redirects=False,
    )
    assert proceeded.status_code == 303
    assert proceeded.location.endswith(base(journey) + "/persona")

    persona = client.get(proceeded.location)
    assert persona.status_code == 200
    assert b"Approved Persona" in persona.data
    assert b"Synthetic Person" in persona.data
    assert b"Edit approvals" in persona.data
    assert b"AI cross-check &amp; discover" in persona.data

    discovery = client.post(
        base(journey) + "/discover-related",
        data={"csrf_token": "test-csrf"},
        follow_redirects=False,
    )
    assert discovery.status_code == 303
    assert discovery.location.endswith("/live/approved-discovery-job")
    assert journey["discovery_launches"][0]["approved_groups"][0]["label"] == (
        "Synthetic Person"
    )


def test_approved_discovery_reuses_exact_url_without_generated_aliases(monkeypatch):
    import maigret.web.app as web_app

    class Store:
        queued = None

        def get_persona(self, persona_id):
            return {"id": persona_id, "display_name": "Jati Pratomo"}

        def repeat_persona_investigation(self, persona_id, usernames, options):
            self.queued = (persona_id, usernames, options)
            return "cross-check-job"

    store = Store()
    monkeypatch.setattr(web_app, "case_store", store)
    monkeypatch.setattr(web_app, "resolve_profile_url_identifiers", lambda _url: {})
    monkeypatch.setattr(
        web_app,
        "parse_search_options",
        lambda _form, plan: {"investigation_spec": plan},
    )
    result = web_app._launch_approved_pipeline_discovery(
        case_id="case-id",
        persona_id="persona-id",
        actor="analyst",
        approved_groups=[
            {
                "kind": "account",
                "normalized": {
                    "canonical_url": "https://linkedin.com/in/jati-pratomo"
                },
            },
            {
                "kind": "claim",
                "normalized": {"predicate": "full_name", "value": "Jati Pratomo"},
            },
        ],
    )

    assert result["job_id"] == "cross-check-job"
    _persona_id, usernames, options = store.queued
    specification = options["investigation_spec"]
    assert usernames == []
    assert specification["generate_name_variants"] is False
    assert specification["profile_url_usernames"] == {
        "https://linkedin.com/in/jati-pratomo": ["jati-pratomo"]
    }
    assert specification["search_targets"] == []


def test_approved_affiliation_can_open_a_separate_investigation_branch(journey):
    groups = journey["pipeline"].upsert_groups(
        journey["case_id"],
        journey["persona_id"],
        {
            "claims": [
                {
                    "canonical_key": "approved-company",
                    "normalized": {
                        "predicate": "company",
                        "value": "Nexorus",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [journey["observation_id"]],
                }
            ]
        },
        projection_revision=journey["pipeline"].projection_revision(
            journey["case_id"], journey["persona_id"]
        ),
    )
    affiliation_id = groups[0]["id"]
    decision = post(
        journey,
        f"/groups/{affiliation_id}/decision",
        {"decision": "include"},
    )
    assert decision.status_code == 201

    branch = journey["client"].post(
        base(journey) + f"/groups/{affiliation_id}/branch-affiliation",
        data={"csrf_token": "test-csrf", "jurisdiction": "ID"},
        follow_redirects=False,
    )

    assert branch.status_code == 303
    job_id = branch.location.rsplit("/", 1)[-1]
    job = journey["store"].get_job(job_id)
    specification = job["options"]["investigation_spec"]
    assert job["kind"] == "affiliation"
    assert specification["affiliation_name"] == "Nexorus"
    assert specification["source_claim_id"] == affiliation_id
    assert specification["target_basis"] == "approved_affiliation_claim"


def test_report_snapshot_exports_operator_approved_findings_without_qc(journey):
    decision = post(
        journey,
        '/groups/' + journey['group_id'] + '/decision',
        {'decision': 'include', 'reason': 'Approved after reviewing the cited source.'},
    )
    assert decision.status_code == 201
    response = journey['client'].post(
        base(journey) + '/report',
        data={'csrf_token': 'test-csrf'},
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert '/export.pdf' in response.headers['Location']
    version_id = response.headers['Location'].split('/versions/', 1)[1].split('/', 1)[0]
    version = journey['pipeline'].get_version(
        version_id,
        case_id=journey['case_id'],
        persona_id=journey['persona_id'],
    )
    assert version['status'] == 'submitted'


def test_authentication_csrf_role_and_foreign_scope_block_mutations(journey):
    client = journey['client']
    version = curate(journey)
    path = base(journey) + f'/versions/{version["id"]}/qc'
    body = {'decision': 'approved', 'expected_hash': version['content_hash']}
    assert client.post(path, json=body).status_code == 403
    with client.session_transaction() as current:
        current['role'] = 'analyst'
    assert (
        client.post(
            path, json=body, headers={'X-OpenLedger-CSRF': 'test-csrf'}
        ).status_code
        == 403
    )
    assert (
        journey['pipeline'].get_final_version(journey['case_id'], journey['persona_id'])
        is None
    )
    with client.session_transaction() as current:
        current['role'] = 'admin'
    foreign = (
        '/cases/foreign-case/pipeline/'
        + journey['persona_id']
        + f'/versions/{version["id"]}/qc'
    )
    assert (
        client.post(
            foreign, json=body, headers={'X-OpenLedger-CSRF': 'test-csrf'}
        ).status_code
        == 404
    )
    assert (
        client.get(
            '/api/cases/foreign-case/pipeline/'
            + journey['persona_id']
            + f'/versions/{version["id"]}'
        ).status_code
        == 404
    )
    with client.session_transaction() as current:
        current['authenticated'] = False
    assert client.get('/api' + base(journey)).status_code == 401


def test_stale_qc_is_rejected_and_final_manifest_survives_working_decision(journey):
    version = curate(journey)
    result = post(
        journey,
        f'/versions/{version["id"]}/qc',
        {'decision': 'approved', 'expected_hash': 'wrong'},
    )
    assert result.status_code == 409
    assert (
        post(
            journey,
            f'/versions/{version["id"]}/qc',
            {'decision': 'approved', 'expected_hash': version['content_hash']},
        ).status_code
        == 200
    )
    before = journey['client'].get('/api' + base(journey) + '/final').get_json()
    assert (
        post(
            journey,
            '/groups/' + journey['group_id'] + '/decision',
            {
                'decision': 'reject',
                'reason': 'New conflicting identity evidence requires review.',
            },
        ).status_code
        == 201
    )
    after = journey['client'].get('/api' + base(journey) + '/final').get_json()
    assert (
        before['content_hash'] == after['content_hash']
        and before['items'] == after['items']
    )
    assert after['review_needed'] is True


def test_frozen_draft_and_final_pdf_have_same_manifest_identifiers(journey):
    version = curate(journey)
    url = base(journey) + f'/versions/{version["id"]}/export.pdf'
    response = journey['client'].get(url)
    assert response.status_code == 200 and response.data.startswith(b'%PDF-')
    assert response.headers['X-OpenLedger-Version'] == version['id']
    assert response.headers['X-OpenLedger-Manifest-Hash'] == version['content_hash']
    assert 'submitted' in response.headers['Content-Disposition']
    assert (
        journey['pipeline'].get_final_version(journey['case_id'], journey['persona_id'])
        is None
    )
    assert (
        post(
            journey,
            f'/versions/{version["id"]}/qc',
            {'decision': 'approved', 'expected_hash': version['content_hash']},
        ).status_code
        == 200
    )
    response = journey['client'].get(url)
    assert (
        response.status_code == 200
        and 'approved' in response.headers['Content-Disposition']
    )


def test_projection_graph_retains_more_than_120_claims_and_all_sources():
    items = [
        {
            'group_id': f'g-{index}',
            'kind': 'claim',
            'normalized': {'value': f'fact-{index}'},
            'decision': {'decision': 'include'},
            'evidence': [
                {'id': f'obs-{index}', 'source_url': f'https://example.test/{index}'}
            ],
        }
        for index in range(137)
    ]
    projection = version_projection(
        {
            'id': 'version',
            'sequence': 1,
            'content_hash': 'hash',
            'status': 'approved',
            'manifest': {'case_id': 'case', 'persona_id': 'subject', 'items': items},
        }
    )
    graph = version_graph(projection)
    assert graph['item_count'] == 137 and graph['observation_count'] == 137
    assert (
        len([edge for edge in graph['edges'] if edge['kind'] == 'curated_fact']) == 137
    )
    assert (
        public_url('javascript:alert(1)') == ''
        and public_url('https://user:pass@example.test') == ''
    )
    assert public_url('https://example.test/source') == 'https://example.test/source'


def test_rendered_navigation_and_observation_pages_are_human_views(journey):
    from bs4 import BeautifulSoup
    from flask import url_for

    client = journey['client']
    with journey['app'].test_request_context():
        for endpoint, extra in [
            ('group_detail', {'group_id': journey['group_id']}),
            ('observations', {}),
            ('final_version', {}),
        ]:
            assert url_for(
                'pipeline.' + endpoint,
                case_id=journey['case_id'],
                persona_id=journey['persona_id'],
                **extra,
            ).startswith('/cases/')
    group = client.get(base(journey) + '/groups/' + journey['group_id'])
    assert group.status_code == 200 and b'Correct this claim' in group.data
    observations = client.get(base(journey) + '/observations')
    assert (
        observations.status_code == 200
        and journey['observation_id'].encode() in observations.data
    )
    assert b'could not be extracted' in observations.data
    version = curate(journey)
    rendered = client.get(base(journey) + '/versions/' + version['id'])
    html = BeautifulSoup(rendered.data, 'html.parser')
    approve = next(
        form
        for form in html.find_all('form')
        if form.find('input', {'value': 'approved'})
    )
    assert (
        approve.find('input', {'name': 'expected_hash'})['value']
        == version['content_hash']
    )
    assert approve.find('input', {'name': 'qc_confirmed'}) is not None
    graph = client.get(base(journey) + '/versions/' + version['id'] + '/graph')
    assert graph.status_code == 302
    assert graph.location.endswith(base(journey) + '/versions/' + version['id'])


def test_withdrawal_requires_version_hash_and_changes_all_presentations(journey):
    version = curate(journey)
    assert (
        post(
            journey,
            f'/versions/{version["id"]}/qc',
            {'decision': 'approved', 'expected_hash': version['content_hash']},
        ).status_code
        == 200
    )
    assert (
        post(
            journey, '/final/withdraw', {'reason': 'Material new contradiction.'}
        ).status_code
        == 400
    )
    assert (
        post(
            journey,
            '/final/withdraw',
            {
                'reason': 'Material new contradiction.',
                'expected_version_id': version['id'],
                'expected_hash': 'stale',
            },
        ).status_code
        == 409
    )
    response = post(
        journey,
        '/final/withdraw',
        {
            'reason': 'Material new contradiction.',
            'expected_version_id': version['id'],
            'expected_hash': version['content_hash'],
        },
    )
    assert response.status_code == 200
    current = journey['client'].get('/api' + base(journey) + '/final').get_json()
    exact = (
        journey['client']
        .get('/api' + base(journey) + '/versions/' + version['id'])
        .get_json()
    )
    assert current['label'] == exact['label'] == 'Withdrawn Persona'
    assert current['content_hash'] == version['content_hash']
    assert current['items'] == exact['items']


def test_claim_correction_preserves_qualifiers_and_rejects_identity_override(journey):
    blocked = post(
        journey,
        '/groups/' + journey['group_id'] + '/decision',
        {
            'decision': 'include',
            'reason': 'Trying to replace scope.',
            'corrected_claim': {
                'case_id': 'another-case',
                'material_identity_conflict': False,
            },
        },
    )
    assert blocked.status_code == 400
    corrected = post(
        journey,
        '/groups/' + journey['group_id'] + '/decision',
        {
            'decision': 'include',
            'reason': 'Corrected spelling from the same source.',
            'corrected_claim': {'value': 'Synthetic Person Corrected'},
        },
    )
    assert corrected.status_code == 201
    version = post(journey, '/versions', {'scope': 'Corrected public name.'}).get_json()
    item = version['manifest']['items'][0]
    assert item['normalized']['value'] == 'Synthetic Person Corrected'
    assert item['normalized']['predicate'] == 'full_name'
    assert item['normalized']['binding_status'] == 'resolved'
    assert item['original_normalized']['value'] == 'Synthetic Person'


def test_split_preserves_observations_and_requires_new_operator_review(journey):
    pipeline, case_id, persona_id = (
        journey["pipeline"],
        journey["case_id"],
        journey["persona_id"],
    )
    query = pipeline.create_request(
        case_id,
        persona_id,
        [{"type": "full_name", "value": "Synthetic Person"}],
        {
            "pipeline_id": "p2-e2e-v1",
            "tasks": [{"engine": "second-fixture", "availability": "active"}],
        },
        actor="analyst",
    )
    attempt = pipeline.start_attempt(query["tasks"][0]["id"], "second-worker")
    pipeline.record_observations(
        attempt["id"],
        [
            {
                "id": "different-person-record",
                "outcome": "found",
                "source_url": "https://example.test/name-collision",
                "value": "Synthetic Person",
            }
        ],
        outcome="found",
        worker_id="second-worker",
    )
    observations = pipeline.list_observations(case_id, persona_id)
    other = next(
        entry for entry in observations if entry["id"] != journey["observation_id"]
    )
    pipeline.upsert_groups(
        case_id,
        persona_id,
        {
            "claims": [
                {
                    "canonical_key": "synthetic-full-name",
                    "normalized": {
                        "predicate": "full_name",
                        "value": "Synthetic Person",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [journey["observation_id"], other["id"]],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(case_id, persona_id),
    )
    invalid = post(
        journey,
        "/groups/" + journey["group_id"] + "/revision",
        {
            "action": "split",
            "observation_ids": ["foreign-observation"],
            "reason": "Scope misuse.",
        },
    )
    assert invalid.status_code == 409
    split = post(
        journey,
        "/groups/" + journey["group_id"] + "/revision",
        {
            "action": "split",
            "observation_ids": [other["id"]],
            "reason": "This public record belongs to a namesake.",
        },
    )
    assert split.status_code == 201, split.get_data(as_text=True)
    successor_group = split.get_json()["details"]["target_group_id"]
    assert (
        pipeline.get_group(case_id, persona_id, successor_group)["latest_decision"]
        is None
    )
    assert len(pipeline.list_observations(case_id, persona_id)) == 2
    assert (
        pipeline.get_group(case_id, persona_id, journey["group_id"])[
            "observation_count"
        ]
        == 1
    )


def test_pdf_text_register_contains_every_curated_fact_and_observation():
    import re
    import shutil
    import subprocess
    from maigret.web.pipeline_pdf import generate_pipeline_pdf

    converter = shutil.which('pdftotext')
    if not converter:
        pytest.skip(
            'Poppler text extraction is required for independent PDF fidelity validation'
        )
    items = [
        {
            'group_id': f'group-{index:03d}',
            'kind': 'claim',
            'normalized': {'predicate': 'public_fact', 'value': f'Fact number {index}'},
            'decision': {'decision': 'include', 'actor': 'human-reviewer'},
            'evidence': [
                {
                    'id': f'obs-{index:03d}',
                    'source_url': f'https://example.test/record/{index}',
                }
            ],
        }
        for index in range(137)
    ]
    projection = version_projection(
        {
            'id': 'version-fidelity',
            'sequence': 1,
            'content_hash': 'a' * 64,
            'status': 'approved',
            'manifest': {
                'case_id': 'case-fidelity',
                'persona_id': 'subject-fidelity',
                'items': items,
            },
        }
    )
    rendered = generate_pipeline_pdf(projection)
    text = subprocess.run(
        [converter, '-', '-'], input=rendered, capture_output=True, check=True
    ).stdout.decode()
    assert set(re.findall(r'Group:\s+(group-\d+)', text)) == {
        item['group_id'] for item in items
    }
    assert set(re.findall(r'Observation\s+(obs-\d+)', text)) == {
        item['evidence'][0]['id'] for item in items
    }
    assert 'Final Persona' in text and 'version-fidelity' in text
    assert 'a' * 64 in re.sub(r'\s+', '', text)


def test_http_structured_scope_cannot_bypass_mandatory_objectives(journey):
    post(
        journey,
        '/groups/' + journey['group_id'] + '/decision',
        {'decision': 'include', 'reason': 'Synthetic attributable source'},
    )
    response = post(
        journey,
        '/versions',
        {
            'scope': {
                'description': 'Verify identity',
                'mandatory_fields': {'full_name': False},
            }
        },
    )
    assert response.status_code == 201
    version = response.get_json()
    version = version.get('result', version)
    assert isinstance(version['manifest']['scope'], dict)
    denied = post(
        journey,
        '/versions/' + version['id'] + '/qc',
        {'decision': 'approved', 'expected_hash': version['content_hash']},
    )
    assert denied.status_code == 409
    assert (
        journey['pipeline'].get_final_version(journey['case_id'], journey['persona_id'])
        is None
    )


def test_resume_route_requires_csrf_and_preserves_case_scope(journey):
    pipeline, case_store = journey['pipeline'], journey['store']
    job_id = case_store.create_investigation(['recoverable-fixture'], {})
    job = case_store.claim_next('worker:recover')
    assert job['job_id'] == job_id
    query = pipeline.requests_for_job(job_id)[0]
    case_store.mark_stale_running(0)
    url = f'/cases/{job["case_id"]}/pipeline/{query["persona_id"]}/requests/{query["id"]}/resume'
    client = journey['client']
    assert client.post(url, json={}).status_code == 403
    wrong_scope = post(journey, '/requests/' + query['id'] + '/resume', {})
    assert wrong_scope.status_code == 404
    assert case_store.get_job(job_id)['status'] == 'interrupted'
    response = client.post(url, json={}, headers={'X-OpenLedger-CSRF': 'test-csrf'})
    assert response.status_code == 202, response.get_data(as_text=True)
    assert response.json['job_id'] == job_id
    assert case_store.get_job(job_id)['case_id'] == job['case_id']
