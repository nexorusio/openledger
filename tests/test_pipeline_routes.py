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
        case_id, persona_id, inputs, plan, actor="system:route-fixture", job_id=job_id
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
    from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups

    pipeline.reconcile_submitted_inputs(case_id, persona_id)
    assess_consolidated_groups(case_store, case_id, persona_id)
    for item in pipeline.get_workspace(case_id, persona_id)["shortlist"]:
        if item["investigator_supplied"]:
            pipeline.decide(
                case_id,
                persona_id,
                item["id"],
                "reject",
                actor="fixture-reviewer",
                reason="Resolved setup anchor for route-fixture isolation",
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
    app.add_url_rule(
        "/live/<job_id>",
        "live_results",
        lambda job_id: "synthetic live result",
    )
    app.context_processor(
        lambda: {
            "csrf_token": session.get("csrf_token"),
            "current_user": session.get("username"),
            "current_role": session.get("role"),
        }
    )
    launches = []
    discovery_launches = []
    source_fetch_launches = []

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

    def launch_approved_source_fetch(**kwargs):
        source_fetch_launches.append(kwargs)
        return {
            "job_id": "approved-source-fetch-job",
            "case_id": kwargs["case_id"],
            "persona_id": kwargs["persona_id"],
            "source_count": 1,
        }

    register_pipeline_routes(
        app,
        lambda: case_store,
        lambda: session.get("role", ""),
        lambda token: token == session.get("csrf_token") == "test-csrf",
        launch_research=launch_research,
        prepare_workspace=lambda **_kwargs: {"status": "prepared"},
        launch_approved_discovery=launch_approved_discovery,
        launch_approved_source_fetch=launch_approved_source_fetch,
        geocode_approved_location=lambda _place: {
            "latitude": -6.1754,
            "longitude": 106.8272,
            "precision": "city",
        },
        affiliation_public_web_enabled=lambda: True,
        google_places_enabled=lambda: True,
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
        source_fetch_launches=source_fetch_launches,
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
    assert b'Review submitted evidence and discoveries' in workspace.data
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
    blocked_report = client.post(
        base(journey) + "/report",
        data={"csrf_token": "test-csrf"},
        follow_redirects=False,
    )
    assert blocked_report.status_code == 302
    assert blocked_report.location.endswith(base(journey) + "#operator-review")

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
    assert b"Research with cited sources" in persona.data

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


def test_step_two_exposes_reconciliation_action_at_the_decision_point(
    journey, monkeypatch
):
    original = PipelineStore.get_workspace

    def pending(self, *args, **kwargs):
        workspace = original(self, *args, **kwargs)
        workspace["projection"]["pending"] = True
        return workspace

    monkeypatch.setattr(PipelineStore, "get_workspace", pending)

    response = journey["client"].get(base(journey))

    assert response.status_code == 200
    assert b"Reconcile evidence and continue" in response.data
    assert response.data.count(b'action="' + base(journey).encode() + b'/prepare"') == 2


def test_page_and_persona_titles_share_the_global_sticky_rule():
    css = (
        Path(__file__).parents[1]
        / "maigret"
        / "web"
        / "static"
        / "openledger.css"
    ).read_text()
    rule = css.split(".page-heading,\n.persona-profile-header", 1)[1].split("}", 1)[0]
    assert "position: sticky" in rule
    assert "top: var(--ol-topbar-height)" in rule


def test_approved_discovery_reuses_exact_url_without_generated_aliases(monkeypatch):
    import maigret.web.app as web_app

    class Store:
        queued = None

        def get_persona(self, persona_id):
            return {"id": persona_id, "display_name": "Jati Pratomo"}

        def repeat_persona_investigation(
            self,
            persona_id,
            usernames,
            options,
            *,
            allow_identifier_free_approved_research=False,
        ):
            self.queued = (
                persona_id,
                usernames,
                options,
                allow_identifier_free_approved_research,
            )
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
    _persona_id, usernames, options, allow_identifier_free = store.queued
    specification = options["investigation_spec"]
    assert usernames == []
    assert allow_identifier_free is False
    assert specification["generate_name_variants"] is False
    assert specification["profile_url_usernames"] == {
        "https://linkedin.com/in/jati-pratomo": ["jati-pratomo"]
    }
    assert specification["search_targets"] == []
    assert specification["discovery_basis"] == "approved_pipeline_findings"
    assert specification["approved_research_questions"]
    research = "\n".join(specification["approved_research_questions"])
    assert "Jati Pratomo" in research
    assert "https://linkedin.com/in/jati-pratomo" in research
    assert "employment, education, memberships" in research


def test_approved_source_fetch_queues_exact_reviewed_urls(monkeypatch):
    import maigret.web.app as web_app

    class Store:
        queued = None

        def get_persona(self, persona_id):
            return {"id": persona_id, "display_name": "Jati Pratomo"}

        def repeat_persona_investigation(
            self,
            persona_id,
            usernames,
            options,
            *,
            allow_identifier_free_approved_research=False,
        ):
            self.queued = (
                persona_id,
                usernames,
                options,
                allow_identifier_free_approved_research,
            )
            return "approved-source-fetch-job"

    store = Store()
    monkeypatch.setattr(web_app, "case_store", store)
    monkeypatch.setattr(
        web_app,
        "parse_search_options",
        lambda _form, plan: {"investigation_spec": plan},
    )
    result = web_app._launch_approved_source_fetch(
        case_id="case-id",
        persona_id="persona-id",
        actor="analyst",
        approved_groups=[
            {
                "kind": "account",
                "normalized": {
                    "canonical_url": "https://www.linkedin.com/in/jati-pratomo/"
                },
            },
            {
                "kind": "claim",
                "normalized": {
                    "predicate": "social_account",
                    "value": {"url": "https://www.linkedin.com/in/jati-pratomo/"},
                },
            },
            {
                "kind": "claim",
                "normalized": {
                    "predicate": "website",
                    "value": "https://example.test/about",
                },
            },
        ],
    )

    assert result == {
        "job_id": "approved-source-fetch-job",
        "case_id": "case-id",
        "persona_id": "persona-id",
        "source_count": 2,
    }
    _persona_id, usernames, options, allow_identifier_free = store.queued
    specification = options["investigation_spec"]
    assert usernames == []
    assert allow_identifier_free is True
    assert specification["discovery_basis"] == "approved_source_fetch"
    assert specification["enable_approved_source_fetch"] is True
    assert "approved_research_questions" not in specification
    assert specification["approved_source_urls"] == [
        "https://www.linkedin.com/in/jati-pratomo/",
        "https://example.test/about",
    ]


def test_fetch_approved_sources_route_queues_only_approved_urls(journey):
    from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups

    assert post(
        journey,
        "/groups/" + journey["group_id"] + "/decision",
        {"decision": "include", "reason": "Approved baseline finding."},
    ).status_code == 201
    groups = journey["pipeline"].upsert_groups(
        journey["case_id"],
        journey["persona_id"],
        {
            "claims": [
                {
                    "canonical_key": "approved-source-url",
                    "normalized": {
                        "predicate": "website",
                        "value": "https://example.test/about",
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
    assess_consolidated_groups(
        journey["store"], journey["case_id"], journey["persona_id"]
    )
    assert post(
        journey,
        "/groups/" + groups[0]["id"] + "/decision",
        {"decision": "include", "reason": "Exact public page selected."},
    ).status_code == 201

    response = journey["client"].post(
        base(journey) + "/fetch-approved-sources",
        data={"csrf_token": "test-csrf"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["Location"].endswith("/live/approved-source-fetch-job")
    assert len(journey["source_fetch_launches"]) == 1
    launch = journey["source_fetch_launches"][0]
    assert launch["case_id"] == journey["case_id"]
    assert launch["persona_id"] == journey["persona_id"]
    assert [
        item["normalized"]["value"]
        for item in launch["approved_groups"]
        if item["normalized"].get("predicate") == "website"
    ] == ["https://example.test/about"]


def test_approved_discovery_uses_cited_research_for_affiliation_without_identifier(
    monkeypatch,
):
    import maigret.web.app as web_app

    class Store:
        queued = None

        def get_persona(self, persona_id):
            return {"id": persona_id, "display_name": "Affiliation-only Persona"}

        def repeat_persona_investigation(
            self,
            persona_id,
            usernames,
            options,
            *,
            allow_identifier_free_approved_research=False,
        ):
            self.queued = (
                persona_id,
                usernames,
                options,
                allow_identifier_free_approved_research,
            )
            return "affiliation-cross-check-job"

    store = Store()
    monkeypatch.setattr(web_app, "case_store", store)
    monkeypatch.setattr(
        web_app,
        "parse_investigation_submission",
        lambda _form: (_ for _ in ()).throw(
            AssertionError(
                "identifier-free approved research must not use the public parser"
            )
        ),
    )
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
                "kind": "claim",
                "normalized": {
                    "predicate": "company",
                    "value": "Nexorus",
                    "binding_status": "resolved",
                },
            },
            {
                "kind": "claim",
                "normalized": {
                    "predicate": "organization_location",
                    "value": "Jakarta, Indonesia",
                    "binding_status": "resolved",
                },
            },
        ],
    )

    assert result["job_id"] == "affiliation-cross-check-job"
    _persona_id, usernames, options, allow_identifier_free = store.queued
    specification = options["investigation_spec"]
    assert usernames == []
    assert specification["identifiers"] == []
    assert specification["search_targets"] == []
    assert specification["discovery_basis"] == "approved_pipeline_findings"
    assert allow_identifier_free is True
    research = "\n".join(specification["approved_research_questions"])
    assert "company: Nexorus" in research
    assert "organization_location: Jakarta, Indonesia" in research


def test_approved_affiliation_can_open_a_separate_investigation_branch(journey):
    from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups

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
    assess_consolidated_groups(
        journey["store"], journey["case_id"], journey["persona_id"]
    )
    affiliation_id = groups[0]["id"]
    decision = post(
        journey,
        f"/groups/{affiliation_id}/decision",
        {"decision": "include"},
    )
    assert decision.status_code == 201
    resolved_existing = post(
        journey,
        f"/groups/{journey['group_id']}/decision",
        {"decision": "reject"},
    )
    assert resolved_existing.status_code == 201

    branch = journey["client"].post(
        base(journey) + f"/groups/{affiliation_id}/branch-affiliation",
        data={"csrf_token": "test-csrf"},
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
    assert specification["official_website"] is None
    assert specification["enable_domain_context"] is False
    assert specification["enable_public_web_research"] is True
    assert specification["enable_google_places_search"] is True


def test_persona_renders_approved_photo_and_persisted_location_map(
    journey, monkeypatch
):
    from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups

    monkeypatch.setenv(
        "OPENLEDGER_MAP_TILE_URL",
        "https://tiles.example.test/{z}/{x}/{y}.png",
    )

    groups = journey["pipeline"].upsert_groups(
        journey["case_id"],
        journey["persona_id"],
        {
            "claims": [
                {
                    "canonical_key": "approved-photo",
                    "normalized": {
                        "predicate": "photograph",
                        "value": "https://cdn.example.test/jati.jpg",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [journey["observation_id"]],
                },
                {
                    "canonical_key": "approved-organization-location",
                    "normalized": {
                        "predicate": "organization_location",
                        "value": "Jakarta, Indonesia",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [journey["observation_id"]],
                },
                {
                    "canonical_key": "approved-occupation",
                    "normalized": {
                        "predicate": "occupation",
                        "value": "Data analyst",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [journey["observation_id"]],
                },
                {
                    "canonical_key": "approved-affiliation",
                    "normalized": {
                        "predicate": "affiliation",
                        "value": "Nexorus",
                        "binding_status": "resolved",
                    },
                    "observation_ids": [journey["observation_id"]],
                },
            ]
        },
        projection_revision=journey["pipeline"].projection_revision(
            journey["case_id"], journey["persona_id"]
        ),
    )
    assess_consolidated_groups(
        journey["store"], journey["case_id"], journey["persona_id"]
    )
    for group in groups:
        assert post(
            journey,
            f"/groups/{group['id']}/decision",
            {"decision": "include"},
        ).status_code == 201
    assert post(
        journey,
        f"/groups/{journey['group_id']}/decision",
        {"decision": "reject"},
    ).status_code == 201

    response = journey["client"].get(base(journey) + "/persona")

    assert response.status_code == 200
    assert b"https://cdn.example.test/jati.jpg" in response.data
    assert b"Approved locations" in response.data
    assert b"106.8272" in response.data
    assert b"Organization location" in response.data
    assert b'https://tiles.example.test/{z}/{x}/{y}.png' in response.data
    assert b"Export Persona PDF" in response.data
    assert b"Case AI assistant" in response.data
    assert b"Relationship evidence" in response.data
    assert b"Data analyst" in response.data
    assert b"Nexorus" in response.data
    assert b"Jakarta, Indonesia" in response.data
    assert b'persona-photo-placeholder" hidden' in response.data

    relationships = journey["client"].get(
        base(journey) + "/persona/relationships"
    )
    assert relationships.status_code == 200
    assert b'"truncated_count": 0' in relationships.data
    assert b"approved-photo" not in relationships.data
    assert journey["observation_id"].encode() in relationships.data


def test_persona_map_popup_uses_text_nodes_for_untrusted_precision():
    template = (
        Path(__file__).parents[1]
        / "maigret"
        / "web"
        / "templates"
        / "pipeline_persona.html"
    ).read_text()

    assert "label.textContent = String(point.label ?? '')" in template
    assert "document.createTextNode(" in template
    assert "String(point.precision ?? '')" in template
    assert "bindPopup(popup)" in template
    assert "· ${point.precision}" not in template


def test_persona_map_uses_the_configured_tile_service():
    template = (
        Path(__file__).parents[1]
        / "maigret"
        / "web"
        / "templates"
        / "pipeline_persona.html"
    ).read_text()

    assert "window.L.tileLayer({{ map_tile_url | tojson }}" in template
    assert "window.L.tileLayer('https://tile.openstreetmap.org" not in template


def test_approved_persona_navigation_uses_the_final_persona_route():
    source = (
        Path(__file__).parents[1] / "maigret" / "web" / "app.py"
    ).read_text()

    persona_workspace = source[source.index("def persona_workspace(") : source.index(
        "@app.route('/personas/<persona_id>/export.pdf')"
    )]
    assert "'pipeline.persona'" in persona_workspace
    assert "'pipeline.workspace'" not in persona_workspace


def test_approved_persona_opens_while_newer_findings_await_review(journey):
    from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups

    assert post(
        journey,
        f"/groups/{journey['group_id']}/decision",
        {"decision": "include", "reason": "Approved from retained evidence."},
    ).status_code == 201
    pending_groups = journey["pipeline"].upsert_groups(
        journey["case_id"],
        journey["persona_id"],
        {
            "claims": [
                {
                    "canonical_key": "pending-occupation",
                    "normalized": {
                        "predicate": "occupation",
                        "value": "Pending analyst review",
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
    assess_consolidated_groups(
        journey["store"], journey["case_id"], journey["persona_id"]
    )
    workspace = journey["pipeline"].get_workspace(
        journey["case_id"], journey["persona_id"]
    )
    assert pending_groups[0]["id"] in {
        item["id"] for item in workspace["shortlist"]
    }
    assert workspace["review_pending_count"] >= 1

    response = journey["client"].get(base(journey) + "/persona")

    assert response.status_code == 200
    assert b"Approved Persona" in response.data
    assert b"Synthetic Person" in response.data
    assert b"awaiting review" in response.data
    assert b"Pending analyst review" not in response.data


def test_persona_evidence_network_starts_with_the_readable_force_layout():
    root = Path(__file__).parents[1] / "maigret" / "web"
    template = (root / "templates" / "relationships.html").read_text()
    script = (root / "static" / "relationships.js").read_text()

    assert '<option value="force" selected>Evidence network</option>' in template
    assert "applyLayout('force');" in script
    assert "applyLayout('hierarchical');" not in script


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
    download = journey['client'].get(response.headers['Location'])
    assert download.status_code == 200
    assert download.mimetype == 'application/pdf'
    assert download.data.startswith(b'%PDF-')
    assert 'attachment' in download.headers['Content-Disposition']
    version_id = response.headers['Location'].split('/versions/', 1)[1].split('/', 1)[0]
    version = journey['pipeline'].get_version(
        version_id,
        case_id=journey['case_id'],
        persona_id=journey['persona_id'],
    )
    assert version['status'] == 'submitted'
    assert version['manifest']['scope']['subject_name'] == (
        journey['pipeline'].get_subject(
            journey['case_id'], journey['persona_id']
        )['display_name']
    )


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
    # Two source observations plus the retained original investigation input.
    assert len(pipeline.list_observations(case_id, persona_id)) == 3
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
    assert 'Evidence ID index' in text
    assert all(
        text.count(item['evidence'][0]['id']) >= 2
        for item in items
    )
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
