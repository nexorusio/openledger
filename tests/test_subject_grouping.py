"""Subject ownership survives alias expansion, review, and later collections."""

import pytest
from sqlalchemy import update
from werkzeug.datastructures import MultiDict

from maigret.web.case_store import CaseStore, investigation_jobs, personas
from maigret.web.investigation_input import (
    InvestigationInputError,
    build_investigation_plan,
    search_usernames,
)


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(f"sqlite:///{tmp_path / 'subjects.db'}", create_schema=True)
    yield instance
    instance.dispose()


def name_plan(*names, mode="same_subject", aliases=None):
    form = {
        "identifier_type": ["full_name"] * len(names),
        "identifier_value": list(names),
        "processing_mode": mode,
        "generate_name_variants": "on",
    }
    if aliases is not None:
        form.update(
            alias_candidates_present="1",
            alias_candidate=aliases,
            selected_alias=aliases,
        )
    return build_investigation_plan(form)


def _groups(plan):
    return [
        {"label": group["label"], "usernames": group["usernames"]}
        for group in plan["subject_groups"]
    ]


def create(store, plan):
    job_id = store.create_investigation(
        search_usernames(plan), {"investigation_spec": plan}
    )
    return job_id, store.get_case(store.get_job(job_id)["case_id"])


def findings(*usernames):
    return {
        "status": "completed",
        "usernames": list(usernames),
        "individual_reports": [
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": f"https://example.test/{username}",
                        "confidence": "strong",
                        "evidence": {"fullname": "Reviewed Name"},
                    }
                ],
            }
            for username in usernames
        ],
    }


def test_name_handle_and_selected_aliases_default_to_one_persona(store):
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name", "username"],
            "identifier_value": ["Alex Example", "alexexample"],
            "generate_name_variants": "on",
        }
    )
    assert plan["processing_mode"] == "same_subject"
    assert _groups(plan) == [
        {"label": "Alex Example", "usernames": search_usernames(plan)}
    ]
    job_id, case = create(store, plan)
    persona_id = case["personas"][0]["id"]
    assert len(case["personas"]) == 1
    assert case["personas"][0]["display_name"] == "Alex Example"
    assert (
        store.get_job(job_id)["options"]["investigation_spec"]["target_persona_id"]
        == persona_id
    )
    store.sync_persona_claims(job_id, findings("alexexample", "alex.example"))
    claims = store.get_persona(persona_id)["claims"]
    assert (
        len([claim for claim in claims if claim["field_name"] == "social_account"]) == 2
    )
    assert all(claim["review_status"] == "pending" for claim in claims)
    assert len(store.get_case(case["id"])["personas"]) == 1


def test_independent_names_keep_their_own_aliases_and_pending_claims(store):
    plan = name_plan(
        "John Doe",
        "Jane Roe",
        mode="independent",
        aliases=["johndoe", "john.doe", "janeroe"],
    )
    assert _groups(plan) == [
        {"label": "John Doe", "usernames": ["johndoe", "john.doe"]},
        {"label": "Jane Roe", "usernames": ["janeroe"]},
    ]
    assert (
        next(
            target for target in plan["search_targets"] if target["value"] == "janeroe"
        )["source_value"]
        == "Jane Roe"
    )
    job_id, case = create(store, plan)
    assert {persona["display_name"] for persona in case["personas"]} == {
        "John Doe",
        "Jane Roe",
    }
    store.sync_persona_claims(job_id, findings(*search_usernames(plan)))
    for persona in case["personas"]:
        expected = (
            {"johndoe", "john.doe"}
            if persona["display_name"] == "John Doe"
            else {"janeroe"}
        )
        claims = store.get_persona(persona["id"])["claims"]
        urls = {
            claim["value"]["url"]
            for claim in claims
            if claim["field_name"] == "social_account"
        }
        assert urls == {f"https://example.test/{value}" for value in expected}
        assert all(claim["review_status"] == "pending" for claim in claims)


def test_shared_alias_does_not_merge_explicitly_distinct_subjects(store):
    plan = name_plan(
        "Alice Smith", "Andrew Smith", mode="independent", aliases=["asmith"]
    )
    assert search_usernames(plan) == ["asmith"]
    job_id, case = create(store, plan)
    assert len(case["personas"]) == 2
    store.sync_persona_claims(job_id, findings("asmith"))
    for persona in case["personas"]:
        assert any(
            claim["field_name"] == "social_account"
            for claim in store.get_persona(persona["id"])["claims"]
        )


def test_distinct_profile_inputs_with_same_handle_keep_their_own_context(store):
    urls = ["https://www.instagram.com/alice/", "https://www.linkedin.com/in/alice/"]
    plan = build_investigation_plan(
        {
            "identifier_type": ["profile_url", "profile_url"],
            "identifier_value": urls,
            "processing_mode": "independent",
        }
    )
    assert search_usernames(plan) == ["alice"]
    job_id, case = create(store, plan)
    assert len(case["personas"]) == 2
    retained = []
    for persona in case["personas"]:
        claims = store.get_persona(persona["id"])["claims"]
        assert len(claims) == 1
        retained.append(claims[0]["value"]["url"])
    assert set(retained) == set(urls)
    store.sync_persona_claims(job_id, {"status": "completed", "individual_reports": []})
    assert all(
        len(store.get_persona(persona["id"])["claims"]) == 1
        for persona in case["personas"]
    )


def test_shared_handle_profile_enrichment_and_rerun_keep_source_ownership(store):
    from maigret.web.collector_adapters import (
        UNFURL_VERSION,
        normalize_unfurl_url_analysis,
    )

    linkedin = "https://www.linkedin.com/in/alexexample/"
    instagram = "https://www.instagram.com/alexexample/"
    plan = build_investigation_plan(
        {
            "identifier_type": ["profile_url", "profile_url"],
            "identifier_value": [linkedin, instagram],
            "processing_mode": "independent",
        }
    )
    job_id, _case = create(store, plan)
    bindings = store.get_job(job_id)["options"]["investigation_spec"][
        "persona_bindings"
    ]
    owners = {
        binding["identifiers"][0]["value"]: binding["persona_id"]
        for binding in bindings
    }

    def instagram_claims():
        return [
            (
                claim["id"],
                claim["value"],
                claim["review_status"],
                [evidence["source_url"] for evidence in claim["evidence"]],
            )
            for claim in store.get_persona(owners[instagram])["claims"]
        ]

    original_instagram_claims = instagram_claims()
    observation = normalize_unfurl_url_analysis(
        {
            "investigated_username": "alexexample",
            "site_name": "LinkedIn",
            "profile_url": linkedin,
        },
        {
            "schema_version": 1,
            "engine": "dfir-unfurl",
            "version": UNFURL_VERSION,
            "remote_lookups": False,
            "nodes": [{"id": 1, "data_type": "url", "value": linkedin}],
        },
    )
    result = {
        "status": "completed",
        "individual_reports": [
            {
                "username": "alexexample",
                "claimed_profiles": [
                    {
                        "site_name": "LinkedIn",
                        "url": linkedin,
                        "confidence": "strong",
                        "evidence": {"fullname": "Alex Example"},
                    }
                ],
            }
        ],
        "collector_observations": [observation],
    }
    store.sync_persona_claims(job_id, result)
    assert instagram_claims() == original_instagram_claims
    assert any(
        claim["field_name"] == "full_name"
        for claim in store.get_persona(owners[linkedin])["claims"]
    )
    ai = store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "alexexample",
                "field_name": "company",
                "value": "Example Company",
                "confidence": 80,
                "source_url": linkedin,
                "source_title": "LinkedIn profile",
                "reason": "The cited profile names the employer.",
            }
        ],
        sources=[{"title": "LinkedIn profile", "url": linkedin}],
        usernames=["alexexample"],
        model="mock-model",
    )
    assert ai["count"] == 1
    assert instagram_claims() == original_instagram_claims
    store.claim_next("mock-worker")
    store.finish(job_id, result)
    refresh_id = store.repeat_persona_investigation(owners[instagram])
    refresh_spec = store.get_job(refresh_id)["options"]["investigation_spec"]
    assert refresh_spec["identifiers"] == [{"type": "profile_url", "value": instagram}]
    assert refresh_spec["search_targets"] == [
        {
            "value": "alexexample",
            "source_type": "profile_url",
            "source_value": instagram,
        }
    ]


def test_explicit_separate_name_and_username_remain_two_subjects(store):
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name", "username"],
            "identifier_value": ["John Doe", "johndoe"],
            "processing_mode": "independent",
            "generate_name_variants": "on",
        }
    )
    _job_id, case = create(store, plan)
    assert {persona["display_name"] for persona in case["personas"]} == {
        "John Doe",
        "johndoe",
    }


def test_ambiguous_edited_alias_requires_subject_assignment():
    with pytest.raises(InvestigationInputError, match="one originating name"):
        name_plan("John Doe", "Jane Roe", mode="independent", aliases=["custom-handle"])
    plan = name_plan("John Doe", mode="independent", aliases=["custom-handle"])
    assert _groups(plan) == [{"label": "John Doe", "usernames": ["custom-handle"]}]


def test_stable_bindings_survive_label_changes_and_preserve_reviews(store):
    plan = name_plan(
        "John Doe", "Jane Roe", mode="independent", aliases=["johndoe", "janeroe"]
    )
    job_id, case = create(store, plan)
    by_name = {persona["display_name"]: persona["id"] for persona in case["personas"]}
    john_id, jane_id = by_name["John Doe"], by_name["Jane Roe"]
    store.sync_persona_claims(job_id, findings("johndoe"))
    reviewed = next(
        claim
        for claim in store.get_persona(john_id)["claims"]
        if claim["field_name"] == "full_name"
    )
    store.review_claim(reviewed["id"], "approved", "analyst", note="Reviewed identity")
    with store.engine.begin() as connection:
        connection.execute(
            update(personas)
            .where(personas.c.id == john_id)
            .values(display_name="Reviewed Name")
        )
    store.sync_persona_claims(job_id, findings("johndoe", "janeroe"))
    proposal_url = "https://example.test/johndoe"
    result = store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "johndoe",
                "field_name": "company",
                "value": "Example Company",
                "confidence": 80,
                "source_url": proposal_url,
                "source_title": "Profile",
                "reason": "Profile states the employer.",
            }
        ],
        sources=[{"title": "Profile", "url": proposal_url}],
        usernames=["johndoe"],
        model="mock-model",
    )
    assert result["count"] == 1
    john = store.get_persona(john_id)
    preserved = next(claim for claim in john["claims"] if claim["id"] == reviewed["id"])
    assert preserved["review_status"] == "approved"
    assert preserved["reviews"][0]["note"] == "Reviewed identity"
    assert any(claim["field_name"] == "company" for claim in john["claims"])
    assert not any(
        claim["field_name"] == "company"
        for claim in store.get_persona(jane_id)["claims"]
    )
    assert {persona["id"] for persona in store.get_case(case["id"])["personas"]} == {
        john_id,
        jane_id,
    }


def test_rerun_uses_selected_persona_targets_after_another_persona_rerun(store):
    plan = name_plan(
        "John Doe",
        "Jane Roe",
        mode="independent",
        aliases=["johndoe", "john.doe", "janeroe"],
    )
    job_id, case = create(store, plan)
    ids = {persona["display_name"]: persona["id"] for persona in case["personas"]}
    store.claim_next("mock-worker")
    store.finish(job_id, findings())
    jane_refresh = store.repeat_persona_investigation(ids["Jane Roe"])
    assert store.get_job(jane_refresh)["usernames"] == ["janeroe"]
    store.claim_next("mock-worker")
    store.finish(jane_refresh, findings())
    john_refresh = store.repeat_persona_investigation(ids["John Doe"])
    john = store.get_job(john_refresh)
    assert john["usernames"] == ["johndoe", "john.doe"]
    assert john["options"]["investigation_spec"]["identifiers"] == [
        {"type": "full_name", "value": "John Doe"}
    ]
    assert john["options"]["investigation_spec"]["target_persona_id"] == ids["John Doe"]


def test_historical_jobs_without_bindings_keep_existing_personas(store):
    job_id = store.create_investigation(["alice", "bob"], {})
    job = store.get_job(job_id)
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(options={})
        )
    before = store.get_case(job["case_id"])["personas"]
    store.sync_persona_claims(job_id, findings("alice", "bob"))
    assert store.get_case(job["case_id"])["personas"] == before
    assert store.get_job(job_id)["options"] == {}
    assert all(store.get_persona(persona["id"])["claims"] for persona in before)


@pytest.mark.parametrize(
    "mode, expected",
    [
        (None, "same_subject"),
        ("same_subject", "same_subject"),
        ("independent", "independent"),
    ],
)
def test_new_legacy_submission_honors_explicit_grouping(mode, expected):
    from maigret.web.app import parse_investigation_submission

    form = MultiDict({"usernames": "alice alice.two"})
    if mode is not None:
        form["processing_mode"] = mode
    usernames, plan = parse_investigation_submission(form)
    assert usernames == ["alice", "alice.two"]
    assert plan["processing_mode"] == expected


@pytest.mark.parametrize(
    "mode, expected_count", [(None, 1), ("same_subject", 1), ("independent", 2)]
)
def test_live_submission_queues_bound_personas_without_running_collectors(
    store, monkeypatch, mode, expected_count
):
    from maigret.web import app as web_app

    monkeypatch.setitem(web_app.app.config, "TESTING", True)
    monkeypatch.setitem(web_app.app.config, "AUTH_REQUIRED", False)
    monkeypatch.setattr(
        web_app, "parse_search_options", lambda form, plan: {"investigation_spec": plan}
    )
    queued = []

    def queue_only(usernames, options):
        job_id = store.create_investigation(usernames, options)
        queued.append(job_id)
        return job_id

    monkeypatch.setattr(web_app, "start_live_job", queue_only)
    client = web_app.app.test_client()
    with client.session_transaction() as session:
        session["csrf_token"] = "test-token"
    data = {
        "csrf_token": "test-token",
        "identifier_type": ["full_name", "username"],
        "identifier_value": ["Alex Example", "alexexample"],
        "generate_name_variants": "on",
    }
    if mode is not None:
        data["processing_mode"] = mode
    response = client.post("/live", data=data)
    assert response.status_code == 302
    assert len(queued) == 1
    job = store.get_job(queued[0])
    case = store.get_case(job["case_id"])
    assert len(case["personas"]) == expected_count
    assert {
        binding["persona_id"]
        for binding in job["options"]["investigation_spec"]["persona_bindings"]
    } == {persona["id"] for persona in case["personas"]}


def test_supplied_profile_is_pending_before_collection_and_retained_on_cancel(store):
    profile_url = "https://www.linkedin.com/in/alex-example/?trk=given"
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name", "profile_url"],
            "identifier_value": ["Alex Example", profile_url],
        }
    )
    job_id, case = create(store, plan)
    persona_id = case["personas"][0]["id"]
    claims = store.get_persona(persona_id)["claims"]
    assert len(claims) == 1
    assert claims[0]["review_status"] == "pending"
    assert claims[0]["value"]["url"] == profile_url
    store.sync_persona_claims(job_id, {"status": "completed", "individual_reports": []})
    assert len(store.get_persona(persona_id)["claims"]) == 1
    store.request_cancel(job_id)
    assert store.get_persona(persona_id)["claims"][0]["id"] == claims[0]["id"]
