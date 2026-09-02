import json

import pytest

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web.collector_adapters import (
    CLOUDFLARE_DNS_ENGINE,
    CLOUDFLARE_DNS_URL,
    FR_BUSINESS_REGISTRY_ENGINE,
    FR_BUSINESS_REGISTRY_URL,
    GLEIF_API_URL,
    GLEIF_ENGINE,
    GITHUB_API_BASE_URL,
    GITHUB_API_VERSION,
    GITHUB_ENGINE,
    GOOGLE_PLACES_ENGINE,
    GOOGLE_PLACES_DETAILS_URL,
    GOOGLE_PLACES_SEARCH_URL,
    ICIJ_OFFSHORE_ENGINE,
    ICIJ_RECONCILE_URL,
    OFFICIAL_WEBSITE_ENGINE,
    PUBLIC_WEB_ORGANIZATION_RESEARCH_ENGINE,
    UNFURL_ENGINE,
    UNFURL_VERSION,
    USER_SCANNER_ENGINE,
    WAYBACK_API_BASE_URL,
    WAYBACK_ENGINE,
    WIKIDATA_ENGINE,
    WIKIDATA_API_URL,
    WIKIDATA_QUERY_URL,
    WIKIPEDIA_API_URL,
    WIKIPEDIA_ENGINE,
    _wikidata_people_query,
    build_business_context_assessment,
    build_organization_resolution_candidates,
    claimed_profile_url_targets,
    extract_github_profile_claims,
    extract_fr_registry_affiliated_people,
    extract_registry_affiliated_people,
    extract_icij_offshore_claims,
    extract_official_website_affiliated_people,
    extract_profile_url_evidence_claims,
    extract_user_scanner_claims,
    extract_wikidata_affiliation_people,
    extract_wikipedia_person_claims,
    github_profile_targets,
    normalize_github_public_profile,
    normalize_fr_business_entities,
    normalize_gleif_legal_entities,
    normalize_google_places_search_candidates,
    normalize_icij_offshore_matches,
    normalize_legal_jurisdiction,
    normalize_cloudflare_dns_context,
    normalize_official_website_url,
    normalize_official_website_public_content,
    normalize_public_web_organization_findings,
    normalize_public_web_organization_sources,
    normalize_unfurl_url_analysis,
    normalize_user_scanner_results,
    normalize_wayback_capture_index,
    normalize_wikidata_affiliated_people,
    normalize_wikidata_entity_candidates,
    normalize_wikidata_organization,
    normalize_wikipedia_candidates,
    run_github_public_profile,
    run_fr_business_registry_search,
    run_gleif_legal_entity_search,
    run_google_places_business_search,
    run_google_places_live_details,
    run_cloudflare_dns_context,
    run_icij_offshore_match,
    run_official_website_public_content,
    run_wayback_capture_index,
    run_wikidata_affiliation_discovery,
    run_wikipedia_person_enrichment,
    user_scanner_email_targets,
    validate_google_places_connection,
)


def _claimed_github(username="alice", url="https://github.com/alice"):
    result = MaigretCheckResult(
        username=username,
        site_name="GitHub",
        site_url_user=url,
        status=MaigretCheckStatus.CLAIMED,
    )
    return (username, "username", {"GitHub": {"status": result, "url_user": url}})


def _claimed_profile(
    username="alice", site_name="Example Social", url="https://social.example/alice"
):
    result = MaigretCheckResult(
        username=username,
        site_name=site_name,
        site_url_user=url,
        status=MaigretCheckStatus.CLAIMED,
    )
    return (username, "username", {site_name: {"status": result, "url_user": url}})


def _github_profile(**overrides):
    profile = {
        "login": "alice",
        "id": 12345,
        "type": "User",
        "html_url": "https://github.com/alice",
        "name": "Alice Example",
        "company": "Example Cooperative",
        "location": "Jakarta",
        "bio": "Public-interest technologist",
        "blog": "https://alice.example",
        "twitter_username": "alice_public",
        "avatar_url": "https://avatars.githubusercontent.com/u/12345?v=4",
        "created_at": "2012-01-02T03:04:05Z",
        "updated_at": "2026-08-30T10:00:00Z",
        "followers": 12,
        "following": 3,
        "public_repos": 8,
        "public_gists": 1,
    }
    profile.update(overrides)
    return profile


def test_user_scanner_email_targets_require_opt_in_and_one_subject_binding():
    plan = {
        "processing_mode": "same_subject",
        "enable_user_scanner_email": True,
        "identifiers": [
            {"type": "username", "value": "alice"},
            {"type": "email", "value": "Alice@Example.test"},
        ],
    }

    assert user_scanner_email_targets(plan) == ["alice@example.test"]
    assert (
        user_scanner_email_targets({**plan, "enable_user_scanner_email": False}) == []
    )
    assert user_scanner_email_targets({**plan, "processing_mode": "independent"}) == []


def test_user_scanner_results_are_bounded_and_normalized():
    observations = normalize_user_scanner_results(
        "alice@example.test",
        [
            {
                "status": "Registered",
                "site_name": "Gravatar",
                "category": "social",
                "url": "https://gravatar.com",
                "extra": {"username": "alice", "nested": {"ignored": True}},
                "media": {"avatar": "https://example.test/avatar.png"},
            },
            {
                "status": "Skipped",
                "site_name": "Medium",
                "reason": "Notifies the target",
            },
        ],
    )

    assert len(observations) == 2
    assert observations[0]["source_engine"] == USER_SCANNER_ENGINE
    assert observations[0]["subject_value"] == "alice@example.test"
    assert observations[0]["extra"] == {"username": "alice"}
    assert observations[0]["media"]["avatar"].startswith("https://")
    assert observations[1]["status"] == "Skipped"


def test_only_positive_registrations_become_pending_claim_candidates():
    observations = normalize_user_scanner_results(
        "alice@example.test",
        [
            {
                "status": "Registered",
                "site_name": "Gravatar",
                "url": "https://gravatar.com",
                "extra": {"username": "alice"},
            },
            {
                "status": "Not Registered",
                "site_name": "Example",
                "url": "https://example.test",
            },
        ],
    )

    candidates = extract_user_scanner_claims(observations)

    assert len(candidates) == 1
    assert candidates[0]["field_name"] == "account_registration"
    assert candidates[0]["source_engine"] == USER_SCANNER_ENGINE
    assert candidates[0]["native_status"] == "registered"
    assert candidates[0]["source_record_id"].startswith(f"{USER_SCANNER_ENGINE}:")
    assert candidates[0]["confidence"] == 55
    assert candidates[0]["evidence"][0]["evidence_type"] == "email_registration_probe"


def test_github_targets_require_opt_in_and_a_native_claimed_exact_profile():
    results = [
        _claimed_github(),
        _claimed_github("mallory", "https://evil.example/mallory"),
    ]

    assert (
        github_profile_targets(
            [_claimed_github("mallory", "https://github.com:invalid/mallory")],
            {"enable_github_profile_enrichment": True},
        )
        == []
    )

    assert github_profile_targets(results, {}) == []
    assert github_profile_targets(
        results, {"enable_github_profile_enrichment": True}
    ) == [
        {
            "investigated_username": "alice",
            "github_login": "alice",
            "profile_url": "https://github.com/alice",
        }
    ]


def test_github_public_profile_maps_facts_and_keeps_activity_as_metadata():
    target = {
        "investigated_username": "alice",
        "github_login": "alice",
        "profile_url": "https://github.com/alice",
    }
    observation = normalize_github_public_profile(target, _github_profile())
    candidates = extract_github_profile_claims([observation])

    assert observation["source_engine"] == GITHUB_ENGINE
    assert observation["source_record_id"] == "github-user:12345"
    assert observation["extra"]["followers"] == 12
    assert {candidate["field_name"] for candidate in candidates} == {
        "social_account",
        "platform_identifier",
        "full_name",
        "company",
        "current_location",
        "summary",
        "website",
        "photograph",
        "linked_profile_lead",
    }
    assert not {
        "created_at",
        "updated_at",
        "followers",
        "following",
        "public_repos",
        "public_gists",
    }.intersection(candidate["field_name"] for candidate in candidates)
    assert all(candidate["native_status"] == "observed" for candidate in candidates)
    assert all(
        candidate["source_record_id"] == "github-user:12345" for candidate in candidates
    )
    identifier = next(
        candidate
        for candidate in candidates
        if candidate["field_name"] == "platform_identifier"
    )
    assert identifier["value"]["identifier_type"] == "github_id"
    assert identifier["evidence"][0]["details"]["human_review_required"] is True


def test_github_organization_observation_never_becomes_a_persona_claim():
    target = {
        "investigated_username": "openai",
        "github_login": "openai",
        "profile_url": "https://github.com/openai",
    }
    observation = normalize_github_public_profile(
        target,
        _github_profile(
            login="openai",
            id=14957082,
            type="Organization",
            html_url="https://github.com/openai",
        ),
    )

    assert observation["status"] == "unsupported_account_type"
    assert extract_github_profile_claims([observation]) == []


class _FakeContent:
    def __init__(self, body):
        self.body = body

    async def read(self, _limit):
        return self.body


class _FakeResponse:
    def __init__(self, *, status, body=b"{}", headers=None):
        self.status = status
        self.headers = headers or {}
        self.content = _FakeContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _FakeSession:
    def __init__(self, response, calls, **options):
        self.response = response
        self.calls = calls
        self.calls.append(("session", options))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, url, **options):
        self.calls.append(("get", {"url": url, **options}))
        return self.response


@pytest.mark.asyncio
async def test_github_request_uses_fixed_origin_version_and_no_redirects():
    calls = []
    response = _FakeResponse(
        status=200,
        body=json.dumps(_github_profile()).encode(),
        headers={"X-RateLimit-Remaining": "59", "X-RateLimit-Reset": "1780000000"},
    )
    target = {
        "investigated_username": "alice",
        "github_login": "alice",
        "profile_url": "https://github.com/alice",
    }

    observation = await run_github_public_profile(
        target,
        session_factory=lambda **options: _FakeSession(response, calls, **options),
    )

    session_options = calls[0][1]
    request_options = calls[1][1]
    assert session_options["headers"]["X-GitHub-Api-Version"] == GITHUB_API_VERSION
    assert "Authorization" not in session_options["headers"]
    assert request_options == {
        "url": f"{GITHUB_API_BASE_URL}/users/alice",
        "allow_redirects": False,
    }
    assert observation["extra"]["rate_limit_remaining"] == "59"


@pytest.mark.asyncio
async def test_github_rate_limit_becomes_a_bounded_diagnostic():
    calls = []
    response = _FakeResponse(
        status=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1780000000"},
    )
    target = {
        "investigated_username": "alice",
        "github_login": "alice",
        "profile_url": "https://github.com/alice",
    }

    observation = await run_github_public_profile(
        target,
        session_factory=lambda **options: _FakeSession(response, calls, **options),
    )

    assert observation["status"] == "rate_limited"
    assert observation["extra"]["rate_limit_remaining"] == "0"
    assert extract_github_profile_claims([observation]) == []


def test_profile_url_evidence_targets_require_opt_in_native_claim_and_safe_url():
    plan = {"enable_archived_url_evidence": True}
    results = [
        _claimed_profile(),
        _claimed_profile("bob", "Other", "https://other.example/bob"),
        _claimed_profile("mallory", "Unsafe", "https://example.test/u?token=secret"),
        _claimed_profile("eve", "Unsafe", "https://example.test/u?apiKey=secret"),
    ]

    assert claimed_profile_url_targets(results, {}) == []
    assert claimed_profile_url_targets(results, plan) == [
        {
            "investigated_username": "alice",
            "site_name": "Example Social",
            "profile_url": "https://social.example/alice",
        },
        {
            "investigated_username": "bob",
            "site_name": "Other",
            "profile_url": "https://other.example/bob",
        },
    ]


def _url_target():
    return {
        "investigated_username": "alice",
        "site_name": "Example Social",
        "profile_url": "https://social.example/alice",
    }


def test_unfurl_analysis_is_offline_bounded_and_structural_only():
    observation = normalize_unfurl_url_analysis(
        _url_target(),
        {
            "schema_version": 1,
            "engine": "dfir-unfurl",
            "version": UNFURL_VERSION,
            "remote_lookups": False,
            "nodes": [
                {
                    "id": 1,
                    "data_type": "url",
                    "key": None,
                    "value": "https://social.example/alice",
                    "parent_id": None,
                },
                {
                    "id": 2,
                    "data_type": "url.query.pair",
                    "key": "access_token",
                    "value": "must-not-survive",
                    "parent_id": 1,
                },
            ],
        },
    )

    assert observation["source_engine"] == UNFURL_ENGINE
    assert observation["status"] == "analyzed"
    assert observation["extra"]["remote_lookups"] is False
    assert observation["extra"]["nodes"][1]["value"] == "[redacted]"
    candidate = extract_profile_url_evidence_claims([observation])[0]
    assert candidate["field_name"] == "social_account"
    assert candidate["confidence"] == 25
    details = candidate["evidence"][0]["details"]
    assert details["structural_analysis_only"] is True
    assert details["does_not_establish_ownership"] is True


def _wayback_rows(original="https://social.example/alice"):
    return [
        ["timestamp", "original", "statuscode", "mimetype", "digest"],
        ["20240102030405", original, "200", "text/html", "DIGESTONE"],
        ["20260304050607", original, "200", "text/html", "DIGESTTWO"],
    ]


def test_wayback_capture_metadata_becomes_evidence_not_an_identity_fact():
    observation = normalize_wayback_capture_index(
        _url_target(), _wayback_rows("https://SOCIAL.EXAMPLE/alice")
    )
    candidates = extract_profile_url_evidence_claims([observation])

    assert observation["source_engine"] == WAYBACK_ENGINE
    assert observation["status"] == "archived"
    assert observation["extra"]["sampled_capture_count"] == 2
    assert observation["extra"]["archived_page_content_fetched"] is False
    assert len(candidates) == 1
    assert candidates[0]["field_name"] == "social_account"
    assert candidates[0]["value"]["url"] == "https://social.example/alice"
    assert candidates[0]["confidence"] == 25
    details = candidates[0]["evidence"][0]["details"]
    assert details["historical_presence_only"] is True
    assert details["does_not_establish_ownership"] is True


def test_wayback_empty_result_is_a_diagnostic_and_mismatched_rows_are_rejected():
    diagnostic = normalize_wayback_capture_index(_url_target(), [])
    assert diagnostic["status"] == "not_archived"
    assert extract_profile_url_evidence_claims([diagnostic]) == []

    with pytest.raises(ValueError, match="no valid exact capture"):
        normalize_wayback_capture_index(
            _url_target(), _wayback_rows("https://evil.example/alice")
        )


@pytest.mark.asyncio
async def test_wayback_request_is_fixed_exact_bounded_and_has_no_redirects():
    calls = []
    response = _FakeResponse(status=200, body=json.dumps(_wayback_rows()).encode())

    observation = await run_wayback_capture_index(
        _url_target(),
        session_factory=lambda **options: _FakeSession(response, calls, **options),
    )

    session_options = calls[0][1]
    request_options = calls[1][1]
    assert "Authorization" not in session_options["headers"]
    assert request_options["url"] == WAYBACK_API_BASE_URL
    assert request_options["allow_redirects"] is False
    params = request_options["params"]
    assert ("url", "https://social.example/alice") in params
    assert ("matchType", "exact") in params
    assert ("limit", "-10") in params
    assert ("filter", "statuscode:200") in params
    assert observation["status"] == "archived"


def _wikidata_search():
    return {
        "search": [
            {
                "id": "Q95",
                "label": "Example Organization",
                "description": "Example",
                "match": {"text": "Example Organization"},
            }
        ]
    }


def _wikidata_organization():
    return {
        "entities": {
            "Q95": {
                "labels": {"en": {"value": "Example Organization"}},
                "descriptions": {"en": {"value": "Example"}},
                "claims": {
                    "P31": [
                        {
                            "mainsnak": {
                                "datavalue": {"value": {"id": "Q43229"}}
                            }
                        }
                    ],
                    "P856": [
                        {"mainsnak": {"datavalue": {"value": "https://example.org"}}}
                    ]
                },
            }
        }
    }


def _wikidata_people():
    return {
        "results": {
            "bindings": [
                {
                    "person": {
                        "type": "uri",
                        "value": "http://www.wikidata.org/entity/Q1001",
                    },
                    "personLabel": {"type": "literal", "value": "Alice Example"},
                    "personDescription": {"type": "literal", "value": "Example person"},
                    "property": {
                        "type": "uri",
                        "value": "http://www.wikidata.org/prop/direct/P108",
                    },
                    "direction": {"type": "literal", "value": "person_to_organization"},
                }
            ]
        }
    }


def _wikidata_unlabeled_people():
    payload = _wikidata_people()
    binding = payload["results"]["bindings"][0]
    binding.pop("personLabel")
    binding.pop("personDescription")
    return payload


def _wikidata_person_entities():
    return {
        "entities": {
            "Q1001": {
                "labels": {"en": {"value": "Alice Example"}},
                "descriptions": {"en": {"value": "Example person"}},
            }
        }
    }


def test_wikidata_affiliation_values_are_bounded_pending_claim_inputs():
    candidates = normalize_wikidata_entity_candidates(
        "Example Organization", _wikidata_search()
    )
    assert candidates[0]["exact_match"] is True
    organization = normalize_wikidata_organization("Q95", _wikidata_organization())
    people = normalize_wikidata_affiliated_people(_wikidata_people())
    proposals = extract_wikidata_affiliation_people(
        {
            "source_engine": WIKIDATA_ENGINE,
            "status": "observed",
            "organization": organization,
            "people": people,
        }
    )
    assert organization["official_websites"] == ["https://example.org"]
    assert {claim["field_name"] for claim in proposals[0]["claims"]} == {
        "full_name",
        "company",
        "platform_identifier",
    }
    assert all(
        claim["evidence"][0]["details"]["human_review_required"] is True
        for claim in proposals[0]["claims"]
    )


def test_wikidata_people_labels_are_resolved_from_the_bounded_entity_api():
    people = normalize_wikidata_affiliated_people(
        _wikidata_unlabeled_people(), _wikidata_person_entities()
    )

    assert people == [
        {
            "id": "Q1001",
            "label": "Alice Example",
            "description": "Example person",
            "url": "https://www.wikidata.org/wiki/Q1001",
            "relations": [
                {
                    "property_id": "P108",
                    "label": "employer",
                    "direction": "person_to_organization",
                }
            ],
        }
    ]


def test_wikidata_affiliation_rejects_malformed_binding_and_relation_documents():
    payload = _wikidata_people()
    payload["results"]["bindings"][0]["personLabel"] = "unexpected scalar"
    assert normalize_wikidata_affiliated_people(payload) == []

    organization = normalize_wikidata_organization("Q95", _wikidata_organization())
    people = _wikidata_people()
    normalized_people = normalize_wikidata_affiliated_people(people)
    normalized_people[0]["relations"][0]["direction"] = "unexpected"
    assert (
        extract_wikidata_affiliation_people(
            {
                "source_engine": WIKIDATA_ENGINE,
                "status": "observed",
                "organization": organization,
                "people": normalized_people,
            }
        )
        == []
    )


def test_wikidata_caps_distinct_people_without_dropping_their_relation_rows():
    bindings = []
    for index in range(1, 52):
        for property_id in ("P108", "P463"):
            bindings.append(
                {
                    "person": {
                        "type": "uri",
                        "value": f"http://www.wikidata.org/entity/Q{1000 + index}",
                    },
                    "personLabel": {
                        "type": "literal",
                        "value": f"Person {index}",
                    },
                    "property": {
                        "type": "uri",
                        "value": ("http://www.wikidata.org/prop/direct/" + property_id),
                    },
                    "direction": {
                        "type": "literal",
                        "value": "person_to_organization",
                    },
                }
            )

    people = normalize_wikidata_affiliated_people({"results": {"bindings": bindings}})
    query = _wikidata_people_query("Q95")

    assert len(people) == 50
    assert len(people[0]["relations"]) == 2
    assert people[-1]["id"] == "Q1050"
    assert "SELECT DISTINCT ?person ?property ?direction" in query
    assert "SERVICE wikibase:label" not in query
    assert "SELECT DISTINCT ?person WHERE" not in query
    assert "ORDER BY" not in query
    assert "LIMIT 450" in query


class _FakeSequenceSession:
    def __init__(self, responses, calls, **options):
        self.responses = list(responses)
        self.calls = calls
        calls.append(("session", options))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, url, **options):
        self.calls.append(("get", {"url": url, **options}))
        return self.responses.pop(0)

    def post(self, url, **options):
        self.calls.append(("post", {"url": url, **options}))
        return self.responses.pop(0)


class _TimeoutResponse:
    async def __aenter__(self):
        raise TimeoutError()

    async def __aexit__(self, *_args):
        return False


@pytest.mark.asyncio
async def test_google_places_text_search_keeps_only_place_ids_and_fixed_origin():
    calls = []
    response = _FakeResponse(
        status=200,
        body=json.dumps(
            {
                "places": [
                    {
                        "id": "ChIJCzjlUUSv4S4RCiu9uL4NlvE",
                        "displayName": {"text": "must not be retained"},
                        "formattedAddress": "must not be retained",
                    }
                ]
            }
        ).encode(),
    )

    observation = await run_google_places_business_search(
        "Unistellar",
        "restricted-server-key",
        legal_jurisdiction="ID",
        session_factory=lambda **options: _FakeSequenceSession(
            [response], calls, **options
        ),
    )

    assert observation["source_engine"] == GOOGLE_PLACES_ENGINE
    assert observation["status"] == "observed"
    assert observation["query_context"]["jurisdiction_code"] == "ID"
    assert observation["durable_google_content_stored"] is False
    assert observation["candidates"] == [
        {
            "place_id": "ChIJCzjlUUSv4S4RCiu9uL4NlvE",
            "source_url": (
                "https://www.google.com/maps/search/?api=1&query=Unistellar&"
                "query_place_id=ChIJCzjlUUSv4S4RCiu9uL4NlvE"
            ),
            "review_status": "pending",
            "automatic_approval_allowed": False,
            "durable_google_content_stored": False,
        }
    ]
    session_options = next(value for kind, value in calls if kind == "session")
    assert session_options["headers"]["X-Goog-Api-Key"] == "restricted-server-key"
    assert session_options["headers"]["X-Goog-FieldMask"] == "places.id"
    request = next(value for kind, value in calls if kind == "post")
    assert request["url"] == GOOGLE_PLACES_SEARCH_URL
    assert request["allow_redirects"] is False
    assert request["json"]["textQuery"] == "Unistellar, Indonesia"
    assert request["json"]["regionCode"] == "ID"
    assert "restricted-server-key" not in json.dumps(request)


def test_google_places_candidate_normalization_rejects_invalid_or_duplicate_ids():
    assert normalize_google_places_search_candidates(
        "Unistellar",
        {
            "places": [
                {"id": "short"},
                {"id": "ChIJCzjlUUSv4S4RCiu9uL4NlvE"},
                {"id": "ChIJCzjlUUSv4S4RCiu9uL4NlvE"},
            ]
        },
    )[0]["place_id"] == "ChIJCzjlUUSv4S4RCiu9uL4NlvE"


@pytest.mark.asyncio
async def test_google_places_details_are_live_review_leads_and_block_private_data():
    calls = []
    responses = [
        _FakeResponse(
            status=200,
            body=json.dumps(
                {
                    "id": "ChIJCzjlUUSv4S4RCiu9uL4NlvE",
                    "displayName": {"text": "Unistellar"},
                    "formattedAddress": (
                        "Jl. Kemang Timur No. 28, Jakarta 12730, Indonesia"
                    ),
                    "businessStatus": "OPERATIONAL",
                    "types": ["establishment", "finance", "point_of_interest"],
                    "googleMapsUri": "https://maps.google.com/?cid=12345",
                }
            ).encode(),
        ),
        _FakeResponse(
            status=200,
            body=json.dumps(
                {
                    "id": "ChIJPrivateResidence123456",
                    "displayName": {"text": "Private residence"},
                    "formattedAddress": "8 Private Road, Jakarta 12730",
                    "types": ["establishment", "point_of_interest"],
                }
            ).encode(),
        ),
        _FakeResponse(
            status=200,
            body=json.dumps(
                {
                    "id": "ChIJPersonalListing123456",
                    "displayName": {"text": "Alice Doe"},
                    "formattedAddress": "8 Oak Road, Jakarta 12730",
                    "types": ["establishment", "point_of_interest"],
                }
            ).encode(),
        ),
    ]

    result = await run_google_places_live_details(
        "Unistellar",
        [
            "ChIJCzjlUUSv4S4RCiu9uL4NlvE",
            "ChIJPrivateResidence123456",
            "ChIJPersonalListing123456",
        ],
        "restricted-server-key",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert result["status"] == "partial"
    assert result["durable_google_content_stored"] is False
    assert len(result["places"]) == 1
    place = result["places"][0]
    assert place["display_name"] == "Unistellar"
    assert place["formatted_address"].startswith("Jl. Kemang Timur")
    assert place["review_status"] == "pending"
    assert place["automatic_approval_allowed"] is False
    assert place["source_url"] == "https://maps.google.com/?cid=12345"
    requests = [value for kind, value in calls if kind == "get"]
    assert all(
        request["url"].startswith(f"{GOOGLE_PLACES_DETAILS_URL}/")
        and request["allow_redirects"] is False
        for request in requests
    )


@pytest.mark.asyncio
async def test_google_places_connection_validation_uses_text_search():
    calls = []
    assert await validate_google_places_connection(
        "restricted-server-key",
        session_factory=lambda **options: _FakeSequenceSession(
            [
                _FakeResponse(
                    status=200,
                    body=json.dumps(
                        {"places": [{"id": "ChIJN1t_tDeuEmsRUsoyG83frY4"}]}
                    ).encode(),
                )
            ],
            calls,
            **options,
        ),
    )
    request = next(value for kind, value in calls if kind == "post")
    assert request["url"] == GOOGLE_PLACES_SEARCH_URL


@pytest.mark.asyncio
async def test_wikidata_runtime_uses_only_fixed_bounded_endpoints():
    calls = []
    responses = [
        _FakeResponse(status=200, body=json.dumps(_wikidata_search()).encode()),
        _FakeResponse(status=200, body=json.dumps(_wikidata_organization()).encode()),
        _FakeResponse(
            status=200, body=json.dumps(_wikidata_unlabeled_people()).encode()
        ),
        _FakeResponse(
            status=200, body=json.dumps(_wikidata_person_entities()).encode()
        ),
    ]
    observation = await run_wikidata_affiliation_discovery(
        "Example Organization",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )
    assert observation["status"] == "observed"
    requests = [item[1] for item in calls if item[0] == "get"]
    assert [item["url"] for item in requests] == [
        WIKIDATA_API_URL,
        WIKIDATA_API_URL,
        WIKIDATA_QUERY_URL,
        WIKIDATA_API_URL,
    ]
    assert all(item["allow_redirects"] is False for item in requests)
    assert "Authorization" not in calls[0][1]["headers"]
    query_request = requests[2]
    assert "LIMIT 450" in query_request["params"]["query"]
    assert "SERVICE wikibase:label" not in query_request["params"]["query"]
    assert requests[3]["params"]["ids"] == "Q1001"
    assert requests[3]["params"]["props"] == "labels|descriptions"


@pytest.mark.asyncio
async def test_wikidata_relation_timeout_preserves_the_resolved_organization():
    calls = []
    responses = [
        _FakeResponse(status=200, body=json.dumps(_wikidata_search()).encode()),
        _FakeResponse(status=200, body=json.dumps(_wikidata_organization()).encode()),
        _TimeoutResponse(),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Organization",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "partial"
    assert observation["organization"]["id"] == "Q95"
    assert observation["people"] == []
    assert observation["extra"]["affiliation_people_status"] == "unavailable"
    assert "No zero-result conclusion" in observation["reason"]


@pytest.mark.asyncio
async def test_wikidata_exact_name_requires_review_when_case_context_conflicts():
    calls = []
    responses = [
        _FakeResponse(status=200, body=json.dumps(_wikidata_search()).encode()),
        _FakeResponse(status=200, body=json.dumps(_wikidata_organization()).encode()),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Organization",
        official_website="https://unistellar.co",
        legal_jurisdiction="ID",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "needs_selection"
    candidate = observation["organization_candidates"][0]
    assert candidate["context_status"] == "conflict"
    assert candidate["context_note"] == (
        "Supplied website unistellar.co differs from Wikidata website example.org. "
        "Jurisdiction ID requires analyst confirmation; an exact name is not "
        "jurisdiction proof."
    )
    assert candidate["official_websites"] == ["https://example.org"]
    assert [item[1]["url"] for item in calls if item[0] == "get"] == [
        WIKIDATA_API_URL,
        WIKIDATA_API_URL,
    ]


@pytest.mark.asyncio
async def test_wikidata_matching_official_domain_can_resolve_without_jurisdiction():
    calls = []
    responses = [
        _FakeResponse(status=200, body=json.dumps(_wikidata_search()).encode()),
        _FakeResponse(status=200, body=json.dumps(_wikidata_organization()).encode()),
        _FakeResponse(
            status=200, body=json.dumps(_wikidata_unlabeled_people()).encode()
        ),
        _FakeResponse(
            status=200, body=json.dumps(_wikidata_person_entities()).encode()
        ),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Organization",
        official_website="https://www.example.org/about",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "observed"
    assert observation["organization"]["id"] == "Q95"
    assert observation["people"][0]["label"] == "Alice Example"


@pytest.mark.asyncio
async def test_wikidata_jurisdiction_pauses_exact_name_auto_selection():
    calls = []
    responses = [
        _FakeResponse(status=200, body=json.dumps(_wikidata_search()).encode()),
        _FakeResponse(status=200, body=json.dumps(_wikidata_organization()).encode()),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Organization",
        legal_jurisdiction="ID",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "needs_selection"
    candidate = observation["organization_candidates"][0]
    assert candidate["context_status"] == "review_required"
    assert "Jurisdiction ID" in candidate["context_note"]
    assert "exact name alone is insufficient" in observation["reason"]


@pytest.mark.asyncio
async def test_wikidata_article_is_retained_but_cannot_be_selected_as_organization():
    calls = []
    search = {
        "search": [
            {
                "id": "Q127199078",
                "label": "Unistellar eVscopes",
                "description": "scholarly article",
                "match": {"text": "Unistellar eVscopes"},
            }
        ]
    }
    entity = {
        "entities": {
            "Q127199078": {
                "labels": {"en": {"value": "Unistellar eVscopes"}},
                "descriptions": {"en": {"value": "scholarly article"}},
                "claims": {
                    "P31": [
                        {
                            "mainsnak": {
                                "datavalue": {"value": {"id": "Q13442814"}}
                            }
                        }
                    ]
                },
            }
        }
    }
    responses = [
        _FakeResponse(status=200, body=json.dumps(search).encode()),
        _FakeResponse(status=200, body=json.dumps(entity).encode()),
        _FakeResponse(
            status=200,
            body=json.dumps(
                {
                    "entities": {
                        "Q13442814": {"claims": {"P279": []}}
                    }
                }
            ).encode(),
        ),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Unistellar eVscopes",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "needs_selection"
    candidate = observation["organization_candidates"][0]
    assert candidate["organization_eligible"] is False
    assert candidate["organization_type_status"] == (
        "not_verified_as_organization"
    )
    assert "type-verified Wikidata organization" in observation["reason"]
    assert len([item for item in calls if item[0] == "get"]) == 3


@pytest.mark.asyncio
async def test_wikidata_organization_subclass_is_type_verified_before_selection():
    calls = []
    search = {
        "search": [
            {
                "id": "Q900001",
                "label": "Example Hospital",
                "description": "public hospital",
                "match": {"text": "Example Hospital"},
            }
        ]
    }
    entity = {
        "entities": {
            "Q900001": {
                "labels": {"en": {"value": "Example Hospital"}},
                "descriptions": {"en": {"value": "public hospital"}},
                "claims": {
                    "P31": [
                        {
                            "mainsnak": {
                                "datavalue": {"value": {"id": "Q16917"}}
                            }
                        }
                    ]
                },
            }
        }
    }
    class_hierarchy = {
        "entities": {
            "Q16917": {
                "claims": {
                    "P279": [
                        {
                            "mainsnak": {
                                "datavalue": {"value": {"id": "Q43229"}}
                            }
                        }
                    ]
                }
            }
        }
    }
    relation_result = {"results": {"bindings": []}}
    responses = [
        _FakeResponse(status=200, body=json.dumps(search).encode()),
        _FakeResponse(status=200, body=json.dumps(entity).encode()),
        _FakeResponse(status=200, body=json.dumps(class_hierarchy).encode()),
        _FakeResponse(status=200, body=json.dumps(relation_result).encode()),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Hospital",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "observed"
    candidate = observation["organization_candidates"][0]
    assert candidate["organization_eligible"] is True
    assert candidate["organization_type_status"] == "verified_organization"
    requests = [item[1] for item in calls if item[0] == "get"]
    assert requests[2]["params"]["ids"] == "Q16917"
    assert requests[2]["params"]["props"] == "claims"
    assert len(requests) == 4


@pytest.mark.asyncio
async def test_wikidata_keeps_verified_branch_when_later_class_fetch_fails():
    calls = []
    search = {
        "search": [
            {
                "id": "Q900020",
                "label": "Example Institute",
                "description": "public institute",
                "match": {"text": "Example Institute"},
            }
        ]
    }
    entity = {
        "entities": {
            "Q900020": {
                "labels": {"en": {"value": "Example Institute"}},
                "descriptions": {"en": {"value": "public institute"}},
                "claims": {
                    "P31": [
                        {"mainsnak": {"datavalue": {"value": {"id": "Q900021"}}}},
                        {"mainsnak": {"datavalue": {"value": {"id": "Q900022"}}}},
                    ]
                },
            }
        }
    }
    first_hierarchy = {
        "entities": {
            "Q900021": {
                "claims": {
                    "P279": [
                        {"mainsnak": {"datavalue": {"value": {"id": "Q43229"}}}}
                    ]
                }
            },
            "Q900022": {
                "claims": {
                    "P279": [
                        {"mainsnak": {"datavalue": {"value": {"id": "Q900023"}}}}
                    ]
                }
            },
        }
    }
    responses = [
        _FakeResponse(status=200, body=json.dumps(search).encode()),
        _FakeResponse(status=200, body=json.dumps(entity).encode()),
        _FakeResponse(status=200, body=json.dumps(first_hierarchy).encode()),
        _FakeResponse(status=429, body=b"", headers={"Retry-After": "60"}),
        _FakeResponse(
            status=200,
            body=json.dumps({"results": {"bindings": []}}).encode(),
        ),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Institute",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "observed"
    candidate = observation["organization_candidates"][0]
    assert candidate["organization_eligible"] is True
    assert candidate["organization_type_status"] == "verified_organization"
    requests = [item[1] for item in calls if item[0] == "get"]
    assert requests[2]["params"]["ids"] == "Q900021|Q900022"
    assert requests[3]["params"]["ids"] == "Q900023"
    assert len(requests) == 5


@pytest.mark.asyncio
async def test_wikidata_type_request_failure_is_not_negative_type_evidence():
    calls = []
    responses = [
        _FakeResponse(status=200, body=json.dumps(_wikidata_search()).encode()),
        _FakeResponse(status=429, body=b"", headers={"Retry-After": "60"}),
    ]

    observation = await run_wikidata_affiliation_discovery(
        "Example Organization",
        session_factory=lambda **options: _FakeSequenceSession(
            responses, calls, **options
        ),
    )

    assert observation["status"] == "rate_limited"
    assert "type verification was unavailable" in observation["reason"]
    assert observation["organization_candidates"][0].get(
        "organization_eligible"
    ) is None


def _gleif_entities():
    return {
        "data": [
            {
                "id": "9695005MSX1OYEMGDF46",
                "attributes": {
                    "lei": "9695005MSX1OYEMGDF46",
                    "entity": {
                        "legalName": {"name": "UNISTELLAR"},
                        "otherNames": [{"name": "Unistellar SAS"}],
                        "jurisdiction": "FR",
                        "registeredAt": {"id": "RA000189"},
                        "registeredAs": "812339356",
                        "status": "ACTIVE",
                        "legalAddress": {
                            "addressLines": ["5 Avenue du General Leclerc"],
                            "city": "Marseille",
                            "region": "FR-13",
                            "country": "FR",
                            "postalCode": "13003",
                        },
                        "headquartersAddress": {
                            "addressLines": ["7 Rue Example"],
                            "city": "Marseille",
                            "region": "FR-13",
                            "country": "FR",
                            "postalCode": "13003",
                        },
                    },
                    "registration": {
                        "status": "ISSUED",
                        "corroborationLevel": "FULLY_CORROBORATED",
                        "initialRegistrationDate": "2020-01-01T00:00:00Z",
                        "lastUpdateDate": "2026-01-01T00:00:00Z",
                    },
                },
            }
        ]
    }


def _fr_business_entities():
    return {
        "results": [
            {
                "siren": "812339356",
                "nom_complet": "UNISTELLAR",
                "nom_raison_sociale": "UNISTELLAR",
                "etat_administratif": "A",
                "date_creation": "2015-07-06",
                "date_mise_a_jour": "2026-08-01T00:00:00Z",
                "nature_juridique": "5710",
                "activite_principale": "26.70Z",
                "libelle_activite_principale": (
                    "Fabrication de matériels optique et photographique"
                ),
                "matching_etablissements": [
                    {
                        "siret": "81233935600048",
                        "adresse": "12 RUE EXAMPLE 13003 MARSEILLE",
                        "libelle_commune": "Marseille",
                        "code_postal": "13003",
                        "etat_administratif": "A",
                    }
                ],
                "siege": {
                    "siret": "81233935600030",
                    "adresse": "5 AVENUE DU GENERAL LECLERC 13003 MARSEILLE",
                    "libelle_commune": "Marseille",
                    "region": "93",
                    "code_postal": "13003",
                },
                "dirigeants": [
                    {
                        "type_dirigeant": "personne physique",
                        "prenoms": "Arnaud",
                        "nom": "Malvache",
                        "qualite": "Président de SAS",
                        "annee_de_naissance": "1977",
                        "mois_de_naissance": "04",
                        "nationalite": "Française",
                    },
                    {
                        "type_dirigeant": "personne physique",
                        "prenoms": "Laurent",
                        "nom": "Marfisi",
                        "qualite": "Directeur Général",
                    },
                ],
            }
        ]
    }


def test_legal_jurisdiction_normalization_is_iso_backed():
    assert normalize_legal_jurisdiction("France") == {
        "code": "FR",
        "label": "France",
        "country_code": "FR",
    }
    assert normalize_legal_jurisdiction("us-de")["code"] == "US-DE"
    assert normalize_legal_jurisdiction("") is None
    with pytest.raises(ValueError, match="country name"):
        normalize_legal_jurisdiction("the moon")


def test_registry_normalization_is_bounded_and_drops_private_person_fields():
    jurisdiction = normalize_legal_jurisdiction("FR")
    gleif = normalize_gleif_legal_entities(
        "Unistellar", jurisdiction, _gleif_entities()
    )
    france = normalize_fr_business_entities(
        "Unistellar", jurisdiction, _fr_business_entities()
    )

    assert gleif[0]["id"] == "9695005MSX1OYEMGDF46"
    assert gleif[0]["exact_name_match"] is True
    assert gleif[0]["headquarters_address"]["lines"] == ["7 Rue Example"]
    assert france[0]["id"] == "812339356"
    assert france[0]["headquarters_identifier"] == "81233935600030"
    assert france[0]["primary_activity_code"] == "26.70Z"
    assert france[0]["establishments"] == [
        {
            "siret": "81233935600048",
            "address": "12 RUE EXAMPLE 13003 MARSEILLE",
            "city": "Marseille",
            "postal_code": "13003",
            "status": "active",
        }
    ]
    assert france[0]["people"] == [
        {"display_name": "Arnaud Malvache", "role": "Président de SAS"},
        {"display_name": "Laurent Marfisi", "role": "Directeur Général"},
    ]
    serialized = json.dumps(france, ensure_ascii=False).casefold()
    assert "naissance" not in serialized
    assert "nationalite" not in serialized
    assert "française" not in serialized


def test_fr_registry_people_require_one_exact_entity_and_remain_review_inputs():
    jurisdiction = normalize_legal_jurisdiction("FR")
    candidates = normalize_fr_business_entities(
        "Unistellar", jurisdiction, _fr_business_entities()
    )
    observation = {
        "source_engine": FR_BUSINESS_REGISTRY_ENGINE,
        "status": "observed",
        "selected_entity": candidates[0],
    }
    people = extract_fr_registry_affiliated_people(observation)

    assert len(people) == 2
    assert {claim["field_name"] for claim in people[0]["claims"]} == {
        "full_name",
        "company",
        "occupation",
    }
    for person in people:
        for claim in person["claims"]:
            assert claim["source_engine"] == FR_BUSINESS_REGISTRY_ENGINE
            details = claim["evidence"][0]["details"]
            assert details["registry_identifier"] == "812339356"
            assert details["registry_identifier_type"] == "siren"
            assert details["human_review_required"] is True
            assert details["automatic_approval_allowed"] is False

    ambiguous = dict(observation, selected_entity=None)
    assert extract_fr_registry_affiliated_people(ambiguous) == []


def test_registry_people_contract_is_source_neutral_for_governed_adapters():
    observation = {
        "source_engine": GLEIF_ENGINE,
        "status": "observed",
        "selected_entity": {
            "id": "9695005MSX1OYEMGDF46",
            "identifier_type": "lei",
            "legal_name": "Example Global Organization",
            "legal_jurisdiction": "ID",
            "source_url": (
                "https://api.gleif.org/api/v1/lei-records/"
                "9695005MSX1OYEMGDF46"
            ),
            "analyst_selected": True,
            "people": [
                {"display_name": "Ayu Example", "role": "Managing Director"}
            ],
        },
    }

    people = extract_registry_affiliated_people(observation)

    assert len(people) == 1
    assert people[0]["registry_person_key"].startswith(
        "registry:gleif_lei_registry:"
    )
    assert {claim["field_name"] for claim in people[0]["claims"]} == {
        "full_name",
        "company",
        "occupation",
    }
    assert all(
        claim["source_engine"] == GLEIF_ENGINE
        for claim in people[0]["claims"]
    )
    assert all(
        claim["evidence"][0]["source_name"] == "GLEIF Global LEI Index"
        for claim in people[0]["claims"]
    )


@pytest.mark.asyncio
async def test_registry_requests_use_only_fixed_bounded_credential_free_endpoints():
    gleif_calls = []
    gleif_observation = await run_gleif_legal_entity_search(
        "Unistellar",
        "FR",
        session_factory=lambda **options: _FakeSequenceSession(
            [
                _FakeResponse(
                    status=200, body=json.dumps(_gleif_entities()).encode()
                )
            ],
            gleif_calls,
            **options,
        ),
    )
    gleif_request = gleif_calls[1][1]
    assert gleif_request["url"] == GLEIF_API_URL
    assert gleif_request["allow_redirects"] is False
    assert gleif_request["params"]["filter[entity.legalAddress.country]"] == "FR"
    assert gleif_request["params"]["page[size]"] == 20
    assert "Authorization" not in gleif_calls[0][1]["headers"]
    assert gleif_observation["source_engine"] == GLEIF_ENGINE

    france_calls = []
    france_observation = await run_fr_business_registry_search(
        "Unistellar",
        "FR",
        session_factory=lambda **options: _FakeSequenceSession(
            [
                _FakeResponse(
                    status=200,
                    body=json.dumps(_fr_business_entities()).encode(),
                )
            ],
            france_calls,
            **options,
        ),
    )
    france_request = france_calls[1][1]
    assert france_request["url"] == FR_BUSINESS_REGISTRY_URL
    assert france_request["allow_redirects"] is False
    assert france_request["params"] == {
        "q": "Unistellar",
        "page": 1,
        "per_page": 20,
    }
    assert "Authorization" not in france_calls[0][1]["headers"]
    assert france_observation["selected_entity"]["id"] == "812339356"

    with pytest.raises(ValueError, match="country-level FR"):
        await run_fr_business_registry_search("Unistellar", "FR-IDF")


def _cloudflare_dns_payload(record_type):
    answers = {
        "A": [{"name": "example.org", "type": 1, "TTL": 300, "data": "93.184.216.34"}],
        "AAAA": [{"name": "example.org", "type": 28, "TTL": 300, "data": "2606:2800:220:1:248:1893:25c8:1946"}],
        "MX": [{"name": "example.org", "type": 15, "TTL": 300, "data": "10 mail.example.org."}],
        "NS": [{"name": "example.org", "type": 2, "TTL": 300, "data": "ns1.example.org."}],
    }
    return {"Status": 0, "Answer": answers[record_type]}


def test_official_website_and_dns_context_are_bounded_observation_only():
    website = normalize_official_website_url("https://Example.org/about")
    context = normalize_cloudflare_dns_context(
        website,
        {
            query_type: _cloudflare_dns_payload(query_type)
            for query_type in ("A", "AAAA", "MX", "NS")
        },
    )

    assert website == {
        "url": "https://Example.org/about",
        "domain": "example.org",
    }
    assert context["record_count"] == 4
    assert context["records"]["mx"][0]["priority"] == 10
    assert context["records"]["ns"][0]["value"] == "ns1.example.org"
    assert context["registration_lookup_url"].endswith("name=example.org")
    with pytest.raises(ValueError, match="standard web ports"):
        normalize_official_website_url("https://example.org:8443")
    with pytest.raises(ValueError, match="public HTTP or HTTPS"):
        normalize_official_website_url("file:///etc/passwd")


@pytest.mark.asyncio
async def test_dns_context_uses_fixed_no_redirect_credential_free_queries():
    calls = []
    observation = await run_cloudflare_dns_context(
        "https://example.org",
        session_factory=lambda **options: _FakeSequenceSession(
            [
                _FakeResponse(
                    status=200,
                    body=json.dumps(_cloudflare_dns_payload(query_type)).encode(),
                )
                for query_type in ("A", "AAAA", "MX", "NS")
            ],
            calls,
            **options,
        ),
    )

    requests = [item[1] for item in calls if item[0] == "get"]
    assert len(requests) == 4
    assert all(item["url"] == CLOUDFLARE_DNS_URL for item in requests)
    assert all(item["allow_redirects"] is False for item in requests)
    assert [item["params"]["type"] for item in requests] == [
        "A",
        "AAAA",
        "MX",
        "NS",
    ]
    assert all(item["params"]["name"] == "example.org" for item in requests)
    assert "Authorization" not in calls[0][1]["headers"]
    assert observation["source_engine"] == CLOUDFLARE_DNS_ENGINE
    assert observation["status"] == "observed"
    assert observation["extra"]["operating_location_inference_allowed"] is False


def _official_website_html():
    return b"""<!doctype html>
    <html><head><title>Example Organization</title>
    <meta name="description" content="Example Organization builds public-interest technology in Indonesia.">
    </head><body><main>
      <h2>Contact and office</h2>
      <address>Jl. Kemang Timur No. 28, Jakarta 12730, Indonesia</address>
      <a href="mailto:corporate@example.org">Contact</a>
      <a href="https://www.linkedin.com/company/example-organization?trk=site">LinkedIn</a>
      <h2>Team</h2>
      <h3>Alice Example</h3><p>Chief Executive Officer</p>
      <p>Alice has worked in technology for many years.</p>
    </main></body></html>"""


def _unistellar_official_website_html():
    return b"""<!doctype html><html><head><title>Unistellar</title>
    <meta name="description" content="A business group with technology, education and data divisions.">
    </head><body><main>
      <h2>TEAM</h2>
      <h3>Prof. Roy Sembel</h3><p>Senior Advisor</p>
      <h3>Pascal Sembel</h3><p>Finance &amp; Investment</p>
      <h3>Ferdinata Suryanto</h3><p>Corporate Finance &amp; Investment</p>
      <h2>CONTACT</h2>
      <a href="mailto:corporate@unistellar.co">corporate@unistellar.co</a>
      <a href="https://www.linkedin.com/company/unistellar">Company profile</a>
    </main></body></html>"""


def _global_company_page_html():
    return b"""<!doctype html><html><head><title>Global Example</title></head>
    <body><main>
      <p>10 Rue de Rivoli, 75001 Paris, France</p>
      <p>Home address: 8 Private Road, 10000 Example</p>
      <a href="/kontakt">Kontakt</a>
      <a href="https://other.example/location">External location</a>
    </main></body></html>"""


def _global_contact_page_html():
    return b"""<!doctype html><html><head><title>Kontakt</title></head>
    <body><footer><div class="standort">
      <span>Friedrichstrasse 123, 10117 Berlin, Germany</span>
    </div></footer></body></html>"""


def test_official_website_content_extracts_exact_cited_context_and_people():
    observation = normalize_official_website_public_content(
        "Example Organization",
        "https://example.org",
        _official_website_html(),
    )
    people = extract_official_website_affiliated_people(observation)
    organization_candidate = build_organization_resolution_candidates(
        {}, website_observation=observation
    )[0]

    assert observation["source_engine"] == OFFICIAL_WEBSITE_ENGINE
    assert observation["status"] == "observed"
    assert observation["addresses"] == [
        "Jl. Kemang Timur No. 28, Jakarta 12730, Indonesia"
    ]
    assert observation["contacts"] == [
        {"type": "email", "value": "corporate@example.org"}
    ]
    assert observation["linked_company_profiles"] == [
        "https://www.linkedin.com/company/example-organization"
    ]
    assert observation["people"] == [
        {"display_name": "Alice Example", "role": "Chief Executive Officer"}
    ]
    assert observation["organization"]["name_observation_status"] == (
        "published_name_match"
    )
    assert organization_candidate["selectable"] is True
    assert organization_candidate["published_addresses"] == [
        "Jl. Kemang Timur No. 28, Jakarta 12730, Indonesia"
    ]
    assert "page title or description" in organization_candidate["basis"]
    assert "does not prove legal registration" in organization_candidate[
        "limitation"
    ]
    assert len(people) == 1
    assert {claim["field_name"] for claim in people[0]["claims"]} == {
        "full_name",
        "company",
        "occupation",
    }
    assert all(
        claim["evidence"][0]["details"]["human_review_required"] is True
        for claim in people[0]["claims"]
    )


@pytest.mark.asyncio
async def test_official_website_crawls_bounded_same_domain_context_pages_with_lineage():
    calls = []
    observation = await run_official_website_public_content(
        "Global Example",
        "https://example.org",
        host_resolver=lambda _hostname, _port: ["93.184.216.34"],
        session_factory=lambda **options: _FakeSequenceSession(
            [
                _FakeResponse(
                    status=200,
                    body=_global_company_page_html(),
                    headers={"Content-Type": "text/html"},
                ),
                _FakeResponse(
                    status=200,
                    body=_global_contact_page_html(),
                    headers={"Content-Type": "text/html"},
                ),
            ],
            calls,
            **options,
        ),
    )

    assert observation["addresses"] == [
        "10 Rue de Rivoli, 75001 Paris, France",
        "Friedrichstrasse 123, 10117 Berlin, Germany",
    ]
    assert observation["collected_pages"] == [
        "https://example.org",
        "https://example.org/kontakt",
    ]
    assert {
        item["source_url"] for item in observation["location_observations"]
    } == {"https://example.org", "https://example.org/kontakt"}
    assert all(
        item["verification_status"] == "pending"
        for item in observation["location_observations"]
    )
    assert all(
        "personal address" in item["limitation"]
        for item in observation["location_observations"]
    )
    assert len([item for item in calls if item[0] == "get"]) == 2


@pytest.mark.parametrize(
    "url",
    ["https://example.org:80", "http://example.org:443"],
)
def test_official_website_rejects_scheme_mismatched_ports(url):
    with pytest.raises(ValueError, match="standard web port"):
        normalize_official_website_url(url)


def test_official_website_rejects_sensitive_query_parameters():
    with pytest.raises(ValueError, match="credential-like"):
        normalize_official_website_url("https://example.org/?access_token=private")


def test_unistellar_site_retains_people_email_and_link_without_inventing_address():
    observation = normalize_official_website_public_content(
        "Unistellar",
        "https://www.unistellar.co/",
        _unistellar_official_website_html(),
    )

    assert observation["addresses"] == []
    assert observation["contacts"] == [
        {"type": "email", "value": "corporate@unistellar.co"}
    ]
    assert observation["people"] == [
        {"display_name": "Prof. Roy Sembel", "role": "Senior Advisor"},
        {"display_name": "Pascal Sembel", "role": "Finance & Investment"},
        {
            "display_name": "Ferdinata Suryanto",
            "role": "Corporate Finance & Investment",
        },
    ]
    assert observation["linked_company_profiles"] == [
        "https://www.linkedin.com/company/unistellar"
    ]


def test_cited_company_profiles_and_map_listings_remain_pending_observations():
    linkedin_url = "https://www.linkedin.com/company/unistellar/"
    maps_url = (
        "https://www.google.com/maps/place/Unistellar/"
        "@-6.2585928,106.8205345,980m/data=!3m1!1e3"
    )
    sources = [
        {"title": "Unistellar | LinkedIn", "url": linkedin_url},
        {"title": "Unistellar - Google Maps", "url": maps_url},
    ]
    proposals = [
        {
            "observation_type": "headquarters",
            "value": "Jl Kemang Timur No. 28, Jakarta 12730, ID",
            "source_url": linkedin_url,
            "source_title": "Unistellar | LinkedIn",
            "source_role": "other_public_source",
            "identity_match_basis": "exact_name_and_official_website",
            "reason": (
                "The cited company profile uses the exact name and links to "
                "unistellar.co while explicitly labelling Jakarta headquarters."
            ),
            "confidence": 84,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "business_address",
            "value": "Jl Kemang Timur No. 28, Jakarta 12730, ID",
            "source_url": maps_url,
            "source_title": "Unistellar - Google Maps",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_and_location",
            "reason": "The cited listing publishes this business address.",
            "confidence": 80,
            "latitude": -6.2585928,
            "longitude": 106.8231094,
        },
    ]

    findings = normalize_public_web_organization_findings(
        "Unistellar",
        proposals,
        sources=sources,
        official_website="https://www.unistellar.co/",
    )

    assert len(findings) == 2
    assert {finding["source_role"] for finding in findings} == {
        "professional_profile",
        "map_listing",
    }
    assert all(
        finding["source_engine"]
        == PUBLIC_WEB_ORGANIZATION_RESEARCH_ENGINE
        for finding in findings
    )
    assert all(finding["review_status"] == "pending" for finding in findings)
    assert all(
        finding["automatic_approval_allowed"] is False for finding in findings
    )
    assert all(
        finding["direct_platform_fetch_performed"] is False for finding in findings
    )
    assert all(finding["confidence"] == 75 for finding in findings)
    assert findings[1]["latitude"] == -6.2585928
    assert "not legal-registry" in findings[1]["limitation"]


def test_public_web_headquarters_requires_an_explicit_source_label():
    source_url = "https://directory.example/unistellar"
    proposal = {
        "observation_type": "headquarters",
        "value": "Jakarta, Indonesia",
        "source_url": source_url,
        "source_title": "Unistellar listing",
        "source_role": "public_directory",
        "identity_match_basis": "exact_name_and_location",
        "reason": "The listing publishes Jakarta as a business location.",
        "confidence": 70,
        "latitude": None,
        "longitude": None,
    }

    assert normalize_public_web_organization_findings(
        "Unistellar",
        [proposal],
        sources=[{"title": "Unistellar listing", "url": source_url}],
    ) == []


def test_public_web_organization_findings_fail_closed_on_weak_or_private_data():
    cited_url = "https://example.org/company"
    proposals = [
        {
            "observation_type": "business_address",
            "value": "Home address: 8 Private Road, Jakarta 12730",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "A directory returned a matching name.",
            "confidence": 90,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "business_address",
            "value": "8 Private Road, Jakarta 12730",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_and_location",
            "reason": (
                "The directory says this is the founder's private residence, "
                "not a company office."
            ),
            "confidence": 70,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Unistellar",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "ambiguous",
            "reason": "The name may refer to several organizations.",
            "confidence": 40,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Alice Doe · +33.1.23.45.67.89",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing exposes a punctuated employee phone number.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "+33\u202f1\u202f23\u202f45\u202f67\u202f89",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing exposes a Unicode-spaced phone number.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "+33\u200b1\u200b23\u200b45\u200b67\u200b89",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing exposes a zero-width-spaced phone number.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "+\u200b376\u200b123\u200b456",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing exposes a short international phone number.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Call\u200b+\u200b376\u200b123\u200b456",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": (
                "The listing separates a label and short international phone "
                "number with zero-width characters."
            ),
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Phone: 202\u200b555\u200b0123",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": (
                "The listing separates a domestic phone number with zero-width "
                "characters."
            ),
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Call + 376 123 456",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing spaces an international phone after its plus.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Call + (376) 123 456",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": (
                "The listing parenthesizes a spaced international country code."
            ),
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "+33\u20111\u201123\u201145\u201167\u201189",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing uses Unicode dashes in an international phone.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Reception +376 123 456x123",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing appends an extension to an international phone.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Contact 01.13.2026 89.01",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The number has an impossible date and time shape.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "business_activity",
            "value": "Think tank",
            "source_url": "https://uncited.example/organization",
            "source_title": "Uncited",
            "source_role": "other_public_source",
            "identity_match_basis": "exact_name_only",
            "reason": "No exact citation was returned.",
            "confidence": 50,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Employee email: alice@example.test",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The directory exposes an employee email address.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "business_activity",
            "value": "Contact +62 812 3456 7890",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The listing includes a personal phone number.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "CEO: Alice Doe",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The page names Alice Doe as CEO.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "C-suite: Alice Doe",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "The page lists Alice Doe in the leadership team.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
        {
            "observation_type": "company_profile",
            "value": "Management-team: Alice Doe",
            "source_url": cited_url,
            "source_title": "Example",
            "source_role": "public_directory",
            "identity_match_basis": "exact_name_only",
            "reason": "Alice Doe is a team-member and joined the board.",
            "confidence": 60,
            "latitude": None,
            "longitude": None,
        },
    ]

    assert normalize_public_web_organization_findings(
        "Unistellar",
        proposals,
        sources=[{"title": "Example", "url": cited_url}],
    ) == []


def test_public_web_financial_figures_and_timestamps_are_not_phone_contacts():
    cited_url = "https://example.org/company-results"
    findings = normalize_public_web_organization_findings(
        "Unistellar",
        [
            {
                "observation_type": "business_activity",
                "value": "2026 revenue: $1,234,567,890 (€ 1.234.567.890)",
                "source_url": cited_url,
                "source_title": "Unistellar results",
                "source_role": "news_or_institutional",
                "identity_match_basis": "exact_name_only",
                "reason": (
                    "The cited company results report this financial figure at "
                    "2026-09-01 12:30."
                ),
                "confidence": 60,
                "latitude": None,
                "longitude": None,
            },
            {
                "observation_type": "business_activity",
                "value": "Reporting cut-off: 01.09.2026 12.30",
                "source_url": cited_url,
                "source_title": "Unistellar results",
                "source_role": "news_or_institutional",
                "identity_match_basis": "exact_name_only",
                "reason": "The cited company results publish this timestamp.",
                "confidence": 60,
                "latitude": None,
                "longitude": None,
            },
        ],
        sources=[{"title": "Unistellar results", "url": cited_url}],
    )

    assert len(findings) == 2
    assert findings[0]["value"] == (
        "2026 revenue: $1,234,567,890 (€ 1.234.567.890)"
    )
    assert findings[1]["value"] == "Reporting cut-off: 01.09.2026 12.30"


def test_public_web_citation_titles_do_not_retain_personal_contact_data():
    assert normalize_public_web_organization_sources(
        [
            {
                "title": "Unistellar company profile",
                "url": "https://www.linkedin.com/company/unistellar/",
            },
            {
                "title": "Employee email alice@example.test",
                "url": "https://example.org/company",
            },
            {
                "title": "Alice Doe – CEO at Acme",
                "url": "https://example.org/leadership",
            },
            {
                "title": "Alice Doe joins Acme's C-suite",
                "url": "https://example.org/executives",
            },
            {
                "title": "Alice Doe joins Acme's board",
                "url": "https://example.org/board",
            },
            {
                "title": "Management-team: Alice Doe",
                "url": "https://example.org/management",
            },
            {
                "title": "Management―team: Alice Doe",
                "url": "https://example.org/management-horizontal-bar",
            },
            {
                "title": "Alice Doe is a team－member",
                "url": "https://example.org/team-fullwidth-hyphen",
            },
            {
                "title": "Management--team: Alice Doe",
                "url": "https://example.org/management-double-hyphen",
            },
            {
                "title": "Alice Doe is a team/_member",
                "url": "https://example.org/team-mixed-separators",
            },
            {
                "title": "Management\u200bteam: Alice Doe",
                "url": "https://example.org/management-zero-width",
            },
            {
                "title": "Alice Doe is a team.member",
                "url": "https://example.org/team-punctuation",
            },
            {
                "title": "Alice Doe · +33.1.23.45.67.89",
                "url": "https://example.org/phone-dots",
            },
            {
                "title": "+33\u202f1\u202f23\u202f45\u202f67\u202f89",
                "url": "https://example.org/phone-unicode-space",
            },
            {
                "title": "+33\u200b1\u200b23\u200b45\u200b67\u200b89",
                "url": "https://example.org/phone-zero-width-space",
            },
            {
                "title": "+\u200b376\u200b123\u200b456",
                "url": "https://example.org/phone-short-zero-width-space",
            },
            {
                "title": "Call\u200b+\u200b376\u200b123\u200b456",
                "url": (
                    "https://example.org/phone-labelled-short-zero-width-space"
                ),
            },
            {
                "title": "Phone: 202\u200b555\u200b0123",
                "url": "https://example.org/phone-domestic-zero-width-space",
            },
            {
                "title": "Call + 376 123 456",
                "url": "https://example.org/phone-spaced-international",
            },
            {
                "title": "Call + (376) 123 456",
                "url": "https://example.org/phone-parenthesized-international",
            },
            {
                "title": "+33\u20111\u201123\u201145\u201167\u201189",
                "url": "https://example.org/phone-unicode-dashes",
            },
            {
                "title": "Reception +376 123 456x123",
                "url": "https://example.org/phone-with-extension",
            },
            {
                "title": "Contact 01.13.2026 89.01",
                "url": "https://example.org/impossible-date-phone",
            },
            {
                "title": "Unistellar update 01.09.2026 12.30",
                "url": "https://example.org/company-update",
            },
            {
                "title": "LinkedIn member",
                "url": "https://www.linkedin.com/in/alice-doe/",
            },
            {
                "title": "LinkedIn member",
                "url": "https://www.linkedin.com/%69n/alice-doe/",
            },
            {
                "title": "LinkedIn member",
                "url": (
                    "https://www.linkedin.com/company/../in/alice-doe/"
                ),
            },
            {
                "title": "LinkedIn member",
                "url": (
                    "https://www.linkedin.com/company/%2e%2e/in/alice-doe/"
                ),
            },
        ]
    ) == [
        {
            "title": "Unistellar company profile",
            "url": "https://www.linkedin.com/company/unistellar/",
        },
        {
            "title": "Unistellar update 01.09.2026 12.30",
            "url": "https://example.org/company-update",
        },
    ]


@pytest.mark.asyncio
async def test_official_website_fetch_pins_public_ip_and_disables_redirects():
    calls = []
    observation = await run_official_website_public_content(
        "Example Organization",
        "https://example.org",
        host_resolver=lambda _hostname, _port: ["93.184.216.34"],
        session_factory=lambda **options: _FakeSession(
            _FakeResponse(
                status=200,
                body=_official_website_html(),
                headers={"Content-Type": "text/html; charset=utf-8"},
            ),
            calls,
            **options,
        ),
    )

    request = calls[1][1]
    assert request == {
        "url": "https://93.184.216.34/",
        "allow_redirects": False,
        "headers": {"Host": "example.org"},
        "server_hostname": "example.org",
    }
    assert "Authorization" not in calls[0][1]["headers"]
    assert observation["status"] == "observed"


@pytest.mark.asyncio
async def test_official_website_fetch_blocks_non_public_resolution_before_request():
    calls = []
    with pytest.raises(ValueError, match="non-public"):
        await run_official_website_public_content(
            "Example Organization",
            "https://example.org",
            host_resolver=lambda _hostname, _port: ["127.0.0.1"],
            session_factory=lambda **options: _FakeSession(
                _FakeResponse(status=200, body=_official_website_html()),
                calls,
                **options,
            ),
        )
    assert [item for item in calls if item[0] == "get"] == []


def test_business_context_states_basis_and_never_converts_dns_to_operations():
    jurisdiction = normalize_legal_jurisdiction("FR")
    candidates = normalize_fr_business_entities(
        "Unistellar", jurisdiction, _fr_business_entities()
    )
    registry_observation = {
        "source_engine": FR_BUSINESS_REGISTRY_ENGINE,
        "source_url": FR_BUSINESS_REGISTRY_URL,
        "selected_entity": candidates[0],
    }
    dns_observation = {
        "source_engine": CLOUDFLARE_DNS_ENGINE,
        "source_url": CLOUDFLARE_DNS_URL,
        "domain": "example.org",
        "records": {
            "a": [{"value": "93.184.216.34"}],
            "aaaa": [],
            "mx": [{"value": "mail.example.org"}],
            "ns": [{"value": "ns1.example.org"}],
        },
    }

    findings = build_business_context_assessment(
        [registry_observation],
        website="https://example.org",
        website_source="operator_input",
        dns_observation=dns_observation,
    )
    categories = {finding["category"] for finding in findings}
    assert categories.issuperset(
        {
            "registered_legal_context",
            "registered_activity_context",
            "registered_establishment_context",
            "official_website_context",
            "technical_domain_context",
        }
    )
    dns_finding = next(
        finding
        for finding in findings
        if finding["category"] == "technical_domain_context"
    )
    assert "do not establish" in dns_finding["limitation"]
    assert "operates in" not in dns_finding["conclusion"].casefold()


def test_business_context_explains_website_evidence_and_external_profile_limit():
    website_observation = normalize_official_website_public_content(
        "Example Organization",
        "https://example.org",
        _official_website_html(),
    )
    findings = build_business_context_assessment(
        [],
        website="https://example.org",
        website_source="operator_input",
        website_observation=website_observation,
    )
    categories = {finding["category"] for finding in findings}
    assert categories.issuperset(
        {
            "official_website_statement",
            "official_website_address",
            "official_contact_context",
            "official_personnel_statement",
            "linked_company_profile_lead",
        }
    )
    linked_lead = next(
        finding
        for finding in findings
        if finding["category"] == "linked_company_profile_lead"
    )
    assert linked_lead["source_url"] == (
        "https://www.linkedin.com/company/example-organization"
    )
    assert "did not fetch or copy" in linked_lead["limitation"]


def _wikipedia_pages(title="Alice Example"):
    return {
        "query": {
            "pages": [
                {
                    "pageid": 123,
                    "ns": 0,
                    "title": title,
                    "fullurl": "https://en.wikipedia.org/wiki/Alice_Example",
                    "extract": "Alice Example is a public-interest technologist.",
                    "thumbnail": {
                        "source": "https://upload.wikimedia.org/alice.jpg"
                    },
                }
            ]
        }
    }


def _icij_matches(*, exact=True):
    return {
        "result": [
            {
                "id": "12126782",
                "name": "Alice Example" if exact else "Alice Other",
                "description": "Officer node extracted from test data.",
                "match": exact,
                "score": 100.0 if exact else 75.0,
                "types": [
                    {
                        "id": "https://offshoreleaks.icij.org/schema/oldb/officer",
                        "name": "Officer",
                    }
                ],
            }
        ]
    }


def test_confirmed_name_normalizers_keep_only_reviewable_exact_records():
    wikipedia = normalize_wikipedia_candidates("Alice Example", _wikipedia_pages())
    assert wikipedia[0]["exact_title_match"] is True
    assert wikipedia[0]["thumbnail_url"].startswith("https://upload.wikimedia.org/")
    assert normalize_icij_offshore_matches("Alice Example", _icij_matches())[0][
        "node_id"
    ] == "12126782"
    assert normalize_icij_offshore_matches(
        "Alice Example", _icij_matches(exact=False)
    ) == []


def test_confirmed_name_findings_become_pending_claim_inputs_with_warnings():
    wikipedia_observation = {
        "source_engine": WIKIPEDIA_ENGINE,
        "status": "observed",
        "page": normalize_wikipedia_candidates(
            "Alice Example", _wikipedia_pages()
        )[0],
    }
    offshore_observation = {
        "source_engine": ICIJ_OFFSHORE_ENGINE,
        "status": "potential_match",
        "matches": normalize_icij_offshore_matches(
            "Alice Example", _icij_matches()
        ),
    }
    wikipedia_claims = extract_wikipedia_person_claims(wikipedia_observation)
    offshore_claims = extract_icij_offshore_claims(offshore_observation)
    assert {claim["field_name"] for claim in wikipedia_claims} == {
        "summary",
        "platform_identifier",
        "photograph",
    }
    assert offshore_claims[0]["field_name"] == "offshore_database_match"
    warning = offshore_claims[0]["evidence"][0]["details"]["identity_warning"]
    assert "not sufficient to confirm identity" in warning
    assert all(
        claim["evidence"][0]["details"]["automatic_approval_allowed"] is False
        for claim in wikipedia_claims + offshore_claims
    )


@pytest.mark.asyncio
async def test_confirmed_name_runtime_uses_only_fixed_credential_free_endpoints():
    wikipedia_calls = []
    wikipedia_response = _FakeResponse(
        status=200, body=json.dumps(_wikipedia_pages()).encode()
    )
    wikipedia = await run_wikipedia_person_enrichment(
        "Alice Example",
        session_factory=lambda **options: _FakeSequenceSession(
            [wikipedia_response], wikipedia_calls, **options
        ),
    )
    assert wikipedia["status"] == "observed"
    assert wikipedia_calls[1][1]["url"] == WIKIPEDIA_API_URL
    assert wikipedia_calls[1][1]["allow_redirects"] is False
    assert "Authorization" not in wikipedia_calls[0][1]["headers"]

    icij_calls = []
    icij_response = _FakeResponse(
        status=200, body=json.dumps(_icij_matches()).encode()
    )
    offshore = await run_icij_offshore_match(
        "Alice Example",
        session_factory=lambda **options: _FakeSequenceSession(
            [icij_response], icij_calls, **options
        ),
    )
    assert offshore["status"] == "potential_match"
    assert icij_calls[1][0] == "post"
    assert icij_calls[1][1]["url"] == ICIJ_RECONCILE_URL
    assert icij_calls[1][1]["allow_redirects"] is False
    assert icij_calls[1][1]["json"]["type"] == "Officer"
    assert "Authorization" not in icij_calls[0][1]["headers"]
