# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""P3R routing and durable plan acceptance regressions."""

import copy

import pytest

from maigret.sites import MaigretDatabase
from maigret.web import app as web_app_module
from maigret.web.case_store import CaseStore
from maigret.web.investigation_input import (
    build_unified_investigation_plan,
    classify_investigation_token,
    search_usernames,
)

PROFILE_ROUTE_FIXTURES = (
    "https://www.linkedin.com/in/fixtureuser",
    "https://www.facebook.com/fixtureuser",
    "https://www.youtube.com/@fixtureuser/about",
    "https://twitter.com/fixtureuser",
    "https://t.me/fixtureuser",
    "https://vk.com/fixtureuser",
    "https://www.reddit.com/user/fixtureuser",
    "https://sourceforge.net/u/fixtureuser/profile",
    "https://dribbble.com/fixtureuser",
    "https://disqus.com/fixtureuser",
    "https://ok.ru/fixtureuser",
    "https://www.snapchat.com/add/fixtureuser",
    "https://app.photobucket.com/u/fixtureuser",
    "https://laracasts.com/@fixtureuser",
    "https://dev.to/fixtureuser",
    "https://pbase.com/fixtureuser/profile",
    "https://community.wolfram.com/web/fixtureuser/home",
    "https://help.nextcloud.com/u/fixtureuser/summary",
    "https://my.flightradar24.com/fixtureuser",
    "https://www.metacritic.com/user/fixtureuser",
    "https://onlyfans.com/fixtureuser",
    "https://www.kongregate.com/accounts/fixtureuser",
    "https://audiojungle.net/user/fixtureuser",
    "https://velog.io/@fixtureuser/posts",
    "https://community.n8n.io/u/fixtureuser/summary",
    "https://ask.fm/fixtureuser",
    "https://archiveofourown.org/users/fixtureuser",
    "https://codesandbox.io/u/fixtureuser",
    "https://lobste.rs/u/fixtureuser",
    "https://github.com/fixtureuser",
    "https://www.tiktok.com/@fixtureuser",
    "https://www.instagram.com/fixtureuser",
)


@pytest.fixture(scope="module")
def profile_database():
    return MaigretDatabase().load_from_path(
        web_app_module.app.config["MAIGRET_DB_FILE"]
    )


@pytest.fixture
def cached_profile_resolver(monkeypatch, profile_database):
    """Keep the real URL detectors while avoiding 32 database reloads."""

    class LoadedDatabase:
        def load_from_path(self, _path):
            return profile_database

    monkeypatch.setattr(web_app_module, "MaigretDatabase", LoadedDatabase)
    return web_app_module.resolve_profile_url_identifiers


@pytest.mark.parametrize("profile_url", PROFILE_ROUTE_FIXTURES)
def test_supported_profile_routes_accept_one_trailing_slash(
    profile_url, cached_profile_resolver
):
    expected = cached_profile_resolver(profile_url)

    assert expected == {"fixtureuser": "username"}
    assert cached_profile_resolver(f"{profile_url}/") == expected


@pytest.mark.parametrize(
    "unsupported_url",
    (
        "https://example.com/fixtureuser/",
        "https://www.linkedin.com/in/",
        "https://github.com/",
        "https://www.youtube.com/watch/",
        "https://www.facebook.com/groups/",
    ),
)
def test_trailing_slash_compatibility_does_not_widen_generic_urls(
    unsupported_url, cached_profile_resolver
):
    assert cached_profile_resolver(unsupported_url) == {}
    classified = classify_investigation_token(
        unsupported_url,
        profile_url_resolver=cached_profile_resolver,
    )
    assert classified["type"] == "public_url"
    assert "account_targets" not in classified


def _target_plan(target_count):
    usernames = [f"account{index:02d}" for index in range(target_count)]
    plan = build_unified_investigation_plan(
        {
            "investigation_token": [*usernames, "analyst@example.org"],
            "investigation_token_type": [
                *(["username"] * target_count),
                "email",
            ],
            "mode": "quick",
            "confirm_email_route": "on",
            "enable_user_scanner_username": "on",
            "user_scanner_platforms_present": "1",
            "user_scanner_platform": ["instagram", "x"],
            "allow_user_scanner_vxtwitter": "on",
            "enable_github_profile_enrichment": "on",
            "enable_archived_url_evidence": "on",
        },
        require_route_confirmation=True,
    )
    return usernames, plan


@pytest.mark.parametrize("target_count", (1, 4, 16))
def test_plan_survives_queue_claim_and_worker_hydration_without_extra_targets(
    tmp_path, target_count
):
    store = CaseStore(
        f"sqlite:///{tmp_path / f'p3r-{target_count}.db'}",
        create_schema=True,
    )
    usernames, submitted_plan = _target_plan(target_count)
    submitted_snapshot = copy.deepcopy(submitted_plan)
    try:
        job_id = store.create_investigation(
            usernames,
            {
                "execution_mode": "focused",
                "investigation_spec": submitted_plan,
                "proxy_configured": False,
                "tor_proxy_configured": False,
                "i2p_proxy_configured": False,
            },
        )
        queued = store.get_job(job_id)
        claimed = store.claim_next(f"worker:p3r:{target_count}")
        hydrated = web_app_module.hydrate_persistent_options(claimed["options"])
        queued_plan = queued["options"]["investigation_spec"]

        assert submitted_plan == submitted_snapshot
        assert all(
            queued_plan[key] == value for key, value in submitted_snapshot.items()
        )
        assert queued["usernames"] == usernames
        assert claimed["usernames"] == usernames
        assert search_usernames(hydrated["investigation_spec"]) == usernames
        assert hydrated["investigation_spec"] == queued_plan
        assert [
            target["value"]
            for target in hydrated["investigation_spec"]["search_targets"]
        ] == usernames
        assert all(
            token["type_source"] == "analyst_override"
            for token in hydrated["investigation_spec"]["tokens"]
        )
        assert hydrated["investigation_spec"]["user_scanner_username_platforms"] == [
            "instagram",
            "x",
        ]
        assert hydrated["investigation_spec"]["allow_user_scanner_vxtwitter"] is True
        assert hydrated["investigation_spec"]["email_route_confirmed"] is True
        assert (
            hydrated["investigation_spec"]["enable_github_profile_enrichment"] is True
        )
        assert hydrated["investigation_spec"]["enable_archived_url_evidence"] is True
    finally:
        store.dispose()


def test_selected_aliases_survive_queue_and_claim_exactly(tmp_path):
    preview = build_unified_investigation_plan(
        {
            "investigation_token": ["Alice Example"],
            "investigation_token_type": ["full_name"],
            "mode": "quick",
            "search_likely_username_aliases": "on",
        }
    )
    candidates = [item["value"] for item in preview["alias_candidates"]]
    selected = candidates[:4]
    submitted = build_unified_investigation_plan(
        {
            "investigation_token": ["Alice Example"],
            "investigation_token_type": ["full_name"],
            "mode": "quick",
            "search_likely_username_aliases": "on",
            "alias_candidates_present": "1",
            "alias_candidate": candidates,
            "selected_alias": selected,
        }
    )
    store = CaseStore(
        f"sqlite:///{tmp_path / 'p3r-aliases.db'}",
        create_schema=True,
    )
    try:
        job_id = store.create_investigation(
            search_usernames(submitted),
            {"execution_mode": "focused", "investigation_spec": submitted},
        )
        claimed = store.claim_next("worker:p3r:aliases")
        persisted = claimed["options"]["investigation_spec"]

        assert claimed["job_id"] == job_id
        assert search_usernames(persisted) == selected
        assert [
            item["value"] for item in persisted["alias_candidates"] if item["selected"]
        ] == selected
        assert [item["value"] for item in persisted["search_targets"]] == selected
    finally:
        store.dispose()
