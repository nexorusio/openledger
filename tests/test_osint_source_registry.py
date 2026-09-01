import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def test_osint_source_registry_passes_static_governance_audit():
    result = subprocess.run(
        [sys.executable, ".github/scripts/check_osint_sources.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Validated 6 governed OSINT source" in result.stdout


def test_github_contract_matches_runtime_limits_and_review_boundary():
    with open(
        os.path.join(ROOT, "config", "osint-sources.json"), encoding="utf-8"
    ) as registry_file:
        registry = json.load(registry_file)

    source = registry["sources"][0]
    assert source["id"] == "github_public_profile"
    assert source["api_version"] == "2026-03-10"
    assert source["access"] == {
        "genuinely_free": True,
        "registration_required": False,
        "credentials_required": False,
        "unauthenticated_requests_per_hour_per_ip": 60,
    }
    assert source["guardrails"]["human_review_required"] is True
    assert source["guardrails"]["automatic_approval_allowed"] is False
    assert set(source["observation_only_fields"]).issuperset(
        {"followers", "following", "created_at", "updated_at"}
    )


def test_osint_source_audit_never_auto_updates_or_auto_merges():
    with open(
        os.path.join(ROOT, ".github", "workflows", "osint-source-audit.yml"),
        encoding="utf-8",
    ) as workflow_file:
        workflow = workflow_file.read().casefold()

    assert "contents: read" in workflow
    assert "pull_request:" in workflow
    assert "schedule:" in workflow
    assert "git push" not in workflow
    assert "gh pr merge" not in workflow


def test_unfurl_and_wayback_contracts_are_offline_or_fixed_origin_and_reviewed():
    with open(
        os.path.join(ROOT, "config", "osint-sources.json"), encoding="utf-8"
    ) as registry_file:
        sources = {
            source["id"]: source for source in json.load(registry_file)["sources"]
        }

    unfurl = sources["unfurl_url_analysis"]
    assert unfurl["access"]["genuinely_free"] is True
    assert unfurl["network_classification"] == "offline_local"
    assert unfurl["guardrails"]["network_access_allowed"] is False
    assert unfurl["guardrails"]["remote_lookups_allowed"] is False
    assert len(unfurl["pinned_commit"]) == 40

    wayback = sources["wayback_cdx"]
    assert wayback["endpoint_origin"] == "https://web.archive.org"
    assert wayback["guardrails"]["match_type"] == "exact"
    assert wayback["guardrails"]["archived_page_content_fetched"] is False
    assert wayback["guardrails"]["automatic_approval_allowed"] is False


def test_wikidata_affiliation_source_is_fixed_bounded_and_review_gated():
    with open(os.path.join(ROOT, "config", "osint-sources.json"), encoding="utf-8") as registry_file:
        sources = {source["id"]: source for source in json.load(registry_file)["sources"]}
    source = sources["wikidata_affiliation"]
    assert source["access"]["credentials_required"] is False
    assert source["additional_endpoint_origins"] == ["https://query.wikidata.org"]
    assert source["guardrails"]["maximum_entity_candidates"] == 5
    assert source["guardrails"]["maximum_people_per_investigation"] == 50
    assert source["guardrails"]["human_review_required"] is True
    assert source["guardrails"]["automatic_approval_allowed"] is False


def test_confirmed_name_sources_are_credential_free_bounded_and_review_gated():
    with open(os.path.join(ROOT, "config", "osint-sources.json"), encoding="utf-8") as registry_file:
        sources = {source["id"]: source for source in json.load(registry_file)["sources"]}
    wikipedia = sources["wikipedia_public_biography"]
    offshore = sources["icij_offshore_leaks"]
    assert wikipedia["access"]["credentials_required"] is False
    assert wikipedia["guardrails"]["maximum_page_candidates"] == 5
    assert wikipedia["guardrails"]["ambiguous_page_requires_operator_selection"] is True
    assert offshore["access"]["credentials_required"] is False
    assert offshore["guardrails"]["maximum_exact_name_matches"] == 5
    assert offshore["guardrails"]["fuzzy_matches_create_alerts"] is False
    assert offshore["guardrails"]["independent_identity_confirmation_required"] is True
    assert offshore["guardrails"]["automatic_approval_allowed"] is False
