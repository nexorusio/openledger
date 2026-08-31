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
    assert "Validated 1 governed OSINT source" in result.stdout


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
