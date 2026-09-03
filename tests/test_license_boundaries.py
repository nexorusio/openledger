"""Regression checks for OpenLedger's scoped license boundary."""

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MIT_NOTICE = REPOSITORY_ROOT / "LICENSES" / "MAIGRET-MIT.txt"
MAIGRET_FORK_BASE = "af3de564c706e677221ab9f82f90166bb8b346ea"
FIRST_OPENLEDGER_COMMIT = "8bc569097af94d992ac2f32a7293eeb6b140bfd4"
LAST_BLANKET_MIT_HEAD = "60b187135c94621d729a42b0d09294c59a3d8cb7"


def test_root_notice_records_ownership_without_revoking_historical_mit_rights():
    notice = (REPOSITORY_ROOT / "LICENSE").read_text(encoding="utf-8")

    assert "PT Daya Prana Inovasi" in notice
    assert MAIGRET_FORK_BASE in notice
    assert FIRST_OPENLEDGER_COMMIT in notice
    assert LAST_BLANKET_MIT_HEAD in notice
    assert "asserts copyright" in notice
    assert "Nothing in this notice withdraws" in notice
    assert "Copyright ownership and licensing are distinct" in notice
    assert "Maigret" in notice


def test_complete_maigret_mit_notice_is_preserved():
    notice = MIT_NOTICE.read_text(encoding="utf-8")

    assert notice.startswith("MIT License\n\nCopyright (c) 2020-2026 Soxoj\n")
    assert "Permission is hereby granted, free of charge" in notice
    assert "copies or substantial portions of the Software" in notice
    assert 'THE SOFTWARE IS PROVIDED "AS IS"' in notice


def test_distribution_metadata_uses_openledger_identity_and_scoped_license():
    project = (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    dockerignore = (REPOSITORY_ROOT / ".dockerignore").read_text(encoding="utf-8")

    assert 'name = "openledger"' in project
    assert 'license = "Proprietary"' in project
    assert 'packages = [{include = "maigret"}]' in project
    assert "PT Daya Prana Inovasi" in project
    assert "PT Daya Prana Inovasi" in dockerfile
    assert "LicenseRef-Nexorus-Proprietary AND MIT" in dockerfile
    assert "!LICENSES/*.txt" in dockerignore

    proprietary_terms = (
        REPOSITORY_ROOT / "LICENSES" / "LicenseRef-Nexorus-Proprietary.txt"
    ).read_text(encoding="utf-8")
    assert "PT Daya Prana Inovasi" in proprietary_terms
    assert "does not apply to Maigret" in proprietary_terms


def test_readme_and_contribution_policy_do_not_claim_blanket_mit_licensing():
    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
    contributing = (REPOSITORY_ROOT / "CONTRIBUTING.md").read_text(
        encoding="utf-8"
    )

    assert "This repository is distributed under the" not in readme
    assert "mixed-license work" in readme
    assert MAIGRET_FORK_BASE in readme
    assert FIRST_OPENLEDGER_COMMIT in readme
    assert LAST_BLANKET_MIT_HEAD in readme
    assert "does not accept external code contributions without" in contributing
    assert "LicenseRef-Nexorus-Proprietary" in contributing


def test_transition_runbook_requires_private_repository_deploy_access_first():
    runbook = (REPOSITORY_ROOT / "docs" / "licensing-transition.md").read_text(
        encoding="utf-8"
    )

    assert "does not revoke the MIT permissions" in runbook
    assert MAIGRET_FORK_BASE in runbook
    assert FIRST_OPENLEDGER_COMMIT in runbook
    assert LAST_BLANKET_MIT_HEAD in runbook
    assert "Ownership is not the same as exclusivity" in runbook
    assert "Do not change visibility until" in runbook
    assert "read-only" in runbook
    assert "GitHub deploy key" in runbook
    assert "complete `LICENSES/` directory" in runbook


def test_pull_request_template_requires_license_and_provenance_review():
    template = (
        REPOSITORY_ROOT / ".github" / "pull_request_template.md"
    ).read_text(encoding="utf-8")

    assert "License and provenance" in template
    assert "Every new file has an identified license" in template
    assert "Maigret MIT notice" in template
