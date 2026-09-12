"""Missing conformance suites cannot silently disappear from release CI."""
import importlib.util
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ci_selection", ROOT / "deploy/ci-test-selection.py")
selection = importlib.util.module_from_spec(spec)
spec.loader.exec_module(selection)


def test_connector_acceptance_is_discovered_and_missing_required_family_refuses(tmp_path):
    tests = tmp_path / "tests"
    tests.mkdir()
    for name in selection.REQUIRED_MODULES:
        (tests / name).write_text("# synthetic discovery fixture\n")
    (tests / "test_connector_new_provider.py").write_text("# future connector\n")
    assert "tests/test_connector_new_provider.py" in selection.select_tests(tmp_path)
    (tests / "test_pipeline_runtime.py").unlink()
    with pytest.raises(ValueError, match="test_pipeline_runtime.py"):
        selection.select_tests(tmp_path)


def test_stacked_p2_pr_runs_broad_python_regressions():
    text = (ROOT / ".github/workflows/python-package.yml").read_text()
    trigger = text.split("  pull_request:", 1)[1].split("jobs:", 1)[0]
    assert '"codex/rollback-to-p2"' in trigger and "main" in trigger


def test_persistence_runs_for_prs_and_protected_branch_pushes_only():
    text = (ROOT / ".github/workflows/openledger-persistence.yml").read_text()
    push = text.split("  push:", 1)[1].split("  workflow_dispatch:", 1)[0]
    assert "branches: [main, dev]" in push
    assert "codex/p2-" not in push
    pull_request = text.split("  pull_request:", 1)[1].split("permissions:", 1)[0]
    assert "branches: [main, dev]" in pull_request


def test_main_pull_requests_must_be_promoted_from_dev():
    text = (ROOT / ".github/workflows/promotion-policy.yml").read_text()
    assert 'BASE_BRANCH" == "main"' in text
    assert 'HEAD_BRANCH" != "dev"' in text
    assert "Pull requests into main must be promoted from dev." in text
