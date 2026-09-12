"""An absent/skipped PostgreSQL run must never become a green release gate."""

import importlib.util
from pathlib import Path
from xml.etree import ElementTree

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "ci_results", ROOT / "deploy/check-ci-results.py"
)
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


def report(tmp_path, names, outcome=None):
    suite = ElementTree.Element("testsuite")
    for name in names:
        case = ElementTree.SubElement(suite, "testcase", name=name)
        if outcome:
            ElementTree.SubElement(case, outcome)
    path = tmp_path / "results.xml"
    ElementTree.ElementTree(suite).write(path)
    return path


@pytest.mark.parametrize("outcome", ["skipped", "failure", "error"])
def test_ci_rejects_unexecuted_or_failed_required_results(tmp_path, outcome):
    with pytest.raises(ValueError, match="did not pass"):
        ci.validate_results(report(tmp_path, ci.REQUIRED_CASES, outcome))


def test_ci_rejects_missing_postgres_coverage(tmp_path):
    with pytest.raises(ValueError, match="missing"):
        ci.validate_results(report(tmp_path, ["only-sqlite-passed"]))


def test_ci_accepts_executed_postgres_backfill_upgrade_and_qc(tmp_path):
    assert ci.validate_results(report(tmp_path, ci.REQUIRED_CASES)) == len(
        ci.REQUIRED_CASES
    )
