"""Check the release evidence gate with local JUnit documents only."""

import importlib.util
from pathlib import Path
from xml.etree import ElementTree

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pipeline_ci_gate", ROOT / "deploy/check-ci-results.py"
)
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def write_results(tmp_path, names, *, outcome=None):
    suite = ElementTree.Element("testsuite")
    for name in names:
        case = ElementTree.SubElement(suite, "testcase", name=name)
        if outcome:
            ElementTree.SubElement(case, outcome)
    path = tmp_path / "results.xml"
    ElementTree.ElementTree(suite).write(path)
    return path


def test_complete_execution_evidence_passes(tmp_path):
    path = write_results(tmp_path, gate.REQUIRED_CASES)
    assert gate.validate_results(path) == len(gate.REQUIRED_CASES)


@pytest.mark.parametrize("missing", gate.REQUIRED_CASES)
def test_missing_required_journey_or_fidelity_evidence_refuses_release(
    tmp_path, missing
):
    path = write_results(tmp_path, [x for x in gate.REQUIRED_CASES if x != missing])
    with pytest.raises(ValueError, match="missing"):
        gate.validate_results(path)


@pytest.mark.parametrize("outcome", ["skipped", "failure", "error"])
def test_nonpassing_acceptance_refuses_release(tmp_path, outcome):
    path = write_results(tmp_path, gate.REQUIRED_CASES, outcome=outcome)
    with pytest.raises(ValueError, match="did not pass"):
        gate.validate_results(path)


def test_empty_acceptance_refuses_release(tmp_path):
    with pytest.raises(ValueError, match="No executed"):
        gate.validate_results(write_results(tmp_path, []))
