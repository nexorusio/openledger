"""GitHub candidate preparation must not silently publish a moving release."""
from pathlib import Path
import re


def test_docker_workflow_only_prepares_pinned_candidate():
    workflow = (
        Path(__file__).resolve().parents[1]
        / '.github/workflows/build-docker-image.yml'
    ).read_text()
    active = '\n'.join(line for line in workflow.splitlines() if not line.lstrip().startswith('#'))
    assert re.search(r'^on:\n  workflow_dispatch:\n', active, re.M)
    assert not re.search(r'^  (push|pull_request|schedule|workflow_run):', active, re.M)
    assert 'ref: ${{ github.sha }}' in active
    assert "deploy/build-reviewed-release.sh --commit '${{ github.sha }}'" in active
    assert '--manifest "$RUNNER_TEMP/p2-candidate.json"' in active
    assert 'contents: read' in active
    assert not re.search(r'\b(push|write)\b', active)
    assert 'uses: docker/' not in active
