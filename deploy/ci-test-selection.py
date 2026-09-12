#!/usr/bin/env python3
"""Select the P2 acceptance modules; required families cannot vanish."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = (
    "test_pipeline*.py", "test_connector*.py", "test_postgres_persistence.py",
    "test_deployment.py", "test_provider_circuit_breaker.py", "test_dns_collection_outcomes.py",
)
REQUIRED_MODULES = (
    "test_pipeline_runtime.py", "test_connector_registry.py", "test_pipeline_reliability_migration.py",
    "test_pipeline_browser_acceptance.py", "test_pipeline_connector_ingestion.py",
    "test_pipeline_projections.py",
)

# These suites require the disposable PostgreSQL/browser/container gates, or
# exercise the retired pre-P2 presentation path. The persistence workflow runs
# the environment-gated suites explicitly; the Python matrix uses the offline
# P2 set for the stacked P2 pull request instead of asserting old behavior.
OFFLINE_EXCLUDED_MODULES = frozenset(
    {
        "test_pipeline_browser_acceptance.py",
        "test_pipeline_postgres_upgrade.py",
        "test_pipeline_store_load.py",
        "test_pipeline_public_search.py",
        "test_postgres_persistence.py",
    }
)


def select_tests(root=ROOT):
    directory = root / "tests"
    tests = sorted({path for pattern in PATTERNS for path in directory.glob(pattern)})
    missing = set(REQUIRED_MODULES) - {path.name for path in tests}
    if missing:
        raise ValueError("Required acceptance modules are absent: " + ", ".join(sorted(missing)))
    return [str(path.relative_to(root)) for path in tests]


def select_offline_tests(root=ROOT):
    return [
        path
        for path in select_tests(root)
        if Path(path).name not in OFFLINE_EXCLUDED_MODULES
    ]


if __name__ == "__main__":
    try:
        selector = select_offline_tests if "--offline" in sys.argv[1:] else select_tests
        print("\n".join(selector()))
    except ValueError as error:
        sys.exit(str(error))
