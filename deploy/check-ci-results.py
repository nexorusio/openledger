#!/usr/bin/env python3
"""Reject skipped/missing mandatory PostgreSQL and pipeline acceptance results."""

from pathlib import Path
import sys
from xml.etree import ElementTree

REQUIRED_CASES = (
    "test_new_connector_needs_only_package_manifest_and_fixtures",
    "test_operator_and_machine_connectors_are_not_query_worker_fallbacks",
    "test_saved_versions_cannot_silently_use_a_different_parser",
    "test_registered_wrapper_keeps_positive_result_and_surfaces_retry_warning",
    "test_browser_four_inputs_review_research_qc_and_final",
    "test_page_commit_advances_cursor_and_atomic_evidence",
    "test_crash_retry_resumes_checkpoint_and_deduplicates_page",
    "test_page_and_cursor_roll_back_together_on_late_record_conflict",
    "test_machine_endpoint_is_authenticated_even_without_browser_login",
    "test_failed_ack_replay_returns_one_durable_job",
    "test_machine_worker_processes_receipt_into_review_without_finalization",
    "test_machine_withdrawal_preserves_final_and_blocks_stale_support",
    "test_concurrent_feed_delivery_queues_one_job",
    "test_runtime_startup_validates_registry_before_attestation",
    "test_workspace_50k_evidence_pages_are_bounded_and_do_not_reassess",
    "test_committed_evidence_invalidates_projection_and_failed_rebuild_is_atomic",
    "test_frozen_historical_migration_does_not_create_future_tables",
    "test_reliability_migration_preserves_evidence_and_refuses_populated_downgrade",
    "test_budget_survives_retry_and_rejects_stale_attempt",
    "test_request_budget_is_atomic_across_workers",
    "test_shared_provider_cooldown_and_single_probe",
    "test_retry_after_is_shared_before_second_transport_call",
    "test_transport_redirects_consume_separate_permits",
    "test_scanner_subprocess_inherits_meter",
    "test_native_retry_honors_shared_cooldown_before_next_send",
    "test_curl_refuses_https_downgrade_before_forwarding_credentials",
    "test_cross_account_birth_dates_block_qc_until_operator_resolves",
    "test_restricted_payload_never_reaches_ledger_or_export",
    "test_fifty_thousand_returned_observations_reconcile",
    "test_postgres_upgrades_existing_legacy_case_and_preserves_backfill_evidence",
    "test_legacy_backfill_is_checkpointed_idempotent_and_never_finalizes[postgres]",
    "test_qc_is_explicit_and_final_is_immutable[postgres]",
    "test_real_app_manual_review_qc_research_worker_and_final_projection",
    "test_pdf_text_register_contains_every_curated_fact_and_observation",
)


def validate_results(path):
    cases = list(ElementTree.parse(path).getroot().iter("testcase"))
    if not cases:
        raise ValueError("No executed acceptance cases were recorded")
    for case in cases:
        if any(
            case.find(outcome) is not None
            for outcome in ("skipped", "error", "failure")
        ):
            raise ValueError(f"Mandatory acceptance did not pass: {case.get('name')}")
    names = {case.get("name") for case in cases}
    # Fixture-parametrized conformance cases retain their exact IDs as well as
    # function names; explicit [postgres] requirements still demand that variant.
    names.update(name.split("[", 1)[0] for name in list(names) if name)
    missing = set(REQUIRED_CASES) - names
    if missing:
        raise ValueError(
            "Required PostgreSQL acceptance cases are missing: "
            + ", ".join(sorted(missing))
        )
    return len(cases)


if __name__ == "__main__":
    try:
        count = validate_results(Path(sys.argv[1]))
    except (OSError, ValueError, IndexError) as error:
        sys.exit(f"Release acceptance refused: {error}")
    print(f"{count} mandatory acceptance cases passed; none skipped")
