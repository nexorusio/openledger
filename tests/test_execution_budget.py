from datetime import datetime, timedelta, timezone

from maigret.web.execution_budget import (
    EXECUTION_BUDGET_POLICY_VERSION,
    EXHAUSTIVE_BUDGET_SECONDS,
    FOCUSED_BUDGET_SECONDS,
    ExecutionBudget,
    apply_execution_budget,
    execution_budget_spec_from_options,
    normalize_execution_mode,
)


def test_legacy_modes_normalize_to_governed_names():
    assert normalize_execution_mode("fast") == "focused"
    assert normalize_execution_mode("quick") == "focused"
    assert normalize_execution_mode("full") == "exhaustive"
    assert normalize_execution_mode("focused") == "focused"
    assert normalize_execution_mode("exhaustive") == "exhaustive"


def test_server_policy_ignores_a_forged_persisted_duration():
    spec = execution_budget_spec_from_options(
        {
            "all_sites": False,
            "execution_budget": {
                "mode": "focused",
                "total_seconds": 99_999_999,
                "policy_version": "client-controlled",
            },
        }
    )

    assert spec == {
        "policy_version": EXECUTION_BUDGET_POLICY_VERSION,
        "mode": "focused",
        "total_seconds": FOCUSED_BUDGET_SECONDS,
    }


def test_apply_execution_budget_controls_all_sites_from_mode():
    options = apply_execution_budget({"all_sites": False}, "full")

    assert options["execution_mode"] == "exhaustive"
    assert options["all_sites"] is True
    assert options["execution_budget"]["total_seconds"] == (
        EXHAUSTIVE_BUDGET_SECONDS
    )


def test_job_deadline_is_absolute_and_queue_time_is_not_consumed():
    claimed_at = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
    budget = ExecutionBudget.from_job(
        {
            "options": {"execution_mode": "focused", "all_sites": False},
            "started_at": claimed_at.isoformat(),
        }
    )

    assert budget.deadline_at == claimed_at + timedelta(
        seconds=FOCUSED_BUDGET_SECONDS
    )
    assert budget.remaining_seconds(now=claimed_at) == FOCUSED_BUDGET_SECONDS


def test_remaining_budget_never_becomes_negative():
    started_at = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
    budget = ExecutionBudget.from_options(
        {"execution_mode": "focused"}, started_at=started_at
    )

    assert budget.remaining_seconds(now=budget.deadline_at) == 0
    assert budget.remaining_seconds(
        now=budget.deadline_at + timedelta(seconds=1)
    ) == 0
    assert budget.is_exhausted(now=budget.deadline_at) is True


def test_persisted_deadline_cannot_extend_the_server_budget():
    started_at = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
    budget = ExecutionBudget.from_job(
        {
            "options": {"execution_mode": "focused"},
            "started_at": started_at.isoformat(),
            "deadline_at": (started_at + timedelta(days=30)).isoformat(),
            "budget_seconds": 99_999_999,
            "budget_policy_version": "client-controlled",
        }
    )

    assert budget.total_seconds == FOCUSED_BUDGET_SECONDS
    assert budget.deadline_at == started_at + timedelta(
        seconds=FOCUSED_BUDGET_SECONDS
    )
    assert budget.policy_version == EXECUTION_BUDGET_POLICY_VERSION
