# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import asyncio

import pytest

from maigret.web.collection_orchestration import (
    CollectionOrchestrationError,
    DEFAULT_CLEANUP_SECONDS,
    OperationLedger,
    StageCounts,
    StageResult,
    StopCause,
    StageSpec,
    profile_stage_engine_ids,
    profile_stage_weights,
    run_collection_stages,
)


def complete(*, planned=1, observations=0):
    return StageResult(
        counts=StageCounts(
            planned=planned,
            started=planned,
            terminal=planned,
            completed=planned,
            errors=0,
            timeouts=0,
            cancelled=0,
            interrupted=0,
            unattempted=0,
            unknown=0,
            observations=observations,
        )
    )


@pytest.mark.asyncio
async def test_source_failure_is_contained_and_diagnostics_do_not_include_subject_data():
    calls, events = [], []

    async def fails(_context):
        calls.append("failed")
        raise RuntimeError("alice@example.invalid must not reach diagnostics")

    async def succeeds(_context):
        calls.append("independent")
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec("first", "first_source", "First", "target", fails),
            StageSpec("second", "second_source", "Second", "target", succeeds),
        ],
        remaining_seconds=1,
        event_sink=events.append,
    )

    assert calls == ["failed", "independent"]
    assert [outcome.status for outcome in summary.outcomes] == ["failed", "completed"]
    assert summary.outcomes[0].reason == "RuntimeError"
    assert summary.outcomes[0].counts.known is False
    assert "alice@example.invalid" not in str(summary.as_dict())
    assert events[-1] == {
        "type": "collection_accounting",
        "collection_accounting": summary.as_dict(),
    }
    assert summary.as_dict()["state"] == "partial"


@pytest.mark.asyncio
async def test_durable_accounting_sink_failure_stops_admission_and_propagates():
    called = False

    async def source(_context):
        nonlocal called
        called = True
        return complete()

    def unavailable_sink(_event):
        raise OSError("storage unavailable")

    with pytest.raises(OSError):
        await run_collection_stages(
            [StageSpec("native", "native", "Native", "query", source)],
            remaining_seconds=1,
            event_sink=unavailable_sink,
        )
    assert called is False


@pytest.mark.asyncio
async def test_partial_upstream_result_can_admit_enrichment_when_ready():
    calls = []

    async def upstream(_context):
        return StageResult(
            value={"supported": True},
            counts=StageCounts(
                planned=2,
                started=1,
                terminal=1,
                completed=0,
                errors=0,
                timeouts=1,
                cancelled=0,
                interrupted=0,
                unattempted=1,
                unknown=0,
                observations=1,
            ),
        )

    async def enrichment(_context):
        calls.append("enrichment")
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec(
                "maigret",
                "maigret",
                "Maigret",
                "site_check",
                upstream,
                planned_units=2,
                weight=55,
            ),
            StageSpec(
                "github",
                "github",
                "GitHub",
                "profile_target",
                enrichment,
                readiness=lambda outcomes: outcomes["maigret"].value["supported"],
                weight=5,
                dependencies=("maigret",),
            ),
        ],
        remaining_seconds=1,
    )

    assert calls == ["enrichment"]
    assert [outcome.status for outcome in summary.outcomes] == [
        "completed",
        "completed",
    ]


@pytest.mark.asyncio
async def test_no_target_and_not_selected_rows_have_known_zero_accounting():
    async def source(_context):
        raise AssertionError("not admitted")

    summary = await run_collection_stages(
        [
            StageSpec(
                "disabled", "disabled", "Disabled", "query", source, selected=False
            ),
            StageSpec("empty", "empty", "Empty", "query", source, planned_units=0),
        ],
        remaining_seconds=1,
    )

    assert [outcome.status for outcome in summary.outcomes] == [
        "not_selected",
        "skipped_no_targets",
    ]
    assert all(outcome.counts.known for outcome in summary.outcomes)
    assert summary.as_dict()["known"] is True


@pytest.mark.asyncio
async def test_selected_no_target_stage_releases_its_share_to_later_work():
    deadlines = []

    async def first(context):
        deadlines.append(context.remaining_seconds())
        return complete()

    async def later(context):
        deadlines.append(context.remaining_seconds())
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", first, weight=1),
            StageSpec(
                "empty", "empty", "Empty", "unit", first, planned_units=0, weight=98
            ),
            StageSpec("later", "later", "Later", "unit", later, weight=1),
        ],
        remaining_seconds=0.2,
    )

    assert [item.status for item in summary.outcomes] == [
        "completed",
        "skipped_no_targets",
        "completed",
    ]
    assert deadlines[0] > 0.08
    assert deadlines[1] > 0.08


@pytest.mark.asyncio
async def test_future_dynamic_zero_plan_reserves_budget_until_upstream_resolves():
    deadlines = []

    async def upstream(context):
        deadlines.append(context.remaining_seconds())
        return StageResult(value={"targets": 0}, counts=complete().counts)

    async def later(context):
        deadlines.append(context.remaining_seconds())
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec("upstream", "upstream", "Upstream", "unit", upstream, weight=1),
            StageSpec(
                "derived",
                "derived",
                "Derived",
                "unit",
                later,
                planned_units=lambda outcomes: (
                    outcomes["upstream"].value["targets"]
                    if "upstream" in outcomes
                    else 0
                ),
                dependencies=("upstream",),
                weight=98,
            ),
            StageSpec("later", "later", "Later", "unit", later, weight=1),
        ],
        remaining_seconds=0.2,
    )

    assert [outcome.status for outcome in summary.outcomes] == [
        "completed",
        "skipped_no_targets",
        "completed",
    ]
    assert deadlines[0] < 0.02
    assert deadlines[1] > 0.15


@pytest.mark.asyncio
async def test_future_known_blocked_stage_releases_its_reserved_share():
    deadlines = []

    async def first(context):
        deadlines.append(context.remaining_seconds())
        return complete()

    async def never(_context):
        raise AssertionError("blocked stage must not run")

    summary = await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", first, weight=1),
            StageSpec(
                "blocked",
                "blocked",
                "Blocked",
                "unit",
                never,
                readiness=False,
                weight=98,
            ),
            StageSpec("last", "last", "Last", "unit", first, weight=1),
        ],
        remaining_seconds=0.2,
    )

    assert summary.outcomes[1].status == "blocked_dependency"
    assert deadlines[0] > 0.08


@pytest.mark.asyncio
async def test_failed_upstream_blocks_dependent_before_its_dynamic_plan_is_read():
    calls = []

    async def fails(_context):
        raise RuntimeError("upstream failed")

    async def dependent(_context):
        calls.append("dependent")
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec("upstream", "upstream", "Upstream", "unit", fails),
            StageSpec(
                "dependent",
                "dependent",
                "Dependent",
                "unit",
                dependent,
                planned_units=lambda outcomes: outcomes["upstream"].value["targets"],
                dependencies=("upstream",),
            ),
        ],
        remaining_seconds=1,
    )

    assert calls == []
    assert summary.outcomes[1].status == "blocked_dependency"
    assert summary.outcomes[1].counts.planned is None


@pytest.mark.asyncio
async def test_job_cancellation_is_distinct_from_budget_timeout():
    cancelled = False

    async def first(_context):
        nonlocal cancelled
        cancelled = True
        return complete()

    async def second(_context):
        raise AssertionError("cancelled stage must not start")

    async def third(_context):
        raise AssertionError("cancelled stage must not start")

    summary = await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", first),
            StageSpec("second", "second", "Second", "unit", second),
            StageSpec("third", "third", "Third", "unit", third),
        ],
        remaining_seconds=1,
        cancellation_check=lambda: cancelled,
    )

    assert [outcome.status for outcome in summary.outcomes] == [
        "completed",
        "cancelled",
        "cancelled",
    ]
    assert summary.state == "cancelled"
    assert summary.cancelled is True
    assert summary.budget_exhausted is False
    assert summary.outcomes[1].counts.unattempted == 1
    assert summary.outcomes[2].reason == "cancellation_requested"
    assert summary.stop_cause is StopCause.OPERATOR_CANCEL
    assert all(
        outcome.stop_cause is StopCause.OPERATOR_CANCEL
        for outcome in summary.outcomes[1:]
    )


def stopped_native_counts():
    return StageCounts(
        planned=3,
        started=1,
        terminal=0,
        completed=0,
        errors=0,
        timeouts=0,
        cancelled=0,
        interrupted=1,
        unattempted=2,
        unknown=0,
        observations=0,
    )


@pytest.mark.asyncio
async def test_stop_request_preserves_bounded_stopped_callback_counts():
    entered = asyncio.Event()

    async def native_like(_context):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return StageResult(counts=stopped_native_counts())

    summary = await run_collection_stages(
        [
            StageSpec(
                "native",
                "native-profile-search",
                "Native",
                "query",
                native_like,
                planned_units=None,
            )
        ],
        remaining_seconds=1,
        cancellation_check=entered.is_set,
    )

    counts = summary.outcomes[0].counts
    assert summary.outcomes[0].status == "cancelled"
    assert (counts.planned, counts.started, counts.terminal) == (3, 1, 0)
    assert (counts.interrupted, counts.unattempted, counts.unknown) == (1, 2, 0)


@pytest.mark.asyncio
async def test_stage_timeout_preserves_bounded_stopped_callback_counts():
    async def native_like(_context):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return StageResult(counts=stopped_native_counts())

    summary = await run_collection_stages(
        [
            StageSpec(
                "native",
                "native-profile-search",
                "Native",
                "query",
                native_like,
                planned_units=None,
            )
        ],
        remaining_seconds=0.01,
    )

    counts = summary.outcomes[0].counts
    assert summary.outcomes[0].status == "timed_out"
    assert summary.outcomes[0].reason == "stage_budget_exhausted"
    assert (counts.planned, counts.started, counts.terminal) == (3, 1, 0)
    assert (counts.interrupted, counts.unattempted, counts.unknown) == (1, 2, 0)


@pytest.mark.asyncio
async def test_parent_cancellation_accounts_for_later_stages_then_reraises():
    events = []
    entered = asyncio.Event()

    async def blocking(_context):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_collection_stages(
            [
                StageSpec("native", "native", "Native", "query", blocking),
                StageSpec(
                    "later", "later", "Later", "target", blocking, planned_units=3
                ),
            ],
            remaining_seconds=1,
            event_sink=events.append,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    final = events[-1]["collection_accounting"]
    assert final["state"] == "interrupted"
    assert [row["status"] for row in final["stages"]] == ["interrupted", "interrupted"]
    assert final["stages"][1]["unattempted"] == 3


@pytest.mark.asyncio
async def test_parent_cancellation_preserves_bounded_stopped_callback_counts():
    events = []
    entered = asyncio.Event()

    async def native_like(context):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return StageResult(
                counts=StageCounts(
                    planned=3,
                    started=1,
                    terminal=0,
                    completed=0,
                    errors=0,
                    timeouts=0,
                    cancelled=0,
                    interrupted=1,
                    unattempted=2,
                    unknown=0,
                    observations=0,
                )
            )

    task = asyncio.create_task(
        run_collection_stages(
            [
                StageSpec(
                    "native",
                    "native-profile-search",
                    "Native",
                    "query",
                    native_like,
                    planned_units=None,
                )
            ],
            remaining_seconds=1,
            event_sink=events.append,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    final = events[-1]["collection_accounting"]["stages"][0]
    assert final["status"] == "interrupted"
    assert {field: final[field] for field in ("planned", "started", "terminal")} == {
        "planned": 3,
        "started": 1,
        "terminal": 0,
    }
    assert final["interrupted"] == 1
    assert final["unattempted"] == 2
    assert final["unknown"] == 0


@pytest.mark.asyncio
async def test_parent_cancellation_is_not_replaced_by_final_sink_failure():
    entered = asyncio.Event()

    def sink(event):
        if event["collection_accounting"]["state"] == "interrupted":
            raise OSError("accounting store unavailable")

    async def blocking(_context):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_collection_stages(
            [StageSpec("native", "native", "Native", "query", blocking)],
            remaining_seconds=1,
            event_sink=sink,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cleanup_incomplete_halts_later_admission_and_marks_snapshot():
    stop = asyncio.Event()
    later_called = False

    async def ignores_cancel(_context):
        while not stop.is_set():
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                continue

    async def later(_context):
        nonlocal later_called
        later_called = True
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec("slow", "slow", "Slow", "target", ignores_cancel),
            StageSpec("later", "later", "Later", "target", later),
        ],
        remaining_seconds=0.01,
        cleanup_seconds=0.01,
    )
    stop.set()
    await asyncio.sleep(0)

    assert [outcome.status for outcome in summary.outcomes] == [
        "interrupted",
        "interrupted",
    ]
    assert summary.outcomes[0].cleanup_complete is False
    assert all(
        outcome.stop_cause is StopCause.CLEANUP_INCOMPLETE
        for outcome in summary.outcomes
    )
    assert [outcome.reason for outcome in summary.outcomes] == [
        "cleanup_incomplete",
        "cleanup_incomplete",
    ]
    assert summary.outcomes[1].counts.unattempted == 1
    assert later_called is False


def test_operation_ledger_tracks_started_unattempted_unknown_and_observations():
    ledger = OperationLedger()
    assert ledger.record_planned("first") is True
    assert ledger.record_planned("second") is True
    assert ledger.record_planned("third") is True
    assert ledger.record_started("first") is True
    assert (
        ledger.record_terminal("first", "completed", attempted=True, observations=3)
        is True
    )
    assert ledger.record_started("third") is True
    assert ledger.record_terminal("second", "completed", attempted=False) is False
    assert ledger.record_terminal("third", "unattempted", attempted=True) is False

    counts = ledger.finalize(unfinished_disposition="interrupted")

    assert counts.planned == 3
    assert counts.started == 2
    assert counts.terminal == 1
    assert counts.completed == 1
    assert counts.interrupted == 1
    assert counts.unattempted == 1
    assert counts.observations is None
    assert ledger.record_planned("fourth") is False
    assert ledger.record_terminal("second", "unattempted", attempted=False) is False


def test_stage_weights_are_server_owned_and_deadline_cannot_be_reset():
    assert sum(profile_stage_weights().values()) == 100

    async def source(_context):
        raise AssertionError("expired source must not run")

    summary = asyncio.run(
        run_collection_stages(
            [StageSpec("native", "native", "Native", "query", source)],
            deadline=0.0,
            clock=lambda: 1.0,
        )
    )
    assert summary.outcomes[0].status == "not_started_budget"
    with pytest.raises(CollectionOrchestrationError):
        asyncio.run(run_collection_stages([], remaining_seconds=1))


@pytest.mark.asyncio
async def test_initial_snapshot_lists_full_plan_and_default_dependency_is_completed_only():
    events = []
    calls = []

    async def failed(_context):
        raise RuntimeError("source failure")

    async def dependent(_context):
        calls.append("dependent")
        return complete()

    await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", failed),
            StageSpec(
                "second",
                "second",
                "Second",
                "unit",
                dependent,
                dependencies=("first",),
            ),
        ],
        remaining_seconds=1,
        event_sink=events.append,
    )

    initial = events[0]["collection_accounting"]
    assert initial["state"] == "running"
    assert [row["stage_id"] for row in initial["stages"]] == ["first", "second"]
    assert calls == []
    assert (
        events[-1]["collection_accounting"]["stages"][1]["status"]
        == "blocked_dependency"
    )


def test_invalid_weights_counts_and_dependency_order_are_rejected():
    async def source(_context):
        return complete()

    with pytest.raises(CollectionOrchestrationError):
        StageSpec("bad", "bad", "Bad", "unit", source, weight=float("inf"))
    with pytest.raises(CollectionOrchestrationError):
        StageCounts(planned=1, started=2)
    with pytest.raises(CollectionOrchestrationError):
        asyncio.run(
            run_collection_stages(
                [
                    StageSpec(
                        "first",
                        "first",
                        "First",
                        "unit",
                        source,
                        dependencies=("second",),
                    ),
                    StageSpec("second", "second", "Second", "unit", source),
                ],
                remaining_seconds=1,
            )
        )
    with pytest.raises(CollectionOrchestrationError, match="between zero and five"):
        asyncio.run(
            run_collection_stages(
                [StageSpec("cleanup", "cleanup", "Cleanup", "unit", source)],
                remaining_seconds=1,
                cleanup_seconds=DEFAULT_CLEANUP_SECONDS + 0.01,
            )
        )
    with pytest.raises(CollectionOrchestrationError):
        asyncio.run(
            run_collection_stages(
                [
                    StageSpec(
                        "negative",
                        "negative",
                        "Negative",
                        "unit",
                        source,
                        planned_units=-1,
                    )
                ],
                remaining_seconds=1,
            )
        )


@pytest.mark.parametrize(
    "counts",
    [
        {"planned": True},
        {"started": 1.0},
        {"terminal": -1},
        {"planned": 1, "started": 1, "terminal": 2},
        {"planned": 2, "started": 1, "unattempted": 0},
        {
            "planned": 1,
            "started": 1,
            "terminal": 1,
            "completed": 0,
            "errors": 0,
            "timeouts": 0,
            "cancelled": 0,
        },
    ],
)
def test_stage_counts_reject_invalid_types_and_inconsistent_components(counts):
    with pytest.raises(CollectionOrchestrationError):
        StageCounts(**counts)


def test_server_owned_planned_count_cannot_be_overridden_by_callback_counts():
    assert StageCounts(planned=99).with_planned(2).planned == 2
    assert StageCounts(planned=99).with_planned(None).planned == 99
    assert profile_stage_engine_ids() == {
        "native": "native-profile-search",
        "maigret": "maigret",
        "github": "github-public-profile",
        "unfurl": "unfurl-url-analysis",
        "wayback": "wayback-cdx",
        "user_scanner_username": "user-scanner-username",
        "user_scanner_email": "user-scanner",
    }


@pytest.mark.asyncio
async def test_stage_timeout_is_not_reported_as_overall_budget_exhaustion():
    async def slow(_context):
        await asyncio.sleep(1)
        return complete()

    summary = await run_collection_stages(
        [StageSpec("slow", "slow", "Slow", "unit", slow)],
        remaining_seconds=0.01,
        cleanup_seconds=0,
    )

    assert summary.outcomes[0].status == "timed_out"
    assert summary.stage_budget_exhausted is True
    assert summary.overall_budget_exhausted is False
    assert summary.budget_exhausted is False
    assert summary.as_dict()["stage_budget_exhausted"] is True
    assert summary.as_dict()["overall_budget_exhausted"] is False


def test_callback_cancellation_at_its_stage_deadline_is_a_stage_timeout():
    readings = iter((0.0, 0.0, 0.0, 0.0, 1.0))

    def clock():
        return next(readings, 1.0)

    async def self_cancel(_context):
        raise asyncio.CancelledError()

    summary = asyncio.run(
        run_collection_stages(
            [StageSpec("slow", "slow", "Slow", "unit", self_cancel)],
            remaining_seconds=1,
            clock=clock,
        )
    )

    assert summary.outcomes[0].status == "timed_out"
    assert summary.outcomes[0].reason == "stage_budget_exhausted"
    assert summary.state == "failed"


def test_completed_rows_with_unattempted_or_unknown_accounting_are_partial():
    partial_counts = StageCounts(
        planned=2,
        started=1,
        terminal=1,
        completed=1,
        errors=0,
        timeouts=0,
        cancelled=0,
        interrupted=0,
        unattempted=1,
        unknown=0,
        observations=0,
    )
    summary = asyncio.run(
        run_collection_stages(
            [
                StageSpec(
                    "partial",
                    "partial",
                    "Partial",
                    "unit",
                    lambda _context: _completed(partial_counts),
                    planned_units=2,
                )
            ],
            remaining_seconds=1,
        )
    )
    assert summary.outcomes[0].status == "completed"
    assert summary.state == "partial"


async def _completed(counts):
    return StageResult(counts=counts)


@pytest.mark.asyncio
async def test_progress_snapshots_use_live_ledger_counts_and_preserve_measurements():
    events, progress = [], []

    async def source(context):
        assert context.operation_ledger.record_planned("site-a")
        assert context.operation_ledger.record_started("site-a")
        assert context.operation_ledger.record_terminal(
            "site-a", "completed", attempted=True, observations=1
        )
        assert context.publish_progress(StageCounts(observations=4))
        return StageResult(counts=StageCounts(observations=9))

    summary = await run_collection_stages(
        [StageSpec("maigret", "maigret", "Maigret", "site_check", source, planned_units=None)],
        remaining_seconds=1,
        event_sink=events.append,
        on_progress=progress.append,
    )

    live = next(
        event["collection_accounting"]
        for event in events
        if event["collection_accounting"]["stages"][0]["observations"] == 4
    )
    assert live["stages"][0]["planned"] == 1
    assert live["stages"][0]["started"] == 1
    assert live["stages"][0]["terminal"] == 1
    assert progress[-1]["collection_accounting"]["revision"] == summary.revision
    assert len(progress[-1]["collection_accounting"]["stages"]) == 1
    assert summary.outcomes[0].counts.observations == 9


@pytest.mark.asyncio
async def test_progress_sink_failure_cleans_up_and_stops_later_admission():
    events, later_calls = [], []

    def sink(event):
        events.append(event)
        if len(events) == 3:
            raise OSError("accounting store unavailable")

    async def source(context):
        with pytest.raises(OSError, match="accounting store unavailable"):
            context.publish_progress()
        raise RuntimeError("source handled the durable error")

    async def later(_context):
        later_calls.append("later")
        return complete()

    with pytest.raises(OSError, match="accounting store unavailable"):
        await run_collection_stages(
            [
                StageSpec("first", "first", "First", "unit", source),
                StageSpec("later", "later", "Later", "unit", later),
            ],
            remaining_seconds=1,
            event_sink=sink,
        )
    assert later_calls == []


@pytest.mark.asyncio
async def test_dynamic_site_plan_uses_ledger_counts_without_username_cap():
    async def source(context):
        for operation_id in ("check-a", "check-b", "check-c"):
            assert context.operation_ledger.record_planned(operation_id)
            assert context.operation_ledger.record_terminal(
                operation_id,
                "completed",
                attempted=True,
                observations=1,
            )
        return StageResult(counts=StageCounts(observations=9))

    summary = await run_collection_stages(
        [StageSpec("maigret", "maigret", "Maigret", "site_check", source, planned_units=None)],
        remaining_seconds=1,
    )

    counts = summary.outcomes[0].counts
    assert counts.planned == 3
    assert counts.started == 3
    assert counts.terminal == 3
    assert counts.completed == 3
    assert counts.observations == 9
    assert counts.known is True


@pytest.mark.asyncio
async def test_typed_stop_cause_reaches_callback_and_persists_downstream_reason():
    entered = asyncio.Event()
    observed = []

    async def slow_source(context):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            observed.append(context.stop_cause())
            return complete()

    async def must_not_run(_context):
        raise AssertionError("typed stop must halt later admission")

    summary = await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", slow_source),
            StageSpec("later", "later", "Later", "unit", must_not_run, planned_units=4),
        ],
        remaining_seconds=1,
        cancellation_check=lambda: "lease_lost" if entered.is_set() else False,
    )

    assert observed == [StopCause.LEASE_LOST]
    assert [outcome.status for outcome in summary.outcomes] == [
        "interrupted",
        "interrupted",
    ]
    assert [outcome.reason for outcome in summary.outcomes] == [
        "lease_lost",
        "lease_lost",
    ]
    assert all(
        outcome.stop_cause is StopCause.LEASE_LOST for outcome in summary.outcomes
    )
    assert summary.stop_cause is StopCause.LEASE_LOST
    assert summary.as_dict()["stop_cause"] == "lease_lost"
    assert summary.as_dict()["stages"][1]["unattempted"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("planned", [1, 4, 16])
async def test_typed_stop_reason_accounts_for_each_unstarted_fixture_target(planned):
    calls = []

    async def first(_context):
        calls.append("first")
        return complete()

    async def later(_context):
        raise AssertionError("job deadline must stop later target admission")

    summary = await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", first),
            StageSpec(
                "later", "later", "Later", "target", later, planned_units=planned
            ),
        ],
        remaining_seconds=1,
        cancellation_check=lambda: StopCause.JOB_DEADLINE if calls else False,
    )

    downstream = summary.outcomes[1]
    assert downstream.status == "timed_out"
    assert downstream.reason == "overall_budget_exhausted"
    assert downstream.stop_cause is StopCause.JOB_DEADLINE
    assert downstream.counts.unattempted == planned


@pytest.mark.asyncio
async def test_cooperative_stage_deadline_keeps_selected_downstream_admissible():
    callbacks = []

    async def slow_source(context):
        callbacks.append("slow")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            assert context.stop_cause() is StopCause.STAGE_DEADLINE
            return complete()

    async def downstream(_context):
        callbacks.append("downstream")
        return complete()

    summary = await run_collection_stages(
        [
            StageSpec("slow", "slow", "Slow", "unit", slow_source),
            StageSpec("downstream", "downstream", "Downstream", "unit", downstream),
        ],
        remaining_seconds=0.1,
        cleanup_seconds=0.05,
    )

    assert callbacks == ["slow", "downstream"]
    assert summary.outcomes[0].status == "timed_out"
    assert summary.outcomes[0].cleanup_complete is True
    assert summary.outcomes[0].stop_cause is StopCause.STAGE_DEADLINE
    assert summary.outcomes[1].status == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause",
    [StopCause.PERSISTENCE_FAILURE, StopCause.WORKER_SHUTDOWN],
)
async def test_typed_boundary_stops_are_never_labeled_operator_cancellation(cause):
    admitted = []

    async def first(_context):
        admitted.append("first")
        return complete()

    async def later(_context):
        raise AssertionError("boundary failure must stop admission")

    summary = await run_collection_stages(
        [
            StageSpec("first", "first", "First", "unit", first),
            StageSpec("later", "later", "Later", "unit", later),
        ],
        remaining_seconds=1,
        cancellation_check=lambda: cause if admitted else False,
    )

    downstream = summary.outcomes[1]
    assert downstream.status == "interrupted"
    assert downstream.reason == cause.value
    assert downstream.stop_cause is cause
    assert "cancellation" not in downstream.reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("signal", "expected_cause", "expected_reason"),
    [
        (True, StopCause.OPERATOR_CANCEL, "cancellation_requested"),
        (StopCause.JOB_DEADLINE, StopCause.JOB_DEADLINE, "overall_budget_exhausted"),
        ("lease_lost", StopCause.LEASE_LOST, "lease_lost"),
        (False, None, "parent_cancelled"),
    ],
)
async def test_parent_cancellation_preserves_active_stop_cause_or_unknown(
    signal, expected_cause, expected_reason
):
    entered = asyncio.Event()
    external_cancel = asyncio.Event()
    events = []

    async def blocking(_context):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_collection_stages(
            [
                StageSpec("first", "first", "First", "unit", blocking),
                StageSpec(
                    "later", "later", "Later", "target", blocking, planned_units=4
                ),
            ],
            remaining_seconds=1,
            cancellation_check=lambda: signal if external_cancel.is_set() else False,
            event_sink=events.append,
        )
    )
    await entered.wait()
    external_cancel.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    rows = events[-1]["collection_accounting"]["stages"]
    assert [row["status"] for row in rows] == ["interrupted", "interrupted"]
    assert [row["reason"] for row in rows] == [expected_reason, expected_reason]
    assert [row["stop_cause"] for row in rows] == [
        expected_cause.value if expected_cause is not None else None,
        expected_cause.value if expected_cause is not None else None,
    ]
