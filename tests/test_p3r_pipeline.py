# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Offline, production-shaped acceptance for the P3R collection driver."""

import asyncio
import hashlib
import queue
import socket
from types import SimpleNamespace

import pytest

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web import app as web_app
from maigret.web.collector_adapters import (
    claimed_profile_url_targets,
    github_profile_targets,
)
from maigret.web.investigation_input import (
    build_unified_investigation_plan,
    search_usernames,
)
from maigret.web.profile_discovery_policy import govern_profile_discovery_options


@pytest.fixture(autouse=True)
def _forbid_external_io(monkeypatch):
    def rejected(*_args, **_kwargs):
        raise AssertionError("offline pipeline acceptance attempted network I/O")

    monkeypatch.setattr(socket, "create_connection", rejected)
    monkeypatch.setattr(socket.socket, "connect", rejected)
    monkeypatch.setattr(socket.socket, "connect_ex", rejected)


class _CompressedBudget:
    def __init__(self, seconds=0.8):
        self.seconds = seconds

    def remaining_seconds(self):
        return self.seconds


def _digest(*parts):
    return hashlib.sha256("\x1f".join(map(str, parts)).encode()).hexdigest()


def _task(stage, target, index):
    source_id = _digest("source", stage)
    target_id = _digest("target", target)
    check_id = _digest("check", source_id, target_id, index)
    return {
        "schema_version": 1,
        "source_id": source_id,
        "target_id": target_id,
        "check_id": check_id,
        "task_id": _digest("task", check_id, 0),
        "attempt": 0,
    }


def _alias_plan(
    alias_count,
    *,
    include_email=False,
    confirm_email=False,
    optional_collectors=True,
):
    tokens = ["Alice Example", "Bob Sample"]
    token_types = ["full_name", "full_name"]
    if include_email:
        tokens.append("analyst@example.org")
        token_types.append("email")
    base = {
        "investigation_token": tokens,
        "investigation_token_type": token_types,
        "mode": "quick",
        "search_likely_username_aliases": "on",
    }
    preview = build_unified_investigation_plan(base)
    candidates = [item["value"] for item in preview["alias_candidates"]]
    form = {
        **base,
        "alias_candidates_present": "1",
        "alias_candidate": candidates,
        "selected_alias": candidates[:alias_count],
    }
    if confirm_email:
        form["confirm_email_route"] = "on"
    if optional_collectors:
        form.update(
            {
                "enable_user_scanner_username": "on",
                "user_scanner_platforms_present": "1",
                "user_scanner_platform": ["instagram", "x"],
                "allow_user_scanner_vxtwitter": "on",
                "enable_github_profile_enrichment": "on",
                "enable_archived_url_evidence": "on",
            }
        )
    plan = build_unified_investigation_plan(
        form,
        require_route_confirmation=not include_email or confirm_email,
    )
    assert len(search_usernames(plan)) == alias_count
    return plan


def _options(plan):
    return govern_profile_discovery_options(
        {
            "execution_mode": "focused",
            "investigation_spec": plan,
        },
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true"},
    )


def _native_result(
    *,
    planned=2,
    executed=2,
    active=0,
    interrupted=0,
    errors=0,
):
    return SimpleNamespace(
        planned_query_count=planned,
        attempted_query_count=executed + active + interrupted,
        executed_query_count=executed,
        error_count=errors,
        interrupted_query_count=interrupted,
        unattempted_query_count=planned - executed - active - interrupted,
        active_query_count=active,
        candidates=(),
    )


def _events(job):
    return list(job["queue"].queue)


def _stage_map(job):
    return {
        stage["stage_id"]: stage for stage in job["collection_accounting"]["stages"]
    }


def _assert_complete_task_log(events):
    planned = {}
    terminal = {}
    for event in events:
        if event.get("type") == "collection_task_plan":
            for task in event["tasks"]:
                assert task["task_id"] not in planned
                planned[task["task_id"]] = task
        elif event.get("type") == "collection_task_terminal":
            for task in event["tasks"]:
                assert task["task_id"] not in terminal
                terminal[task["task_id"]] = task
    assert planned
    assert set(terminal) == set(planned)
    assert all(
        task["attempted"]
        or task["disposition"] in {"unattempted", "unknown", "interrupted"}
        for task in terminal.values()
    )
    return planned, terminal


def _assert_accounting_identity(stage):
    counts = {
        name: stage[name]
        for name in (
            "planned",
            "started",
            "terminal",
            "completed",
            "errors",
            "timeouts",
            "cancelled",
            "interrupted",
            "unattempted",
            "unknown",
        )
    }
    assert all(value is not None for value in counts.values())
    assert counts["planned"] == counts["started"] + counts["unattempted"]
    assert counts["started"] == (
        counts["terminal"] + counts["interrupted"] + counts["unknown"]
    )
    assert counts["terminal"] == (
        counts["completed"]
        + counts["errors"]
        + counts["timeouts"]
        + counts["cancelled"]
    )


def _install_fake_adapters(monkeypatch, control):
    control.setdefault("calls", [])
    control.setdefault("contexts", [])
    control.setdefault("scanner_policies", [])

    async def native(job, _options, cancellation_check=None):
        control["calls"].append(("native", None))
        if control.get("stall") == "native":
            control["stall_entered"].set()
            await asyncio.Event().wait()
        progress = job["native_profile_progress_sink"]
        progress(_native_result(planned=2, executed=0, active=1))
        await asyncio.sleep(0)
        result = _native_result()
        progress(result)
        return result

    async def maigret(username, _options, query_notify=None):
        control["calls"].append(("maigret", username))
        context = query_notify.stage_context
        control["contexts"].append(context)
        assert context.stage_id == "maigret"
        assert 0 < context.remaining_seconds() <= control["budget_seconds"]
        tasks = [_task("maigret", username, index) for index in range(2)]
        query_notify.task_plan(tasks)

        completed = {
            **tasks[0],
            "disposition": "completed",
            "attempted": True,
            "cleanup_state": "not_required",
        }
        query_notify.task_terminal(completed)
        result = MaigretCheckResult(
            username=username,
            site_name="GitHub",
            site_url_user=f"https://github.com/{username}",
            status=MaigretCheckStatus.CLAIMED,
            ids_data={"github_id": _digest(username)[:16]},
            http_status=200,
        )
        query_notify.set_total(2)
        query_notify.update(result)

        if control.get("cancel_after_first") and len(control["contexts"]) == 2:
            context.operation_ledger.record_started(tasks[1]["task_id"])
            control["stall_entered"].set()
            try:
                await asyncio.Event().wait()
            finally:
                query_notify.task_terminal(
                    {
                        **tasks[1],
                        "disposition": "interrupted",
                        "attempted": True,
                        "cleanup_state": "complete",
                    }
                )
            raise AssertionError("unreachable")

        if control.get("stall") == "maigret":
            context.operation_ledger.record_started(tasks[1]["task_id"])
            control["stall_entered"].set()
            try:
                await asyncio.Event().wait()
            finally:
                query_notify.task_terminal(
                    {
                        **tasks[1],
                        "disposition": "interrupted",
                        "attempted": True,
                        "cleanup_state": "complete",
                    }
                )
            raise AssertionError("unreachable")

        query_notify.task_terminal(
            {
                **tasks[1],
                "disposition": "error",
                "attempted": True,
                "cleanup_state": "not_required",
                "reason": "fixture_error",
            }
        )
        return {
            "GitHub": {
                "status": result,
                "url_user": result.site_url_user,
                "http_status": 200,
            }
        }

    async def github(target):
        control["calls"].append(("github", target["github_login"]))
        return {
            "source_engine": "github_public_profile",
            "subject_value": target["github_login"],
            "status": "observed",
            "source_url": target["profile_url"],
        }

    async def unfurl(target):
        control["calls"].append(("unfurl", target["profile_url"]))
        return {
            "source_engine": "unfurl_url_analysis",
            "subject_value": target["investigated_username"],
            "status": "analyzed",
            "source_url": target["profile_url"],
        }

    async def wayback(target):
        control["calls"].append(("wayback", target["profile_url"]))
        return {
            "source_engine": "wayback_cdx",
            "subject_value": target["investigated_username"],
            "status": "archived",
            "source_url": target["profile_url"],
        }

    async def usernames(
        targets,
        *,
        platforms,
        allow_vxtwitter,
        observation_sink,
        cancellation_check,
    ):
        control["calls"].append(("usernames", tuple(targets)))
        control["scanner_policies"].append((tuple(platforms), allow_vxtwitter))
        assert not cancellation_check()
        observations = [
            {
                "source_engine": "user_scanner_username",
                "subject_value": target,
                "status": "registered",
            }
            for target in targets
        ]
        observation_sink(observations)
        return observations

    async def email(target, *, cancellation_check):
        control["calls"].append(("email", target))
        assert not cancellation_check()
        return {
            "source_engine": "user_scanner",
            "subject_value": target,
            "status": "registered",
        }

    monkeypatch.setattr(web_app, "run_native_profile_search_phase", native)
    monkeypatch.setattr(web_app, "maigret_search", maigret)
    monkeypatch.setattr(web_app, "run_github_public_profile", github)
    monkeypatch.setattr(web_app, "run_unfurl_url_analysis", unfurl)
    monkeypatch.setattr(web_app, "run_wayback_capture_index", wayback)
    monkeypatch.setattr(web_app, "run_user_scanner_usernames", usernames)
    monkeypatch.setattr(web_app, "run_user_scanner_email", email)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_count", (1, 4, 16))
async def test_pipeline_preserves_aliases_and_accounts_six_eligible_stages(
    monkeypatch,
    alias_count,
):
    plan = _alias_plan(alias_count)
    options = _options(plan)
    usernames = search_usernames(plan)
    checkpoints = []
    control = {"budget_seconds": 0.8}
    _install_fake_adapters(monkeypatch, control)
    job = {
        "job_id": f"pipeline-{alias_count}",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(control["budget_seconds"]),
        "collection_checkpoint_sink": lambda snapshot: (
            checkpoints.append(snapshot) or True
        ),
        "native_profile_checkpoint_sink": lambda _result: True,
    }

    general_results = await web_app._stream_search(job, usernames, options)

    assert [item[0] for item in general_results] == usernames
    assert len(control["contexts"]) == alias_count
    assert control["scanner_policies"] == [(("instagram", "x"), True)] * alias_count
    stages = _stage_map(job)
    assert {
        stage_id for stage_id, row in stages.items() if row["status"] == "completed"
    } == {
        "native",
        "maigret",
        "github",
        "unfurl",
        "wayback",
        "user_scanner_username",
    }
    assert stages["user_scanner_email"]["status"] == "not_selected"
    assert stages["user_scanner_email"]["planned"] == 0
    assert job["collection_accounting"]["state"] == "partial"
    assert job["collection_accounting"]["known"] is True
    assert stages["maigret"]["planned"] == 2 * alias_count
    assert stages["maigret"]["completed"] == alias_count
    assert stages["maigret"]["errors"] == alias_count
    assert stages["maigret"]["observations"] == alias_count

    expected_github = len(github_profile_targets(general_results, plan))
    expected_urls = len(claimed_profile_url_targets(general_results, plan))
    assert stages["github"]["planned"] == expected_github
    assert stages["unfurl"]["planned"] == expected_urls
    assert stages["wayback"]["planned"] == expected_urls
    assert stages["user_scanner_username"]["planned"] == alias_count
    for stage in stages.values():
        _assert_accounting_identity(stage)

    planned, terminal = _assert_complete_task_log(_events(job))
    assert len(planned) == (
        2 * alias_count + expected_github + 2 * expected_urls + alias_count
    )
    assert sum(item["disposition"] == "error" for item in terminal.values()) == (
        alias_count
    )
    assert checkpoints
    assert checkpoints[-1]["usernames"] == usernames
    assert checkpoints[-1]["found_count"] == alias_count
    assert len(checkpoints[-1]["collector_observations"]) == (
        expected_github + 2 * expected_urls + alias_count
    )
    accounting_events = [
        event for event in _events(job) if event["type"] == "collection_accounting"
    ]
    native_progress = [
        _stage_map({"collection_accounting": event["collection_accounting"]})["native"]
        for event in accounting_events
        if _stage_map({"collection_accounting": event["collection_accounting"]})[
            "native"
        ]["status"]
        == "running"
    ]
    assert any(
        row["planned"] == 2
        and row["started"] == 1
        and row["terminal"] == 0
        and row["unknown"] is None
        for row in native_progress
    )
    assert stages["native"]["planned"] == 2
    assert stages["native"]["completed"] == 2
    assert stages["native"]["terminal"] == 2
    assert [
        event["collection_accounting"]["revision"] for event in accounting_events
    ] == list(range(1, len(accounting_events) + 1))
    assert (
        accounting_events[-1]["collection_accounting"] == job["collection_accounting"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("confirmed", "expected_status", "expected_calls"),
    ((False, "not_selected", 0), (True, "completed", 1)),
)
async def test_email_route_requires_confirmation_and_known_zero_is_preserved(
    monkeypatch,
    confirmed,
    expected_status,
    expected_calls,
):
    plan = _alias_plan(
        1,
        include_email=True,
        confirm_email=confirmed,
        optional_collectors=False,
    )
    options = _options(plan)
    control = {"budget_seconds": 0.5}
    _install_fake_adapters(monkeypatch, control)
    job = {
        "job_id": f"email-{confirmed}",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(control["budget_seconds"]),
    }

    await web_app._stream_search(job, search_usernames(plan), options)

    email = _stage_map(job)["user_scanner_email"]
    assert email["status"] == expected_status
    assert email["planned"] == expected_calls
    assert email["unattempted"] == 0
    assert sum(name == "email" for name, _value in control["calls"]) == expected_calls
    _assert_accounting_identity(email)


@pytest.mark.asyncio
@pytest.mark.parametrize("stalled_stage", ("native", "maigret"))
async def test_stage_stalls_use_reserved_deadlines_and_later_work_still_accounts(
    monkeypatch,
    stalled_stage,
):
    plan = _alias_plan(1)
    options = _options(plan)
    control = {
        "budget_seconds": 0.25,
        "stall": stalled_stage,
        "stall_entered": asyncio.Event(),
    }
    _install_fake_adapters(monkeypatch, control)
    checkpoints = []
    job = {
        "job_id": f"stall-{stalled_stage}",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(control["budget_seconds"]),
        "collection_checkpoint_sink": lambda snapshot: (
            checkpoints.append(snapshot) or True
        ),
        "native_profile_checkpoint_sink": lambda _result: True,
    }

    await asyncio.wait_for(
        web_app._stream_search(job, search_usernames(plan), options),
        timeout=1,
    )

    stages = _stage_map(job)
    assert stages[stalled_stage]["status"] == "timed_out"
    assert stages[stalled_stage]["reason"] == "stage_budget_exhausted"
    assert all(
        stage["reason"] != "overall_budget_exhausted" for stage in stages.values()
    )
    if stalled_stage == "native":
        assert stages["maigret"]["status"] == "completed"
        assert stages["user_scanner_username"]["status"] == "completed"
    else:
        assert stages["user_scanner_username"]["status"] == "completed"
        # The first Maigret observation was checkpointed before its second
        # operation stalled, so dependent targets remain eligible.
        assert stages["github"]["status"] == "completed"
        assert stages["unfurl"]["status"] == "completed"
        assert stages["wayback"]["status"] == "completed"
    assert checkpoints


@pytest.mark.asyncio
async def test_cancellation_retains_partial_checkpoint_and_accounts_every_task(
    monkeypatch,
):
    plan = _alias_plan(4)
    options = _options(plan)
    checkpoints = []
    control = {
        "budget_seconds": 0.8,
        "cancel_after_first": True,
        "stall_entered": asyncio.Event(),
    }
    _install_fake_adapters(monkeypatch, control)
    job = {
        "job_id": "cancel-partial",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(control["budget_seconds"]),
        "collection_checkpoint_sink": lambda snapshot: (
            checkpoints.append(snapshot) or True
        ),
        "native_profile_checkpoint_sink": lambda _result: True,
    }

    running = asyncio.create_task(
        web_app._stream_search(job, search_usernames(plan), options)
    )
    await asyncio.wait_for(control["stall_entered"].wait(), timeout=1)
    job["cancelled"] = True
    await asyncio.wait_for(running, timeout=1)

    stages = _stage_map(job)
    assert job["collection_accounting"]["state"] == "cancelled"
    assert stages["maigret"]["status"] == "cancelled"
    assert stages["maigret"]["observations"] == sum(len(item[2]) for item in job['general_results'])
    # A cancelled in-flight provider check has no source response.  Its
    # terminal outcome remains unknown rather than becoming a negative.
    assert stages["maigret"]["unknown"] == 1
    assert stages["maigret"]["unattempted"] == 0
    assert all(
        stages[stage_id]["status"] == "cancelled"
        for stage_id in (
            "github",
            "unfurl",
            "wayback",
            "user_scanner_username",
        )
    )
    assert stages["user_scanner_email"]["status"] == "not_selected"
    _assert_complete_task_log(_events(job))
    assert checkpoints
    assert checkpoints[-1]["found_count"] == 2
    assert len(checkpoints[-1]["individual_reports"]) == 2
    assert checkpoints[-1]["collection_accounting"]["state"] == "cancelled"


@pytest.mark.asyncio
async def test_maigret_cancellation_drains_source_finalizer_before_notifier_close(
    monkeypatch,
):
    """A cooperative adapter may emit its terminal event from ``finally``."""
    plan = _alias_plan(2)
    options = _options(plan)
    control = {
        "budget_seconds": 0.8,
        "cancel_after_first": True,
        "stall_entered": asyncio.Event(),
    }
    _install_fake_adapters(monkeypatch, control)
    job = {
        "job_id": "cancel-drain-maigret",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(control["budget_seconds"]),
        "collection_checkpoint_sink": lambda _snapshot: True,
        "native_profile_checkpoint_sink": lambda _result: True,
    }

    running = asyncio.create_task(
        web_app._stream_search(job, search_usernames(plan), options)
    )
    await asyncio.wait_for(control["stall_entered"].wait(), timeout=1)
    job["cancelled"] = True
    await asyncio.wait_for(running, timeout=1)

    planned, terminal = _assert_complete_task_log(_events(job))
    assert set(terminal) == set(planned)
    maigret = _stage_map(job)["maigret"]
    assert maigret["status"] == "cancelled"
    # The source committed the interrupted terminal event before notifier
    # closure. The stage ledger preserves its in-flight disposition as unknown.
    assert maigret["unknown"] == 1
