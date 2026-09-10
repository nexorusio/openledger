# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Offline fairness regressions for Maigret's selected alias scheduling."""

import asyncio
import hashlib
import queue
import socket

import pytest

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web import app as web_app
from maigret.web.investigation_input import (
    build_unified_investigation_plan,
    search_usernames,
)
from maigret.web.profile_discovery_policy import govern_profile_discovery_options


@pytest.fixture(autouse=True)
def _forbid_external_io(monkeypatch):
    def rejected(*_args, **_kwargs):
        raise AssertionError("alias fairness fixture attempted network I/O")

    monkeypatch.setattr(socket, "create_connection", rejected)
    monkeypatch.setattr(socket.socket, "connect", rejected)
    monkeypatch.setattr(socket.socket, "connect_ex", rejected)
    monkeypatch.setenv("OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED", "false")
    monkeypatch.setenv("OPENLEDGER_MAIGRET_DISCOVERY_ENABLED", "true")
    monkeypatch.setenv("OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED", "true")


class _CompressedBudget:
    def __init__(self, seconds=0.2):
        self.seconds = seconds

    def remaining_seconds(self):
        return self.seconds


def _digest(*parts):
    return hashlib.sha256("\x1f".join(map(str, parts)).encode()).hexdigest()


def _task(username):
    check_id = _digest("maigret", username)
    return {
        "schema_version": 1,
        "source_id": _digest("source", "maigret"),
        "target_id": _digest("target", username),
        "check_id": check_id,
        "task_id": _digest("task", check_id),
        "attempt": 0,
    }


def _options(alias_count):
    base = {
        "investigation_token": ["Alice Example", "Bob Sample"],
        "investigation_token_type": ["full_name", "full_name"],
        "mode": "quick",
        "search_likely_username_aliases": "on",
    }
    preview = build_unified_investigation_plan(base)
    candidates = [item["value"] for item in preview["alias_candidates"]]
    plan = build_unified_investigation_plan(
        {
            **base,
            "alias_candidates_present": "1",
            "alias_candidate": candidates,
            "selected_alias": candidates[:alias_count],
            "enable_github_profile_enrichment": "on",
        }
    )
    usernames = search_usernames(plan)
    assert len(usernames) == alias_count
    options = govern_profile_discovery_options(
        {"execution_mode": "focused", "investigation_spec": plan},
        environ={
            "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "false",
            "OPENLEDGER_MAIGRET_DISCOVERY_ENABLED": "true",
            "OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED": "true",
        },
    )
    return options, usernames


def _stage(job, stage_id):
    return next(
        row
        for row in job["collection_accounting"]["stages"]
        if row["stage_id"] == stage_id
    )


def _assert_counts_reconcile(row):
    counts = {
        name: row[name]
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


def _claimed_result(username):
    return MaigretCheckResult(
        username=username,
        site_name="GitHub",
        site_url_user=f"https://github.com/{username}",
        status=MaigretCheckStatus.CLAIMED,
        ids_data={"github_id": _digest(username)[:16]},
        http_status=200,
    )


def _install_alias_adapters(monkeypatch, *, usernames, first_entered, operator_mode):
    completed_aliases = []
    github_calls = []

    async def maigret(username, _options, query_notify=None):
        task = _task(username)
        query_notify.task_plan([task])
        if username == usernames[0]:
            first_entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # The source-stage cancellation can close the notifier before
                # the child unwinds.  A target-slice timeout is awaited by the
                # driver and has a durable terminal task; an operator stop
                # deliberately leaves this planned task unattempted.
                if not operator_mode:
                    query_notify.task_terminal(
                        {
                            **task,
                            "disposition": "timeout",
                            "attempted": True,
                            "cleanup_state": "not_required",
                        }
                    )
                assert query_notify.cancellation_check()
                return {}
        result = _claimed_result(username)
        query_notify.task_terminal(
            {
                **task,
                "disposition": "completed",
                "attempted": True,
                "cleanup_state": "not_required",
            }
        )
        completed_aliases.append(username)
        return {
            "GitHub": {
                "status": result,
                "url_user": result.site_url_user,
                "http_status": result.http_status,
            }
        }

    async def github(target):
        github_calls.append(target["github_login"])
        return {
            "source_engine": "github_public_profile",
            "subject_value": target["github_login"],
            "status": "observed",
            "source_url": target["profile_url"],
        }

    async def forbidden_native(*_args, **_kwargs):
        raise AssertionError("native source was not selected")

    monkeypatch.setattr(web_app, "maigret_search", maigret)
    monkeypatch.setattr(web_app, "run_github_public_profile", github)
    monkeypatch.setattr(web_app, "run_native_profile_search_phase", forbidden_native)
    return completed_aliases, github_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_count", [1, 4, 16])
async def test_cooperative_alias_timeout_releases_later_aliases_and_github(
    monkeypatch, alias_count
):
    options, usernames = _options(alias_count)
    first_entered = asyncio.Event()
    completed_aliases, github_calls = _install_alias_adapters(
        monkeypatch,
        usernames=usernames,
        first_entered=first_entered,
        operator_mode=False,
    )
    job = {
        "job_id": f"alias-timeout-{alias_count}",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(seconds=0.25),
    }

    general_results = await web_app._stream_search(job, usernames, options)

    assert first_entered.is_set()
    assert completed_aliases == usernames[1:]
    assert [item[0] for item in general_results] == usernames
    maigret = _stage(job, "maigret")
    github = _stage(job, "github")
    assert maigret["status"] == "completed"
    assert maigret["stop_cause"] is None
    assert maigret["planned"] == alias_count
    assert maigret["timeouts"] == 1
    assert maigret["completed"] == alias_count - 1
    if alias_count == 1:
        assert github_calls == []
        assert github["status"] == "skipped_no_targets"
    else:
        assert github_calls
        assert set(github_calls).issubset(set(usernames[1:]))
        assert github["status"] == "completed"
        assert github["planned"] == len(github_calls)
        assert github["completed"] == len(github_calls)
    _assert_counts_reconcile(maigret)
    _assert_counts_reconcile(github)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_count", [1, 4, 16])
async def test_operator_cancel_stops_source_and_downstream_unlike_alias_timeout(
    monkeypatch, alias_count
):
    options, usernames = _options(alias_count)
    first_entered = asyncio.Event()
    completed_aliases, github_calls = _install_alias_adapters(
        monkeypatch,
        usernames=usernames,
        first_entered=first_entered,
        operator_mode=True,
    )
    job = {
        "job_id": f"alias-operator-{alias_count}",
        "queue": queue.Queue(),
        "cancelled": False,
        "execution_budget_object": _CompressedBudget(seconds=1),
    }
    stream = asyncio.create_task(web_app._stream_search(job, usernames, options))
    await first_entered.wait()
    job["cancelled"] = True
    await stream

    assert completed_aliases == []
    assert github_calls == []
    maigret = _stage(job, "maigret")
    github = _stage(job, "github")
    assert maigret["status"] == "cancelled"
    assert maigret["reason"] == "operator_cancel"
    assert maigret["stop_cause"] == "operator_cancel"
    assert github["status"] == "skipped_no_targets"
    assert github["reason"] == "no_eligible_targets"
    assert github["stop_cause"] is None
    _assert_counts_reconcile(maigret)
