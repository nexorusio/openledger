"""P3R native-search checkpoint contract tests."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import asyncio

import pytest

from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_orchestrator import ProfileSearchOrchestrator


def _plan():
    return {
        "identifiers": [],
        "search_targets": [{"value": "alice_example", "source_type": "username"}],
    }


def _success(query):
    return ProfileSearchRun(
        query=query,
        provenance=ProfileSearchProvenance.for_query(
            query,
            provider="brave",
            retrieved_at="2026-09-10T12:00:00Z",
        ),
        evidence=(
            ProfileSearchEvidence(
                result_rank=1,
                source_url=f"https://{query.platform}.com/alice_example",
                title="Alice Example",
            ),
        ),
    )


@pytest.mark.asyncio
async def test_running_checkpoints_account_for_active_then_interrupted_query():
    second_started = asyncio.Event()

    class Client:
        def __init__(self):
            self.calls = 0

        async def search(self, query):
            self.calls += 1
            if self.calls == 1:
                return _success(query)
            second_started.set()
            await asyncio.Event().wait()

    progress, terminal = [], []
    task = asyncio.create_task(
        ProfileSearchOrchestrator(Client()).discover(
            _plan(),
            platforms=("instagram", "x", "tiktok"),
            progress_sink=progress.append,
            result_sink=terminal.append,
        )
    )
    await second_started.wait()
    task.cancel()
    result = await task

    assert [
        (
            snapshot.status,
            snapshot.final,
            snapshot.executed_query_count,
            snapshot.active_query_count,
            snapshot.attempted_query_count,
            snapshot.unattempted_query_count,
        )
        for snapshot in progress
    ] == [
        ("running", False, 0, 1, 1, 2),
        ("running", False, 1, 0, 1, 2),
        ("running", False, 1, 1, 2, 1),
    ]
    assert result.status == "stopped"
    assert result.final is True
    assert result.active_query_count == 0
    assert result.interrupted_query_count == 1
    assert result.executed_query_count == 1
    assert result.attempted_query_count == 2
    assert result.unattempted_query_count == 1
    assert result.as_dict()["interrupted_query_count"] == 1
    assert terminal == [result]


@pytest.mark.asyncio
async def test_checkpoint_sink_failure_prevents_provider_dispatch():
    calls = []

    class Client:
        async def search(self, query):
            calls.append(query)
            return _success(query)

    def unavailable(_snapshot):
        raise OSError("checkpoint storage unavailable")

    with pytest.raises(OSError, match="checkpoint storage unavailable"):
        await ProfileSearchOrchestrator(Client()).discover(
            _plan(),
            platforms=("instagram",),
            progress_sink=unavailable,
        )
    assert calls == []


@pytest.mark.asyncio
async def test_final_result_sink_failure_is_not_converted_to_a_search_outcome():
    class Client:
        async def search(self, query):
            return _success(query)

    def unavailable(_result):
        raise OSError("audit storage unavailable")

    with pytest.raises(OSError, match="audit storage unavailable"):
        await ProfileSearchOrchestrator(Client()).discover(
            _plan(),
            platforms=("instagram",),
            result_sink=unavailable,
        )
