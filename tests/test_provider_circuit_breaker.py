import asyncio

import pytest

from maigret.web.provider_circuit_breaker import (
    ProviderCircuitBreaker,
    ProviderCircuitOpen,
)


def _run(coroutine):
    return asyncio.run(coroutine)


def _transient_error(error):
    return isinstance(error, RuntimeError)


def _transient_result(result):
    return result == "unavailable"


def test_three_transient_failures_open_circuit_without_retrying_operation():
    now = [0.0]
    breaker = ProviderCircuitBreaker(clock=lambda: now[0])
    calls = []

    async def fail_once():
        calls.append("called")
        raise RuntimeError("temporary provider failure")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            _run(
                breaker.call(
                    "github",
                    fail_once,
                    transient_error=_transient_error,
                    transient_result=_transient_result,
                )
            )

    assert len(calls) == 3
    assert breaker.snapshot("github")["status"] == "open"
    with pytest.raises(ProviderCircuitOpen) as blocked:
        _run(
            breaker.call(
                "github",
                fail_once,
                transient_error=_transient_error,
                transient_result=_transient_result,
            )
        )
    assert blocked.value.retry_after_seconds == 60
    assert len(calls) == 3


def test_only_one_half_open_probe_runs_and_success_closes_circuit():
    now = [0.0]
    breaker = ProviderCircuitBreaker(clock=lambda: now[0])
    for _ in range(3):
        permit = breaker.acquire("wayback")
        breaker.record_failure(permit)

    now[0] = 60.0
    probe = breaker.acquire("wayback")
    assert probe.half_open_probe is True
    assert breaker.snapshot("wayback")["status"] == "half_open"
    with pytest.raises(ProviderCircuitOpen):
        breaker.acquire("wayback")

    breaker.record_success(probe)
    assert breaker.snapshot("wayback") == {
        "provider": "wayback",
        "status": "closed",
        "consecutive_failures": 0,
        "failure_threshold": 3,
        "cooldown_seconds": 60.0,
        "retry_after_seconds": 0,
    }


def test_failed_half_open_probe_restarts_full_cooldown():
    now = [10.0]
    breaker = ProviderCircuitBreaker(clock=lambda: now[0])
    for _ in range(3):
        breaker.record_failure(breaker.acquire("user-scanner"))

    now[0] = 70.0
    probe = breaker.acquire("user-scanner")
    breaker.record_failure(probe)
    assert breaker.snapshot("user-scanner")["retry_after_seconds"] == 60

    now[0] = 129.5
    with pytest.raises(ProviderCircuitOpen) as blocked:
        breaker.acquire("user-scanner")
    assert blocked.value.retry_after_seconds == 1


def test_success_and_non_transient_errors_do_not_accumulate_failures():
    breaker = ProviderCircuitBreaker()

    async def transient_failure():
        raise RuntimeError("temporary")

    async def invalid_request():
        raise ValueError("invalid operator input")

    async def success():
        return "observed"

    with pytest.raises(RuntimeError):
        _run(
            breaker.call(
                "wikidata",
                transient_failure,
                transient_error=_transient_error,
                transient_result=_transient_result,
            )
        )
    assert breaker.snapshot("wikidata")["consecutive_failures"] == 1
    assert (
        _run(
            breaker.call(
                "wikidata",
                success,
                transient_error=_transient_error,
                transient_result=_transient_result,
            )
        )
        == "observed"
    )
    assert breaker.snapshot("wikidata")["consecutive_failures"] == 0

    with pytest.raises(ValueError):
        _run(
            breaker.call(
                "wikidata",
                invalid_request,
                transient_error=_transient_error,
                transient_result=_transient_result,
            )
        )
    assert breaker.snapshot("wikidata")["consecutive_failures"] == 0


def test_transient_diagnostic_results_count_without_becoming_exceptions():
    breaker = ProviderCircuitBreaker()

    async def unavailable():
        return "unavailable"

    for expected_failures in (1, 2, 3):
        assert (
            _run(
                breaker.call(
                    "google-places",
                    unavailable,
                    transient_error=_transient_error,
                    transient_result=_transient_result,
                )
            )
            == "unavailable"
        )
        assert (
            breaker.snapshot("google-places")["consecutive_failures"]
            == expected_failures
        )
    assert breaker.snapshot("google-places")["status"] == "open"
