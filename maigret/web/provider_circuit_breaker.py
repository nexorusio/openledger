"""Process-local circuit breakers for governed external providers."""

from __future__ import annotations

import asyncio
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

PROVIDER_FAILURE_THRESHOLD = 3
PROVIDER_COOLDOWN_SECONDS = 60


class ProviderCircuitOpen(RuntimeError):
    """Raised when provider work is skipped because its circuit is open."""

    def __init__(self, provider: str, retry_after_seconds: int):
        self.provider = provider
        self.retry_after_seconds = max(0, int(retry_after_seconds))
        super().__init__(
            f"Provider {provider} is temporarily unavailable; retry after "
            f"{self.retry_after_seconds} seconds"
        )


@dataclass(frozen=True)
class ProviderPermit:
    provider: str
    half_open_probe: bool


@dataclass
class _ProviderState:
    consecutive_failures: int = 0
    opened_at: Optional[float] = None
    half_open_probe_in_flight: bool = False


class ProviderCircuitBreaker:
    """Thread-safe provider circuits with one probe after a fixed cooldown."""

    def __init__(
        self,
        *,
        failure_threshold: int = PROVIDER_FAILURE_THRESHOLD,
        cooldown_seconds: float = PROVIDER_COOLDOWN_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.failure_threshold = max(1, int(failure_threshold))
        self.cooldown_seconds = max(0.001, float(cooldown_seconds))
        self._clock = clock
        self._lock = threading.Lock()
        self._states: Dict[str, _ProviderState] = {}

    @staticmethod
    def _provider_key(provider: str) -> str:
        key = str(provider or "").strip().casefold()
        if not key:
            raise ValueError("A provider circuit key is required")
        return key

    def acquire(self, provider: str) -> ProviderPermit:
        """Admit a normal call or the sole half-open probe; never retry here."""
        key = self._provider_key(provider)
        now = self._clock()
        with self._lock:
            state = self._states.setdefault(key, _ProviderState())
            if state.opened_at is None:
                return ProviderPermit(key, False)
            remaining = self.cooldown_seconds - (now - state.opened_at)
            if remaining > 0:
                raise ProviderCircuitOpen(key, math.ceil(remaining))
            if state.half_open_probe_in_flight:
                raise ProviderCircuitOpen(key, 0)
            state.half_open_probe_in_flight = True
            return ProviderPermit(key, True)

    def record_success(self, permit: ProviderPermit) -> None:
        with self._lock:
            state = self._states.setdefault(permit.provider, _ProviderState())
            if permit.half_open_probe:
                state.consecutive_failures = 0
                state.opened_at = None
                state.half_open_probe_in_flight = False
            elif state.opened_at is None:
                state.consecutive_failures = 0

    def record_failure(self, permit: ProviderPermit) -> None:
        now = self._clock()
        with self._lock:
            state = self._states.setdefault(permit.provider, _ProviderState())
            if permit.half_open_probe:
                state.consecutive_failures = self.failure_threshold
                state.opened_at = now
                state.half_open_probe_in_flight = False
                return
            # Ignore old in-flight outcomes after another call opened the circuit.
            if state.opened_at is not None:
                return
            state.consecutive_failures += 1
            if state.consecutive_failures >= self.failure_threshold:
                state.opened_at = now

    def abandon(self, permit: ProviderPermit) -> None:
        """Release a cancelled/non-transient half-open call without judging health."""
        if not permit.half_open_probe:
            return
        with self._lock:
            state = self._states.setdefault(permit.provider, _ProviderState())
            state.half_open_probe_in_flight = False

    async def call(
        self,
        provider: str,
        operation: Callable[[], Awaitable[Any]],
        *,
        transient_error: Callable[[Exception], bool],
        transient_result: Callable[[Any], bool],
    ) -> Any:
        """Run one admitted provider operation and record only its outcome."""
        permit = self.acquire(provider)
        try:
            result = await operation()
        except asyncio.CancelledError:
            self.abandon(permit)
            raise
        except Exception as error:
            if transient_error(error):
                self.record_failure(permit)
            else:
                self.abandon(permit)
            raise
        if transient_result(result):
            self.record_failure(permit)
        else:
            self.record_success(permit)
        return result

    def snapshot(self, provider: str) -> Dict[str, Any]:
        """Return bounded operational state without exposing request data."""
        key = self._provider_key(provider)
        now = self._clock()
        with self._lock:
            state = self._states.get(key, _ProviderState())
            if state.opened_at is None:
                status = "closed"
                retry_after = 0
            elif state.half_open_probe_in_flight:
                status = "half_open"
                retry_after = 0
            else:
                status = "open"
                retry_after = max(
                    0,
                    math.ceil(self.cooldown_seconds - (now - state.opened_at)),
                )
            return {
                "provider": key,
                "status": status,
                "consecutive_failures": state.consecutive_failures,
                "failure_threshold": self.failure_threshold,
                "cooldown_seconds": self.cooldown_seconds,
                "retry_after_seconds": retry_after,
            }

    def reset(self) -> None:
        """Clear process-local state; intended for worker restart and tests."""
        with self._lock:
            self._states.clear()


provider_circuits = ProviderCircuitBreaker()
