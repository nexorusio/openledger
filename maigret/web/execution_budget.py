"""Server-owned execution budgets for governed profile discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional


EXECUTION_BUDGET_POLICY_VERSION = "profile-discovery-v1"
FOCUSED_BUDGET_SECONDS = 10 * 60
EXHAUSTIVE_BUDGET_SECONDS = 30 * 60

_MODE_ALIASES = {
    "fast": "focused",
    "focused": "focused",
    "full": "exhaustive",
    "exhaustive": "exhaustive",
}


def normalize_execution_mode(value: Any, *, all_sites: bool = False) -> str:
    """Normalize legacy mode names without accepting client-defined budgets."""
    normalized = _MODE_ALIASES.get(str(value or "").strip().casefold())
    if normalized:
        return normalized
    return "exhaustive" if all_sites else "focused"


def execution_budget_spec(mode: Any, *, all_sites: bool = False) -> dict[str, Any]:
    """Return the immutable server policy for one discovery mode."""
    normalized_mode = normalize_execution_mode(mode, all_sites=all_sites)
    total_seconds = (
        EXHAUSTIVE_BUDGET_SECONDS
        if normalized_mode == "exhaustive"
        else FOCUSED_BUDGET_SECONDS
    )
    return {
        "policy_version": EXECUTION_BUDGET_POLICY_VERSION,
        "mode": normalized_mode,
        "total_seconds": total_seconds,
    }


def execution_budget_spec_from_options(options: Mapping[str, Any]) -> dict[str, Any]:
    """Derive a trusted policy from persisted options, ignoring forged durations."""
    stored = options.get("execution_budget")
    stored = stored if isinstance(stored, Mapping) else {}
    requested_mode = options.get("execution_mode") or stored.get("mode")
    return execution_budget_spec(
        requested_mode,
        all_sites=bool(options.get("all_sites")),
    )


def apply_execution_budget(
    options: Mapping[str, Any], requested_mode: Any = None
) -> dict[str, Any]:
    """Copy scan options and attach the canonical, server-owned budget policy."""
    normalized = dict(options)
    spec = execution_budget_spec(
        requested_mode,
        all_sites=bool(normalized.get("all_sites")),
    )
    normalized["execution_mode"] = spec["mode"]
    normalized["all_sites"] = spec["mode"] == "exhaustive"
    normalized["execution_budget"] = spec
    return normalized


def _as_utc_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class ExecutionBudget:
    """A claimed job's absolute deadline and immutable policy metadata."""

    policy_version: str
    mode: str
    total_seconds: int
    deadline_at: datetime

    @classmethod
    def from_options(
        cls,
        options: Mapping[str, Any],
        *,
        started_at: Optional[datetime] = None,
    ) -> "ExecutionBudget":
        spec = execution_budget_spec_from_options(options)
        started = _as_utc_datetime(started_at) or datetime.now(timezone.utc)
        return cls(
            policy_version=str(spec["policy_version"]),
            mode=str(spec["mode"]),
            total_seconds=int(spec["total_seconds"]),
            deadline_at=started + timedelta(seconds=int(spec["total_seconds"])),
        )

    @classmethod
    def from_job(
        cls,
        job: Mapping[str, Any],
        *,
        now: Optional[datetime] = None,
    ) -> "ExecutionBudget":
        options = job.get("options")
        options = options if isinstance(options, Mapping) else {}
        spec = execution_budget_spec_from_options(options)
        total_seconds = int(job.get("budget_seconds") or spec["total_seconds"])
        if total_seconds not in {FOCUSED_BUDGET_SECONDS, EXHAUSTIVE_BUDGET_SECONDS}:
            total_seconds = int(spec["total_seconds"])
        started = _as_utc_datetime(job.get("started_at")) or _as_utc_datetime(now)
        started = started or datetime.now(timezone.utc)
        policy_deadline = started + timedelta(seconds=total_seconds)
        persisted_deadline = _as_utc_datetime(job.get("deadline_at"))
        deadline = (
            min(persisted_deadline, policy_deadline)
            if persisted_deadline is not None
            else policy_deadline
        )
        return cls(
            policy_version=str(spec["policy_version"]),
            mode=str(spec["mode"]),
            total_seconds=total_seconds,
            deadline_at=deadline,
        )

    def remaining_seconds(self, *, now: Optional[datetime] = None) -> float:
        current = _as_utc_datetime(now) or datetime.now(timezone.utc)
        return max(0.0, (self.deadline_at - current).total_seconds())

    def is_exhausted(self, *, now: Optional[datetime] = None) -> bool:
        return self.remaining_seconds(now=now) <= 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "mode": self.mode,
            "total_seconds": self.total_seconds,
            "deadline_at": self.deadline_at.isoformat(),
        }
