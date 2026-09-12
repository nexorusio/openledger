"""Versioned, serializable contracts shared by every P2 pipeline adapter.

Route availability, execution lifecycle and evidence outcome are independent.
Nothing in this module turns a finding, account, score or input into an approved
identity fact. All outputs must enter the evidence ledger before presentation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

PIPELINE_ID = "p2-e2e-v1"
PIPELINE_SCHEMA_REVISION = "e2e3c9d1f703"
PIPELINE_CONTRACT_VERSION = 1
ROUTE_STATES = frozenset({"active", "conditional", "unavailable", "excluded"})
TASK_STATES = frozenset({"planned", "running", "completed", "cancelled"})
EVIDENCE_OUTCOMES = frozenset(
    {
        "found",
        "not_found",
        "candidate",
        "inconclusive",
        "blocked",
        "timeout",
        "error",
        "cancelled",
        "not_executed",
        "partial",
    }
)
MAJOR_PLATFORMS = ("facebook", "instagram", "threads", "tiktok", "x")


def canonical_digest(value: Any) -> str:
    """Hash JSON values identically before and after persistence."""
    serialized = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def stable_id(kind: str, *parts: Any) -> str:
    return f"{kind}:" + canonical_digest(parts).removeprefix("sha256:")


@dataclass(frozen=True)
class EngineCapability:
    engine_id: str
    execution_key: str
    module: str
    input_types: tuple[str, ...]
    label: str
    platforms: tuple[str, ...] = ()
    prerequisites: tuple[str, ...] = ()
    option: str = ""
    timeout_seconds: int = 30
    retry_ceiling: int = 1
    request_budget: int = 1
    retention: str = "bounded_source_evidence"
    trigger: str = "query"

    def as_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(asdict(self)))


def _load_capabilities():
    # The reviewed source manifest is the single declaration for routing and
    # execution. Importing contracts does not import collectors or credentials.
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "config" / "osint-sources.json"
    sources = json.loads(path.read_text(encoding="utf-8"))["sources"]
    capabilities = []
    seen = set()
    for row in sorted(sources, key=lambda row: row["connector"].get("order", 1000)):
        values = dict(row["connector"]["capability"])
        if values.get("engine_id") != row["id"] or row["id"] in seen:
            raise ValueError(
                "Connector manifest has inconsistent or duplicate identity"
            )
        seen.add(row["id"])
        for field in ("input_types", "platforms", "prerequisites"):
            if field in values:
                values[field] = tuple(values[field])
        capabilities.append(EngineCapability(**values))
    return tuple(capabilities)


ENGINE_CAPABILITIES = _load_capabilities()
ENGINE_REGISTRY = {item.engine_id: item for item in ENGINE_CAPABILITIES}


def capability_inventory() -> list[dict[str, Any]]:
    return [item.as_dict() for item in ENGINE_CAPABILITIES]


def validate_task(task: Mapping[str, Any], *, registry=None) -> None:
    """Fail closed before dispatch if a saved task does not match its adapter."""
    engine = (ENGINE_REGISTRY if registry is None else registry).get(
        str(task.get("engine_id", ""))
    )
    if engine is None or task.get("execution_key") != engine.execution_key:
        raise ValueError("Task has no registered execution adapter.")
    if task.get("pipeline_id") != PIPELINE_ID:
        raise ValueError("Task belongs to a different pipeline.")
    if task.get("route_state") not in ROUTE_STATES:
        raise ValueError("Task has an invalid route state.")
    if task.get("input_type") and task["input_type"] not in engine.input_types:
        raise ValueError("Task input is incompatible with its adapter.")
    if engine.platforms and task.get("platform") not in engine.platforms:
        raise ValueError("Task has an unsupported platform.")
    for name, maximum in (
        ("timeout_seconds", 1800),
        ("retry_ceiling", 2),
        ("request_budget", 100000),
    ):
        value = task.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= maximum
        ):
            raise ValueError(f"Invalid task {name}.")
    if not task.get("timeout_seconds"):
        raise ValueError("Task timeout must be positive.")
    if task.get("route_state") == "active" and not task.get("input_value"):
        raise ValueError("Active task requires an input.")
