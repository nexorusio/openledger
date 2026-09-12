"""One manifest binds capability, execution, normalization and source policy.

Only deployment-owned Python modules are loaded. Manifest edits are reviewed
code changes, never accepted from an investigation or a connector feed.
"""

from __future__ import annotations

import importlib
import inspect
import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping

from maigret.web.pipeline_contract import (
    EngineCapability,
    ENGINE_REGISTRY,
    validate_task,
)

MANIFEST_PATH = Path(__file__).resolve().parents[3] / "config" / "osint-sources.json"
POLICY_GROUPS = frozenset(
    {
        "maigret",
        "scanner",
        "native_search",
        "public_search",
        "google_places",
        "ai",
        "enrichment",
        "operator",
        "connector",
    }
)
EXECUTION_MODES = frozenset({"worker", "operator", "machine"})
RETENTION_POLICIES = frozenset(
    {
        "retained",
        "metadata_only",
        "transient",
        "live_only",
        "prohibited",
        "bounded_source_evidence",
        "permitted_place_ids_and_status",
        "transient_display_only",
    }
)


def resolve_callable(reference: str) -> Callable:
    module, separator, attribute = str(reference).partition(":")
    if (
        not separator
        or not module.startswith("maigret.web.")
        or not attribute.isidentifier()
    ):
        raise ValueError(
            "Connector callables must be deployment-owned maigret.web module:attribute references"
        )
    function = getattr(importlib.import_module(module), attribute, None)
    if not callable(function):
        raise ValueError(f"Connector callable is missing: {reference}")
    return function


def validate_default_configuration(
    source: Mapping[str, Any], configuration: Mapping[str, Any]
) -> None:
    """Validate capability availability without reading or returning secrets."""
    access = source.get("access") or {}
    if access.get("credentials_required") and not configuration.get(
        "credentials_configured"
    ):
        raise ValueError("Required connector credentials are not configured.")
    if access.get("genuinely_free") is False and not configuration.get(
        "paid_access_enabled"
    ):
        raise ValueError(
            "Paid connector access is not enabled by server configuration."
        )


@dataclass(frozen=True)
class ConnectorSpec:
    capability: EngineCapability
    collect: Callable
    normalize: Callable
    validate_configuration: Callable
    adapter_version: str
    parser_version: str
    policy_group: str
    provider_key: str
    execution_mode: str
    source: Mapping[str, Any]

    def configuration_snapshot(self, values=None):
        """Only availability booleans leave environment-owned secret storage."""
        configuration = dict(values or {})
        for field, names in (
            self.source["connector"].get("configuration_env", {}).items()
        ):
            names = [names] if isinstance(names, str) else names
            if field == "credentials_configured":
                available = all(
                    bool(os.environ.get(name, "").strip()) for name in names
                )
            else:
                available = all(
                    os.environ.get(name, "").strip().lower()
                    in {"1", "true", "yes", "on"}
                    for name in names
                )
            configuration[field] = (
                available and configuration.get(field, True) is not False
            )
        return configuration

    def task_metadata(self, provider: str = "") -> dict[str, str]:
        return {
            "engine_version": self.adapter_version,
            "parser_version": self.parser_version,
            "provider_key": provider or self.provider_key,
            "connector_mode": self.execution_mode,
        }


class ConnectorRegistry:
    def __init__(self, sources, capabilities=None):
        capabilities = ENGINE_REGISTRY if capabilities is None else capabilities
        self._specs = {}
        for source in sources:
            key = source["id"]
            if key in self._specs or key not in capabilities:
                raise ValueError("Duplicate or undeclared connector identity")
            declaration = source["connector"]
            capability = capabilities[key]
            environment = declaration.get("configuration_env", {})
            if not isinstance(environment, dict) or set(environment) - {
                "enabled",
                "credentials_configured",
                "paid_access_enabled",
            }:
                raise ValueError(
                    "Connector configuration_env supports availability fields only"
                )
            for names in environment.values():
                names = [names] if isinstance(names, str) else names
                if (
                    not isinstance(names, list)
                    or not names
                    or not all(
                        isinstance(name, str)
                        and re.fullmatch(r"OPENLEDGER_[A-Z0-9_]+", name)
                        for name in names
                    )
                ):
                    raise ValueError(
                        "Connector configuration_env must name OPENLEDGER_ environment variables"
                    )
            if declaration["capability"] != capability.as_dict():
                raise ValueError(f"Connector capability drift: {key}")
            mode, policy = declaration["execution_mode"], declaration["policy_group"]
            if mode not in EXECUTION_MODES or policy not in POLICY_GROUPS:
                raise ValueError(f"Invalid connector execution mode or policy: {key}")
            if capability.retention not in RETENTION_POLICIES:
                raise ValueError(f"Unknown connector retention policy: {key}")
            if mode == "operator" and capability.trigger != "operator":
                raise ValueError(
                    "Operator connectors require explicit operator trigger"
                )
            if mode == "machine" and capability.trigger != "machine":
                raise ValueError("Machine connectors require explicit machine trigger")
            versions = [
                declaration.get(name) for name in ("adapter_version", "parser_version")
            ]
            if any(
                not isinstance(version, str)
                or not version.strip()
                or version == "unknown"
                for version in versions
            ):
                raise ValueError("Connector adapter/parser versions must be explicit")
            self._specs[key] = ConnectorSpec(
                capability,
                resolve_callable(declaration["collector"]),
                resolve_callable(declaration["normalizer"]),
                resolve_callable(declaration["configuration_validator"]),
                *versions,
                policy,
                declaration["provider_key"],
                mode,
                source,
            )
        if set(self._specs) != set(capabilities):
            raise ValueError(
                "Every declared connector requires an executable implementation"
            )

    def get(self, engine_id: str) -> ConnectorSpec:
        try:
            return self._specs[engine_id]
        except KeyError as error:
            raise ValueError(
                "No executable connector is registered for this task"
            ) from error

    def values(self):
        return tuple(self._specs.values())


@lru_cache(maxsize=1)
def get_connector_registry() -> ConnectorRegistry:
    return ConnectorRegistry(
        json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))["sources"]
    )


async def collect_registered(task, context, *, registry=None, operator=False):
    registry = registry or get_connector_registry()
    validate_task(
        task,
        registry={
            spec.capability.engine_id: spec.capability for spec in registry.values()
        },
    )
    spec = registry.get(task["engine_id"])
    if spec.source.get("status") != "active":
        raise ValueError("Connector source is not active in the reviewed manifest")
    if spec.execution_mode == "operator" and not operator:
        raise ValueError("This connector requires its explicit operator submission")
    if spec.execution_mode != "operator" and operator:
        raise ValueError("This connector does not accept operator execution")
    if (
        spec.execution_mode == "machine"
        and (context.job.get("job_kind") or context.job.get("kind"))
        != "connector_ingestion"
    ):
        raise ValueError("Machine connectors require an authenticated ingestion job")
    for field, expected in spec.task_metadata().items():
        # Previously saved tasks lack versions; present versions must never be
        # silently interpreted by a different adapter/parser after deployment.
        if (
            field in {"engine_version", "parser_version"}
            and task.get(field, expected) != expected
        ):
            raise ValueError(f"Saved connector {field} changed; replan this task")
    result = spec.collect(task, context)
    return await result if inspect.isawaitable(result) else result


def normalize_connector_result(task, result, **scope):
    spec = get_connector_registry().get(task["engine_id"])
    scope.setdefault("engine", spec.capability.engine_id)
    scope.setdefault("engine_version", spec.adapter_version)
    scope.setdefault("parser_version", spec.parser_version)
    scope["retention_policy"] = [
        spec.capability.retention,
        scope.pop("retention_policy", None),
    ]
    return spec.normalize(result, **scope)
