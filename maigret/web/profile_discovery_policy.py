"""Server-owned feature flags and mode policy for profile discovery."""

from __future__ import annotations

import os
from typing import Any, Mapping, MutableMapping, Optional

from maigret.web.execution_budget import apply_execution_budget

PROFILE_DISCOVERY_POLICY_VERSION = "profile-discovery-routing-v2"
PROFILE_DISCOVERY_JOB_KINDS = frozenset({"live", "refresh"})

_FLAG_ENVIRONMENT = {
    "profile_discovery_enabled": "OPENLEDGER_PROFILE_DISCOVERY_ENABLED",
    "focused_mode_enabled": "OPENLEDGER_FOCUSED_DISCOVERY_ENABLED",
    "exhaustive_mode_enabled": "OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED",
    "maigret_enabled": "OPENLEDGER_MAIGRET_DISCOVERY_ENABLED",
    "user_scanner_enabled": "OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED",
    "enrichment_providers_enabled": "OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED",
    "provider_circuit_breakers_enabled": (
        "OPENLEDGER_PROVIDER_CIRCUIT_BREAKERS_ENABLED"
    ),
    "search_first_enabled": "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED",
}
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_DEFAULT_OFF_FLAGS = frozenset({"search_first_enabled"})


class ProfileDiscoveryPolicyError(ValueError):
    """A policy refusal with guidance deliberately approved for public responses.

    Callers must supply policy guidance, never a serialized lower-level
    exception. Keep internal diagnostics in a chained cause instead.
    """

    def __init__(self, public_message: str) -> None:
        if not isinstance(public_message, str):
            raise TypeError("Profile discovery policy guidance must be a string.")
        self._public_message = public_message
        super().__init__(public_message)

    @property
    def public_message(self) -> str:
        """Return the authored guidance independently of exception diagnostics."""
        return self._public_message


def _server_flag(
    name: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    environment = os.environ if environ is None else environ
    raw_value = (
        str(environment.get(_FLAG_ENVIRONMENT[name], ""))
        .strip()
        .casefold()
    )
    if name in _DEFAULT_OFF_FLAGS:
        # New outbound capabilities must be explicitly enabled. Unknown values
        # stay off so a typo cannot widen production network access.
        return raw_value in _TRUE_VALUES
    if raw_value in _FALSE_VALUES:
        return False
    # Preserve the P1 default-on contract, including malformed legacy values.
    return True


def profile_discovery_flags(
    *, environ: Optional[Mapping[str, str]] = None
) -> dict[str, bool]:
    """Read a bounded flag snapshot exclusively from the server environment."""
    return {
        name: _server_flag(name, environ=environ)
        for name in _FLAG_ENVIRONMENT
    }


def profile_discovery_flag_enabled(name: str) -> bool:
    if name not in _FLAG_ENVIRONMENT:
        raise KeyError(name)
    return _server_flag(name)


def govern_profile_discovery_options(
    options: Mapping[str, Any],
    requested_mode: Any = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """Canonicalize a scan using server flags and fixed budgets."""
    source: MutableMapping[str, Any] = dict(options)
    flags = profile_discovery_flags(environ=environ)
    if not flags["profile_discovery_enabled"]:
        raise ProfileDiscoveryPolicyError(
            "Profile discovery is temporarily disabled by server policy."
        )

    mode_input = requested_mode
    if mode_input is None:
        mode_input = source.get("execution_mode")
    governed = apply_execution_budget(source, mode_input)
    mode = governed["execution_mode"]
    if not flags[f"{mode}_mode_enabled"]:
        raise ProfileDiscoveryPolicyError(
            f"{mode.title()} profile discovery is disabled by server policy."
        )
    if not flags["maigret_enabled"]:
        raise ProfileDiscoveryPolicyError(
            "Maigret profile discovery is disabled by server policy."
        )

    specification = governed.get("investigation_spec")
    if isinstance(specification, Mapping):
        specification = dict(specification)
        user_scanner_requested = bool(
            specification.get("enable_user_scanner_email")
            or specification.get("enable_user_scanner_username")
        )
        if user_scanner_requested and not flags["user_scanner_enabled"]:
            raise ProfileDiscoveryPolicyError(
                "User Scanner verification is disabled by server policy."
            )
        governed["investigation_spec"] = specification

    # Replace any client-supplied policy document with the server snapshot.
    governed["profile_discovery_policy"] = {
        "policy_version": PROFILE_DISCOVERY_POLICY_VERSION,
        "mode": mode,
        "flags": flags,
        "safety_controls": {
            "server_owned_execution_budget": True,
            "durable_cancellation": True,
            "worker_lease": True,
            "provider_circuit_breaker": flags[
                "provider_circuit_breakers_enabled"
            ],
        },
    }
    return governed
