"""Pure server policy for bounded, analyst-directed evidence pivots."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping
from typing import Any, Dict
from urllib.parse import parse_qsl, urlsplit

from maigret.web.evidence_correlation_contract import (
    canonical_profile_identity,
)

GOVERNED_PIVOT_POLICY_VERSION = "governed-pivots-v1"
MAX_GOVERNED_PIVOT_DEPTH = 1

FULL_NAME_SOURCE_ROUTES = (
    "wikipedia_public_biography",
    "icij_offshore_leaks",
)
VERIFIED_PROFILE_SOURCE_ROUTES = (
    "facebook",
    "instagram",
    "threads",
    "tiktok",
    "x",
)

FULL_NAME_MAX_PLANNED_REQUESTS = 2
VERIFIED_PROFILE_MAX_PLANNED_REQUESTS = 25
FULL_NAME_EXECUTION_BUDGET_SECONDS = 120
VERIFIED_PROFILE_EXECUTION_BUDGET_SECONDS = 600

_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CONTROLLED_FIELDS = frozenset(
    {
        "aiallowed",
        "allowedroutes",
        "allsites",
        "automaticapproval",
        "autoapprovalallowed",
        "autoapproved",
        "authorization",
        "budget",
        "budgets",
        "consent",
        "depth",
        "declaredpurpose",
        "executionbudget",
        "executionmode",
        "externalaiconsent",
        "featureflags",
        "featuresnapshot",
        "governance",
        "inputdepth",
        "maxdepth",
        "maxrequests",
        "maxsourceroutes",
        "maxsources",
        "maximumdepth",
        "maximumplannedrequests",
        "maximumrequests",
        "maximumroutes",
        "maximumsourceroutes",
        "maximumsources",
        "mode",
        "outputdepth",
        "planid",
        "pivotkind",
        "policyversion",
        "purpose",
        "requestbudget",
        "requestedby",
        "requireshumanreview",
        "resultreviewstatus",
        "serverowned",
        "sourcebudget",
        "scopeconfirmed",
        "sourceroutes",
        "timeout",
        "timeoutseconds",
        "totalseconds",
    }
)
_CREDENTIAL_KEYS = frozenset(
    {
        "accesskey",
        "accesstoken",
        "apikey",
        "authorization",
        "clientsecret",
        "connectionstring",
        "cookie",
        "databaseurl",
        "password",
        "passwd",
        "privatekey",
        "refreshtoken",
        "secret",
        "sessiontoken",
        "token",
    }
)
_Payload = Mapping[str, Any]


class GovernedPivotPolicyError(ValueError):
    """Raised when a requested evidence pivot violates server policy."""


def _normalized_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _text(value: Any, field_name: str, *, maximum: int) -> str:
    if not isinstance(value, str):
        raise GovernedPivotPolicyError(f"{field_name} must be text")
    candidate = unicodedata.normalize("NFKC", value).strip()
    if not candidate:
        raise GovernedPivotPolicyError(f"{field_name} is required")
    if len(candidate) > maximum:
        raise GovernedPivotPolicyError(f"{field_name} is too large")
    if any(ord(character) < 32 for character in candidate):
        raise GovernedPivotPolicyError(
            f"{field_name} contains prohibited control characters"
        )
    return candidate


def _identifier(value: Any, field_name: str) -> str:
    candidate = _text(value, field_name, maximum=128)
    if not _ID_PATTERN.fullmatch(candidate):
        raise GovernedPivotPolicyError(f"Invalid {field_name}")
    return candidate


def _actor(value: Any) -> str:
    candidate = _text(value, "requested_by", maximum=200)
    return " ".join(candidate.split())


def _reject_controlled_fields(value: _Payload, field_name: str) -> None:
    if any(not isinstance(key, str) for key in value):
        message = f"{field_name} contains an invalid field"
        raise GovernedPivotPolicyError(message)
    if any(_normalized_key(key) in _CONTROLLED_FIELDS for key in value):
        raise GovernedPivotPolicyError(
            f"{field_name} must not define client-controlled policy or budgets"
        )


def _matches_scope(
    source_claim: Mapping[str, Any],
    field_name: str,
    expected: str,
) -> None:
    supplied = source_claim.get(field_name)
    if supplied is None:
        return
    if supplied != expected:
        raise GovernedPivotPolicyError(
            f"Source claim {field_name} does not match the requested scope"
        )


def _has_credential_key(items: list[tuple[str, str]]) -> bool:
    return any(
        (normalized := _normalized_key(key)) in _CREDENTIAL_KEYS
        or any(normalized.endswith(item) for item in _CREDENTIAL_KEYS)
        for key, _value in items
    )


def _profile_url(source_claim: Mapping[str, Any]) -> str:
    value = source_claim.get("value")
    if isinstance(value, Mapping):
        _reject_controlled_fields(value, "source_claim.value")
        if any(_normalized_key(key) in _CREDENTIAL_KEYS for key in value):
            raise GovernedPivotPolicyError(
                "source_claim.value must not contain credentials"
            )
        value = value.get("url")
    url = _text(value, "source_claim.value.url", maximum=2_000)
    try:
        parsed = urlsplit(url)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        fragment = parse_qsl(parsed.fragment, keep_blank_values=True)
    except ValueError as exc:
        message = "source_claim.value.url is invalid"
        raise GovernedPivotPolicyError(message) from exc
    if _has_credential_key(query) or _has_credential_key(fragment):
        raise GovernedPivotPolicyError(
            "source_claim.value.url must not contain credentials"
        )
    return url


def _full_name_target(source_claim: Mapping[str, Any]) -> Dict[str, str]:
    name = _text(source_claim.get("value"), "source_claim.value", maximum=500)
    name = " ".join(name.split())
    return {
        "kind": "full_name",
        "value": name,
        "normalized_value": name.casefold(),
    }


def _profile_target(source_claim: Mapping[str, Any]) -> Dict[str, str]:
    identity = canonical_profile_identity(_profile_url(source_claim))
    if identity is None:
        claim_kind = "Approved social_account"
        raise GovernedPivotPolicyError(
            f"{claim_kind} must contain a supported public profile URL"
        )
    return {"kind": "verified_profile", **identity}


def _stable_plan_id(plan: Mapping[str, Any]) -> str:
    material = json.dumps(
        plan,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"governed-pivot:{hashlib.sha256(material).hexdigest()}"


def build_governed_pivot_plan(
    source_claim: Mapping[str, Any],
    *,
    case_id: str,
    persona_id: str,
    requested_by: str,
    purpose: str,
    scope_confirmed: bool,
    depth: int = 0,
) -> Dict[str, Any]:
    """Build one deterministic, case-scoped pivot plan without network access.

    The source claim is expected to come from the durable Persona store. Client
    budget, mode, approval, feature, and depth fields are refused rather than
    copied. All assertions produced by executing the plan remain pending until
    a separate human review decision is recorded.
    """
    if not isinstance(source_claim, Mapping):
        raise GovernedPivotPolicyError("source_claim must be an object")
    _reject_controlled_fields(source_claim, "source_claim")

    normalized_case_id = _identifier(case_id, "case_id")
    normalized_persona_id = _identifier(persona_id, "persona_id")
    claim_id = _identifier(source_claim.get("id"), "source_claim.id")
    actor = _actor(requested_by)
    declared_purpose = " ".join(_text(purpose, "purpose", maximum=2_000).split())
    if scope_confirmed is not True:
        raise GovernedPivotPolicyError(
            "The analyst must confirm lawful purpose and authorized scope"
        )
    _matches_scope(source_claim, "case_id", normalized_case_id)
    _matches_scope(source_claim, "persona_id", normalized_persona_id)

    if isinstance(depth, bool) or not isinstance(depth, int):
        raise GovernedPivotPolicyError("depth must be an integer")
    if depth != 0:
        raise GovernedPivotPolicyError(
            "Governed pivots may originate only at root depth 0"
        )

    review_status = source_claim.get("review_status")
    if review_status != "approved":
        raise GovernedPivotPolicyError(
            "Source claim must be explicitly approved by an analyst"
        )
    field_name = source_claim.get("field_name")
    if field_name == "full_name":
        pivot_kind = "confirmed_name_enrichment"
        target = _full_name_target(source_claim)
        source_routes = FULL_NAME_SOURCE_ROUTES
        maximum_requests = FULL_NAME_MAX_PLANNED_REQUESTS
        total_seconds = FULL_NAME_EXECUTION_BUDGET_SECONDS
    elif field_name == "social_account":
        pivot_kind = "verified_profile_discovery"
        target = _profile_target(source_claim)
        source_routes = VERIFIED_PROFILE_SOURCE_ROUTES
        maximum_requests = VERIFIED_PROFILE_MAX_PLANNED_REQUESTS
        total_seconds = VERIFIED_PROFILE_EXECUTION_BUDGET_SECONDS
    else:
        raise GovernedPivotPolicyError(
            "Only approved full_name and social_account claims may pivot"
        )

    plan: Dict[str, Any] = {
        "policy_version": GOVERNED_PIVOT_POLICY_VERSION,
        "case_id": normalized_case_id,
        "persona_id": normalized_persona_id,
        "requested_by": actor,
        "governance": {
            "declared_purpose": declared_purpose,
            "scope_confirmed": True,
            "confirmed_by": actor,
            "authorization_basis": "analyst_confirmed_lawful_scope",
            "external_ai_consent": False,
        },
        "source_claim": {
            "id": claim_id,
            "field_name": field_name,
            "review_status": "approved",
        },
        "pivot_kind": pivot_kind,
        "target": target,
        "input_depth": depth,
        "output_depth": MAX_GOVERNED_PIVOT_DEPTH,
        "maximum_depth": MAX_GOVERNED_PIVOT_DEPTH,
        "source_budget": {
            "server_owned": True,
            "maximum_routes": len(source_routes),
            "allowed_routes": list(source_routes),
        },
        "request_budget": {
            "server_owned": True,
            "maximum_planned_requests": maximum_requests,
        },
        "execution_mode": "focused",
        "execution_budget": {
            "server_owned": True,
            "mode": "focused",
            "total_seconds": total_seconds,
        },
        "result_review_status": "pending",
        "requires_human_review": True,
        "auto_approval_allowed": False,
        "ai_allowed": False,
    }
    return {"plan_id": _stable_plan_id(plan), **plan}
