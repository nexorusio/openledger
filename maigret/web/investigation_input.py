"""Validated, capability-aware input planning for OpenLedger investigations."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import unicodedata
from typing import Any, Callable, Dict, List, Mapping, Optional
from urllib.parse import unquote, urlsplit, urlunsplit

from maigret.utils import is_plausible_username
from maigret.web.execution_budget import execution_budget_spec
from maigret.web.profile_search_planner import plan_profile_search_queries
from maigret.web.username_aliases import (
    MAX_ALIAS_CANDIDATES,
    MAX_SELECTED_ALIASES,
    normalize_context_numbers,
    normalize_nicknames,
    rank_username_aliases,
)

SCHEMA_VERSION = 1
UNIFIED_INVESTIGATION_SCHEMA_VERSION = 2
TOKEN_SCHEMA_VERSION = 1
ROUTE_PLAN_SCHEMA_VERSION = 1
UNIFIED_INPUT_CONTRACT = "investigation-tokens-v1"
ROUTE_PLAN_POLICY_VERSION = "investigation-routes-v2"
TOKEN_FORM_FIELD = "investigation_token"
TOKEN_TYPE_FORM_FIELD = "investigation_token_type"
TOKEN_TYPES = {
    "email",
    "full_name",
    "phone",
    "profile_url",
    "public_url",
    "social_handle",
    "username",
}
IDENTIFIER_TYPES = {
    "username",
    "social_handle",
    "profile_url",
    "full_name",
    "email",
    "phone",
}
PROCESSING_MODES = {"independent", "same_subject"}
MAX_IDENTIFIERS = 24
MAX_TERMS = 20
MAX_SOURCE_TAGS = 64
MAX_USERNAME_LENGTH = 128
MAX_CONTEXT_LENGTH = 500
MAX_VARIANTS = 16
MAX_USER_SCANNER_USERNAME_TARGETS = 16
MAX_TOKEN_LENGTH = 2000
USER_SCANNER_USERNAME_PLATFORMS = {
    "facebook",
    "instagram",
    "threads",
    "tiktok",
    "x",
}

_EMAIL_PATTERN = re.compile(
    r"^[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+$",
    re.IGNORECASE,
)
_TERM_SPLIT_PATTERN = re.compile(r"[,\n\r]+")
_SOURCE_TAG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_SCHEME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_INDONESIAN_LOCAL_MOBILE_PATTERN = re.compile(r"^08[1-9][0-9]{7,10}$")
_INDONESIAN_COUNTRY_MOBILE_PATTERN = re.compile(r"^628[1-9][0-9]{7,10}$")
_DOMAIN_LABEL_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_BLOCKED_PUBLIC_HOST_SUFFIXES = (
    ".example",
    ".home.arpa",
    ".internal",
    ".invalid",
    ".local",
    ".localhost",
    ".test",
)
_COLLECTION_ROUTES = frozenset(
    {
        "archived_profile_evidence",
        "github_profile_enrichment",
        "maigret",
        "native_profile_search",
        "user_scanner_email",
        "user_scanner_username",
    }
)
_GENERIC_PROFILE_SEGMENTS = {
    "account",
    "accounts",
    "channel",
    "channels",
    "in",
    "member",
    "members",
    "people",
    "profile",
    "profiles",
    "u",
    "user",
    "users",
}


class InvestigationInputError(ValueError):
    """A user-facing validation error for a submitted investigation plan."""


def _token_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = " ".join(text.split())
    if not text:
        raise InvestigationInputError("Enter an investigation token.")
    if len(text) > MAX_TOKEN_LENGTH:
        raise InvestigationInputError(
            f"Investigation tokens must be {MAX_TOKEN_LENGTH} characters or fewer."
        )
    return text


def _duplicate_key(token_type: str, value: str) -> str:
    digest = hashlib.sha256(value.casefold().encode("utf-8")).hexdigest()
    return f"{token_type}:{digest}"


def _public_hostname(hostname: str) -> str:
    try:
        host = hostname.rstrip(".").encode("idna").decode("ascii").casefold()
    except UnicodeError as error:
        raise InvestigationInputError(
            "Enter a valid public HTTP or HTTPS URL."
        ) from error
    if not host or len(host) > 253:
        raise InvestigationInputError("Enter a valid public HTTP or HTTPS URL.")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if (
            "." not in host
            or all(character.isdigit() or character == "." for character in host)
            or host == "localhost"
            or host.endswith(_BLOCKED_PUBLIC_HOST_SUFFIXES)
            or any(
                not _DOMAIN_LABEL_PATTERN.fullmatch(label) for label in host.split(".")
            )
        ):
            raise InvestigationInputError(
                "Investigation URLs must use a public Internet hostname."
            )
    else:
        if not address.is_global:
            raise InvestigationInputError(
                "Investigation URLs must not target private or local addresses."
            )
    return host


def normalize_public_url(value: Any) -> str:
    """Normalize a public URL without resolving DNS or fetching the destination."""
    url = _token_text(value)
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise InvestigationInputError(
            "Enter a valid public HTTP or HTTPS URL."
        ) from error
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or not parsed.hostname:
        raise InvestigationInputError("Enter a complete public HTTP or HTTPS URL.")
    if parsed.username is not None or parsed.password is not None:
        raise InvestigationInputError(
            "Investigation URLs must not contain credentials."
        )
    if port is not None and port != (80 if scheme == "http" else 443):
        raise InvestigationInputError(
            "Investigation URLs may use only the default HTTP or HTTPS port."
        )
    host = _public_hostname(parsed.hostname)
    netloc = f"[{host}]" if ":" in host else host
    path = parsed.path or "/"
    return urlunsplit((scheme, netloc, path, parsed.query, ""))


def _explicit_phone_token(value: str) -> bool:
    has_tel_prefix = value.casefold().startswith("tel:")
    candidate = value[4:].strip() if has_tel_prefix else value
    if not candidate or not re.fullmatch(r"[0-9+().\-\s]+", candidate):
        return False
    digits = re.sub(r"\D", "", candidate)
    indonesian_mobile = bool(
        _INDONESIAN_LOCAL_MOBILE_PATTERN.fullmatch(digits)
        or _INDONESIAN_COUNTRY_MOBILE_PATTERN.fullmatch(digits)
    )
    return bool(
        has_tel_prefix
        or candidate.startswith("+")
        or re.search(r"[().\-\s]", candidate)
        or indonesian_mobile
    )


def _ambiguous_token_types(value: str) -> List[str]:
    """Expose numeric account/phone ambiguity without changing route authority."""
    candidate = value[4:].strip() if value.casefold().startswith("tel:") else value
    if re.fullmatch(r"[0-9]{7,15}", candidate):
        return ["phone", "username"]
    return []


def _resolved_profile_accounts(
    url: str,
    resolver: Optional[Callable[[str], Dict[str, str]]],
) -> List[str]:
    if resolver is None:
        return []
    resolved: List[str] = []
    for identifier, identifier_type in (resolver(url) or {}).items():
        if identifier_type != "username":
            continue
        try:
            username = normalize_username(identifier)
        except InvestigationInputError:
            continue
        if username.casefold() not in {item.casefold() for item in resolved}:
            resolved.append(username)
    return resolved


def classify_investigation_token(
    value: Any,
    *,
    type_override: Any = None,
    profile_url_resolver: Optional[Callable[[str], Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Classify one bounded token in the fixed, server-owned precedence order."""
    token = _token_text(value)
    account_targets: List[str] = []
    if _EMAIL_PATTERN.fullmatch(token):
        token_type = "email"
        normalized = normalize_email(token)
    elif _SCHEME_PATTERN.match(token):
        normalized = normalize_public_url(token)
        account_targets = _resolved_profile_accounts(normalized, profile_url_resolver)
        token_type = "profile_url" if account_targets else "public_url"
    elif _explicit_phone_token(token):
        token_type = "phone"
        phone_value = (
            token[4:].strip() if token.casefold().startswith("tel:") else token
        )
        normalized = normalize_phone(phone_value)
    elif token.startswith("@"):
        token_type = "social_handle"
        normalized = normalize_username(token)
    elif any(character.isspace() for character in token):
        token_type = "full_name"
        normalized = token
        if len(normalized) < 2 or len(normalized) > 300:
            raise InvestigationInputError(
                "Enter a complete name of 300 characters or fewer."
            )
    else:
        token_type = "username"
        normalized = normalize_username(token)

    predicted_type = token_type
    requested_type = str(type_override or "").strip().casefold()
    if requested_type:
        if requested_type not in TOKEN_TYPES:
            raise InvestigationInputError(
                "Select a supported investigation value type."
            )
        token_type = requested_type
        account_targets = []
        if token_type == "email":
            normalized = normalize_email(token)
        elif token_type == "phone":
            phone_value = (
                token[4:].strip() if token.casefold().startswith("tel:") else token
            )
            normalized = normalize_phone(phone_value)
        elif token_type in {"username", "social_handle"}:
            normalized = normalize_username(token)
        elif token_type == "full_name":
            normalized = token
            if len(normalized) < 2 or len(normalized) > 300:
                raise InvestigationInputError(
                    "Enter a complete name of 300 characters or fewer."
                )
        elif token_type in {"profile_url", "public_url"}:
            normalized = normalize_public_url(token)
            account_targets = _resolved_profile_accounts(
                normalized, profile_url_resolver
            )
            if token_type == "profile_url" and not account_targets:
                raise InvestigationInputError(
                    "Select Profile URL only for a supported public account URL."
                )

    classified = {
        "schema_version": TOKEN_SCHEMA_VERSION,
        "type": token_type,
        "value": normalized,
        "duplicate_key": _duplicate_key(token_type, normalized),
        "context_only": token_type in {"phone", "public_url"},
        "input": token,
        "predicted_type": predicted_type,
        "type_source": "analyst_override" if requested_type else "automatic",
        "ambiguous_types": _ambiguous_token_types(token),
    }
    if token_type == "profile_url" and account_targets:
        classified["account_targets"] = account_targets
    return classified


def classify_investigation_tokens(
    values: List[Any],
    *,
    type_overrides: Optional[List[Any]] = None,
    profile_url_resolver: Optional[Callable[[str], Dict[str, str]]] = None,
) -> List[Dict[str, Any]]:
    if len(values) > MAX_IDENTIFIERS:
        raise InvestigationInputError(
            f"Use no more than {MAX_IDENTIFIERS} investigation tokens."
        )
    if type_overrides is not None and len(type_overrides) != len(values):
        raise InvestigationInputError(
            "Investigation values and type selections must remain aligned."
        )
    classified: List[Dict[str, Any]] = []
    seen = set()
    for index, value in enumerate(values):
        token = classify_investigation_token(
            value,
            type_override=(type_overrides or [])[index] if type_overrides else None,
            profile_url_resolver=profile_url_resolver,
        )
        duplicate_key = token["duplicate_key"]
        if duplicate_key in seen:
            continue
        seen.add(duplicate_key)
        classified.append(token)
    if not classified:
        raise InvestigationInputError("Add at least one investigation token.")
    return classified


def _normalize_text(value: Any, *, limit: int = MAX_CONTEXT_LENGTH) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(text.split())[:limit]


def normalize_username(value: Any) -> str:
    username = _normalize_text(value, limit=MAX_USERNAME_LENGTH + 1).lstrip("@").strip()
    if not username:
        raise InvestigationInputError("Enter a username or social handle.")
    if len(username) > MAX_USERNAME_LENGTH:
        raise InvestigationInputError(
            f"Usernames must be {MAX_USERNAME_LENGTH} characters or fewer."
        )
    if not is_plausible_username(username) or "#" in username:
        raise InvestigationInputError(
            f"{value!s} is not a valid username or social handle."
        )
    return username


def normalize_email(value: Any) -> str:
    email = _normalize_text(value, limit=255).casefold()
    if len(email) > 254 or not _EMAIL_PATTERN.fullmatch(email):
        raise InvestigationInputError(f"{value!s} is not a valid email address.")
    return email


def normalize_phone(value: Any) -> str:
    raw = _normalize_text(value, limit=80)
    if not raw:
        raise InvestigationInputError("Enter a phone number.")
    if re.search(r"[^0-9+().\-\s]", raw):
        raise InvestigationInputError(f"{value!s} is not a valid phone number.")
    digits = re.sub(r"\D", "", raw)
    if not 7 <= len(digits) <= 15:
        raise InvestigationInputError(
            "Phone numbers must contain between 7 and 15 digits."
        )
    if _INDONESIAN_LOCAL_MOBILE_PATTERN.fullmatch(digits):
        return f"+62{digits[1:]}"
    if _INDONESIAN_COUNTRY_MOBILE_PATTERN.fullmatch(digits):
        return f"+{digits}"
    return f"+{digits}" if raw.startswith("+") else digits


def normalize_profile_url(value: Any) -> str:
    url = _normalize_text(value, limit=2000)
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise InvestigationInputError(
            "Profile URLs must be complete HTTP or HTTPS URLs."
        )
    if parsed.username or parsed.password:
        raise InvestigationInputError("Profile URLs must not contain credentials.")
    return url


def _fallback_handle_from_profile_url(url: str) -> Optional[str]:
    parsed = urlsplit(url)
    segments = [unquote(segment) for segment in parsed.path.split("/") if segment]
    if not segments:
        return None
    candidate = segments[-1].lstrip("@").strip()
    if candidate.casefold() in _GENERIC_PROFILE_SEGMENTS:
        return None
    try:
        return normalize_username(candidate)
    except InvestigationInputError:
        return None


def extract_profile_usernames(
    url: str,
    resolver: Optional[Callable[[str], Dict[str, str]]] = None,
) -> List[str]:
    resolved: List[str] = []
    if resolver is not None:
        for identifier, identifier_type in (resolver(url) or {}).items():
            if identifier_type != "username":
                continue
            try:
                username = normalize_username(identifier)
            except InvestigationInputError:
                continue
            if username not in resolved:
                resolved.append(username)
    fallback = _fallback_handle_from_profile_url(url)
    if fallback and fallback not in resolved:
        resolved.append(fallback)
    if not resolved:
        raise InvestigationInputError(
            "OpenLedger could not extract a username from this profile URL. "
            "Add the account handle instead."
        )
    return resolved


def generate_username_variants(full_name: str) -> List[str]:
    """Backward-compatible value view of the ranked alias planner."""
    return [
        candidate["value"]
        for candidate in rank_username_aliases([full_name])
        if candidate.get("selected")
    ][:MAX_VARIANTS]


def parse_terms(value: Any) -> List[str]:
    terms: List[str] = []
    for raw_term in _TERM_SPLIT_PATTERN.split(str(value or "")):
        term = _normalize_text(raw_term, limit=120)
        if term and term.casefold() not in {item.casefold() for item in terms}:
            terms.append(term)
        if len(terms) >= MAX_TERMS:
            break
    return terms


def _form_list(form: Any, key: str) -> List[str]:
    getter = getattr(form, "getlist", None)
    if callable(getter):
        return [str(value) for value in getter(key)]
    value = form.get(key, [])
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return [str(value)] if value not in (None, "") else []


def parse_source_tags(form: Any, key: str) -> List[str]:
    """Normalize a bounded case-scoped source filter without trusting the form."""
    tags: List[str] = []
    for raw_tag in _form_list(form, key):
        tag = _normalize_text(raw_tag, limit=64).casefold()
        if not tag or not _SOURCE_TAG_PATTERN.fullmatch(tag):
            raise InvestigationInputError("Select a valid source category or country.")
        if tag not in tags:
            tags.append(tag)
        if len(tags) >= MAX_SOURCE_TAGS:
            break
    return tags


_UNIFIED_MODE_ALIASES = {
    "fast": ("quick", "focused"),
    "focused": ("quick", "focused"),
    "quick": ("quick", "focused"),
    "full": ("full", "exhaustive"),
    "exhaustive": ("full", "exhaustive"),
}


def normalize_unified_scan_mode(value: Any) -> tuple[str, str]:
    requested = str(value or "quick").strip().casefold()
    normalized = _UNIFIED_MODE_ALIASES.get(requested)
    if normalized is None:
        raise InvestigationInputError("Select Quick Scan or Full Scan.")
    return normalized


def is_unified_investigation_plan(plan: Any) -> bool:
    return bool(
        isinstance(plan, Mapping)
        and plan.get("schema_version") == UNIFIED_INVESTIGATION_SCHEMA_VERSION
        and plan.get("input_contract") == UNIFIED_INPUT_CONTRACT
        and plan.get("token_schema_version") == TOKEN_SCHEMA_VERSION
    )


def build_unified_investigation_plan(
    form: Any,
    *,
    profile_url_resolver: Optional[Callable[[str], Dict[str, str]]] = None,
    require_route_confirmation: bool = False,
) -> Dict[str, Any]:
    """Build the schema-v2 same-subject plan used by the unified token journey."""
    raw_tokens = [
        value
        for value in _form_list(form, TOKEN_FORM_FIELD)
        if str(value or "").strip()
    ]
    raw_type_overrides = _form_list(form, TOKEN_TYPE_FORM_FIELD)
    if raw_type_overrides and len(raw_type_overrides) != len(raw_tokens):
        raise InvestigationInputError(
            "Investigation values and type selections must remain aligned."
        )
    tokens = classify_investigation_tokens(
        raw_tokens,
        type_overrides=raw_type_overrides or None,
        profile_url_resolver=profile_url_resolver,
    )
    requested_mode, execution_mode = normalize_unified_scan_mode(form.get("mode"))
    search_likely_aliases = "search_likely_username_aliases" in form
    enable_user_scanner_username = "enable_user_scanner_username" in form
    enable_github_profile_enrichment = "enable_github_profile_enrichment" in form
    enable_archived_url_evidence = "enable_archived_url_evidence" in form
    requested_username_platforms = _form_list(form, "user_scanner_platform")
    if (
        enable_user_scanner_username
        and not requested_username_platforms
        and "user_scanner_platforms_present" not in form
    ):
        requested_username_platforms = sorted(USER_SCANNER_USERNAME_PLATFORMS)
    username_platforms: List[str] = []
    for raw_platform in requested_username_platforms:
        platform = str(raw_platform or "").strip().casefold()
        if platform not in USER_SCANNER_USERNAME_PLATFORMS:
            raise InvestigationInputError("Select a supported username platform.")
        if platform not in username_platforms:
            username_platforms.append(platform)
    if enable_user_scanner_username and not username_platforms:
        raise InvestigationInputError(
            "Select at least one platform for User Scanner username verification."
        )
    if not enable_user_scanner_username:
        username_platforms = []
    allow_user_scanner_vxtwitter = bool(
        enable_user_scanner_username
        and "x" in username_platforms
        and "allow_user_scanner_vxtwitter" in form
    )
    identifiers: List[Dict[str, Any]] = []
    search_targets: List[Dict[str, Any]] = []
    full_names: List[str] = []
    confirmed_usernames: List[str] = []

    def add_target(
        username: str,
        source_type: str,
        source_value: str,
        *,
        alias_score: Optional[int] = None,
        alias_reason: str = "",
    ) -> None:
        if username.casefold() in {
            str(target["value"]).casefold() for target in search_targets
        }:
            return
        target: Dict[str, Any] = {
            "value": username,
            "source_type": source_type,
            "source_value": source_value,
        }
        if alias_score is not None:
            target["alias_score"] = alias_score
            target["alias_reason"] = alias_reason[:240]
        search_targets.append(target)

    for token in tokens:
        token_type = str(token["type"])
        value = str(token["value"])
        identifiers.append({"type": token_type, "value": value})
        if token_type in {"username", "social_handle"}:
            add_target(value, token_type, value)
        elif token_type == "profile_url":
            for username in list(token.get("account_targets") or []):
                add_target(str(username), "profile_url", value)
                confirmed_usernames.append(str(username))
        elif token_type == "full_name":
            full_names.append(value)

    email_count = sum(token["type"] == "email" for token in tokens)
    if email_count > 1:
        raise InvestigationInputError(
            "The current bounded email route accepts one email per investigation."
        )
    email_route_confirmed = str(
        form.get("confirm_email_route", "")
    ).strip().casefold() in {"1", "on", "true", "yes"}
    if require_route_confirmation and email_count and not email_route_confirmed:
        raise InvestigationInputError(
            "Review and confirm the bounded public email route before starting "
            "this investigation."
        )

    alias_candidates = (
        rank_username_aliases(
            full_names,
            confirmed_usernames=confirmed_usernames,
        )
        if search_likely_aliases and full_names
        else []
    )
    submitted_alias_values = _form_list(form, "selected_alias")
    alias_selection_present = bool(
        search_likely_aliases and "alias_candidates_present" in form
    )
    if submitted_alias_values and not search_likely_aliases:
        raise InvestigationInputError(
            "Enable likely username aliases before selecting aliases."
        )
    if alias_selection_present:
        selected_keys = {
            normalize_username(value).casefold()
            for value in submitted_alias_values
            if str(value or "").strip()
        }
        candidate_keys = {
            str(candidate["value"]).casefold() for candidate in alias_candidates
        }
        if selected_keys.difference(candidate_keys):
            raise InvestigationInputError(
                "Select aliases from the displayed server-ranked plan."
            )
        for candidate in alias_candidates:
            candidate["selected"] = str(candidate["value"]).casefold() in selected_keys
    selected_aliases = [
        candidate for candidate in alias_candidates if candidate.get("selected")
    ]
    if len(selected_aliases) > MAX_SELECTED_ALIASES:
        raise InvestigationInputError(
            f"Select no more than {MAX_SELECTED_ALIASES} username aliases."
        )
    selected_keys = {
        str(candidate["value"]).casefold() for candidate in selected_aliases
    }
    for candidate in alias_candidates:
        candidate["selected"] = str(candidate["value"]).casefold() in selected_keys
    for candidate in selected_aliases:
        add_target(
            str(candidate["value"]),
            "ranked_alias",
            full_names[0],
            alias_score=int(candidate["score"]),
            alias_reason=str(candidate["reason"]),
        )
    if enable_user_scanner_username and not search_targets:
        raise InvestigationInputError(
            "User Scanner username verification requires at least one username "
            "target. Select a username alias or add a username, social handle, "
            "or supported profile URL."
        )
    if (
        enable_user_scanner_username
        and len(search_targets) > MAX_USER_SCANNER_USERNAME_TARGETS
    ):
        raise InvestigationInputError(
            "User Scanner username verification accepts no more than "
            f"{MAX_USER_SCANNER_USERNAME_TARGETS} total account targets."
        )

    has_potential_route = bool(search_targets or full_names or email_count)
    if not has_potential_route:
        raise InvestigationInputError(
            "These tokens are context only. Add a name, username, social handle, "
            "supported public profile URL, or email with an available authorized route."
        )

    subject_label = next(
        (str(token["value"]) for token in tokens if token["type"] == "full_name"),
        "",
    )
    if not subject_label:
        subject_label = (
            str(search_targets[0]["value"])
            if search_targets
            else str(tokens[0]["value"])
        )
    return {
        "schema_version": UNIFIED_INVESTIGATION_SCHEMA_VERSION,
        "input_contract": UNIFIED_INPUT_CONTRACT,
        "token_schema_version": TOKEN_SCHEMA_VERSION,
        "processing_mode": "same_subject",
        "requested_mode": requested_mode,
        "execution_mode": execution_mode,
        "search_likely_username_aliases": search_likely_aliases,
        # Preserve the established internal keys while the schema-v2 contract
        # keeps route selection bounded and server-authoritative.
        "generate_name_variants": search_likely_aliases,
        "allow_ai_context": False,
        "enable_user_scanner_email": bool(email_count),
        "email_route_confirmed": email_route_confirmed,
        "enable_user_scanner_username": enable_user_scanner_username,
        "user_scanner_username_platforms": username_platforms,
        "allow_user_scanner_vxtwitter": allow_user_scanner_vxtwitter,
        "enable_github_profile_enrichment": enable_github_profile_enrichment,
        "enable_archived_url_evidence": enable_archived_url_evidence,
        "subject_label": subject_label[:500],
        "tokens": tokens,
        "identifiers": identifiers,
        "alias_nicknames": [],
        "alias_context_numbers": [],
        "alias_candidates": alias_candidates,
        "tags": [],
        "excluded_tags": [],
        "include_terms": [],
        "exclude_terms": [],
        "search_targets": search_targets,
    }


def _route(
    route: str,
    label: str,
    *,
    kind: str,
    target_count: int,
    token_types: List[str],
) -> Dict[str, Any]:
    return {
        "route": route,
        "label": label,
        "kind": kind,
        "target_count": int(target_count),
        "token_types": sorted(set(token_types)),
    }


def finalize_investigation_route_plan(
    plan: Mapping[str, Any],
    *,
    flags: Mapping[str, Any],
    execution_mode: Any = None,
) -> Dict[str, Any]:
    """Replace any supplied route document with a deterministic server plan."""
    normalized = dict(plan)
    if not is_unified_investigation_plan(normalized):
        return normalized
    requested_mode, contract_mode = normalize_unified_scan_mode(
        execution_mode
        or normalized.get("execution_mode")
        or normalized.get("requested_mode")
    )
    budget = execution_budget_spec(contract_mode)
    tokens = [
        item for item in list(normalized.get("tokens") or []) if isinstance(item, dict)
    ]
    token_types = [str(item.get("type") or "") for item in tokens]
    search_targets = [
        item
        for item in list(normalized.get("search_targets") or [])
        if isinstance(item, dict) and item.get("value")
    ]
    full_name_count = token_types.count("full_name")
    email_count = token_types.count("email")
    requested_routes: List[Dict[str, Any]] = []
    effective_routes: List[Dict[str, Any]] = []
    skipped_routes: List[Dict[str, Any]] = []

    if normalized.get("search_likely_username_aliases") and full_name_count:
        alias_route = _route(
            "likely_username_aliases",
            "Search likely username aliases",
            kind="planning",
            target_count=sum(
                bool(item.get("selected"))
                for item in list(normalized.get("alias_candidates") or [])
                if isinstance(item, dict)
            ),
            token_types=["full_name"],
        )
        requested_routes.append(alias_route)
        effective_routes.append(dict(alias_route))

    if search_targets:
        maigret_route = _route(
            "maigret",
            "Public account discovery",
            kind="collection",
            target_count=len(search_targets),
            token_types=[
                item
                for item in token_types
                if item in {"profile_url", "social_handle", "username"}
            ]
            + (
                ["ranked_alias"]
                if normalized.get("search_likely_username_aliases")
                else []
            ),
        ) | {
            "coverage": (
                "all_eligible_enabled_non_quarantined"
                if budget["mode"] == "exhaustive"
                else "configured_top_ranked_enabled_non_quarantined"
            )
        }
        requested_routes.append(maigret_route)
        if flags.get("maigret_enabled"):
            effective_routes.append(dict(maigret_route))
        else:
            skipped_routes.append({**maigret_route, "reason_code": "server_disabled"})
    elif full_name_count:
        skipped_routes.append(
            _route(
                "maigret",
                "Public account discovery",
                kind="collection",
                target_count=0,
                token_types=["full_name"],
            )
            | {"reason_code": "no_username_targets"}
        )

    if normalized.get("enable_user_scanner_username"):
        scanner_route = _route(
            "user_scanner_username",
            "Major-platform username verification",
            kind="collection",
            target_count=len(search_targets),
            token_types=["username"],
        ) | {
            "platforms": list(
                normalized.get("user_scanner_username_platforms") or []
            ),
            "third_party_x_enabled": bool(
                normalized.get("allow_user_scanner_vxtwitter")
            ),
        }
        requested_routes.append(scanner_route)
        if flags.get("user_scanner_enabled"):
            effective_routes.append(dict(scanner_route))
        else:
            skipped_routes.append(
                {**scanner_route, "reason_code": "server_disabled"}
            )

    for capability, route_name, label in (
        (
            "enable_github_profile_enrichment",
            "github_profile_enrichment",
            "GitHub profile enrichment",
        ),
        (
            "enable_archived_url_evidence",
            "archived_profile_evidence",
            "Supported-profile archive evidence",
        ),
    ):
        if not normalized.get(capability):
            continue
        follow_up_route = _route(
            route_name,
            label,
            kind="collection",
            target_count=len(search_targets),
            token_types=["supported_profile"],
        ) | {"conditional_on_supported_profile": True}
        requested_routes.append(follow_up_route)
        if flags.get("enrichment_providers_enabled"):
            effective_routes.append(dict(follow_up_route))
        else:
            skipped_routes.append(
                {**follow_up_route, "reason_code": "server_disabled"}
            )

    if search_targets or full_name_count:
        native_queries = plan_profile_search_queries(normalized)
        native_route = _route(
            "native_profile_search",
            "Major-platform public profile search",
            kind="collection",
            target_count=len(
                {(query.seed_kind, query.seed_value) for query in native_queries}
            ),
            token_types=[
                item
                for item in token_types
                if item in {"full_name", "profile_url", "social_handle", "username"}
            ],
        ) | {"planned_request_count": len(native_queries)}
        requested_routes.append(native_route)
        if flags.get("search_first_enabled"):
            effective_routes.append(dict(native_route))
        else:
            skipped_routes.append({**native_route, "reason_code": "server_disabled"})

    if email_count:
        email_route = _route(
            "user_scanner_email",
            "Bounded public email discovery",
            kind="collection",
            target_count=email_count,
            token_types=["email"],
        ) | {"requires_confirmation": True}
        requested_routes.append(email_route)
        if not normalized.get("email_route_confirmed"):
            skipped_routes.append(
                {**email_route, "reason_code": "confirmation_required"}
            )
        elif flags.get("user_scanner_enabled"):
            effective_routes.append(dict(email_route))
        else:
            skipped_routes.append({**email_route, "reason_code": "server_disabled"})

    for context_type, label in (
        ("phone", "Phone retained as unverified context"),
        ("public_url", "Generic public URL retained as unverified context"),
    ):
        count = token_types.count(context_type)
        if count:
            skipped_routes.append(
                _route(
                    "context_only",
                    label,
                    kind="context",
                    target_count=count,
                    token_types=[context_type],
                )
                | {"reason_code": "context_only_no_outbound"}
            )

    input_sha256 = hashlib.sha256(
        json.dumps(
            {
                "tokens": tokens,
                "search_targets": search_targets,
                "alias_candidates": list(normalized.get("alias_candidates") or []),
                "requested_capabilities": {
                    "search_likely_username_aliases": bool(
                        normalized.get("search_likely_username_aliases")
                    ),
                    "enable_user_scanner_username": bool(
                        normalized.get("enable_user_scanner_username")
                    ),
                    "user_scanner_username_platforms": list(
                        normalized.get("user_scanner_username_platforms") or []
                    ),
                    "allow_user_scanner_vxtwitter": bool(
                        normalized.get("allow_user_scanner_vxtwitter")
                    ),
                    "enable_github_profile_enrichment": bool(
                        normalized.get("enable_github_profile_enrichment")
                    ),
                    "enable_archived_url_evidence": bool(
                        normalized.get("enable_archived_url_evidence")
                    ),
                },
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    unsigned_route_plan = {
        "schema_version": ROUTE_PLAN_SCHEMA_VERSION,
        "policy_version": ROUTE_PLAN_POLICY_VERSION,
        "server_authoritative": True,
        "input_sha256": input_sha256,
        "requested_mode": requested_mode,
        "execution_mode": str(budget["mode"]),
        "budget_seconds": int(budget["total_seconds"]),
        "requested_routes": requested_routes,
        "effective_routes": effective_routes,
        "skipped_routes": skipped_routes,
    }
    route_plan_sha256 = hashlib.sha256(
        json.dumps(
            unsigned_route_plan,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    normalized["requested_mode"] = requested_mode
    normalized["execution_mode"] = str(budget["mode"])
    normalized["route_plan"] = {
        **unsigned_route_plan,
        "sha256": route_plan_sha256,
    }
    return normalized


def investigation_has_effective_collection_route(plan: Any) -> bool:
    if not is_unified_investigation_plan(plan):
        return False
    route_plan = plan.get("route_plan")
    if not isinstance(route_plan, Mapping):
        return False
    return any(
        isinstance(item, Mapping) and item.get("route") in _COLLECTION_ROUTES
        for item in list(route_plan.get("effective_routes") or [])
    )


def build_investigation_plan(
    form: Any,
    *,
    profile_url_resolver: Optional[Callable[[str], Dict[str, str]]] = None,
) -> Dict[str, Any]:
    types = _form_list(form, "identifier_type")
    values = _form_list(form, "identifier_value")
    if len(types) != len(values):
        raise InvestigationInputError("The identifier rows are incomplete.")
    if len(types) > MAX_IDENTIFIERS:
        raise InvestigationInputError(
            f"Use no more than {MAX_IDENTIFIERS} identifiers per investigation."
        )

    processing_mode = str(form.get("processing_mode", "same_subject"))
    if processing_mode not in PROCESSING_MODES:
        raise InvestigationInputError("Select a valid identifier processing mode.")
    generate_variants = "generate_name_variants" in form
    allow_ai_context = "allow_ai_context" in form
    enable_user_scanner_email = "enable_user_scanner_email" in form
    enable_user_scanner_username = "enable_user_scanner_username" in form
    enable_github_profile_enrichment = "enable_github_profile_enrichment" in form
    enable_archived_url_evidence = "enable_archived_url_evidence" in form
    if enable_user_scanner_email and processing_mode != "same_subject":
        raise InvestigationInputError(
            "User Scanner email evidence requires One subject mode so observations "
            "cannot be attached to the wrong Persona."
        )
    requested_username_platforms = _form_list(form, "user_scanner_platform")
    if (
        enable_user_scanner_username
        and not requested_username_platforms
        and "user_scanner_platforms_present" not in form
    ):
        requested_username_platforms = sorted(USER_SCANNER_USERNAME_PLATFORMS)
    username_platforms: List[str] = []
    for raw_platform in requested_username_platforms:
        platform = str(raw_platform or "").strip().casefold()
        if platform not in USER_SCANNER_USERNAME_PLATFORMS:
            raise InvestigationInputError("Select a supported username platform.")
        if platform not in username_platforms:
            username_platforms.append(platform)
    if enable_user_scanner_username and not username_platforms:
        raise InvestigationInputError(
            "Select at least one platform for User Scanner username verification."
        )
    allow_user_scanner_vxtwitter = bool(
        enable_user_scanner_username
        and "x" in username_platforms
        and "allow_user_scanner_vxtwitter" in form
    )
    tags = parse_source_tags(form, "tags")
    excluded_tags = parse_source_tags(form, "excluded_tags")
    if set(tags).intersection(excluded_tags):
        raise InvestigationInputError(
            "A source category or country cannot be both included and excluded."
        )
    identifiers: List[Dict[str, Any]] = []
    search_targets: List[Dict[str, Any]] = []
    full_names: List[str] = []
    confirmed_usernames: List[str] = []

    def add_target(
        username: str,
        source_type: str,
        source_value: str,
        *,
        alias_score: Optional[int] = None,
        alias_reason: str = "",
    ) -> None:
        if username.casefold() in {
            target["value"].casefold() for target in search_targets
        }:
            return
        target: Dict[str, Any] = {
            "value": username,
            "source_type": source_type,
            "source_value": source_value,
        }
        if alias_score is not None:
            target["alias_score"] = alias_score
            target["alias_reason"] = alias_reason[:240]
        search_targets.append(target)

    for identifier_type, raw_value in zip(types, values):
        identifier_type = identifier_type.strip()
        if not identifier_type and not str(raw_value).strip():
            continue
        if identifier_type not in IDENTIFIER_TYPES:
            raise InvestigationInputError("Select a valid identifier type.")
        if not str(raw_value).strip():
            raise InvestigationInputError("Every identifier row needs a value.")

        if identifier_type in {"username", "social_handle"}:
            normalized = normalize_username(raw_value)
            add_target(normalized, identifier_type, normalized)
        elif identifier_type == "profile_url":
            normalized = normalize_profile_url(raw_value)
            for username in extract_profile_usernames(
                normalized, resolver=profile_url_resolver
            ):
                add_target(username, identifier_type, normalized)
                confirmed_usernames.append(username)
        elif identifier_type == "full_name":
            normalized = _normalize_text(raw_value)
            if len(normalized) < 2:
                raise InvestigationInputError("Enter a complete name.")
            full_names.append(normalized)
        elif identifier_type == "email":
            normalized = normalize_email(raw_value)
        else:
            normalized = normalize_phone(raw_value)
        identifiers.append({"type": identifier_type, "value": normalized})

    alias_nicknames: List[str] = []
    alias_context_numbers: List[str] = []
    if generate_variants:
        try:
            alias_nicknames = normalize_nicknames(
                _form_list(form, "alias_nicknames")
            )
            alias_context_numbers = normalize_context_numbers(
                _form_list(form, "alias_context_numbers")
            )
        except ValueError as error:
            raise InvestigationInputError(str(error)) from error

    generated_aliases = (
        rank_username_aliases(
            full_names,
            nicknames=alias_nicknames,
            contextual_numbers=alias_context_numbers,
            confirmed_usernames=confirmed_usernames,
        )
        if generate_variants
        else []
    )
    generated_by_value = {
        str(candidate["value"]).casefold(): candidate for candidate in generated_aliases
    }
    alias_candidates: List[Dict[str, Any]] = []
    raw_alias_candidates = _form_list(form, "alias_candidate")
    submitted_alias_plan = bool(
        generate_variants
        and "alias_candidates_present" in form
        and raw_alias_candidates
    )
    if submitted_alias_plan:
        selected_values = set()
        for raw_selected in _form_list(form, "selected_alias"):
            if not str(raw_selected).strip():
                continue
            selected_values.add(normalize_username(raw_selected).casefold())
        seen_aliases = set()
        for raw_candidate in raw_alias_candidates[:MAX_ALIAS_CANDIDATES]:
            if not str(raw_candidate).strip():
                continue
            candidate_value = normalize_username(raw_candidate)
            key = candidate_value.casefold()
            if key in seen_aliases:
                continue
            seen_aliases.add(key)
            generated = generated_by_value.get(key)
            alias_candidates.append(
                {
                    "value": candidate_value,
                    "score": int((generated or {}).get("score", 70)),
                    "reason": str(
                        (generated or {}).get(
                            "reason", "Analyst-edited alias candidate"
                        )
                    )[:240],
                    "selected": key in selected_values,
                }
            )
        unknown_selections = selected_values.difference(seen_aliases)
        if unknown_selections:
            raise InvestigationInputError("Select aliases from the displayed plan.")
    else:
        alias_candidates = [dict(candidate) for candidate in generated_aliases]

    selected_aliases = [
        candidate for candidate in alias_candidates if candidate.get("selected")
    ]
    if len(selected_aliases) > MAX_SELECTED_ALIASES:
        raise InvestigationInputError(
            f"Select no more than {MAX_SELECTED_ALIASES} username aliases."
        )
    if enable_user_scanner_username:
        scanner_target_keys = {
            str(target["value"]).casefold() for target in search_targets
        }
        if submitted_alias_plan:
            scanner_target_keys.update(
                str(candidate["value"]).casefold()
                for candidate in selected_aliases
            )
            if len(scanner_target_keys) > MAX_USER_SCANNER_USERNAME_TARGETS:
                raise InvestigationInputError(
                    "User Scanner username verification accepts no more than "
                    f"{MAX_USER_SCANNER_USERNAME_TARGETS} total account targets. "
                    "Deselect aliases or disable the additional verification."
                )
        else:
            selected_alias_count = 0
            for candidate in alias_candidates:
                candidate_key = str(candidate["value"]).casefold()
                candidate["selected"] = bool(
                    int(candidate["score"]) >= 78
                    and candidate_key not in scanner_target_keys
                    and len(scanner_target_keys) < MAX_USER_SCANNER_USERNAME_TARGETS
                    and selected_alias_count < MAX_SELECTED_ALIASES
                )
                if candidate["selected"]:
                    scanner_target_keys.add(candidate_key)
                    selected_alias_count += 1
            selected_aliases = [
                candidate for candidate in alias_candidates if candidate.get("selected")
            ]
    for candidate in selected_aliases:
        add_target(
            str(candidate["value"]),
            "ranked_alias",
            full_names[0] if full_names else str(candidate["value"]),
            alias_score=int(candidate["score"]),
            alias_reason=str(candidate["reason"]),
        )
    if (
        enable_user_scanner_username
        and len(search_targets) > MAX_USER_SCANNER_USERNAME_TARGETS
    ):
        raise InvestigationInputError(
            "User Scanner username verification accepts no more than "
            f"{MAX_USER_SCANNER_USERNAME_TARGETS} total account targets."
        )

    if not identifiers:
        raise InvestigationInputError("Add at least one investigation identifier.")
    if not search_targets:
        raise InvestigationInputError(
            "Add a username, social handle, supported profile URL, or enable "
            "reviewable username variants for a name. Email and phone values are "
            "retained as context and are not sent to the username scanner."
        )

    email_identifier_count = sum(
        identifier["type"] == "email" for identifier in identifiers
    )
    if enable_user_scanner_email and email_identifier_count == 0:
        raise InvestigationInputError(
            "Add an email identifier before enabling User Scanner email checks."
        )
    if enable_user_scanner_email and email_identifier_count > 1:
        raise InvestigationInputError(
            "The initial User Scanner integration accepts one email per "
            "investigation. Run additional addresses as separate cases."
        )

    full_name = next(
        (
            identifier["value"]
            for identifier in identifiers
            if identifier["type"] == "full_name"
        ),
        "",
    )
    subject_label = full_name or search_targets[0]["value"]
    return {
        "schema_version": SCHEMA_VERSION,
        "processing_mode": processing_mode,
        "generate_name_variants": generate_variants,
        "allow_ai_context": allow_ai_context,
        "enable_user_scanner_email": enable_user_scanner_email,
        "enable_user_scanner_username": enable_user_scanner_username,
        "user_scanner_username_platforms": username_platforms,
        "allow_user_scanner_vxtwitter": allow_user_scanner_vxtwitter,
        "enable_github_profile_enrichment": enable_github_profile_enrichment,
        "enable_archived_url_evidence": enable_archived_url_evidence,
        "subject_label": subject_label,
        "identifiers": identifiers,
        "alias_nicknames": alias_nicknames,
        "alias_context_numbers": alias_context_numbers,
        "alias_candidates": alias_candidates,
        "tags": tags,
        "excluded_tags": excluded_tags,
        # Retain legacy keys so old stored jobs and API clients remain readable.
        # The browser investigation builder no longer collects free-form terms.
        "include_terms": parse_terms(form.get("include_terms", "")),
        "exclude_terms": parse_terms(form.get("exclude_terms", "")),
        "search_targets": search_targets,
    }


def search_usernames(plan: Dict[str, Any]) -> List[str]:
    return [
        str(target["value"])
        for target in plan.get("search_targets", [])
        if isinstance(target, dict) and target.get("value")
    ]


def public_ai_context(plan: Any) -> Dict[str, Any]:
    """Return bounded operator context only after explicit external-use consent."""
    if not isinstance(plan, dict) or not plan.get("allow_ai_context"):
        return {}
    return {
        "subject_label": _normalize_text(plan.get("subject_label", "")),
        "identifiers": [
            {
                "type": str(identifier.get("type", ""))[:40],
                "value": _normalize_text(identifier.get("value", ""), limit=500),
            }
            for identifier in list(plan.get("identifiers") or [])[:MAX_IDENTIFIERS]
            if isinstance(identifier, dict)
        ],
        "include_terms": [
            _normalize_text(term, limit=120)
            for term in list(plan.get("include_terms") or [])[:MAX_TERMS]
        ],
        "exclude_terms": [
            _normalize_text(term, limit=120)
            for term in list(plan.get("exclude_terms") or [])[:MAX_TERMS]
        ],
    }


def grouped_subject(plan: Any) -> bool:
    return isinstance(plan, dict) and plan.get("processing_mode") == "same_subject"
