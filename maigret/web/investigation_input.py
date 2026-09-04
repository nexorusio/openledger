"""Validated, capability-aware input planning for OpenLedger investigations."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import unquote, urlsplit

from maigret.utils import is_plausible_username
from maigret.web.username_aliases import (
    MAX_ALIAS_CANDIDATES,
    MAX_SELECTED_ALIASES,
    normalize_context_numbers,
    normalize_nicknames,
    rank_username_aliases,
)

SCHEMA_VERSION = 1
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

    try:
        alias_nicknames = normalize_nicknames(_form_list(form, "alias_nicknames"))
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
        selected_values = {
            str(value).strip().casefold()
            for value in _form_list(form, "selected_alias")
            if str(value).strip()
        }
        seen_aliases = set()
        for raw_candidate in raw_alias_candidates[:MAX_ALIAS_CANDIDATES]:
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
        remaining_scanner_slots = max(
            0, MAX_USER_SCANNER_USERNAME_TARGETS - len(search_targets)
        )
        if submitted_alias_plan:
            if len(selected_aliases) > remaining_scanner_slots:
                raise InvestigationInputError(
                    "User Scanner username verification accepts no more than "
                    f"{MAX_USER_SCANNER_USERNAME_TARGETS} total account targets. "
                    "Deselect aliases or disable the additional verification."
                )
        else:
            selected_keys = {
                str(candidate["value"]).casefold()
                for candidate in selected_aliases[:remaining_scanner_slots]
            }
            for candidate in alias_candidates:
                candidate["selected"] = (
                    str(candidate["value"]).casefold() in selected_keys
                )
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
