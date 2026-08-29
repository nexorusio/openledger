"""Validated, capability-aware input planning for OpenLedger investigations."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import unquote, urlsplit

from maigret.utils import is_plausible_username

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
MAX_USERNAME_LENGTH = 128
MAX_CONTEXT_LENGTH = 500
MAX_VARIANTS = 16

_EMAIL_PATTERN = re.compile(
    r"^[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+$",
    re.IGNORECASE,
)
_TERM_SPLIT_PATTERN = re.compile(r"[,\n\r]+")
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


def _name_tokens(full_name: str) -> List[str]:
    tokens = []
    for raw_token in full_name.casefold().split():
        token = "".join(character for character in raw_token if character.isalnum())
        if token:
            tokens.append(token)
    return tokens[:6]


def generate_username_variants(full_name: str) -> List[str]:
    """Generate a small, reviewable set rather than a Cartesian explosion."""
    tokens = _name_tokens(full_name)
    if not tokens:
        return []
    combinations: List[List[str]] = [tokens]
    if len(tokens) > 1:
        first_last = [tokens[0], tokens[-1]]
        last_first = [tokens[-1], tokens[0]]
        combinations.extend([first_last, last_first])
    variants: List[str] = []
    for parts in combinations:
        for separator in ("", ".", "_", "-"):
            candidate = separator.join(parts)
            if candidate and candidate not in variants:
                variants.append(candidate)
        if len(parts) > 1:
            for candidate in (parts[0][0] + parts[-1], parts[0] + parts[-1][0]):
                if candidate and candidate not in variants:
                    variants.append(candidate)
        if len(variants) >= MAX_VARIANTS:
            break
    return variants[:MAX_VARIANTS]


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
    identifiers: List[Dict[str, Any]] = []
    search_targets: List[Dict[str, str]] = []

    def add_target(username: str, source_type: str, source_value: str) -> None:
        if username.casefold() in {
            target["value"].casefold() for target in search_targets
        }:
            return
        search_targets.append(
            {
                "value": username,
                "source_type": source_type,
                "source_value": source_value,
            }
        )

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
        elif identifier_type == "full_name":
            normalized = _normalize_text(raw_value)
            if len(normalized) < 2:
                raise InvestigationInputError("Enter a complete name.")
            if generate_variants:
                for username in generate_username_variants(normalized):
                    add_target(username, "generated_name_variant", normalized)
        elif identifier_type == "email":
            normalized = normalize_email(raw_value)
        else:
            normalized = normalize_phone(raw_value)
        identifiers.append({"type": identifier_type, "value": normalized})

    if not identifiers:
        raise InvestigationInputError("Add at least one investigation identifier.")
    if not search_targets:
        raise InvestigationInputError(
            "Add a username, social handle, supported profile URL, or enable "
            "reviewable username variants for a name. Email and phone values are "
            "retained as context and are not sent to the username scanner."
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
        "subject_label": subject_label,
        "identifiers": identifiers,
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
