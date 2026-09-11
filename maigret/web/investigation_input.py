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
    """A rejected plan with guidance deliberately approved for public responses.

    Callers must supply validation guidance, never a serialized lower-level
    exception. Keep internal diagnostics in a chained cause instead.
    """

    def __init__(self, public_message: str) -> None:
        if not isinstance(public_message, str):
            raise TypeError("Investigation input guidance must be a string.")
        self._public_message = public_message
        super().__init__(public_message)

    @property
    def public_message(self) -> str:
        """Return the authored guidance independently of exception diagnostics."""
        return self._public_message


def _normalize_text(value: Any, *, limit: int = MAX_CONTEXT_LENGTH) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(text.split())[:limit]


def normalize_username(value: Any) -> str:
    username = _normalize_text(value, limit=MAX_USERNAME_LENGTH + 1).lstrip("@").strip()
    if not username:
        raise InvestigationInputError("Enter a username, @handle, or profile URL.")
    if len(username) > MAX_USERNAME_LENGTH:
        raise InvestigationInputError(
            f"Usernames must be {MAX_USERNAME_LENGTH} characters or fewer."
        )
    if not is_plausible_username(username) or "#" in username:
        raise InvestigationInputError(
            f"{value!s} is not a valid username or @handle."
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
    profile_usernames: Dict[str, List[str]] = {}

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

    def add_identifier(identifier_type: str, value: str) -> None:
        """Store one canonical identifier while retaining profile URL provenance."""
        canonical_type = (
            "username" if identifier_type == "social_handle" else identifier_type
        )
        comparison_value = (
            value.casefold() if canonical_type != "profile_url" else value
        )
        if any(
            item["type"] == canonical_type
            and (
                item["value"].casefold()
                if canonical_type != "profile_url"
                else item["value"]
            )
            == comparison_value
            for item in identifiers
        ):
            return
        identifiers.append({"type": canonical_type, "value": value})

    for identifier_type, raw_value in zip(types, values):
        identifier_type = identifier_type.strip()
        if not identifier_type and not str(raw_value).strip():
            continue
        if identifier_type not in IDENTIFIER_TYPES:
            raise InvestigationInputError("Select a valid identifier type.")
        if not str(raw_value).strip():
            raise InvestigationInputError("Every identifier row needs a value.")

        # The public form has one account input. Continue accepting historical
        # type names, and detect complete profile URLs inside that same input.
        if identifier_type in {"username", "social_handle"} and _normalize_text(
            raw_value, limit=2000
        ).casefold().startswith(("http://", "https://")):
            identifier_type = "profile_url"

        if identifier_type in {"username", "social_handle"}:
            normalized = normalize_username(raw_value)
            add_target(normalized, "username", normalized)
        elif identifier_type == "profile_url":
            normalized = normalize_profile_url(raw_value)
            profile_usernames[normalized] = extract_profile_usernames(
                normalized, resolver=profile_url_resolver
            )
            for username in profile_usernames[normalized]:
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
        add_identifier(identifier_type, normalized)

    alias_nicknames: List[str] = []
    alias_context_numbers: List[str] = []
    if generate_variants:
        try:
            alias_nicknames = normalize_nicknames(_form_list(form, "alias_nicknames"))
        except ValueError as error:
            raise InvestigationInputError(
                "Enter one nickname per comma-separated value."
            ) from error
        try:
            alias_context_numbers = normalize_context_numbers(
                _form_list(form, "alias_context_numbers")
            )
        except ValueError as error:
            raise InvestigationInputError(
                "Contextual numbers must contain 1 to 6 digits each."
            ) from error

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
    # Keep each name's aliases attached to that entered subject. The combined
    # ranking deliberately deduplicates scan targets, not the people being studied.
    alias_source_names: Dict[str, List[str]] = {}
    if generate_variants:
        for name in full_names:
            for candidate in rank_username_aliases(
                [name],
                nicknames=alias_nicknames,
                contextual_numbers=alias_context_numbers,
                confirmed_usernames=confirmed_usernames,
            ):
                sources = alias_source_names.setdefault(
                    str(candidate["value"]).casefold(), []
                )
                if name not in sources:
                    sources.append(name)
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
                str(candidate["value"]).casefold() for candidate in selected_aliases
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
        source_names = alias_source_names.get(str(candidate["value"]).casefold())
        if not source_names:
            source_names = list(dict.fromkeys(full_names))
            if processing_mode == "independent" and len(source_names) != 1:
                raise InvestigationInputError(
                    "Edited aliases need one originating name in Separate subjects "
                    "mode. Use One subject mode or submit each subject separately."
                )
            alias_source_names[str(candidate["value"]).casefold()] = source_names
        add_target(
            str(candidate["value"]),
            "ranked_alias",
            source_names[0] if source_names else str(candidate["value"]),
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
            "Add a username, @handle, supported profile URL, or enable "
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
    if processing_mode == "same_subject":
        subject_groups = [
            {
                "label": subject_label,
                "usernames": [target["value"] for target in search_targets],
                "identifiers": identifiers,
            }
        ]
    else:
        subject_groups = []
        selected_alias_keys = {
            str(candidate["value"]).casefold() for candidate in selected_aliases
        }
        for identifier in identifiers:
            identifier_type, value = identifier["type"], identifier["value"]
            if identifier_type not in {"username", "profile_url", "full_name"}:
                continue
            if identifier_type == "full_name":
                targets = [
                    target["value"]
                    for target in search_targets
                    if any(
                        name.casefold() == value.casefold()
                        for name in alias_source_names.get(
                            target["value"].casefold(), []
                        )
                    )
                    and target["value"].casefold() in selected_alias_keys
                ]
                subject_groups.append(
                    {
                        "label": value,
                        "usernames": targets,
                        "identifiers": [identifier],
                    }
                )
                continue

            targets = (
                profile_usernames[value]
                if identifier_type == "profile_url"
                else [value]
            )
            target_keys = {target.casefold() for target in targets}
            matching_groups = [
                group
                for group in subject_groups
                if group.get("account_group")
                and target_keys.intersection(
                    username.casefold() for username in group["usernames"]
                )
            ]
            if not matching_groups:
                subject_groups.append(
                    {
                        "label": targets[0],
                        "usernames": list(targets),
                        "identifiers": [identifier],
                        "account_group": True,
                    }
                )
                continue

            primary = matching_groups[0]
            for target in targets:
                if target.casefold() not in {
                    username.casefold() for username in primary["usernames"]
                }:
                    primary["usernames"].append(target)
            if identifier not in primary["identifiers"]:
                primary["identifiers"].append(identifier)
            for merged in matching_groups[1:]:
                for target in merged["usernames"]:
                    if target.casefold() not in {
                        username.casefold() for username in primary["usernames"]
                    }:
                        primary["usernames"].append(target)
                for merged_identifier in merged["identifiers"]:
                    if merged_identifier not in primary["identifiers"]:
                        primary["identifiers"].append(merged_identifier)
                subject_groups.remove(merged)
        for group in subject_groups:
            group.pop("account_group", None)
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
        "subject_groups": subject_groups,
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
        "profile_url_usernames": profile_usernames,
    }


def search_usernames(plan: Dict[str, Any]) -> List[str]:
    return [
        str(target["value"])
        for target in plan.get("search_targets", [])
        if isinstance(target, dict) and target.get("value")
    ]


def public_identifier_scope(plan: Any) -> Dict[str, List[Dict[str, Any]]]:
    """Present account inputs as canonical usernames with URL provenance attached."""
    if not isinstance(plan, dict):
        return {"identifiers": [], "profile_urls": []}

    canonical: List[Dict[str, str]] = []
    seen = set()

    def add(identifier_type: str, value: Any) -> None:
        text = _normalize_text(
            value, limit=2000 if identifier_type == "profile_url" else 500
        )
        if not text:
            return
        key = (identifier_type, text.casefold())
        if key in seen:
            return
        seen.add(key)
        canonical.append({"type": identifier_type, "value": text})

    for target in list(plan.get("search_targets") or [])[:100]:
        if not isinstance(target, dict) or target.get("source_type") not in {
            "username",
            "social_handle",
            "profile_url",
        }:
            continue
        try:
            username = normalize_username(target.get("value"))
        except InvestigationInputError:
            continue
        add("username", username)

    profile_urls = []
    seen_profile_urls = set()
    profile_map = plan.get("profile_url_usernames")
    profile_map = profile_map if isinstance(profile_map, dict) else {}
    for identifier in list(plan.get("identifiers") or [])[:MAX_IDENTIFIERS]:
        if not isinstance(identifier, dict):
            continue
        identifier_type = str(identifier.get("type") or "").strip().casefold()
        value = identifier.get("value")
        if identifier_type in {"username", "social_handle"}:
            try:
                add("username", normalize_username(value))
            except InvestigationInputError:
                continue
        elif identifier_type == "profile_url":
            try:
                url = normalize_profile_url(value)
            except (InvestigationInputError, ValueError):
                continue
            if url in seen_profile_urls:
                continue
            seen_profile_urls.add(url)
            linked = []
            for username in list(profile_map.get(url) or []):
                try:
                    normalized = normalize_username(username)
                except InvestigationInputError:
                    continue
                if normalized.casefold() not in {item.casefold() for item in linked}:
                    linked.append(normalized)
                    add("username", normalized)
            if not linked:
                raw_linked = [
                    target.get("value")
                    for target in list(plan.get("search_targets") or [])[:100]
                    if isinstance(target, dict)
                    and target.get("source_type") == "profile_url"
                    and target.get("source_value") == url
                ] or extract_profile_usernames(url)
                for username in raw_linked:
                    try:
                        normalized = normalize_username(username)
                    except InvestigationInputError:
                        continue
                    if normalized.casefold() not in {
                        item.casefold() for item in linked
                    }:
                        linked.append(normalized)
                        add("username", normalized)
            profile_urls.append({"url": url, "usernames": linked})
        elif identifier_type in {"full_name", "email", "phone"}:
            add(identifier_type, value)
    return {"identifiers": canonical, "profile_urls": profile_urls}


def public_ai_context(plan: Any) -> Dict[str, Any]:
    """Return bounded operator context only after explicit external-use consent."""
    if not isinstance(plan, dict) or not plan.get("allow_ai_context"):
        return {}
    scope = public_identifier_scope(plan)
    context = {
        "subject_label": _normalize_text(plan.get("subject_label", "")),
        "identifiers": scope["identifiers"],
        "include_terms": [
            _normalize_text(term, limit=120)
            for term in list(plan.get("include_terms") or [])[:MAX_TERMS]
        ],
        "exclude_terms": [
            _normalize_text(term, limit=120)
            for term in list(plan.get("exclude_terms") or [])[:MAX_TERMS]
        ],
    }
    if scope["profile_urls"]:
        context["supplied_profile_urls"] = scope["profile_urls"]
    return context


def grouped_subject(plan: Any) -> bool:
    return isinstance(plan, dict) and plan.get("processing_mode") == "same_subject"
