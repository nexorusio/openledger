"""Bounded, explainable username-alias planning for investigations."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Sequence

MAX_ALIAS_CANDIDATES = 24
MAX_SELECTED_ALIASES = 16
MAX_NICKNAMES = 8
MAX_CONTEXT_NUMBERS = 6

_SEPARATORS = ("", ".", "_", "-")
_CONTEXT_NUMBER_RE = re.compile(r"^[0-9]{1,6}$")


def _ascii_tokens(value: Any) -> List[str]:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").casefold()
    return [token for token in re.findall(r"[a-z0-9]+", text) if token][:6]


def normalize_context_numbers(values: Iterable[Any]) -> List[str]:
    """Accept only explicit contextual numbers; never manufacture a range."""
    normalized: List[str] = []
    for raw_value in values:
        for item in re.split(r"[,\s]+", str(raw_value or "").strip()):
            if not item:
                continue
            if not _CONTEXT_NUMBER_RE.fullmatch(item):
                raise ValueError("Contextual numbers must contain 1 to 6 digits each.")
            if item not in normalized:
                normalized.append(item)
            if len(normalized) >= MAX_CONTEXT_NUMBERS:
                return normalized
    return normalized


def normalize_nicknames(values: Iterable[Any]) -> List[str]:
    nicknames: List[str] = []
    for raw_value in values:
        for item in re.split(r"[,\n\r]+", str(raw_value or "")):
            tokens = _ascii_tokens(item)
            if len(tokens) != 1:
                if item.strip():
                    raise ValueError("Enter one nickname per comma-separated value.")
                continue
            nickname = tokens[0]
            if nickname not in nicknames:
                nicknames.append(nickname)
            if len(nicknames) >= MAX_NICKNAMES:
                return nicknames
    return nicknames


def _learned_separators(usernames: Iterable[str]) -> List[str]:
    learned: List[str] = []
    for username in usernames:
        for separator in (".", "_", "-"):
            if separator in str(username) and separator not in learned:
                learned.append(separator)
    return learned


def rank_username_aliases(
    full_names: Sequence[str],
    *,
    nicknames: Sequence[str] = (),
    contextual_numbers: Sequence[str] = (),
    confirmed_usernames: Sequence[str] = (),
) -> List[Dict[str, Any]]:
    """Return a deterministic ranked alias plan without Cartesian expansion."""
    candidates: Dict[str, Dict[str, Any]] = {}
    learned_separators = _learned_separators(confirmed_usernames)
    normalized_nicknames = [
        tokens[0]
        for nickname in nicknames
        if len(tokens := _ascii_tokens(nickname)) == 1
    ][:MAX_NICKNAMES]

    def add(value: str, score: int, reason: str) -> None:
        value = value.strip("._-")
        if not value or len(value) > 128:
            return
        existing = candidates.get(value.casefold())
        entry = {
            "value": value,
            "score": max(0, min(100, int(score))),
            "reason": reason[:240],
        }
        if existing is None or entry["score"] > existing["score"]:
            candidates[value.casefold()] = entry

    for full_name in full_names:
        tokens = _ascii_tokens(full_name)
        if not tokens:
            continue
        if len(tokens) == 1:
            add(tokens[0], 100, "Transliterated mononym")
            for number in contextual_numbers:
                add(
                    tokens[0] + number,
                    86,
                    f"Mononym with analyst-approved contextual number {number}",
                )
            continue

        first, last = tokens[0], tokens[-1]
        whole = tokens
        pairs = [
            (whole, 100, "Full name in natural order"),
            ([first, last], 96, "First and last name"),
            ([last, first], 82, "Reversed last and first name"),
        ]
        for parts, base_score, reason in pairs:
            for separator_index, separator in enumerate(_SEPARATORS):
                learned_bonus = (
                    3 if separator and separator in learned_separators else 0
                )
                suffix = (
                    "; separator learned from a confirmed profile"
                    if learned_bonus
                    else ""
                )
                add(
                    separator.join(parts),
                    base_score - separator_index + learned_bonus,
                    reason + suffix,
                )

        add(first[0] + last, 90, "First initial plus last name")
        add(first + last[0], 88, "First name plus last initial")
        add("".join(token[0] for token in tokens), 78, "Name initials")
        add(first, 64, "First-name mononym; collision risk")
        add(last, 60, "Last-name mononym; collision risk")

        for nickname in normalized_nicknames:
            add(nickname + last, 94, "Analyst-supplied nickname plus last name")
            add(nickname + "." + last, 92, "Nickname and last name with separator")
            add(nickname, 70, "Analyst-supplied nickname; collision risk")

        number_bases = (first + last, first[0] + last)
        for number in contextual_numbers:
            for base in number_bases:
                add(
                    base + number,
                    84,
                    f"Name pattern with analyst-approved contextual number {number}",
                )

    ranked = sorted(
        candidates.values(),
        key=lambda item: (-item["score"], item["value"].casefold()),
    )[:MAX_ALIAS_CANDIDATES]
    for index, candidate in enumerate(ranked):
        candidate["selected"] = (
            index < MAX_SELECTED_ALIASES and candidate["score"] >= 78
        )
    return ranked
