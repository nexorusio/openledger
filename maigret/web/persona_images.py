"""Deterministic profile-image selection shared by Persona projections."""

from __future__ import annotations

from collections.abc import Iterable, Mapping


def ordered_profile_image_urls(candidates: Iterable[Mapping[str, object]]) -> list[str]:
    """Return usable profile-image URLs with an operator choice first.

    A choice is kept in the immutable decision record. If no choice exists,
    URL order is a stable fallback, so HTML and PDF projections agree.
    """
    images = []
    for candidate in candidates:
        url = str(candidate.get("url") or "").strip()
        if not url:
            continue
        images.append(
            {
                "url": url,
                "selected": bool(candidate.get("selected")),
                "selected_at": str(candidate.get("selected_at") or ""),
            }
        )
    selected = [image for image in images if image["selected"]]
    if selected:
        selected.sort(
            key=lambda image: (image["selected_at"], image["url"].casefold()),
            reverse=True,
        )
        remaining = [image for image in images if not image["selected"]]
        remaining.sort(key=lambda image: image["url"].casefold())
        return [image["url"] for image in selected + remaining]
    return [
        image["url"]
        for image in sorted(images, key=lambda image: image["url"].casefold())
    ]
