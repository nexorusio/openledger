"""Pure policy for resolving public affiliation sites.

This module deliberately does not perform network I/O, persistence, map rendering,
or provider-specific matching.  It turns already-retained public affiliation
address evidence and provider-neutral candidates into a reviewable selection.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

ADDRESS_TYPES = frozenset(
    {
        "hq",
        "registered",
        "operating",
        "branch",
        "campus",
        "mailing",
        "area",
        "unknown",
    }
)
RESOLUTION_STATUSES = frozenset({"resolved", "ambiguous", "unmapped", "needs_review"})
PRECISE_SITE_PRECISIONS = frozenset(
    {"rooftop", "entrance", "address", "building", "parcel", "campus"}
)
_OFFICIAL_SOURCE_ENGINES = frozenset(
    {
        "official_website_public_content",
        "official_registry",
        "institutional_publication",
    }
)

LandValidator = Callable[[float, float, str, int], bool]


def _text(value: Any, *, limit: int = 1500) -> str:
    return " ".join(str(value or "").split())[:limit]


def _normalise_choice(value: Any, *, allowed: frozenset[str], default: str) -> str:
    choice = _text(value, limit=80).casefold().replace("-", "_").replace(" ", "_")
    return choice if choice in allowed else default


def _normalise_match_text(value: Any) -> str:
    return " ".join(_text(value).casefold().replace(",", " ").replace(".", " ").split())


_ADDRESS_TOKEN_ALIASES = {
    # Keep common road abbreviations equivalent without making an arbitrary
    # nearby house number look like the cited address.
    "ave": "avenue",
    "av": "avenue",
    "blvd": "boulevard",
    "dr": "drive",
    "hwy": "highway",
    "jl": "jalan",
    "ln": "lane",
    "rd": "road",
    "st": "street",
}


def _address_tokens(value: Any) -> List[str]:
    return [
        _ADDRESS_TOKEN_ALIASES.get(token, token)
        for token in re.findall(r"[a-z0-9]+", _normalise_match_text(value))
    ]


def _address_match_tokens(value: Any) -> List[str]:
    """Return address components that must survive provider normalization.

    Country codes can be expanded by a provider (``ID`` -> ``Indonesia``) and
    are separately checked when the provider returns a country code.  Numeric
    components, including single-digit and alphanumeric house numbers, must
    never be discarded just because they are short.
    """
    return [
        token
        for token in _address_tokens(value)
        if any(character.isdigit() for character in token) or len(token) > 2
    ]


def _candidate_address_matches_source(
    candidate_address: Any, source_address: Any
) -> bool:
    """Require more than a shared city/country before accepting a selection."""
    candidate_normalized = _normalise_match_text(candidate_address)
    source_normalized = _normalise_match_text(source_address)
    source_tokens = _address_tokens(source_address)
    required_tokens = _address_match_tokens(source_address)
    candidate_tokens = set(_address_tokens(candidate_address))
    # A city or city/country label is not a site-address match. A specific
    # source address has a number, or at least four meaningful components.
    if len(source_tokens) < 3 or (
        not any(any(character.isdigit() for character in token) for token in source_tokens)
        and len(required_tokens) < 4
    ):
        return False
    if source_normalized == candidate_normalized:
        return True
    return bool(required_tokens) and all(
        token in candidate_tokens for token in required_tokens
    )


def _number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_int(value: Any) -> Optional[int]:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _coordinates(value: Mapping[str, Any]) -> Optional[Tuple[float, float]]:
    latitude = _number(value.get("latitude", value.get("lat")))
    longitude = _number(value.get("longitude", value.get("lon")))
    if latitude is None or longitude is None:
        return None
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        return None
    return latitude, longitude


def _https_url(value: Any) -> Optional[str]:
    url = _text(value, limit=2000)
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
    ):
        return None
    return url


def canonicalize_candidate(
    candidate: Mapping[str, Any], *, fallback_id: str = ""
) -> Dict[str, Any]:
    """Return one JSON-safe provider-neutral candidate without selecting it."""
    if not isinstance(candidate, Mapping):
        raise ValueError("A location candidate must be an object")
    candidate_id = _text(candidate.get("candidate_id") or fallback_id, limit=300)
    if not candidate_id:
        raise ValueError("A location candidate needs a stable candidate_id")
    address = _text(candidate.get("address") or candidate.get("display_name"))
    display_name = _text(candidate.get("display_name") or address, limit=500)
    coordinates = _coordinates(candidate)
    precision = _normalise_choice(
        candidate.get("precision"),
        allowed=frozenset(
            {
                "rooftop",
                "entrance",
                "address",
                "building",
                "parcel",
                "campus",
                "street",
                "place",
                "area",
                "unknown",
            }
        ),
        default="unknown",
    )
    return {
        "candidate_id": candidate_id,
        "display_name": display_name,
        "address": address,
        "latitude": coordinates[0] if coordinates else None,
        "longitude": coordinates[1] if coordinates else None,
        "address_type": _normalise_choice(
            candidate.get("address_type"), allowed=ADDRESS_TYPES, default="unknown"
        ),
        "precision": precision,
        "method": _text(candidate.get("method") or "provider_candidate", limit=120),
        "provider_id": _text(candidate.get("provider_id"), limit=300) or None,
        "source_url": _https_url(candidate.get("source_url")),
        "source_date": _text(candidate.get("source_date"), limit=80) or None,
        "geometry_dataset_version": _text(
            candidate.get("geometry_dataset_version"), limit=160
        )
        or None,
        "validation_dataset_version": _text(
            candidate.get("validation_dataset_version"), limit=160
        )
        or None,
        "validation_resolution_meters": _positive_int(
            candidate.get("validation_resolution_meters")
        ),
        "lookup_id": _text(candidate.get("lookup_id"), limit=300) or None,
        "retrieved_at": _text(candidate.get("retrieved_at"), limit=80) or None,
        "match_basis": {
            "source_address": _text(
                (candidate.get("match_basis") or {}).get("source_address")
                if isinstance(candidate.get("match_basis"), Mapping)
                else ""
            ),
            "organization_name": _text(
                (candidate.get("match_basis") or {}).get("organization_name")
                if isinstance(candidate.get("match_basis"), Mapping)
                else ""
            ),
            "site_basis": _text(
                (
                    (candidate.get("match_basis") or {}).get("site_basis")
                    if isinstance(candidate.get("match_basis"), Mapping)
                    else ""
                ),
                limit=500,
            ),
        },
        # These two optional matching fields are not map claims. They are used
        # solely by the pure policy to prevent a selection crossing countries.
        "country_code": _text(candidate.get("country_code"), limit=16).upper() or None,
    }


def canonicalize_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Canonicalize bounded candidates and remove duplicate candidate IDs."""
    canonical: List[Dict[str, Any]] = []
    seen = set()
    for index, candidate in enumerate(list(candidates or [])[:20]):
        try:
            normalized = canonicalize_candidate(
                candidate, fallback_id=f"candidate:{index + 1}"
            )
        except (TypeError, ValueError):
            continue
        if normalized["candidate_id"] in seen:
            continue
        seen.add(normalized["candidate_id"])
        canonical.append(normalized)
    return canonical


def public_affiliation_evidence_error(evidence: Mapping[str, Any]) -> Optional[str]:
    """Return why evidence cannot be disclosed/resolved, or ``None`` if eligible."""
    if not isinstance(evidence, Mapping):
        return "Address evidence is missing."
    if not _text(evidence.get("address")):
        return "The public affiliation address is missing."
    if not _https_url(evidence.get("source_url")):
        return "A cited public HTTPS source is required."
    if (evidence.get('is_private') is True or evidence.get('is_sensitive') is True
            or evidence.get('is_confidential') is True
            or _text(evidence.get('classification')).casefold() in {'private', 'sensitive', 'confidential'}):
        return 'Private, confidential or sensitive addresses cannot be disclosed.'
    source_engine = _text(evidence.get("source_engine"), limit=120)
    is_official = (
        evidence.get("is_official") is True or source_engine in _OFFICIAL_SOURCE_ENGINES
    )
    if not is_official:
        return (
            "Only official organization or institutional address evidence is eligible."
        )
    if evidence.get("is_public") is not True:
        return "Explicit public-address evidence is required."
    if (
        evidence.get("is_residential") is True
        or _text(evidence.get("address_type"), limit=80).casefold() == "residential"
    ):
        return "Residential addresses are never affiliation-site candidates."
    if (
        evidence.get("is_person_location") is True
        or evidence.get("person_current_location") is True
    ):
        return "Affiliation evidence cannot be used as a person's current location."
    if not _text(evidence.get("observation_key"), limit=300):
        return "A stable source observation key is required."
    return None


def _response(
    status: str,
    *,
    reason: str,
    candidates: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Any],
    candidate: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    if status not in RESOLUTION_STATUSES:
        raise ValueError("Unknown location resolution status")
    result: Dict[str, Any] = {
        "status": status,
        "reason": reason,
        "candidate": dict(candidate) if candidate else None,
        "candidates": [dict(item) for item in candidates],
        "observation_key": _text(evidence.get("observation_key"), limit=300) or None,
        "source_url": _https_url(evidence.get("source_url")),
        "source_date": _text(evidence.get("source_date"), limit=80) or None,
        "address_type": _normalise_choice(
            evidence.get("address_type"), allowed=ADDRESS_TYPES, default="unknown"
        ),
        "is_current_affiliation": evidence.get("is_current_affiliation") is not False,
        # This invariant is intentionally explicit for every outcome so callers
        # cannot repurpose a site coordinate as a personal-location claim.
        "is_person_location": False,
    }
    return result


def _country_matches(evidence: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    expected = _text(evidence.get("country_code"), limit=16).upper()
    actual = _text(candidate.get("country_code"), limit=16).upper()
    return not expected or not actual or expected == actual


def _requires_precise_site(evidence: Mapping[str, Any]) -> bool:
    return (
        _normalise_choice(
            evidence.get("address_type"), allowed=ADDRESS_TYPES, default="unknown"
        )
        != "area"
    )


def _selection_match_error(
    evidence: Mapping[str, Any], candidate: Mapping[str, Any]
) -> Optional[str]:
    """Require an analyst's selected candidate to remain tied to source evidence."""
    if not _candidate_address_matches_source(
        candidate.get("address"), evidence.get("address")
    ):
        return (
            "The selected provider address does not match the cited specific "
            "source address."
        )
    basis = candidate.get("match_basis")
    if not isinstance(basis, Mapping):
        return (
            "The selected candidate has no recorded address and organization/site "
            "match basis."
        )
    if _normalise_match_text(basis.get("source_address")) != _normalise_match_text(
        evidence.get("address")
    ):
        return "The selected candidate does not match the exact cited source address."
    organization_name = _text(evidence.get("organization_name"), limit=500)
    if not organization_name:
        return "A selected candidate requires the source organization identity."
    if _normalise_match_text(basis.get("organization_name")) != _normalise_match_text(
        organization_name
    ):
        return "The selected candidate does not match the cited organization identity."
    if not _text(basis.get("site_basis"), limit=500):
        return "The selected candidate has no recorded site match basis."
    return None


def _validated_area_coordinate(
    geometry: Mapping[str, Any],
    *,
    latitude: Any,
    longitude: Any,
    land_validator: Optional[LandValidator],
    validation_dataset_version: Optional[str],
    validation_resolution_meters: Optional[int],
) -> Optional[Dict[str, Any]]:
    """Validate an explicit area point; never use this for precise sites."""
    coordinates = _coordinates({"latitude": latitude, "longitude": longitude})
    dataset_version = _text(validation_dataset_version, limit=160)
    resolution = _positive_int(validation_resolution_meters)
    if (
        not coordinates
        or not callable(land_validator)
        or not dataset_version
        or not resolution
    ):
        return None
    point_latitude, point_longitude = coordinates
    if not any(
        _polygon_contains(point_longitude, point_latitude, polygon)
        for polygon in _geometry_polygons(geometry)
    ):
        return None
    try:
        is_land = land_validator(
            point_latitude, point_longitude, dataset_version, resolution
        )
    except Exception:
        return None
    if not is_land:
        return None
    return {
        "latitude": point_latitude,
        "longitude": point_longitude,
        "validation_dataset_version": dataset_version,
        "validation_resolution_meters": resolution,
    }


def _unwrap_longitude(longitude: float, reference: float) -> float:
    while longitude - reference > 180:
        longitude -= 360
    while longitude - reference < -180:
        longitude += 360
    return longitude


def _as_ring(value: Any) -> List[Tuple[float, float]]:
    ring: List[Tuple[float, float]] = []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ring
    for point in value:
        if (
            not isinstance(point, Sequence)
            or isinstance(point, (str, bytes))
            or len(point) < 2
        ):
            return []
        longitude, latitude = _number(point[0]), _number(point[1])
        if (
            longitude is None
            or latitude is None
            or not -180 <= longitude <= 180
            or not -90 <= latitude <= 90
        ):
            return []
        ring.append((longitude, latitude))
    if len(ring) < 4:
        return []
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def _unwrap_ring(ring: Sequence[Tuple[float, float]]) -> List[Tuple[float, float]]:
    if not ring:
        return []
    result = [ring[0]]
    reference = ring[0][0]
    for longitude, latitude in ring[1:]:
        unwrapped = _unwrap_longitude(longitude, reference)
        result.append((unwrapped, latitude))
        reference = unwrapped
    return result


def _point_on_segment(
    x: float, y: float, start: Tuple[float, float], end: Tuple[float, float]
) -> bool:
    x1, y1 = start
    x2, y2 = end
    cross = (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1)
    if abs(cross) > 1e-10:
        return False
    return (
        min(x1, x2) - 1e-10 <= x <= max(x1, x2) + 1e-10
        and min(y1, y2) - 1e-10 <= y <= max(y1, y2) + 1e-10
    )


def _point_in_ring(x: float, y: float, ring: Sequence[Tuple[float, float]]) -> bool:
    inside = False
    for start, end in zip(ring, ring[1:]):
        if _point_on_segment(x, y, start, end):
            return True
        x1, y1 = start
        x2, y2 = end
        if (y1 > y) != (y2 > y):
            crossing = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x < crossing:
                inside = not inside
    return inside


def _polygon_contains(
    longitude: float,
    latitude: float,
    polygon: Sequence[Sequence[Tuple[float, float]]],
) -> bool:
    if not polygon or not polygon[0]:
        return False
    outer = _unwrap_ring(polygon[0])
    x = _unwrap_longitude(longitude, outer[0][0])
    if not _point_in_ring(x, latitude, outer):
        return False
    return not any(
        _point_in_ring(x, latitude, _unwrap_ring(hole)) for hole in polygon[1:]
    )


def _geometry_polygons(
    geometry: Mapping[str, Any],
) -> List[List[List[Tuple[float, float]]]]:
    if not isinstance(geometry, Mapping):
        return []
    if geometry.get("type") == "Feature":
        geometry = geometry.get("geometry")
    if not isinstance(geometry, Mapping):
        return []
    kind = geometry.get("type")
    coordinates = geometry.get("coordinates")
    raw_polygons = (
        [coordinates]
        if kind == "Polygon"
        else coordinates if kind == "MultiPolygon" else []
    )
    polygons = []
    if not isinstance(raw_polygons, Sequence) or isinstance(raw_polygons, (str, bytes)):
        return polygons
    for raw_polygon in raw_polygons:
        if not isinstance(raw_polygon, Sequence) or isinstance(
            raw_polygon, (str, bytes)
        ):
            continue
        rings = [_as_ring(ring) for ring in raw_polygon]
        if rings and rings[0] and all(ring for ring in rings):
            polygons.append(rings)
    return polygons


def _polygon_sample_points(
    polygon: Sequence[Sequence[Tuple[float, float]]],
) -> List[Tuple[float, float]]:
    outer = _unwrap_ring(polygon[0])
    xs = [point[0] for point in outer[:-1]]
    ys = [point[1] for point in outer[:-1]]
    if not xs or not ys:
        return []
    minimum_x, maximum_x = min(xs), max(xs)
    minimum_y, maximum_y = min(ys), max(ys)
    # A bounded interior grid is conservative: failure to find a validated land
    # point is an unmapped result, never permission to use a geometric centroid.
    points = []
    for y_fraction in (0.5, 0.25, 0.75, 0.125, 0.375, 0.625, 0.875):
        for x_fraction in (0.5, 0.25, 0.75, 0.125, 0.375, 0.625, 0.875):
            longitude = minimum_x + (maximum_x - minimum_x) * x_fraction
            latitude = minimum_y + (maximum_y - minimum_y) * y_fraction
            if _polygon_contains(longitude, latitude, polygon):
                points.append((longitude, latitude))
    return points


def _wrapped_longitude(longitude: float) -> float:
    if longitude == 180:
        return 180.0
    return ((longitude + 180) % 360) - 180


def find_land_safe_area_point(
    geometry: Mapping[str, Any],
    *,
    land_validator: Optional[LandValidator],
    validation_dataset_version: Optional[str],
    validation_resolution_meters: Optional[int],
) -> Optional[Dict[str, Any]]:
    """Find an interior point accepted by a declared land dataset.

    The validator is intentionally required.  This function is only for an
    explicit area fallback and must never be called to test or move a precise
    office coordinate.
    """
    for polygon in _geometry_polygons(geometry):
        for longitude, latitude in _polygon_sample_points(polygon):
            point = _validated_area_coordinate(
                geometry,
                latitude=latitude,
                longitude=_wrapped_longitude(longitude),
                land_validator=land_validator,
                validation_dataset_version=validation_dataset_version,
                validation_resolution_meters=validation_resolution_meters,
            )
            if point:
                return point
    return None


def resolve_public_affiliation_site(
    evidence: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    selected_candidate_id: Optional[str] = None,
    area_geometry: Optional[Mapping[str, Any]] = None,
    land_validator: Optional[LandValidator] = None,
    validation_dataset_version: Optional[str] = None,
    validation_resolution_meters: Optional[int] = None,
) -> Dict[str, Any]:
    """Resolve a reviewed affiliation site without inferring a person's location."""
    error = public_affiliation_evidence_error(evidence)
    if error:
        return _response("needs_review", reason=error, candidates=[], evidence=evidence)

    normalized = canonicalize_candidates(candidates)
    expected_type = _normalise_choice(
        evidence.get("address_type"), allowed=ADDRESS_TYPES, default="unknown"
    )
    requested_id = _text(selected_candidate_id, limit=300)

    if requested_id:
        selected = next(
            (item for item in normalized if item["candidate_id"] == requested_id), None
        )
        if not selected:
            return _response(
                "needs_review",
                reason=(
                    "The selected candidate is no longer available for this "
                    "source observation."
                ),
                candidates=normalized,
                evidence=evidence,
            )
        if expected_type == "area":
            validated_area = _validated_area_coordinate(
                area_geometry or {},
                latitude=selected["latitude"],
                longitude=selected["longitude"],
                land_validator=land_validator,
                validation_dataset_version=validation_dataset_version,
                validation_resolution_meters=validation_resolution_meters,
            )
            if not validated_area:
                return _response(
                    "unmapped",
                    reason=(
                        "The selected area candidate is not a validated land point "
                        "inside the declared area geometry."
                    ),
                    candidates=normalized,
                    evidence=evidence,
                )
            selected = dict(selected)
            selected.update(validated_area)
        match_error = _selection_match_error(evidence, selected)
        if match_error:
            return _response(
                "needs_review",
                reason=match_error,
                candidates=normalized,
                evidence=evidence,
            )
        if not _country_matches(evidence, selected):
            return _response(
                "needs_review",
                reason="The selected candidate conflicts with the evidence country.",
                candidates=normalized,
                evidence=evidence,
            )
        if selected["latitude"] is None or selected["longitude"] is None:
            return _response(
                "needs_review",
                reason="The selected candidate has no valid coordinate.",
                candidates=normalized,
                evidence=evidence,
            )
        if (
            _requires_precise_site(evidence)
            and selected["precision"] not in PRECISE_SITE_PRECISIONS
        ):
            return _response(
                "needs_review",
                reason=(
                    "The selected candidate is too coarse for a specific public "
                    "site."
                ),
                candidates=normalized,
                evidence=evidence,
            )
        return _response(
            "resolved",
            reason=(
                "Selected public affiliation-site candidate. It does not establish "
                "the person's current location or branch assignment."
            ),
            candidates=normalized,
            evidence=evidence,
            candidate=selected,
        )

    viable = [
        item
        for item in normalized
        if item["latitude"] is not None
        and item["longitude"] is not None
        and _country_matches(evidence, item)
    ]

    if expected_type == "area" and area_geometry is not None and not viable:
        point = find_land_safe_area_point(
            area_geometry,
            land_validator=land_validator,
            validation_dataset_version=validation_dataset_version,
            validation_resolution_meters=validation_resolution_meters,
        )
        if point:
            identity = hashlib.sha256(
                (str(evidence.get("observation_key")) + repr(area_geometry)).encode(
                    "utf-8"
                )
            ).hexdigest()[:24]
            area_candidate = {
                "candidate_id": f"area:{identity}",
                "display_name": _text(evidence.get("address"), limit=500),
                "address": _text(evidence.get("address")),
                "latitude": point["latitude"],
                "longitude": point["longitude"],
                "address_type": "area",
                "precision": "area",
                "method": "validated_land_area_fallback",
                "provider_id": None,
                "source_url": _https_url(evidence.get("source_url")),
                "source_date": _text(evidence.get("source_date"), limit=80) or None,
                "geometry_dataset_version": _text(
                    evidence.get("geometry_dataset_version"), limit=160
                )
                or None,
                "validation_dataset_version": point["validation_dataset_version"],
                "validation_resolution_meters": point["validation_resolution_meters"],
                "country_code": _text(evidence.get("country_code"), limit=16).upper()
                or None,
            }
            return _response(
                "resolved",
                reason=(
                    "Validated land-safe area fallback for explicitly area-level "
                    "evidence."
                ),
                candidates=[*normalized, area_candidate],
                evidence=evidence,
                candidate=area_candidate,
            )
        return _response(
            "unmapped",
            reason=(
                "No land-safe area point could be validated at the declared dataset "
                "resolution."
            ),
            candidates=normalized,
            evidence=evidence,
        )

    if not viable:
        return _response(
            "unmapped",
            reason=(
                "No valid configured-geocoder candidate matched the public "
                "address evidence."
            ),
            candidates=normalized,
            evidence=evidence,
        )
    if len(viable) > 1:
        return _response(
            "ambiguous",
            reason=(
                "More than one candidate remains; an analyst must record the "
                "site match."
            ),
            candidates=normalized,
            evidence=evidence,
        )
    return _response(
        "needs_review",
        reason=(
            "A single candidate is available, but explicit analyst site selection "
            "is required."
        ),
        candidates=normalized,
        evidence=evidence,
    )
