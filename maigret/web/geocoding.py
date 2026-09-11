"""Bounded server-side place geocoding for analyst-approved map records."""

from __future__ import annotations

import json
import math
from typing import Any, Callable, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


DEFAULT_GEOCODER_URL = "https://nominatim.openstreetmap.org/search"
MAX_RESPONSE_BYTES = 256_000


class GeocodingError(RuntimeError):
    """Raised when a configured geocoder cannot provide a safe place center."""


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise GeocodingError("The geocoder returned invalid coordinates") from error
    if not math.isfinite(parsed):
        raise GeocodingError("The geocoder returned invalid coordinates")
    return parsed


def _place_center(result: Dict[str, Any]) -> tuple[float, float]:
    """Prefer the returned bounding-box centroid over a provider label point."""
    bounding_box = result.get("boundingbox")
    if isinstance(bounding_box, list) and len(bounding_box) == 4:
        south, north, west, east = (_number(value) for value in bounding_box)
        if south <= north and west <= east:
            latitude = (south + north) / 2
            longitude = (west + east) / 2
        else:
            raise GeocodingError("The geocoder returned an invalid bounding box")
    else:
        latitude = _number(result.get("lat"))
        longitude = _number(result.get("lon"))
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise GeocodingError("The geocoder returned out-of-range coordinates")
    return latitude, longitude


def geocode_place_center(
    place: str,
    *,
    endpoint: str = DEFAULT_GEOCODER_URL,
    timeout_seconds: int = 10,
    opener: Callable[..., Any] = urlopen,
) -> Optional[Dict[str, Any]]:
    """Resolve one approved place name to a coarse bounding-box centroid.

    Only HTTPS endpoints are accepted. The caller decides whether a particular
    claim is suitable to disclose to the configured service and persists the
    returned coordinates so repeat page loads never re-query the provider.
    """
    query = " ".join(str(place or "").split())[:500]
    if len(query) < 2:
        return None
    parsed_endpoint = urlsplit(str(endpoint or ""))
    if (
        parsed_endpoint.scheme != "https"
        or not parsed_endpoint.netloc
        or parsed_endpoint.username
        or parsed_endpoint.password
    ):
        raise GeocodingError("The configured geocoder must use an HTTPS URL")
    separator = "&" if parsed_endpoint.query else "?"
    request_url = str(endpoint) + separator + urlencode(
        {
            "q": query,
            "format": "jsonv2",
            "limit": 1,
            "addressdetails": 0,
        }
    )
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "OpenLedger/1.0 approved-place-geocoder",
        },
    )
    try:
        with opener(request, timeout=max(1, min(int(timeout_seconds), 30))) as response:
            status = getattr(response, "status", 200)
            if status != 200:
                raise GeocodingError(f"The geocoder returned HTTP {status}")
            payload = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as error:
        raise GeocodingError(f"The geocoder returned HTTP {error.code}") from error
    except (URLError, TimeoutError, OSError) as error:
        raise GeocodingError("The geocoder could not be reached") from error
    if len(payload) > MAX_RESPONSE_BYTES:
        raise GeocodingError("The geocoder response was too large")
    try:
        results = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GeocodingError("The geocoder returned invalid JSON") from error
    if not isinstance(results, list) or not results:
        return None
    if not isinstance(results[0], dict):
        raise GeocodingError("The geocoder returned an invalid result")
    latitude, longitude = _place_center(results[0])
    return {
        "latitude": latitude,
        "longitude": longitude,
        "display_name": str(results[0].get("display_name") or query)[:500],
        "precision": "place",
    }
