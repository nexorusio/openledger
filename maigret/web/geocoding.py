"""Configured-geocoder boundary for reviewed public affiliation addresses.

Provider matching remains separate from the pure location-resolution policy.
No raw person claim is ever sent to a public geocoder through this module.
"""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import math
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from maigret.web.location_resolution import (
    canonicalize_candidates,
    public_affiliation_evidence_error,
)

DEFAULT_GEOCODER_URL = "https://nominatim.openstreetmap.org/search"
MAX_RESPONSE_BYTES = 256_000
MAX_CANDIDATES = 5
NOMINATIM_CACHE_SECONDS = 24 * 60 * 60
MAX_NOMINATIM_WAIT_SECONDS = 2


class GeocodingError(RuntimeError):
    """Raised when a configured geocoder cannot safely provide candidates."""


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise GeocodingError("The geocoder returned invalid coordinates") from error
    if not math.isfinite(parsed):
        raise GeocodingError("The geocoder returned invalid coordinates")
    return parsed


def _provider_point(result: Mapping[str, Any]) -> tuple[float, float]:
    """Read the provider point without deriving a bounding-box midpoint."""
    latitude = _number(result.get("lat", result.get("latitude")))
    longitude = _number(result.get("lon", result.get("longitude")))
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise GeocodingError("The geocoder returned out-of-range coordinates")
    return latitude, longitude


def _safe_endpoint(endpoint: str) -> Any:
    parsed_endpoint = urlsplit(str(endpoint or ""))
    if (
        parsed_endpoint.scheme != "https"
        or not parsed_endpoint.netloc
        or parsed_endpoint.username
        or parsed_endpoint.password
    ):
        raise GeocodingError("The configured geocoder must use an HTTPS URL")
    return parsed_endpoint


def _is_public_nominatim(endpoint: str) -> bool:
    parsed = urlsplit(endpoint)
    return parsed.hostname in {
        "nominatim.openstreetmap.org",
        "www.nominatim.openstreetmap.org",
    }


def _is_google_endpoint(endpoint: str) -> bool:
    hostname = (urlsplit(endpoint).hostname or "").casefold()
    return (
        hostname == "google.com"
        or hostname.endswith(".google.com")
        or hostname.endswith(".googleapis.com")
    )


def _cache_paths(cache_dir: os.PathLike[str] | str) -> tuple[Path, Path]:
    root = Path(cache_dir).expanduser()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(root, 0o700)
    except OSError:
        pass
    return root / "nominatim-shared.lock", root / "nominatim-shared-state.json"


@contextmanager
def _nominatim_lock(cache_dir: os.PathLike[str] | str) -> Iterator[tuple[Path, Path]]:
    lock_path, state_path = _cache_paths(cache_dir)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield lock_path, state_path
    except OSError as error:
        raise GeocodingError(
            "The shared Nominatim cache could not be locked"
        ) from error
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _read_cache(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {"entries": {}, "next_request_at": 0.0}
    if not isinstance(value, dict):
        return {"entries": {}, "next_request_at": 0.0}
    entries = value.get("entries")
    return {
        "entries": entries if isinstance(entries, dict) else {},
        "next_request_at": value.get("next_request_at", 0.0),
    }


def _write_cache(path: Path, value: Mapping[str, Any]) -> None:
    temporary_name = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".nominatim-", suffix=".tmp", dir=path.parent
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
            json.dump(value, temporary, separators=(",", ":"), ensure_ascii=True)
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, path)
    except OSError as error:
        raise GeocodingError(
            "The shared Nominatim cache could not be updated"
        ) from error
    finally:
        if temporary_name:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _cache_key(endpoint: str, address: str, country_code: str) -> str:
    return hashlib.sha256(
        json.dumps(
            [endpoint, address, country_code], separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


def _request_url(endpoint: str, address: str, country_code: str) -> str:
    parsed_endpoint = _safe_endpoint(endpoint)
    separator = "&" if parsed_endpoint.query else "?"
    parameters = {
        "q": address,
        "format": "jsonv2",
        "limit": MAX_CANDIDATES,
        "addressdetails": 1,
    }
    if country_code:
        parameters["countrycodes"] = country_code.casefold()
    return str(endpoint) + separator + urlencode(parameters)


def _request_payload(
    request_url: str,
    *,
    timeout_seconds: int,
    opener: Callable[..., Any],
) -> List[Mapping[str, Any]]:
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "OpenLedger/1.0 reviewed-affiliation-address-geocoder",
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
    if not isinstance(results, list):
        raise GeocodingError("The geocoder returned an invalid result")
    return [item for item in results[:MAX_CANDIDATES] if isinstance(item, Mapping)]


def _nominatim_precision(result: Mapping[str, Any]) -> str:
    category = str(result.get("category") or result.get("class") or "").casefold()
    kind = str(result.get("type") or "").casefold()
    if kind in {"house", "house_number", "building", "office", "commercial"}:
        return "address"
    if category == "building":
        return "building"
    if kind in {"university", "college", "school", "hospital"}:
        return "campus"
    if kind in {"street", "road", "residential"}:
        return "street"
    if category in {"boundary", "place"} or kind in {
        "city",
        "town",
        "village",
        "administrative",
    }:
        return "area"
    return "unknown"


def _nominatim_source_url(result: Mapping[str, Any]) -> Optional[str]:
    osm_type = str(result.get("osm_type") or "").casefold()
    osm_id = str(result.get("osm_id") or "")
    if osm_type in {"node", "way", "relation"} and osm_id.isdigit():
        return f"https://www.openstreetmap.org/{osm_type}/{osm_id}"
    return None


def configured_geocoder_candidates(
    results: Sequence[Mapping[str, Any]],
    *,
    evidence: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Adapt bounded Nominatim-compatible results to the neutral contract."""
    address_type = str(evidence.get("address_type") or "unknown")
    candidates: List[Dict[str, Any]] = []
    for result in list(results or [])[:MAX_CANDIDATES]:
        try:
            latitude, longitude = _provider_point(result)
        except GeocodingError:
            continue
        provider_id = str(result.get("place_id") or result.get("osm_id") or "").strip()
        if not provider_id:
            provider_id = hashlib.sha256(
                json.dumps(result, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()[:24]
        address = str(
            result.get("display_name") or evidence.get("address") or ""
        ).strip()[:1500]
        candidate_address = result.get("address")
        country = ""
        if isinstance(candidate_address, Mapping):
            country = str(candidate_address.get("country_code") or "").upper()
        candidates.append(
            {
                "candidate_id": f"configured-geocoder:{provider_id}",
                "display_name": address[:500],
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
                # Provider categories do not establish HQ/branch/operating status;
                # preserve the type asserted by the cited official evidence.
                "address_type": address_type,
                "precision": _nominatim_precision(result),
                "method": "configured_geocoder_candidate",
                "provider_id": provider_id,
                "source_url": _nominatim_source_url(result),
                "source_date": None,
                "geometry_dataset_version": None,
                "validation_dataset_version": None,
                "lookup_id": None,
                "retrieved_at": None,
                "country_code": country or None,
            }
        )
    return canonicalize_candidates(candidates)


def geocode_public_affiliation_address_candidates(
    evidence: Mapping[str, Any],
    *,
    endpoint: str = DEFAULT_GEOCODER_URL,
    timeout_seconds: int = 10,
    opener: Callable[..., Any] = urlopen,
    cache_dir: os.PathLike[str] | str | None = None,
) -> List[Dict[str, Any]]:
    """Look up only cited public organization addresses at a configured boundary.

    Public Nominatim requires a deployment-shared directory (for example a
    mounted reports/cache volume).  The file lock serializes all WSGI processes
    that share it, caches results, and reserves at most one live request per
    second.  It intentionally fails closed when a shared directory is absent.
    """
    error = public_affiliation_evidence_error(evidence)
    if error:
        raise GeocodingError(error)
    _safe_endpoint(endpoint)
    if _is_google_endpoint(str(endpoint)):
        raise GeocodingError(
            "Google-derived coordinates are transient and cannot populate "
            "affiliation sites"
        )
    address = " ".join(str(evidence.get("address") or "").split())[:1500]
    country_code = str(evidence.get("country_code") or "").strip().upper()[:16]
    request_url = _request_url(str(endpoint), address, country_code)

    if not _is_public_nominatim(str(endpoint)):
        return configured_geocoder_candidates(
            _request_payload(
                request_url, timeout_seconds=timeout_seconds, opener=opener
            ),
            evidence=evidence,
        )
    if cache_dir is None:
        raise GeocodingError(
            "Public Nominatim requires a configured shared cache directory "
            "for rate limiting"
        )

    cache_id = _cache_key(str(endpoint), address, country_code)
    with _nominatim_lock(cache_dir) as (_lock_path, state_path):
        state = _read_cache(state_path)
        now = time.time()
        cached = state["entries"].get(cache_id)
        if isinstance(cached, Mapping):
            try:
                cache_valid = float(cached.get("expires_at", 0)) > now
            except (TypeError, ValueError):
                cache_valid = False
            if cache_valid:
                cached_results = cached.get("results")
                if isinstance(cached_results, list):
                    return copy.deepcopy(cached_results)
        try:
            next_request_at = float(state.get("next_request_at", 0))
        except (TypeError, ValueError):
            next_request_at = 0
        wait_seconds = next_request_at - now
        if wait_seconds > MAX_NOMINATIM_WAIT_SECONDS:
            raise GeocodingError(
                "The shared Nominatim cache has an invalid future retry time"
            )
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        # Reserve and persist the next slot before the request.  A provider
        # error therefore cannot cause another WSGI process to retry instantly.
        state["next_request_at"] = time.time() + 1.0
        _write_cache(state_path, state)
        # Holding the lock through the request prevents independent WSGI
        # processes sharing the mounted path from issuing concurrent calls.
        raw_results = _request_payload(
            request_url, timeout_seconds=timeout_seconds, opener=opener
        )
        candidates = configured_geocoder_candidates(raw_results, evidence=evidence)
        entries = state["entries"]
        entries[cache_id] = {
            "expires_at": time.time() + NOMINATIM_CACHE_SECONDS,
            "results": candidates,
        }
        # Bound public-address retention and avoid an unbounded cache file.
        if len(entries) > 256:

            def expiry(key: str) -> float:
                value = entries.get(key)
                try:
                    return (
                        float(value.get("expires_at", 0))
                        if isinstance(value, Mapping)
                        else 0.0
                    )
                except (TypeError, ValueError):
                    return 0.0

            oldest = sorted(entries, key=expiry)[: len(entries) - 256]
            for key in oldest:
                entries.pop(key, None)
        _write_cache(state_path, state)
        return copy.deepcopy(candidates)


def geocode_place_center(
    place: str,
    *,
    endpoint: str = DEFAULT_GEOCODER_URL,
    timeout_seconds: int = 10,
    opener: Callable[..., Any] = urlopen,
    evidence: Optional[Mapping[str, Any]] = None,
    cache_dir: os.PathLike[str] | str | None = None,
) -> Optional[Dict[str, Any]]:
    """Legacy compatibility entry point, deliberately fail-closed.

    Historical callers supply raw person-claim strings.  Those are not safe to
    disclose to a public geocoder and this function therefore makes no request
    without validated public affiliation evidence.  Even with such evidence it
    returns no automatic coordinate: the caller must retain the candidate list
    and record an analyst site selection through the R9 resolver.
    """
    del place, endpoint, timeout_seconds, opener, cache_dir
    if evidence is not None:
        # Validate the attempted use so accidental bypasses are visible to the
        # caller, but never turn a legacy auto-approval into a site selection.
        error = public_affiliation_evidence_error(evidence)
        if error:
            raise GeocodingError(error)
    return None
