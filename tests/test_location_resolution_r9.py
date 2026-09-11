import json
import time
from urllib.error import URLError

import pytest

from maigret.web import geocoding
from maigret.web.geocoding import (
    GeocodingError,
    geocode_place_center,
    geocode_public_affiliation_address_candidates,
)
from maigret.web.location_resolution import resolve_public_affiliation_site


class FakeResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit):
        return json.dumps(self.payload).encode("utf-8")


def evidence(**overrides):
    value = {
        "address": "100 Example Avenue, Jakarta, ID",
        "source_url": "https://example.org/contact",
        "observation_key": "official-website-location:office-1",
        "source_engine": "official_website_public_content",
        "is_public": True,
        "address_type": "operating",
        "country_code": "ID",
        "organization_name": "Example Organization",
    }
    value.update(overrides)
    return value


def candidate(candidate_id="office", **overrides):
    value = {
        "candidate_id": candidate_id,
        "display_name": "Example Office, Jakarta, Indonesia",
        "address": "100 Example Avenue, Jakarta, Indonesia",
        "latitude": -6.1841,
        "longitude": 106.831,
        "address_type": "operating",
        "precision": "address",
        "method": "fixture",
        "provider_id": candidate_id,
        "source_url": "https://provider.example/office",
        "country_code": "ID",
        "lookup_id": "location-lookup:fixture-office",
        "retrieved_at": "2026-09-10T12:00:00Z",
        "match_basis": {
            "source_address": "100 Example Avenue, Jakarta, ID",
            "organization_name": "Example Organization",
            "site_basis": "Analyst matched the cited public operating site.",
        },
    }
    value.update(overrides)
    return value


def test_r9_official_office_requires_explicit_candidate_selection():
    pending = resolve_public_affiliation_site(evidence(), [candidate()])

    assert pending["status"] == "needs_review"
    assert pending["candidate"] is None
    assert pending["is_person_location"] is False

    resolved = resolve_public_affiliation_site(
        evidence(), [candidate()], selected_candidate_id="office"
    )

    assert resolved["status"] == "resolved"
    assert resolved["candidate"]["address_type"] == "operating"
    assert resolved["candidate"]["latitude"] == pytest.approx(-6.1841)
    assert resolved["candidate"]["lookup_id"] == "location-lookup:fixture-office"
    assert resolved["candidate"]["retrieved_at"] == "2026-09-10T12:00:00Z"


@pytest.mark.parametrize(
    "address_type, precision", [("branch", "building"), ("campus", "campus")]
)
def test_r9_branch_and_campus_keep_site_type(address_type, precision):
    result = resolve_public_affiliation_site(
        evidence(address_type=address_type),
        [candidate(address_type, address_type=address_type, precision=precision)],
        selected_candidate_id=address_type,
    )

    assert result["status"] == "resolved"
    assert result["candidate"]["address_type"] == address_type
    assert result["is_person_location"] is False


@pytest.mark.parametrize("address_type", ["registered", "mailing"])
def test_r9_registered_and_mailing_sites_are_labelled_not_person_locations(
    address_type,
):
    result = resolve_public_affiliation_site(
        evidence(address_type=address_type),
        [candidate(address_type, address_type=address_type)],
        selected_candidate_id=address_type,
    )

    assert result["status"] == "resolved"
    assert result["address_type"] == address_type
    assert result["is_person_location"] is False


def test_r9_wrong_country_candidate_is_not_resolved():
    result = resolve_public_affiliation_site(
        evidence(country_code="ID"),
        [candidate(country_code="SG")],
        selected_candidate_id="office",
    )

    assert result["status"] == "needs_review"
    assert "country" in result["reason"]


def test_r9_same_country_candidate_needs_exact_address_and_organization_site_basis():
    wrong_address = candidate(
        address="200 Other Avenue, Jakarta, Indonesia",
        # A copied source-match attestation must not override the actual
        # configured-geocoder address for a different same-country business.
        match_basis={
            "source_address": "100 Example Avenue, Jakarta, ID",
            "organization_name": "Example Organization",
            "site_basis": "Analyst selection",
        },
    )
    wrong_organization = candidate(
        match_basis={
            "source_address": "100 Example Avenue, Jakarta, ID",
            "organization_name": "Different Organization",
            "site_basis": "Analyst selection",
        }
    )

    address_result = resolve_public_affiliation_site(
        evidence(), [wrong_address], selected_candidate_id="office"
    )
    organization_result = resolve_public_affiliation_site(
        evidence(), [wrong_organization], selected_candidate_id="office"
    )

    assert address_result["status"] == "needs_review"
    assert "provider address" in address_result["reason"]
    assert organization_result["status"] == "needs_review"
    assert "organization identity" in organization_result["reason"]


@pytest.mark.parametrize(
    ("source_address", "provider_address"),
    [
        ("12 Main Street, Jakarta, Indonesia", "99 Main Street, Jakarta, Indonesia"),
        ("1 Main Street, Jakarta, Indonesia", "2 Main Street, Jakarta, Indonesia"),
        ("12A Main Street, Jakarta, Indonesia", "12B Main Street, Jakarta, Indonesia"),
    ],
)
def test_r9_selected_provider_address_cannot_change_house_identity(
    source_address, provider_address
):
    selected = candidate(
        address=provider_address,
        display_name=provider_address,
        match_basis={
            "source_address": source_address,
            "organization_name": "Example Organization",
            "site_basis": "Analyst reviewed the cited office.",
        },
    )

    result = resolve_public_affiliation_site(
        evidence(address=source_address), [selected], selected_candidate_id="office"
    )

    assert result["status"] == "needs_review"
    assert "provider address" in result["reason"]


def test_r9_common_road_abbreviation_keeps_the_same_house_identity():
    source_address = "12 Jl. Merdeka, Jakarta, Indonesia"
    provider_address = "12 Jalan Merdeka, Jakarta, Indonesia"
    selected = candidate(
        address=provider_address,
        display_name=provider_address,
        match_basis={
            "source_address": source_address,
            "organization_name": "Example Organization",
            "site_basis": "Analyst reviewed the cited office.",
        },
    )

    result = resolve_public_affiliation_site(
        evidence(address=source_address), [selected], selected_candidate_id="office"
    )

    assert result["status"] == "resolved"


def test_r9_village_label_cannot_be_promoted_to_a_precise_site():
    selected = candidate(
        address="Example Village, West Java, Indonesia",
        display_name="Example Village, West Java, Indonesia",
        precision="area",
        match_basis={
            "source_address": "Example Village, West Java, Indonesia",
            "organization_name": "Example Organization",
            "site_basis": "The source names only a village-level area.",
        },
    )

    result = resolve_public_affiliation_site(
        evidence(address="Example Village, West Java, Indonesia"),
        [selected],
        selected_candidate_id="office",
    )

    assert result["status"] == "needs_review"
    assert "too coarse" in result["reason"]


def test_r9_multiple_same_name_candidates_are_ambiguous_without_selection():
    result = resolve_public_affiliation_site(
        evidence(),
        [candidate("central"), candidate("branch", longitude=106.9)],
    )

    assert result["status"] == "ambiguous"
    assert [item["candidate_id"] for item in result["candidates"]] == [
        "central",
        "branch",
    ]


def test_r9_historical_affiliation_remains_explicitly_noncurrent():
    result = resolve_public_affiliation_site(
        evidence(is_current_affiliation=False),
        [candidate()],
        selected_candidate_id="office",
    )

    assert result["status"] == "resolved"
    assert result["is_current_affiliation"] is False
    assert result["is_person_location"] is False


def test_r9_residential_or_nonofficial_evidence_never_reaches_candidates():
    residential = resolve_public_affiliation_site(
        evidence(address_type="residential"), [candidate()]
    )
    unofficial = resolve_public_affiliation_site(
        evidence(source_engine="search_result", is_public=True), [candidate()]
    )

    assert residential["status"] == "needs_review"
    assert "Residential" in residential["reason"]
    assert residential["candidates"] == []
    assert unofficial["status"] == "needs_review"
    assert "official" in unofficial["reason"]


def test_r9_failed_or_coarse_geocoder_result_is_unmapped_or_needs_review():
    failed = resolve_public_affiliation_site(evidence(), [])
    coarse = resolve_public_affiliation_site(
        evidence(), [candidate(precision="area")], selected_candidate_id="office"
    )

    assert failed["status"] == "unmapped"
    assert coarse["status"] == "needs_review"
    assert "too coarse" in coarse["reason"]


def test_r9_precise_office_is_not_validated_or_rejected_by_area_land_mask():
    calls = []

    def fail_if_called(*args):
        calls.append(args)
        return False

    result = resolve_public_affiliation_site(
        evidence(),
        [candidate()],
        selected_candidate_id="office",
        land_validator=fail_if_called,
        validation_dataset_version="coarse-mask-v1",
        validation_resolution_meters=10000,
    )

    assert result["status"] == "resolved"
    assert calls == []


def test_r9_area_requires_validated_land_dataset_and_handles_antimeridian():
    dateline_geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [179.2, 10.0],
                [-179.2, 10.0],
                [-179.2, 11.0],
                [179.2, 11.0],
                [179.2, 10.0],
            ]
        ],
    }
    unavailable = resolve_public_affiliation_site(
        evidence(address="Dateline region", address_type="area"),
        [],
        area_geometry=dateline_geometry,
    )
    calls = []

    def land(latitude, longitude, dataset_version, resolution):
        calls.append((latitude, longitude, dataset_version, resolution))
        return abs(longitude) > 170 and 10 <= latitude <= 11

    resolved = resolve_public_affiliation_site(
        evidence(address="Dateline region", address_type="area"),
        [],
        area_geometry=dateline_geometry,
        land_validator=land,
        validation_dataset_version="authoritative-land-2026-09",
        validation_resolution_meters=30,
    )

    assert unavailable["status"] == "unmapped"
    assert resolved["status"] == "resolved"
    assert abs(resolved["candidate"]["longitude"]) > 170
    assert (
        resolved["candidate"]["validation_dataset_version"]
        == "authoritative-land-2026-09"
    )
    assert calls


def test_r9_selected_area_candidate_must_be_validated_land_inside_geometry():
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [179.2, 10.0],
                [-179.2, 10.0],
                [-179.2, 11.0],
                [179.2, 11.0],
                [179.2, 10.0],
            ]
        ],
    }
    selected_area = candidate(
        "area-candidate",
        latitude=0,
        longitude=0,
        address_type="area",
        precision="area",
        match_basis={
            "source_address": "Dateline region",
            "organization_name": "Example Organization",
            "site_basis": "Analyst selected area candidate",
        },
    )

    result = resolve_public_affiliation_site(
        evidence(address="Dateline region", address_type="area"),
        [selected_area],
        selected_candidate_id="area-candidate",
        area_geometry=geometry,
        land_validator=lambda *_args: False,
        validation_dataset_version="authoritative-land-2026-09",
        validation_resolution_meters=30,
    )

    assert result["status"] == "unmapped"
    assert "validated land point" in result["reason"]


def test_r9_public_geocoder_uses_shared_cache_and_never_accepts_raw_person_strings(
    tmp_path,
):
    captured = []

    def opener(request, timeout):
        captured.append((request.full_url, timeout))
        return FakeResponse(
            [
                {
                    "place_id": 44,
                    "display_name": "100 Example Avenue, Jakarta, Indonesia",
                    "lat": "-6.1841",
                    "lon": "106.831",
                    "osm_type": "way",
                    "osm_id": 123,
                    "type": "house",
                    "address": {"country_code": "id"},
                }
            ]
        )

    first = geocode_public_affiliation_address_candidates(
        evidence(), opener=opener, cache_dir=tmp_path
    )
    second = geocode_public_affiliation_address_candidates(
        evidence(), opener=opener, cache_dir=tmp_path
    )

    assert len(captured) == 1
    assert "limit=5" in captured[0][0]
    assert first == second
    assert first[0]["precision"] == "address"
    assert geocode_place_center("private person address", opener=opener) is None
    assert len(captured) == 1


def test_r9_public_nominatim_requires_shared_cache_and_validated_evidence(tmp_path):
    with pytest.raises(GeocodingError, match="shared cache"):
        geocode_public_affiliation_address_candidates(
            evidence(), opener=lambda *_args, **_kwargs: FakeResponse([])
        )
    with pytest.raises(GeocodingError, match="official"):
        geocode_public_affiliation_address_candidates(
            evidence(source_engine="search_result"),
            opener=lambda *_args, **_kwargs: FakeResponse([]),
            cache_dir=tmp_path,
        )


def test_r9_failed_public_nominatim_request_persists_limiter_and_bounds_corruption(
    tmp_path, monkeypatch
):
    def failing_opener(*_args, **_kwargs):
        raise URLError("fixture failure")

    started = time.time()
    with pytest.raises(GeocodingError, match="could not be reached"):
        geocode_public_affiliation_address_candidates(
            evidence(), opener=failing_opener, cache_dir=tmp_path
        )

    state_path = tmp_path / "nominatim-shared-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["next_request_at"] > started

    sleeps = []
    monkeypatch.setattr(geocoding.time, "sleep", lambda seconds: sleeps.append(seconds))
    geocode_public_affiliation_address_candidates(
        evidence(),
        opener=lambda *_args, **_kwargs: FakeResponse([]),
        cache_dir=tmp_path,
    )
    assert sleeps and 0 < sleeps[0] <= geocoding.MAX_NOMINATIM_WAIT_SECONDS

    state["next_request_at"] = time.time() + 3600
    state_path.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(GeocodingError, match="future retry time"):
        geocode_public_affiliation_address_candidates(
            evidence(),
            opener=lambda *_args, **_kwargs: FakeResponse([]),
            cache_dir=tmp_path,
        )
