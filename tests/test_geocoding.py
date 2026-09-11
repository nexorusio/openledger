import json

import pytest

from maigret.web.geocoding import GeocodingError, geocode_place_center


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


def _public_affiliation_evidence(**overrides):
    evidence = {
        "address": "100 Example Avenue, Jakarta, Indonesia",
        "source_url": "https://example.org/contact",
        "observation_key": "official-website-location:example-office",
        "source_engine": "official_website_public_content",
        "is_public": True,
        "address_type": "operating",
    }
    evidence.update(overrides)
    return evidence


def test_legacy_geocoder_never_sends_raw_person_claim_to_provider():
    calls = []

    def opener(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse([])

    assert (
        geocode_place_center("A person's current home address", opener=opener) is None
    )
    assert calls == []


def test_legacy_geocoder_does_not_autoapprove_affiliation_site_point():
    calls = []

    def opener(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse([])

    assert (
        geocode_place_center(
            "100 Example Avenue, Jakarta, Indonesia",
            evidence=_public_affiliation_evidence(),
            opener=opener,
        )
        is None
    )
    assert calls == []


@pytest.mark.parametrize(
    "evidence",
    [
        _public_affiliation_evidence(is_public=False),
        _public_affiliation_evidence(source_engine="search_result"),
        _public_affiliation_evidence(address_type="residential"),
    ],
)
def test_legacy_geocoder_rejects_ineligible_evidence(evidence):
    with pytest.raises(GeocodingError):
        geocode_place_center("address", evidence=evidence)
