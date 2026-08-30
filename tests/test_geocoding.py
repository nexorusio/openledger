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


def test_geocoder_uses_approved_place_bounding_box_centroid():
    captured = {}

    def opener(request, timeout):
        captured.update(url=request.full_url, timeout=timeout, headers=request.headers)
        return FakeResponse(
            [
                {
                    "display_name": "Jakarta, Indonesia",
                    "lat": "-6.1754",
                    "lon": "106.8272",
                    "boundingbox": ["-6.3745", "-5.9937", "106.689", "106.973"],
                }
            ]
        )

    result = geocode_place_center("Jakarta, Indonesia", opener=opener)

    assert result["latitude"] == pytest.approx((-6.3745 + -5.9937) / 2)
    assert result["longitude"] == pytest.approx((106.689 + 106.973) / 2)
    assert result["precision"] == "place"
    assert "q=Jakarta%2C+Indonesia" in captured["url"]
    assert captured["timeout"] == 10


def test_geocoder_returns_none_when_place_is_not_found():
    assert geocode_place_center(
        "Unknown place",
        opener=lambda *_args, **_kwargs: FakeResponse([]),
    ) is None


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.test/search",
        "file:///tmp/geocoder",
        "https://user:secret@example.test/search",
    ],
)
def test_geocoder_rejects_unsafe_endpoints(endpoint):
    with pytest.raises(GeocodingError, match="HTTPS URL"):
        geocode_place_center("Jakarta", endpoint=endpoint)


def test_geocoder_rejects_out_of_range_provider_data():
    with pytest.raises(GeocodingError, match="out-of-range"):
        geocode_place_center(
            "Invalid",
            opener=lambda *_args, **_kwargs: FakeResponse(
                [{"lat": "95", "lon": "106"}]
            ),
        )
