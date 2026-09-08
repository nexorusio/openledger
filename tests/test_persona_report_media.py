import io
import os
import socket
import time

import pytest
from PIL import Image

from maigret.web import persona_report_media as media


def _png_bytes(size=(256, 256), color="#DCE8E8"):
    output = io.BytesIO()
    Image.new("RGB", size, color).save(output, format="PNG")
    return output.getvalue()


class _Response:
    def __init__(self, body=b"image", *, status=200, headers=None):
        self.status = status
        self.headers = headers or {"content-type": "image/png"}
        self._body = body
        self.released = False

    def read(self, amount):
        chunk, self._body = self._body[:amount], self._body[amount:]
        return chunk

    def read1(self, amount):
        return self.read(amount)

    def release_conn(self):
        self.released = True


class _Pool:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.closed = False

    def urlopen(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return self.response

    def close(self):
        self.closed = True


def test_media_url_rejects_credentials_local_hosts_and_non_default_ports():
    for url in (
        "https://user:secret@example.test/photo.jpg",
        "https://localhost/photo.jpg",
        "https://profile.internal/photo.jpg",
        "https://example.test:8443/photo.jpg",
        "http://example.test:443/photo.jpg",
        "file:///etc/passwd",
    ):
        with pytest.raises(ValueError):
            media._validated_media_url(url)


def test_media_dns_answer_fails_closed_if_any_address_is_private(monkeypatch):
    monkeypatch.setattr(
        media.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ],
    )

    with pytest.raises(ValueError, match="non-public"):
        media._validated_public_addresses("example.test", 443)


def test_public_image_fetch_is_dns_pinned_bounded_and_uses_report_identity(
    monkeypatch,
):
    response = _Response(
        b"small-image",
        headers={"content-type": "image/png", "content-length": "11"},
    )
    pool = _Pool(response)
    pinned_calls = []
    monkeypatch.setattr(
        media,
        "_validated_public_addresses",
        lambda hostname, port: ("93.184.216.34",),
    )

    def pinned_pool(hostname, address, port, scheme):
        pinned_calls.append((hostname, address, port, scheme))
        return pool, {
            "Host": hostname,
            "User-Agent": media.REPORT_USER_AGENT,
        }

    monkeypatch.setattr(media, "_pinned_pool", pinned_pool)

    result = media.fetch_public_image(
        "https://images.example.test/alice.png?size=small",
        maximum_bytes=20,
    )

    assert result == b"small-image"
    assert pinned_calls == [("images.example.test", "93.184.216.34", 443, "https")]
    assert pool.calls[0][0:2] == ("GET", "/alice.png?size=small")
    assert pool.calls[0][2]["redirect"] is False
    assert pool.calls[0][2]["headers"]["User-Agent"] == media.REPORT_USER_AGENT
    assert response.released is True
    assert pool.closed is True


def test_public_image_fetch_rejects_large_or_non_image_responses(monkeypatch):
    monkeypatch.setattr(
        media,
        "_validated_public_addresses",
        lambda hostname, port: ("93.184.216.34",),
    )
    responses = iter(
        [
            _Response(
                b"x",
                headers={"content-type": "image/png", "content-length": "100"},
            ),
            _Response(b"not-image", headers={"content-type": "text/html"}),
        ]
    )
    monkeypatch.setattr(
        media,
        "_pinned_pool",
        lambda *args: (_Pool(next(responses)), {}),
    )

    with pytest.raises(ValueError, match="too large"):
        media.fetch_public_image("https://example.test/large.png", maximum_bytes=10)
    with pytest.raises(ValueError, match="supported image"):
        media.fetch_public_image("https://example.test/not-image", maximum_bytes=10)


def test_public_image_fetch_enforces_wall_clock_transfer_deadline(monkeypatch):
    now = [0.0]

    class _TrickleResponse(_Response):
        def read1(self, amount):
            now[0] += media.MAX_MEDIA_FETCH_SECONDS + 1
            return b"x"

    response = _TrickleResponse(headers={"content-type": "image/png"})
    pool = _Pool(response)
    monkeypatch.setattr(media.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        media,
        "_validated_public_addresses",
        lambda hostname, port: ("93.184.216.34",),
    )
    monkeypatch.setattr(media, "_pinned_pool", lambda *args: (pool, {}))

    with pytest.raises(ValueError, match="transfer deadline"):
        media.fetch_public_image("https://example.test/trickle.png")

    assert response.released is True
    assert pool.closed is True


def test_redirect_target_is_revalidated_and_private_target_is_rejected(monkeypatch):
    redirect = _Response(
        status=302,
        headers={"location": "http://127.0.0.1/private.png"},
    )

    def public_addresses(hostname, port):
        if hostname == "127.0.0.1":
            raise ValueError("Media hostname resolved to a non-public address")
        return ("93.184.216.34",)

    monkeypatch.setattr(media, "_validated_public_addresses", public_addresses)
    monkeypatch.setattr(
        media,
        "_pinned_pool",
        lambda *args: (_Pool(redirect), {}),
    )

    with pytest.raises(ValueError, match="non-public"):
        media.fetch_public_image("https://example.test/photo.png")


def test_portrait_is_verified_cropped_and_report_failure_falls_back(monkeypatch):
    prepared = media.prepare_portrait(_png_bytes((320, 480)), size=160)
    with Image.open(io.BytesIO(prepared)) as portrait:
        assert portrait.size == (160, 160)
        assert portrait.mode == "RGBA"

    monkeypatch.setattr(
        media,
        "fetch_public_image",
        lambda url: (_ for _ in ()).throw(ValueError("blocked")),
    )
    assert media.load_approved_portrait("https://example.test/photo.png") is None


def test_location_map_uses_a_bounded_tile_set_and_visible_output(monkeypatch):
    tile_calls = []

    def tile(zoom, tile_x, tile_y):
        tile_calls.append((zoom, tile_x, tile_y))
        return _png_bytes()

    monkeypatch.setattr(media, "_cached_tile", tile)

    map_bytes = media.render_location_map(-6.2088, 106.8456)

    assert map_bytes is not None
    assert 1 <= len(tile_calls) <= media.MAX_MAP_TILES
    assert all(call[0] == media.MAP_ZOOM for call in tile_calls)
    assert media.OSM_TILE_TEMPLATE == ("https://tile.openstreetmap.org/{z}/{x}/{y}.png")
    assert media.MAP_ATTRIBUTION == "Map data: OpenStreetMap contributors"
    with Image.open(io.BytesIO(map_bytes)) as location_map:
        assert location_map.size == (media.MAP_WIDTH, media.MAP_HEIGHT)


def test_map_tiles_are_cached_for_at_least_seven_days(monkeypatch, tmp_path):
    cache_root = tmp_path / "report-map-cache"
    monkeypatch.setattr(media, "_tile_cache_root", lambda: cache_root)
    fetch_calls = []

    def fetcher(url, *, maximum_bytes):
        fetch_calls.append((url, maximum_bytes))
        return _png_bytes()

    first = media._cached_tile(11, 12, 13, fetcher=fetcher)
    second = media._cached_tile(11, 12, 13, fetcher=fetcher)

    assert first == second
    assert len(fetch_calls) == 1
    cache_file = next(cache_root.glob("*.png"))
    old_time = time.time() - media.MAP_CACHE_SECONDS - 1
    os.utime(cache_file, (old_time, old_time))
    media._cached_tile(11, 12, 13, fetcher=fetcher)
    assert len(fetch_calls) == 2
    assert media.MAP_CACHE_SECONDS >= 7 * 24 * 60 * 60
