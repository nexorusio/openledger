"""Regression coverage for Persona browser-map tiles.

The browser must use a same-origin endpoint so an upstream OpenStreetMap block
page can never be rendered as a tile inside a Persona.
"""

from email.message import Message
from urllib.error import HTTPError


class _UpstreamTile:
    def __init__(self, payload, content_type="image/png"):
        self.payload = payload
        self.headers = Message()
        self.headers["Content-Type"] = content_type

    def read(self, _limit):
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def test_legacy_public_osm_setting_uses_cached_same_origin_route(monkeypatch):
    import maigret.web.app as web_app

    monkeypatch.setenv(
        "OPENLEDGER_MAP_TILE_URL", "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    )

    assert web_app.persona_map_tile_url() == "/map-tiles/{z}/{x}/{y}.png"


def test_map_tile_proxy_caches_valid_png_and_never_relays_block_page(
    monkeypatch, tmp_path
):
    import maigret.web.app as web_app

    monkeypatch.setitem(web_app.app.config, "TESTING", True)
    monkeypatch.setitem(web_app.app.config, "AUTH_REQUIRED", False)
    monkeypatch.setattr(
        web_app,
        "_map_tile_cache_path",
        lambda z, x, y: tmp_path / str(z) / str(x) / f"{y}.png",
    )
    calls = []

    def fetch(request, timeout):
        calls.append((request.full_url, timeout, request.get_header("User-agent")))
        return _UpstreamTile(b"valid-png")

    monkeypatch.setattr(web_app, "urlopen", fetch)
    client = web_app.app.test_client()

    first = client.get("/map-tiles/3/4/2.png")
    second = client.get("/map-tiles/3/4/2.png")

    assert first.status_code == 200
    assert first.mimetype == "image/png"
    assert first.data == b"valid-png"
    assert second.status_code == 200
    assert len(calls) == 1
    assert calls[0][0] == "https://tile.openstreetmap.org/3/4/2.png"
    assert "OpenLedger" in calls[0][2]
    assert first.cache_control.max_age == 86400

    monkeypatch.setattr(
        web_app,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            HTTPError(
                "https://tile.openstreetmap.org/3/5/2.png", 403, "blocked", {}, None
            )
        ),
    )
    blocked = client.get("/map-tiles/3/5/2.png")
    invalid = client.get("/map-tiles/20/0/0.png")

    assert blocked.status_code == 503
    assert b"Access blocked" not in blocked.data
    assert blocked.cache_control.no_store
    assert invalid.status_code == 404
