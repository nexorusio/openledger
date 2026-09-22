"""Regression coverage for Persona browser-map tiles.

The browser must use a same-origin endpoint so an upstream OpenStreetMap block
page can never be rendered as a tile inside a Persona.
"""

from email.message import Message
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Event, Lock
from urllib.error import HTTPError


PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0dIDAT\x08\xd7c\xf8\xcf\xc0\xf0\x1f\x00\x05\x00\x01\xff\x89\x99=\x1d"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


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
        return _UpstreamTile(PNG)

    monkeypatch.setattr(web_app, "urlopen", fetch)
    client = web_app.app.test_client()

    first = client.get("/map-tiles/3/4/2.png")
    second = client.get("/map-tiles/3/4/2.png")

    assert first.status_code == 200
    assert first.mimetype == "image/png"
    assert first.data == PNG
    assert second.status_code == 200
    assert len(calls) == 1
    assert calls[0][0] == "https://tile.openstreetmap.org/3/4/2.png"
    assert "OpenLedger" in calls[0][2]
    assert first.cache_control.max_age == web_app.MAP_TILE_CACHE_SECONDS

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
    enormous = client.get("/map-tiles/100000000000/0/0.png")

    assert blocked.status_code == 503
    assert b"Access blocked" not in blocked.data
    assert blocked.cache_control.no_store
    assert invalid.status_code == 404
    assert enormous.status_code == 404


def test_map_tile_proxy_rejects_incomplete_png(monkeypatch, tmp_path):
    import maigret.web.app as web_app

    monkeypatch.setitem(web_app.app.config, "TESTING", True)
    monkeypatch.setitem(web_app.app.config, "AUTH_REQUIRED", False)
    monkeypatch.setattr(
        web_app,
        "_map_tile_cache_path",
        lambda z, x, y: tmp_path / str(z) / str(x) / f"{y}.png",
    )
    monkeypatch.setattr(
        web_app, "urlopen", lambda *_args, **_kwargs: _UpstreamTile(PNG[:-1])
    )

    response = web_app.app.test_client().get("/map-tiles/3/4/2.png")

    assert response.status_code == 503


def test_map_tile_proxy_queues_leaflet_burst_without_returning_holes(
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
    monkeypatch.setattr(web_app, "map_tile_fetch_slots", BoundedSemaphore(value=3))

    release = Event()
    first_batch_started = Event()
    calls = []
    calls_lock = Lock()

    def fetch(request, timeout):
        with calls_lock:
            calls.append(request.full_url)
            if len(calls) == 3:
                first_batch_started.set()
        assert release.wait(timeout=1)
        return _UpstreamTile(PNG)

    monkeypatch.setattr(web_app, "urlopen", fetch)

    def request_tile(tile_x):
        with web_app.app.test_client() as client:
            return client.get(f"/map-tiles/3/{tile_x}/2.png")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(request_tile, tile_x) for tile_x in range(4)]
        assert first_batch_started.wait(timeout=1)
        release.set()
        responses = [future.result(timeout=2) for future in futures]

    assert [response.status_code for response in responses] == [200, 200, 200, 200]
    assert len(calls) == 4
