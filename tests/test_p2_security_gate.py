"""Offline regressions for the remaining P2 security boundary findings."""

import asyncio
import json
import socket
from http.cookies import SimpleCookie

import pytest

from maigret import ai
from maigret.web import app as web_app


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def blocked(*_args, **_kwargs):
        pytest.fail("Security gate regressions must not use the network")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)


class _ModelResponse:
    """Mock transport output while retaining aiohttp's real redirect engine."""

    _raw_cookie_headers = ()
    _cookies = None
    connection = None

    def __init__(self, request, status):
        self.status = status
        self.method = request.method
        self.url = request.url
        self.cookies = SimpleCookie()
        self.headers = (
            {"Location": "https://127.0.0.1:9443/private-fixture"}
            if status != 200
            else {}
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def release(self):
        pass

    def close(self):
        pass

    async def read(self):
        return b"{}"

    async def json(self):
        return {"id": "gpt-5.4"}


def _mock_model_transport(monkeypatch, initial_status):
    original_session = ai.aiohttp.ClientSession
    captured_urls = []

    async def fixture_response(request, _handler):
        captured_urls.append(str(request.url))
        status = initial_status if len(captured_urls) == 1 else 200
        return _ModelResponse(request, status)

    def session_factory(**kwargs):
        return original_session(**kwargs, middlewares=(fixture_response,))

    monkeypatch.setattr(ai.aiohttp, "ClientSession", session_factory)
    return captured_urls


@pytest.mark.parametrize("redirect_status", [301, 302, 303, 307, 308])
def test_model_verification_does_not_follow_redirects(monkeypatch, redirect_status):
    captured_urls = _mock_model_transport(monkeypatch, redirect_status)

    with pytest.raises(RuntimeError, match=f"HTTP {redirect_status}"):
        asyncio.run(ai.validate_openai_connection("fixture-not-a-key", "gpt-5.4"))

    assert captured_urls == ["https://api.openai.com/v1/models/gpt-5.4"]


def test_model_verification_accepts_successful_response(monkeypatch):
    captured_urls = _mock_model_transport(monkeypatch, 200)

    assert (
        asyncio.run(ai.validate_openai_connection("fixture-not-a-key", "gpt-5.4"))
        == "gpt-5.4"
    )
    assert captured_urls == ["https://api.openai.com/v1/models/gpt-5.4"]


@pytest.fixture
def report_metadata(monkeypatch, tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    session_dir = reports / "search_fixture"
    session_dir.mkdir()
    monkeypatch.setitem(web_app.app.config, "REPORTS_FOLDER", str(reports))
    result = web_app.normalize_persisted_result(
        "fixture",
        {
            "status": "failed",
            "session_folder": "search_fixture",
            "error": "fixture-only",
            "usernames": ["fixture"],
        },
    )
    payload = json.dumps(
        {
            "schema_version": web_app.SESSION_METADATA_SCHEMA_VERSION,
            "session_key": "fixture",
            "result": result,
        }
    )
    return session_dir / web_app.SESSION_METADATA_FILENAME, payload


def test_persisted_metadata_accepts_ordinary_file(report_metadata):
    metadata, payload = report_metadata
    metadata.write_text(payload, encoding="utf-8")

    loaded = web_app.load_persisted_job_result("search_fixture")

    assert loaded is not None
    assert loaded[0] == "fixture"
    assert loaded[1]["usernames"] == ["fixture"]


@pytest.mark.parametrize("outside_reports", [False, True])
def test_persisted_metadata_rejects_terminal_symlinks(
    report_metadata, tmp_path, outside_reports
):
    metadata, payload = report_metadata
    target_root = tmp_path if outside_reports else metadata.parent
    target = target_root / "symlink-target.json"
    target.write_text(payload, encoding="utf-8")
    metadata.symlink_to(target)

    assert web_app.load_persisted_job_result("search_fixture") is None
    assert target.read_text(encoding="utf-8") == payload


def test_persisted_metadata_rejects_directory_symlink(monkeypatch, tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (reports / "search_fixture").symlink_to(outside, target_is_directory=True)
    monkeypatch.setitem(web_app.app.config, "REPORTS_FOLDER", str(reports))

    assert web_app.load_persisted_job_result("search_fixture") is None
