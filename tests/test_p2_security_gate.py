"""Offline regressions for the remaining P2 security boundary findings."""

import asyncio
import errno
import json
import os
import socket
from http.cookies import SimpleCookie
from urllib.parse import unquote, urlsplit

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


@pytest.mark.parametrize("model", [
    "../chat/completions",
    "../../private-fixture",
    "//127.0.0.1:9443/private-fixture",
    "https://other.example.test/private-fixture",
    "gpt-5.4?target=https://127.0.0.1:9443/private-fixture",
    "gpt-5.4#private-fragment",
    "..\\private-fixture",
    "%2e%2e%2fprivate-fixture",
    "gpt-5.4\r\nInjected: value",
    "team/model:revision",
])
def test_model_verification_keeps_hostile_delimiters_in_one_identifier(monkeypatch, model):
    captured_urls = _mock_model_transport(monkeypatch, 200)

    asyncio.run(ai.validate_openai_connection("fixture-not-a-key", model))

    assert len(captured_urls) == 1
    actual = urlsplit(captured_urls[0])
    assert actual.scheme == "https"
    assert actual.netloc == "api.openai.com"
    assert actual.query == actual.fragment == ""
    assert actual.path.startswith("/v1/models/")
    component = actual.path.removeprefix("/v1/models/")
    assert "/" not in component and "\\" not in component
    assert unquote(component) == model


@pytest.mark.parametrize("model", ["", ".", "..", None])
def test_model_verification_rejects_empty_or_dot_segments_before_a_request(monkeypatch, model):
    captured_urls = _mock_model_transport(monkeypatch, 200)

    with pytest.raises(ValueError, match="non-empty identifier"):
        asyncio.run(ai.validate_openai_connection("fixture-not-a-key", model))

    assert captured_urls == []


@pytest.mark.parametrize("component", [
    "model/../private", "model?query=1", "model#fragment", "model\\private",
    "model%", "model%2", "model%GG", "model\n", "model ", "model😀", "", ".", "..",
])
def test_model_verification_rejects_encoder_contract_violations_before_a_request(monkeypatch, component):
    captured_urls = _mock_model_transport(monkeypatch, 200)
    monkeypatch.setattr(ai, "quote", lambda _model, *, safe: component)

    with pytest.raises(ValueError, match="URL component is invalid"):
        asyncio.run(ai.validate_openai_connection("fixture-not-a-key", "gpt-5.4"))

    assert captured_urls == []


@pytest.mark.parametrize("base,options", [
    ("https://gateway.example.test/openai/v1", {"allow_custom_endpoint": True}),
    ("http://127.0.0.1:11434/v1", {"allow_custom_endpoint": True}),
    ("https://10.20.30.40/v1", {"allow_custom_endpoint": True, "allow_private_endpoint": True}),
])
def test_model_verification_preserves_authorized_provider_configuration(monkeypatch, base, options):
    captured_urls = _mock_model_transport(monkeypatch, 200)

    asyncio.run(ai.validate_openai_connection(
        "fixture-not-a-key", "team/model:revision", api_base_url=base, **options,
    ))

    assert len(captured_urls) == 1
    configured, actual = urlsplit(base), urlsplit(captured_urls[0])
    assert (actual.scheme, actual.netloc) == (configured.scheme, configured.netloc)
    assert actual.path.startswith(configured.path + "/models/")
    assert unquote(actual.path.removeprefix(configured.path + "/models/")) == "team/model:revision"
    assert actual.query == actual.fragment == ""


@pytest.mark.parametrize("base,options", [
    ("https://gateway.example.test/openai/v1", {}),
    ("https://10.20.30.40/v1", {"allow_custom_endpoint": True}),
])
def test_model_verification_requires_provider_opt_ins_before_a_request(monkeypatch, base, options):
    captured_urls = _mock_model_transport(monkeypatch, 200)

    with pytest.raises(ValueError, match="authorization"):
        asyncio.run(ai.validate_openai_connection(
            "fixture-not-a-key", "team/model:revision", api_base_url=base, **options,
        ))

    assert captured_urls == []


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


@pytest.mark.parametrize(
    "session_folder",
    [
        None,
        b"search_fixture",
        "",
        ".",
        "..",
        "search_",
        "other_fixture",
        "/search_fixture",
        "//search_fixture",
        "./search_fixture",
        "search_fixture/",
        "search_fixture/.",
        "search_other/../search_fixture",
        "search_fixture/../../outside",
        "search_fixture\\outside",
        "search_fixture\x00",
        "search_fixture\n",
        "search_\uFF0E\uFF0E",
        "search_\uFF26ixture",
        "search_" + "a" * 129,
    ],
)
def test_persisted_metadata_rejects_noncanonical_components_before_open(
    report_metadata, monkeypatch, session_folder
):
    metadata, payload = report_metadata
    metadata.write_text(payload, encoding="utf-8")

    def unexpected_open(*_args, **_kwargs):
        pytest.fail("Invalid session components must be rejected before file access")

    monkeypatch.setattr(os, "open", unexpected_open)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {unexpected_open})

    assert web_app.load_persisted_job_result(session_folder) is None


@pytest.mark.parametrize("session_key", ["0", "A_key-9", "a" * 128])
def test_persisted_metadata_preserves_valid_session_components(
    report_metadata, session_key
):
    metadata, payload = report_metadata
    session_folder = f"search_{session_key}"
    session_dir = metadata.parent.with_name(session_folder)
    session_dir.mkdir()
    document = json.loads(payload)
    document["session_key"] = session_key
    document["result"]["session_folder"] = session_folder
    (session_dir / metadata.name).write_text(json.dumps(document), encoding="utf-8")

    loaded = web_app.load_persisted_job_result(session_folder)

    assert loaded is not None
    assert loaded[0] == session_key
    assert loaded[1]["session_folder"] == session_folder


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


def test_persisted_metadata_rejects_directory_swap_before_session_open(
    report_metadata, tmp_path, monkeypatch
):
    metadata, payload = report_metadata
    metadata.write_text(payload, encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / metadata.name).write_text(payload, encoding="utf-8")
    session_dir = metadata.parent
    original_lstat = os.lstat
    original_open = os.open
    swapped = False

    def swap_directory():
        nonlocal swapped
        if not swapped:
            swapped = True
            session_dir.rename(session_dir.with_name("original_directory"))
            session_dir.symlink_to(outside, target_is_directory=True)

    def racing_lstat(path, *args, **kwargs):
        # Reproduce the original gap after realpath validation and before
        # lstat: both the later stat and full-path open saw the outside file.
        if os.fspath(path) == str(metadata):
            swap_directory()
        return original_lstat(path, *args, **kwargs)

    def racing_open(path, flags, *args, **kwargs):
        # At the equivalent point in the anchored reader, O_NOFOLLOW must
        # refuse the replaced session directory before opening its contents.
        if path == session_dir.name and kwargs.get("dir_fd") is not None:
            swap_directory()
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", racing_lstat)
    monkeypatch.setattr(os, "open", racing_open)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {racing_open})

    assert web_app.load_persisted_job_result("search_fixture") is None
    assert swapped
    assert (outside / metadata.name).read_text(encoding="utf-8") == payload


def test_persisted_metadata_uses_open_session_after_directory_swap(
    report_metadata, tmp_path, monkeypatch
):
    metadata, payload = report_metadata
    metadata.write_text(payload, encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_payload = json.loads(payload)
    outside_payload["result"]["error"] = "outside controlled fixture"
    (outside / metadata.name).write_text(json.dumps(outside_payload), encoding="utf-8")
    session_dir = metadata.parent
    original_open = os.open
    swapped = False

    def racing_open(path, flags, *args, **kwargs):
        nonlocal swapped
        descriptor = original_open(path, flags, *args, **kwargs)
        if path == session_dir.name and kwargs.get("dir_fd") is not None:
            swapped = True
            session_dir.rename(session_dir.with_name("original_directory"))
            session_dir.symlink_to(outside, target_is_directory=True)
        return descriptor

    monkeypatch.setattr(os, "open", racing_open)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {racing_open})

    loaded = web_app.load_persisted_job_result("search_fixture")

    assert swapped
    assert loaded is not None
    assert loaded[1]["error"] == "fixture-only"


@pytest.mark.parametrize("kind", ["valid", "invalid_json", "directory", "fifo", "missing"])
def test_persisted_metadata_closes_descriptors(report_metadata, monkeypatch, kind):
    metadata, payload = report_metadata
    if kind == "valid":
        metadata.write_text(payload, encoding="utf-8")
    elif kind == "invalid_json":
        metadata.write_text("{invalid", encoding="utf-8")
    elif kind == "directory":
        metadata.mkdir()
    elif kind == "fifo":
        os.mkfifo(metadata)
    original_open = os.open
    opened = []

    def recording_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(os, "open", recording_open)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {recording_open})

    loaded = web_app.load_persisted_job_result("search_fixture")

    assert (loaded is not None) == (kind == "valid")
    assert len(opened) == (2 if kind == "missing" else 3)
    for descriptor in opened:
        with pytest.raises(OSError) as error:
            os.fstat(descriptor)
        assert error.value.errno == errno.EBADF


@pytest.mark.parametrize("capability", ["dir_fd", "O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK"])
def test_persisted_metadata_fails_closed_on_unsupported_platform(
    report_metadata, monkeypatch, capability
):
    metadata, payload = report_metadata
    metadata.write_text(payload, encoding="utf-8")

    def unexpected_open(*_args, **_kwargs):
        pytest.fail("Unsupported platforms must not fall back to unsafe metadata reads")

    monkeypatch.setattr(os, "open", unexpected_open)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {unexpected_open})
    if capability == "dir_fd":
        monkeypatch.setattr(os, "supports_dir_fd", set())
    else:
        monkeypatch.delattr(os, capability)

    assert web_app.load_persisted_job_result("search_fixture") is None
