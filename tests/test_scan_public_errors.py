"""Offline HTTP boundaries for deliberately public scan validation errors."""

import socket

import pytest

from maigret.web import app as web_app
from maigret.web import investigation_input
from maigret.web.investigation_input import InvestigationInputError
from maigret.web.profile_discovery_policy import ProfileDiscoveryPolicyError


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def blocked(*_args, **_kwargs):
        pytest.fail("Scan error regressions must not use sockets or DNS")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)


@pytest.fixture
def scan_client(monkeypatch):
    for key, value in {
        "AUTH_REQUIRED": False,
        "TESTING": True,
        "DEBUG": False,
        "PROPAGATE_EXCEPTIONS": False,
        "SECRET_KEY": "offline-scan-error-fixture",
    }.items():
        monkeypatch.setitem(web_app.app.config, key, value)

    def no_collection(*_args, **_kwargs):
        pytest.fail("Validation failures must not start collection")

    monkeypatch.setattr(web_app, "start_live_job", no_collection)
    monkeypatch.setattr(web_app, "parse_search_options", lambda *_args: {})
    client = web_app.app.test_client()
    with client.session_transaction() as session:
        session["csrf_token"] = "offline-csrf-fixture"
    return client


def _post_scan(client, **data):
    return client.post(
        "/api/scan",
        data=data,
        headers={"X-OpenLedger-CSRF": "offline-csrf-fixture"},
    )


@pytest.mark.parametrize(
    "exception_class", [InvestigationInputError, ProfileDiscoveryPolicyError]
)
def test_domain_public_message_is_separate_from_exception_diagnostics(exception_class):
    error = exception_class("Choose a supported option.")
    error.args = ("private fixture diagnostic",)
    error.__cause__ = RuntimeError("private fixture cause")

    assert error.public_message == "Choose a supported option."
    with pytest.raises(TypeError, match="must be a string"):
        exception_class(RuntimeError("private fixture exception"))


@pytest.mark.parametrize(
    "exception_class, expected_status",
    [(InvestigationInputError, 400), (ProfileDiscoveryPolicyError, 503)],
)
def test_scan_uses_only_domain_public_guidance(
    scan_client, monkeypatch, exception_class, expected_status
):
    error = exception_class("Choose a supported option.")
    error.args = ("private fixture diagnostic",)
    error.__cause__ = RuntimeError("private fixture cause")

    def reject(*_args):
        raise error

    monkeypatch.setattr(web_app, "parse_investigation_submission", reject)
    response = _post_scan(scan_client, usernames="alexexample")

    assert response.status_code == expected_status
    assert response.get_json() == {"error": "Choose a supported option."}
    assert "private fixture" not in response.get_data(as_text=True)


def test_scan_preserves_helpful_input_validation(scan_client):
    response = _post_scan(scan_client, usernames="")

    assert response.status_code == 400
    assert response.get_json() == {
        "error": "Add at least one username or social handle."
    }


def test_scan_preserves_helpful_policy_refusal(scan_client, monkeypatch):
    from maigret.web.profile_discovery_policy import govern_profile_discovery_options

    def server_policy(*_args):
        return govern_profile_discovery_options(
            {}, environ={"OPENLEDGER_PROFILE_DISCOVERY_ENABLED": "false"}
        )

    monkeypatch.setattr(web_app, "parse_search_options", server_policy)
    response = _post_scan(scan_client, usernames="alexexample")

    assert response.status_code == 503
    assert response.get_json() == {
        "error": "Profile discovery is temporarily disabled by server policy."
    }


@pytest.mark.parametrize(
    "normalizer, form_field, public_message",
    [
        (
            "normalize_nicknames",
            "alias_nicknames",
            "Enter one nickname per comma-separated value.",
        ),
        (
            "normalize_context_numbers",
            "alias_context_numbers",
            "Contextual numbers must contain 1 to 6 digits each.",
        ),
    ],
)
def test_alias_validation_never_copies_underlying_error_text(
    scan_client, monkeypatch, normalizer, form_field, public_message
):
    def reject(*_args):
        raise ValueError("private fixture storage path and diagnostic")

    monkeypatch.setattr(investigation_input, normalizer, reject)
    response = _post_scan(
        scan_client,
        identifier_type="full_name",
        identifier_value="Alex Example",
        generate_name_variants="on",
        **{form_field: "invalid-fixture"},
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": public_message}
    assert "private fixture" not in response.get_data(as_text=True)


@pytest.mark.parametrize(
    "failure_stage", ["parse_investigation_submission", "start_live_job"]
)
def test_scan_keeps_unexpected_failures_private(
    scan_client, monkeypatch, failure_stage
):
    def unexpected(*_args):
        error = RuntimeError("private fixture database credential and path")
        error.public_message = "Untrusted generic exceptions cannot opt into exposure."
        raise error

    monkeypatch.setattr(web_app, failure_stage, unexpected)
    response = _post_scan(scan_client, usernames="alexexample")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    assert "private fixture" not in body
    assert "Untrusted generic exceptions" not in body
