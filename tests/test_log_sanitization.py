"""Untrusted diagnostic values remain useful, bounded, single-line log fields."""

import logging
import socket

import pytest

from tests.test_web import web_app  # noqa: F401


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('Logging fixtures must not use sockets or DNS')

    for name in (
        'getaddrinfo',
        'gethostbyname',
        'gethostbyname_ex',
        'create_connection',
    ):
        monkeypatch.setattr(socket, name, blocked)
    for name in ('connect', 'connect_ex', 'sendto'):
        monkeypatch.setattr(socket.socket, name, blocked)


@pytest.mark.parametrize(
    'separator',
    [
        '\r',
        '\n',
        '\r\n',
        '\x00',
        '\x1b',
        '\x7f',
        '\t',
        '\v',
        '\f',
        '\x1c',
        '\x1d',
        '\x1e',
        '\x85',
        '\u2028',
        '\u2029',
    ],
)
def test_log_field_removes_record_and_terminal_controls(web_app, separator):
    assert web_app.safe_log_value(f'  HTTP 403{separator}retry denied  ') == (
        'HTTP 403 retry denied'
    )


def test_log_field_preserves_printable_diagnostics_and_enforces_bounds(web_app):
    message = 'Café 東京 @fixture: HTTP 403; source="Instagram"; 100%'
    assert web_app.safe_log_value(message) == message
    assert web_app.safe_log_value(ValueError(f'{message}\r\nretry denied')) == (
        f'{message} retry denied'
    )
    assert web_app.safe_log_value('a' * 600) == 'a' * 500
    assert web_app.safe_log_value('a\n' * 600, limit=32) == ('a ' * 16)
    assert web_app.safe_log_value(message, limit=0) == ''
    assert web_app.safe_log_value(None) == ''


@pytest.mark.parametrize(
    'state, expected',
    [
        ('unknown', 'Invalid search session:'),
        ('completed_without_result', 'No results found for completed session:'),
        ('failed', 'Search failed for session='),
    ],
)
def test_status_logs_keep_one_record_per_message(
    web_app,
    monkeypatch,
    caplog,
    state,
    expected,
):
    timestamp = 'fixture\r\nFORGED\x00\x1b\u2028record'
    if state != 'unknown':
        web_app.background_jobs[timestamp] = {'completed': True}
    if state == 'failed':
        web_app.job_results[timestamp] = {
            'status': 'failed',
            'error': 'HTTP 403\nrate limit\u2029retry later',
        }
    monkeypatch.setattr(web_app, 'load_persisted_job_result', lambda value: None)
    with web_app.app.test_request_context('/status/fixture'):
        with caplog.at_level(logging.INFO):
            response = web_app.status(timestamp)
    assert response.status_code == 302
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 2
    assert messages[0] == 'Status check for timestamp=fixture FORGED record'
    assert expected in messages[1]
    assert 'fixture FORGED record' in messages[1]
    assert all(message.splitlines() == [message] for message in messages)
    assert all('\x00' not in message and '\x1b' not in message for message in messages)
    if state == 'failed':
        assert 'HTTP 403 rate limit retry later' in messages[1]


def test_rejected_metadata_and_missing_results_log_sanitized_values(
    web_app,
    monkeypatch,
    caplog,
):
    session_id = 'search_fixture\r\nFORGED\u2028record'
    assert web_app.load_persisted_job_result(session_id) is None
    metadata_message = caplog.records[-1].getMessage()
    assert 'search_fixture FORGED record' in metadata_message
    assert metadata_message.splitlines() == [metadata_message]
    monkeypatch.setattr(web_app, 'find_result_by_session', lambda value: None)
    with web_app.app.test_request_context('/results/fixture'):
        response = web_app.results(session_id)
    assert response.status_code == 302
    result_message = caplog.records[-1].getMessage()
    assert result_message == (
        'Results for session search_fixture FORGED record not found in job_results.'
    )


def test_failed_login_logs_sanitized_request_origin(web_app, monkeypatch, caplog):
    monkeypatch.setitem(web_app.app.config, 'AUTH_REQUIRED', True)
    monkeypatch.setattr(web_app, 'load_auth_credentials', lambda: None)
    monkeypatch.setattr(web_app, 'is_valid_csrf', lambda value: True)
    monkeypatch.setattr(
        web_app, 'login_attempt_key', lambda: 'fixture\nFORGED\x1b\u2029origin'
    )
    with web_app.app.test_request_context('/login', method='POST', data={}):
        response = web_app.login()
    assert response.status_code == 302
    assert caplog.records[-1].getMessage() == (
        'Rejected OpenLedger login from fixture FORGED origin'
    )
