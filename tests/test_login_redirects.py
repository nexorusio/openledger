"""Login return URLs remain internal without reinterpreting encoded URL data."""

from urllib.parse import parse_qs, urlsplit

import pytest

from tests.test_web import _csrf_token, client, web_app  # noqa: F401


UNTRUSTED_DESTINATIONS = [
    'https://evil.example/steal',
    'https:/evil.example/steal',
    'https:///evil.example/steal',
    'javascript:alert(1)',
    '//evil.example/steal',
    '///evil.example/steal',
    '/\\evil.example/steal',
    '/%5cevil.example/steal',
    '/%2fevil.example/steal',
    '/%2f%2fevil.example/steal',
    '/safe%0d%0aLocation:%20https://evil.example/steal',
    '/safe\n//evil.example/steal',
    ' //evil.example/steal',
    'https://[invalid-host/',
    '//[invalid-host/',
    'history',
]
INTERNAL_DESTINATIONS = [
    '/history',
    '/cases/case-123?tab=evidence#review',
    '/history?subject=Alice%20Example&literal=a%26b%3Dc#evidence',
    '/reports/case%2Fname?filter=%2526hidden%253D1#part%231',
    '/history?next=https%3A%2F%2Fevil.example%2F#//evil.example',
    '/history?subject=Alice+Example&subject=Bob#people',
    '/history?literal=%253F%2523%2526',
]


@pytest.mark.parametrize('candidate', UNTRUSTED_DESTINATIONS)
def test_safe_next_path_rejects_external_or_ambiguous_destinations(web_app, candidate):
    with web_app.app.test_request_context('/'):
        assert web_app.safe_next_path(candidate) == '/'


@pytest.mark.parametrize('candidate', INTERNAL_DESTINATIONS)
def test_safe_next_path_preserves_components_across_login_hops(web_app, candidate):
    with web_app.app.test_request_context('/'):
        normalized = web_app.safe_next_path(candidate)
        assert normalized == candidate
        assert web_app.safe_next_path(normalized) == candidate
    parsed = urlsplit(normalized)
    assert not parsed.scheme
    assert not parsed.netloc
    assert parsed.path.startswith('/')
    assert not parsed.path.startswith('//')


def test_return_url_cannot_turn_query_data_into_extra_parameters(web_app):
    candidate = '/history?q=alice%26role%3Dadmin%23fragment#actual-fragment'
    with web_app.app.test_request_context('/'):
        parsed = urlsplit(web_app.safe_next_path(candidate))
    assert parse_qs(parsed.query) == {'q': ['alice&role=admin#fragment']}
    assert parsed.fragment == 'actual-fragment'


@pytest.fixture
def auth_client(client, web_app):
    web_app.app.config['AUTH_REQUIRED'] = True
    web_app.save_auth_credentials('operator', 'correct-horse-battery-staple')
    return client


@pytest.mark.parametrize('candidate', UNTRUSTED_DESTINATIONS)
def test_already_authenticated_login_uses_only_internal_locations(auth_client, candidate):
    auth_client.get('/login')
    auth_client.post(
        '/login',
        data={
            'csrf_token': _csrf_token(auth_client),
            'username': 'operator',
            'password': 'correct-horse-battery-staple',
        },
    )
    response = auth_client.get('/login', query_string={'next': candidate})
    assert response.status_code == 302
    assert response.headers['Location'] == '/'


@pytest.mark.parametrize('candidate', INTERNAL_DESTINATIONS)
def test_successful_and_existing_login_preserve_internal_destination(auth_client, candidate):
    page = auth_client.get('/login', query_string={'next': candidate})
    assert page.status_code == 200
    response = auth_client.post(
        '/login',
        data={
            'csrf_token': _csrf_token(auth_client),
            'next': candidate,
            'username': 'operator',
            'password': 'correct-horse-battery-staple',
        },
    )
    assert response.status_code == 302
    assert response.headers['Location'] == candidate
    existing_session = auth_client.get('/login', query_string={'next': candidate})
    assert existing_session.status_code == 302
    assert existing_session.headers['Location'] == candidate
