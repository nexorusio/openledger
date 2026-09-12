"""A DNS outage must never enter the pipeline as an absent account."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiodns
import pytest

from maigret.checking import AiodnsDomainResolver, process_site_result
from maigret.result import MaigretCheckStatus
from maigret.sites import MaigretSite
from maigret.web.pipeline_evidence import normalize_status


async def checked(error, monkeypatch):
    query = AsyncMock(side_effect=error) if error else AsyncMock(
        return_value=[SimpleNamespace(host='192.0.2.1')]
    )
    monkeypatch.setattr(aiodns, 'DNSResolver', lambda **kwargs: SimpleNamespace(query=query))
    resolver = AiodnsDomainResolver(logger=Mock())
    resolver.prepare('synthetic.example.test')
    response = await resolver.check()
    query.assert_awaited_once_with('synthetic.example.test', 'A')
    site = MaigretSite('Synthetic DNS', {
        'url': 'https://{username}.example.test',
        'urlMain': 'https://example.test',
        'checkType': 'status_code',
        'usernameClaimed': 'synthetic',
        'usernameUnclaimed': 'absent',
    })
    result = process_site_result(response, Mock(), Mock(), {
        'username': 'synthetic', 'parsing_enabled': False,
        'url_user': 'https://synthetic.example.test',
    }, site)
    return response, result['status'], normalize_status(result)[0]


@pytest.mark.asyncio
@pytest.mark.parametrize('code', [aiodns.error.ARES_ENOTFOUND, aiodns.error.ARES_ENODATA])
async def test_authoritative_negative_dns_remains_absent(code, monkeypatch):
    response, detector, normalized = await checked(aiodns.error.DNSError(code, 'negative answer'), monkeypatch)
    assert response == ('', 404, None)
    assert detector.status == MaigretCheckStatus.AVAILABLE
    assert normalized == 'not_found'


@pytest.mark.asyncio
@pytest.mark.parametrize('code', [
    aiodns.error.ARES_ETIMEOUT, aiodns.error.ARES_ESERVFAIL,
    aiodns.error.ARES_EREFUSED, aiodns.error.ARES_ECONNREFUSED,
    aiodns.error.ARES_EBADRESP, aiodns.error.ARES_EOF,
    aiodns.error.ARES_ECANCELLED,
])
async def test_failed_resolution_remains_unknown_through_detector_and_pipeline(code, monkeypatch):
    response, detector, normalized = await checked(aiodns.error.DNSError(code, 'synthetic resolver failure'), monkeypatch)
    assert response[1] == 0 and response[2] is not None
    assert detector.status == MaigretCheckStatus.UNKNOWN
    assert normalized == ('timeout' if code == aiodns.error.ARES_ETIMEOUT else 'error')


@pytest.mark.asyncio
@pytest.mark.parametrize('error', [TimeoutError('deadline'), OSError('network unavailable')])
async def test_python_network_errors_do_not_become_negative(error, monkeypatch):
    response, detector, normalized = await checked(error, monkeypatch)
    assert response[1] == 0 and response[2] is not None
    assert detector.status == MaigretCheckStatus.UNKNOWN
    assert normalized == ('timeout' if isinstance(error, TimeoutError) else 'error')


@pytest.mark.asyncio
async def test_successful_dns_answer_still_reaches_pipeline(monkeypatch):
    response, detector, normalized = await checked(None, monkeypatch)
    assert response == ('192.0.2.1', 200, None)
    assert detector.status == MaigretCheckStatus.CLAIMED
    assert normalized == 'found'
