"""Smoke tests for the Flask web interface in maigret.web.app.

The goal is to catch breakage in the basic user flow (render index, kick off
search, redirect to results) without making real network calls. Heavy maigret
internals are mocked; the report-generation smoke test keeps `save_graph_report`
unmocked so regressions like `nt.options.groups = ...` (AttributeError on a
plain dict) are caught automatically.
"""

import asyncio
import json
import os
import types

import pytest

import maigret
import maigret.report
import maigret.settings
from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web import app as web_app_module
from maigret.web.case_store import CaseStore

CUR_PATH = os.path.dirname(os.path.realpath(__file__))
TEST_DB = os.path.join(CUR_PATH, 'db.json')


class _SyncThread:
    """Drop-in for threading.Thread that runs target synchronously on start()."""

    def __init__(self, target=None, args=(), kwargs=None, **_):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}

    def start(self):
        self._target(*self._args, **self._kwargs)


@pytest.fixture
def web_app(tmp_path):
    web_app_module.app.config['TESTING'] = True
    web_app_module.app.config['REPORTS_FOLDER'] = str(tmp_path)
    web_app_module.app.config['MAIGRET_DB_FILE'] = TEST_DB
    web_app_module.app.config['SETTINGS_FILE'] = str(tmp_path / 'web_settings.json')
    web_app_module.app.config['OPENAI_API_KEY_FILE'] = str(
        tmp_path / 'secrets' / 'openai_api_key'
    )
    web_app_module.app.config['AUTH_FILE'] = str(tmp_path / 'secrets' / 'auth.json')
    web_app_module.app.config['AUTH_REQUIRED'] = False

    web_app_module.background_jobs.clear()
    web_app_module.job_results.clear()
    web_app_module.analysis_locks.clear()
    web_app_module.live_jobs.clear()
    web_app_module.login_attempts.clear()

    yield web_app_module

    web_app_module.background_jobs.clear()
    web_app_module.job_results.clear()
    web_app_module.analysis_locks.clear()
    web_app_module.live_jobs.clear()
    web_app_module.login_attempts.clear()
    web_app_module.app.config['AUTH_REQUIRED'] = False


@pytest.fixture
def client(web_app):
    return web_app.app.test_client()


def test_index_renders(client):
    resp = client.get('/')
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'name="identifier_type"' in body
    assert 'name="identifier_value"' in body
    assert '<form' in body


def test_index_kpis_summarize_saved_investigations(client, web_app, tmp_path):
    assessed_folder = tmp_path / 'search_assessed'
    assessed_folder.mkdir()
    (assessed_folder / 'ai_analysis.md').write_text('assessment', encoding='utf-8')
    web_app.job_results.update(
        {
            'assessed': {
                'status': 'completed',
                'session_folder': 'search_assessed',
                'found_count': 7,
            },
            'failed': {
                'status': 'failed',
                'session_folder': 'search_failed',
                'found_count': 0,
            },
        }
    )

    body = client.get('/').get_data(as_text=True)

    assert 'Saved investigations' in body
    assert '1 completed · 1 failed' in body
    assert 'Profiles discovered' in body
    assert '>7</strong>' in body
    assert 'AI assessments' in body
    assert '>1</strong>' in body
    assert 'Investigation flow' not in body


def test_username_input_strips_platform_at_prefix(web_app):
    assert web_app.parse_usernames({'usernames': '@mastercorbuzier, soxoj'}) == [
        'mastercorbuzier',
        'soxoj',
    ]


def test_country_filter_keeps_country_and_global_sources(web_app):
    sites = {
        'Indonesia Local': types.SimpleNamespace(tags=['id', 'social']),
        'Global Platform': types.SimpleNamespace(tags=['global', 'social']),
        'Unscoped Major Platform': types.SimpleNamespace(tags=['social']),
        'US Local': types.SimpleNamespace(tags=['us', 'social']),
    }

    class FakeDatabase:
        def ranked_sites_dict(self, **kwargs):
            assert kwargs['tags'] == ['social']
            return sites

    selected = web_app.select_sites_for_search(
        FakeDatabase(),
        top_sites=500,
        all_sites=False,
        tags=['social', 'id'],
        excluded_tags=[],
        site_list=[],
    )

    assert list(selected) == [
        'Indonesia Local',
        'Global Platform',
        'Unscoped Major Platform',
    ]


def test_healthz(client):
    resp = client.get('/healthz')
    assert resp.status_code == 200
    assert resp.get_json() == {'status': 'ok'}


def _csrf_token(client):
    with client.session_transaction() as browser_session:
        token = browser_session.get('csrf_token')
    if not token:
        client.get('/')
        with client.session_transaction() as browser_session:
            token = browser_session['csrf_token']
    return token


def test_typed_investigation_builder_creates_a_grouped_query_plan(
    client, web_app, monkeypatch
):
    captured = {}

    def fake_start(usernames, options):
        captured['usernames'] = usernames
        captured['options'] = options
        return 'typed-plan'

    monkeypatch.setattr(web_app, 'start_live_job', fake_start)
    response = client.post(
        '/live',
        data={
            'csrf_token': _csrf_token(client),
            'identifier_type': ['full_name', 'social_handle', 'email', 'phone'],
            'identifier_value': [
                'Jati Pratomo',
                '@jatipratomo',
                'jati@example.com',
                '+62 812 3456 789',
            ],
            'processing_mode': 'same_subject',
            'generate_name_variants': 'on',
            'allow_ai_context': 'on',
            'include_terms': 'Jakarta, urban planning',
            'exclude_terms': 'fan page, football',
            'mode': 'fast',
        },
    )

    assert response.status_code == 302
    assert response.location.endswith('/live/typed-plan')
    assert captured['usernames'][0:4] == [
        'jatipratomo',
        'jati.pratomo',
        'jati_pratomo',
        'jati-pratomo',
    ]
    spec = captured['options']['investigation_spec']
    assert spec['processing_mode'] == 'same_subject'
    assert spec['subject_label'] == 'Jati Pratomo'
    assert spec['allow_ai_context'] is True
    assert spec['exclude_terms'] == ['fan page', 'football']
    assert 'jati@example.com' not in captured['usernames']
    assert '+628123456789' not in captured['usernames']


def test_context_only_investigation_is_rejected_before_queueing(
    client, web_app, monkeypatch
):
    monkeypatch.setattr(
        web_app,
        'start_live_job',
        lambda *args, **kwargs: pytest.fail('invalid plan must not be queued'),
    )
    response = client.post(
        '/live',
        data={
            'csrf_token': _csrf_token(client),
            'identifier_type': ['email', 'phone'],
            'identifier_value': ['jati@example.com', '+628123456789'],
            'processing_mode': 'same_subject',
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'retained as context' in body


def test_investigation_builder_explains_identifier_capabilities(client):
    body = client.get('/').get_data(as_text=True)

    assert 'Known identifiers' in body
    assert 'A full name may contain spaces' in body
    assert 'Phone and email values are never permuted' in body
    assert 'Exclude terms' in body
    assert 'Query plan' in body


def test_application_login_replaces_browser_authentication(client, web_app):
    web_app.app.config['AUTH_REQUIRED'] = True
    web_app.save_auth_credentials('operator', 'correct-horse-battery-staple')

    protected = client.get('/history')
    assert protected.status_code == 302
    assert '/login?next=/history' in protected.location
    assert 'WWW-Authenticate' not in protected.headers
    assert client.get('/healthz').status_code == 200
    assert client.post('/api/analysis/unknown').status_code == 401

    login_page = client.get('/login?next=/history')
    assert login_page.status_code == 200
    body = login_page.get_data(as_text=True)
    assert 'Sign in to OpenLedger' in body
    assert 'name="username"' in body
    assert 'name="password"' in body

    response = client.post(
        '/login',
        data={
            'csrf_token': _csrf_token(client),
            'next': '/history',
            'username': 'operator',
            'password': 'correct-horse-battery-staple',
        },
    )
    assert response.status_code == 302
    assert response.location.endswith('/history')
    assert client.get('/history').status_code == 200


def test_login_rejects_external_redirects_and_invalid_credentials(client, web_app):
    web_app.app.config['AUTH_REQUIRED'] = True
    web_app.save_auth_credentials('operator', 'correct-horse-battery-staple')
    client.get('/login?next=https://evil.example/steal')

    response = client.post(
        '/login',
        data={
            'csrf_token': _csrf_token(client),
            'next': 'https://evil.example/steal',
            'username': 'operator',
            'password': 'incorrect-password',
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert 'Invalid username or password.' in response.get_data(as_text=True)
    assert response.request.path == '/login'


def test_password_change_is_protected_and_invalidates_other_sessions(web_app):
    web_app.app.config['AUTH_REQUIRED'] = True
    web_app.save_auth_credentials('operator', 'correct-horse-battery-staple')
    first_client = web_app.app.test_client()
    second_client = web_app.app.test_client()

    for browser in (first_client, second_client):
        browser.get('/login')
        response = browser.post(
            '/login',
            data={
                'csrf_token': _csrf_token(browser),
                'username': 'operator',
                'password': 'correct-horse-battery-staple',
            },
        )
        assert response.status_code == 302

    response = first_client.post(
        '/security',
        data={
            'csrf_token': _csrf_token(first_client),
            'current_password': 'correct-horse-battery-staple',
            'new_password': 'a-new-long-operator-password',
            'confirm_password': 'a-new-long-operator-password',
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert 'Password changed successfully.' in response.get_data(as_text=True)
    assert first_client.get('/security').status_code == 200
    assert second_client.get('/history').location.startswith('/login')

    auth_path = web_app.app.config['AUTH_FILE']
    assert os.stat(auth_path).st_mode & 0o777 == 0o600
    credentials = web_app.load_auth_credentials()
    assert web_app.verify_password(
        'a-new-long-operator-password', credentials['password']
    )
    assert not web_app.verify_password(
        'correct-horse-battery-staple', credentials['password']
    )


def test_logout_requires_csrf_and_ends_session(client, web_app):
    web_app.app.config['AUTH_REQUIRED'] = True
    web_app.save_auth_credentials('operator', 'correct-horse-battery-staple')
    client.get('/login')
    client.post(
        '/login',
        data={
            'csrf_token': _csrf_token(client),
            'username': 'operator',
            'password': 'correct-horse-battery-staple',
        },
    )

    response = client.post('/logout', data={'csrf_token': _csrf_token(client)})
    assert response.status_code == 302
    assert response.location.endswith('/login')
    assert client.get('/history').location.startswith('/login')


def test_search_empty_input_redirects_to_index(client):
    resp = client.post('/search', data={'usernames': ''})
    assert resp.status_code == 302
    assert resp.location.rstrip('/').endswith('') or resp.location.endswith('/')


def test_search_redirects_to_status(client, web_app, monkeypatch):
    monkeypatch.setattr(web_app, 'process_search_task', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    resp = client.post('/search', data={'usernames': 'soxoj'})

    assert resp.status_code == 302
    assert '/status/' in resp.location


def test_invalid_timestamp_redirects_to_index(client):
    resp = client.get('/status/nonexistent_ts')
    assert resp.status_code == 302
    assert resp.location.endswith('/')


def test_status_running_renders_status_page(client, web_app, monkeypatch):
    """While the background job is still running, /status/<ts> returns 200."""

    def never_completes(usernames, options, timestamp):
        # leave background_jobs[timestamp]['completed'] as False
        pass

    monkeypatch.setattr(web_app, 'process_search_task', never_completes)
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    post = client.post('/search', data={'usernames': 'soxoj'})
    status_resp = client.get(post.location)

    assert status_resp.status_code == 200


def test_completed_search_redirects_to_results(client, web_app, monkeypatch):
    """Happy path: POST /search → background completes → /status/<ts> → /results/<session>."""

    def fake_task(usernames, options, timestamp):
        web_app.job_results[timestamp] = {
            'status': 'completed',
            'session_folder': f'search_{timestamp}',
            'graph_file': f'search_{timestamp}/combined_graph.html',
            'usernames': usernames,
            'individual_reports': [],
        }
        web_app.background_jobs[timestamp]['completed'] = True

    monkeypatch.setattr(web_app, 'process_search_task', fake_task)
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    post = client.post('/search', data={'usernames': 'soxoj'})
    assert post.status_code == 302

    status_resp = client.get(post.location)
    assert status_resp.status_code == 302
    assert '/results/search_' in status_resp.location

    results_resp = client.get(status_resp.location)
    assert results_resp.status_code == 200
    assert b'soxoj' in results_resp.data


def test_results_report_links_open_in_new_tab(client, web_app, monkeypatch):
    """CSV/JSON/PDF/HTML report links must open in a new tab, not navigate away
    from the results page."""

    def fake_task(usernames, options, timestamp):
        web_app.job_results[timestamp] = {
            'status': 'completed',
            'session_folder': f'search_{timestamp}',
            'graph_file': f'search_{timestamp}/combined_graph.html',
            'usernames': usernames,
            'individual_reports': [
                {
                    'username': 'soxoj',
                    'csv_file': f'search_{timestamp}/report_soxoj.csv',
                    'json_file': f'search_{timestamp}/report_soxoj.json',
                    'pdf_file': f'search_{timestamp}/report_soxoj.pdf',
                    'html_file': f'search_{timestamp}/report_soxoj.html',
                    'claimed_profiles': [],
                }
            ],
        }
        web_app.background_jobs[timestamp]['completed'] = True

    monkeypatch.setattr(web_app, 'process_search_task', fake_task)
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    post = client.post('/search', data={'usernames': 'soxoj'})
    status_resp = client.get(post.location)
    results_resp = client.get(status_resp.location)
    body = results_resp.get_data(as_text=True)

    for label in ('CSV Report', 'JSON Report', 'PDF Report', 'HTML Report'):
        # crude but effective: the link and its target="_blank" must appear
        # within the same <a> tag, not just somewhere on the page.
        idx = body.index(label)
        tag_start = body.rindex('<a ', 0, idx)
        tag = body[tag_start : idx + len(label)]
        assert 'target="_blank"' in tag, f'{label} link missing target="_blank"'


def test_ai_analysis_requires_csrf_token(client, web_app, monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'server-only-test-key')
    web_app.job_results['session1'] = {
        'status': 'completed',
        'session_folder': 'search_session1',
        'graph_file': 'search_session1/combined_graph.html',
        'usernames': ['soxoj'],
        'individual_reports': [],
    }

    resp = client.post('/api/analysis/search_session1')

    assert resp.status_code == 403


def test_ai_analysis_requires_server_key(client, web_app, monkeypatch):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    web_app.job_results['session1'] = {
        'status': 'completed',
        'session_folder': 'search_session1',
        'graph_file': 'search_session1/combined_graph.html',
        'usernames': ['soxoj'],
        'individual_reports': [],
    }
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'test-csrf'

    resp = client.post(
        '/api/analysis/search_session1',
        headers={'X-OpenLedger-CSRF': 'test-csrf'},
    )

    assert resp.status_code == 503
    assert 'not configured' in resp.get_json()['error']


def test_ai_analysis_is_generated_once_and_cached(
    client, web_app, monkeypatch, tmp_path
):
    monkeypatch.setenv('OPENAI_API_KEY', 'server-only-test-key')
    calls = []

    async def fake_analysis(**kwargs):
        calls.append(kwargs)
        return {
            'analysis': '# Assessment\n\nOne claimed profile requires verification.',
            'sources': [
                {'title': 'Official profile', 'url': 'https://example.com/profile'}
            ],
        }

    async def fake_proposals(**kwargs):
        return []

    monkeypatch.setattr(web_app, 'get_enriched_ai_analysis', fake_analysis)
    monkeypatch.setattr(web_app, 'get_ai_evidence_proposals', fake_proposals)
    web_app.job_results['session1'] = {
        'status': 'completed',
        'session_folder': 'search_session1',
        'graph_file': 'search_session1/combined_graph.html',
        'usernames': ['soxoj'],
        'individual_reports': [
            {
                'username': 'soxoj',
                'claimed_profiles': [
                    {
                        'site_name': 'GitHub',
                        'url': 'https://github.com/soxoj',
                        'tags': ['coding'],
                    }
                ],
            }
        ],
    }
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'test-csrf'

    legacy_directory = tmp_path / 'search_session1'
    legacy_directory.mkdir()
    (legacy_directory / 'ai_analysis.md').write_text(
        '# Legacy assessment\n\nUnknown subject.', encoding='utf-8'
    )

    first = client.post(
        '/api/analysis/search_session1',
        headers={'X-OpenLedger-CSRF': 'test-csrf'},
    )
    second = client.post(
        '/api/analysis/search_session1',
        headers={'X-OpenLedger-CSRF': 'test-csrf'},
    )

    assert first.status_code == 200
    assert first.get_json()['cached'] is False
    assert second.status_code == 200
    assert second.get_json()['cached'] is True
    assert len(calls) == 1
    assert 'GitHub' in calls[0]['investigation_evidence']
    assert calls[0]['web_search_enabled'] is True
    assert first.get_json()['sources'][0]['title'] == 'Official profile'
    assert first.get_json()['proposal_status'] == 'storage_unavailable'
    saved = tmp_path / 'search_session1' / 'ai_analysis.md'
    assert saved.exists()
    assert 'requires verification' in saved.read_text(encoding='utf-8')


def test_ai_analysis_creates_pending_cited_proposals_and_preserves_rejection(
    client, web_app, monkeypatch, tmp_path
):
    monkeypatch.setenv('OPENAI_API_KEY', 'server-only-test-key')
    store = CaseStore(f"sqlite:///{tmp_path / 'cases.db'}", create_schema=True)
    monkeypatch.setattr(web_app, 'case_store', store)
    job_id = store.create_investigation(['alice'], {})
    store.claim_next('worker:test')
    result = {
        'status': 'completed',
        'session_folder': f'search_{job_id}',
        'graph_file': f'search_{job_id}/combined_graph.html',
        'usernames': ['alice'],
        'individual_reports': [],
    }
    store.finish(job_id, result)
    web_app.job_results[job_id] = result

    async def fake_analysis(**kwargs):
        return {
            'analysis': '# Assessment\n\nThe official biography supports the identity.',
            'sources': [
                {
                    'title': 'Official biography',
                    'url': 'https://example.test/alice',
                }
            ],
        }

    async def fake_proposals(**kwargs):
        return [
            {
                'username': 'alice',
                'field_name': 'full_name',
                'value': 'Alice Example',
                'confidence': 82,
                'source_url': 'https://example.test/alice',
                'source_title': 'Official biography',
                'reason': 'The official biography identifies Alice Example.',
            },
            {
                'username': 'alice',
                'field_name': 'summary',
                'value': 'Alice Example is a research engineer.',
                'confidence': 75,
                'source_url': 'https://example.test/alice',
                'source_title': 'Official biography',
                'reason': 'The cited biography supports this concise summary.',
            },
        ]

    monkeypatch.setattr(web_app, 'get_enriched_ai_analysis', fake_analysis)
    monkeypatch.setattr(web_app, 'get_ai_evidence_proposals', fake_proposals)
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'test-csrf'

    first = client.post(
        f'/api/analysis/search_{job_id}',
        headers={'X-OpenLedger-CSRF': 'test-csrf'},
    )
    assert first.status_code == 200
    assert first.get_json()['proposal_count'] == 2
    assert first.get_json()['proposal_status'] == 'pending_review'
    persona_id = store.get_case(store.get_job(job_id)['case_id'])['personas'][0]['id']
    persona = store.get_persona(persona_id)
    claim = next(
        item for item in persona['claims'] if item['field_name'] == 'full_name'
    )
    assert claim['review_status'] == 'pending'
    assert claim['source_engine'] == 'openai_web_research'
    summary = next(
        item for item in persona['claims'] if item['field_name'] == 'summary'
    )
    assert summary['review_status'] == 'pending'
    persona_page = client.get(f'/personas/{persona_id}').get_data(as_text=True)
    assert 'Alice Example is a research engineer.' in persona_page
    assert '2 accepted proposals' in persona_page
    assert '1 source' in persona_page

    store.review_claim(claim['id'], 'rejected', 'analyst', 'False attribution')
    cached = client.post(
        f'/api/analysis/search_{job_id}',
        headers={'X-OpenLedger-CSRF': 'test-csrf'},
    )
    assert cached.status_code == 200
    assert cached.get_json()['cached'] is True
    refreshed_claim = next(
        item
        for item in store.get_persona(persona_id)['claims']
        if item['field_name'] == 'full_name'
    )
    assert refreshed_claim['review_status'] == 'rejected'
    store.dispose()


def test_ai_assessment_survives_structured_proposal_failure(
    client, web_app, monkeypatch, tmp_path
):
    monkeypatch.setenv('OPENAI_API_KEY', 'server-only-test-key')
    web_app.job_results['session1'] = {
        'status': 'completed',
        'session_folder': 'search_session1',
        'graph_file': 'search_session1/combined_graph.html',
        'usernames': ['alice'],
        'individual_reports': [],
    }

    async def fake_analysis(**kwargs):
        return {
            'analysis': '# Assessment\n\nCited narrative remains useful.',
            'sources': [
                {'title': 'Source', 'url': 'https://example.test/alice'}
            ],
        }

    async def failing_proposals(**kwargs):
        raise RuntimeError('structured output unavailable')

    monkeypatch.setattr(web_app, 'get_enriched_ai_analysis', fake_analysis)
    monkeypatch.setattr(web_app, 'get_ai_evidence_proposals', failing_proposals)
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'test-csrf'

    response = client.post(
        '/api/analysis/search_session1',
        headers={'X-OpenLedger-CSRF': 'test-csrf'},
    )

    assert response.status_code == 200
    assert response.get_json()['proposal_status'] == 'unavailable'
    assert 'Cited narrative' in response.get_json()['analysis']
    metadata = json.loads(
        (tmp_path / 'search_session1' / 'ai_analysis.json').read_text(
            encoding='utf-8'
        )
    )
    assert metadata['proposal_status'] == 'unavailable'


def test_failed_task_redirects_to_index(client, web_app, monkeypatch):
    def failing_task(usernames, options, timestamp):
        web_app.job_results[timestamp] = {'status': 'failed', 'error': 'boom'}
        web_app.background_jobs[timestamp]['completed'] = True

    monkeypatch.setattr(web_app, 'process_search_task', failing_task)
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    post = client.post('/search', data={'usernames': 'soxoj'})
    status_resp = client.get(post.location)

    assert status_resp.status_code == 302
    assert status_resp.location.endswith('/')


def test_download_report_serves_file_inside_reports_folder(client, web_app, tmp_path):
    """Happy path: a real file inside REPORTS_FOLDER is served back."""
    target = tmp_path / 'session1'
    target.mkdir()
    (target / 'report.json').write_text('{"ok": true}')

    resp = client.get('/reports/session1/report.json')

    assert resp.status_code == 200
    assert resp.get_data() == b'{"ok": true}'


def test_download_report_blocks_dotdot_traversal(client, web_app, tmp_path):
    """A literal ../ in the path must not escape REPORTS_FOLDER."""
    secret = tmp_path.parent / 'outside_secret.txt'
    secret.write_text('SECRET')

    resp = client.get('/reports/..%2Foutside_secret.txt')

    assert resp.status_code == 404
    assert b'SECRET' not in resp.get_data()


def test_download_report_blocks_sibling_prefix_bypass(client, web_app, tmp_path):
    """Regression: the previous startswith() check let `<reports_root>2/secret`
    bypass containment because '/tmp/maigret_reports2'.startswith('/tmp/maigret_reports')
    is True. send_from_directory enforces a real boundary."""
    sibling = tmp_path.parent / (tmp_path.name + '_sibling')
    sibling.mkdir()
    (sibling / 'leak.txt').write_text('LEAK')

    encoded = '..%2F' + sibling.name + '%2Fleak.txt'
    resp = client.get('/reports/' + encoded)

    assert resp.status_code == 404
    assert b'LEAK' not in resp.get_data()


def test_download_report_blocks_absolute_path(client, web_app, tmp_path):
    """An absolute filename must not escape REPORTS_FOLDER."""
    secret = tmp_path.parent / 'abs_secret.txt'
    secret.write_text('ABSOLUTE')

    resp = client.get('/reports/' + str(secret).lstrip('/'))

    assert resp.status_code == 404
    assert b'ABSOLUTE' not in resp.get_data()


def test_search_passes_cloudflare_bypass_from_settings(client, web_app, monkeypatch):
    """If settings.json enables cloudflare_bypass with a valid FlareSolverr module,
    the web search must forward that config to maigret.search via the
    cloudflare_bypass kwarg. Guards the wiring in maigret_search()."""

    captured = {}

    async def fake_search(*args, **kwargs):
        captured.update(kwargs)
        return {}

    def fake_load(self, paths=None):
        self.cloudflare_bypass = {
            "enabled": True,
            "session_prefix": "test-prefix",
            "trigger_protection": ["cf_js_challenge"],
            "modules": [
                {
                    "name": "flaresolverr",
                    "method": "json_api",
                    "url": "http://flare.test:8191/v1",
                    "max_timeout_ms": 60000,
                }
            ],
        }
        return True, ""

    monkeypatch.setattr(maigret.settings.Settings, 'load', fake_load)
    monkeypatch.setattr(maigret, 'search', fake_search)
    monkeypatch.setattr(maigret.report, 'save_graph_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    client.post('/search', data={'usernames': 'testuser'})

    assert (
        'cloudflare_bypass' in captured
    ), 'maigret.search was not given a cloudflare_bypass kwarg'
    cf = captured['cloudflare_bypass']
    assert cf is not None
    assert cf['session_prefix'] == 'test-prefix'
    assert cf['trigger_protection'] == ['cf_js_challenge']
    assert len(cf['modules']) == 1
    assert cf['modules'][0]['url'] == 'http://flare.test:8191/v1'
    assert cf['modules'][0]['method'] == 'json_api'


def test_search_omits_cloudflare_bypass_when_disabled(client, web_app, monkeypatch):
    """When settings has no cloudflare_bypass (or enabled=false), the kwarg
    must be None so the default checker pipeline runs."""

    captured = {}

    async def fake_search(*args, **kwargs):
        captured.update(kwargs)
        return {}

    def fake_load(self, paths=None):
        # no cloudflare_bypass attribute at all
        return True, ""

    monkeypatch.setattr(maigret.settings.Settings, 'load', fake_load)
    monkeypatch.setattr(maigret, 'search', fake_search)
    monkeypatch.setattr(maigret.report, 'save_graph_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    client.post('/search', data={'usernames': 'testuser'})

    assert captured.get('cloudflare_bypass') is None


def test_live_scan_streams_found_and_done(client, web_app, monkeypatch):
    """POST /api/scan starts a background scan; GET .../stream yields the per-site
    'found' event and a terminating 'done' event. Guards the SSE + StreamNotify wiring.
    """

    async def fake_search(*args, **kwargs):
        notify = kwargs['query_notify']
        result = MaigretCheckResult(
            username='soxoj',
            site_name='GitHub',
            site_url_user='https://github.com/soxoj',
            status=MaigretCheckStatus.CLAIMED,
            ids_data={'fullname': 'Soxoj', '_extractor': 'x'},
            tags=['dev'],
        )
        notify.update(result)
        return {'GitHub': {'status': result, 'url_user': result.site_url_user}}

    monkeypatch.setattr(maigret, 'search', fake_search)
    # csv/json/pdf report internals are exercised by
    # test_real_report_generation_does_not_crash; here we only care that a
    # completed live scan wires into the same report + results-page flow.
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})

    client.get('/')
    start = client.post(
        '/api/scan',
        data={'usernames': 'soxoj'},
        headers={'X-OpenLedger-CSRF': _csrf_token(client)},
    )
    assert start.status_code == 200
    job_id = start.get_json()['job_id']

    body = client.get(f'/api/scan/{job_id}/stream').get_data(as_text=True)
    events = [
        json.loads(line[6:]) for line in body.splitlines() if line.startswith('data: ')
    ]
    types_seen = [e['type'] for e in events]

    assert 'done' in types_seen
    found = [e for e in events if e['type'] == 'found']
    assert found and found[0]['site'] == 'GitHub'
    # _extractor metadata is stripped from the graph payload
    assert '_extractor' not in found[0]['ids']
    assert found[0]['ids']['fullname'] == 'Soxoj'

    # Regression guard: a completed live scan must still produce the same
    # report files + profile list as the classic /search flow, and hand the
    # browser a redirect to the results page that shows them.
    done_event = next(e for e in events if e['type'] == 'done')
    assert done_event['redirect'] == f'/results/search_{job_id}'

    result = web_app.job_results[job_id]
    assert result['status'] == 'completed'
    reports = result['individual_reports']
    assert reports and reports[0]['username'] == 'soxoj'
    assert reports[0]['claimed_profiles'][0]['site_name'] == 'GitHub'

    results_page = client.get(done_event['redirect']).get_data(as_text=True)
    assert 'GitHub' in results_page
    assert 'CSV Report' in results_page


def test_live_scan_empty_username_rejected(client, web_app):
    client.get('/')
    resp = client.post(
        '/api/scan',
        data={'usernames': ''},
        headers={'X-OpenLedger-CSRF': _csrf_token(client)},
    )
    assert resp.status_code == 400


def test_live_scan_start_requires_csrf(client, web_app):
    resp = client.post('/api/scan', data={'usernames': 'soxoj'})
    assert resp.status_code == 403


def test_live_scan_stop_unknown_job_404(client, web_app):
    resp = client.post('/api/scan/nope/stop')
    assert resp.status_code == 404


def test_live_start_empty_username_redirects_to_index(client, web_app):
    resp = client.post('/live', data={'usernames': ''})
    assert resp.status_code == 302
    assert resp.location.endswith('/')


def test_live_start_redirects_to_dedicated_live_page(client, web_app, monkeypatch):
    """POST /live starts a job on a NEW page (/live/<job_id>), not inline on
    the index page. That page must show the graph + a Stop button, and must
    NOT unconditionally redirect away on completion (only via the Open reports
    button — see test_live_scan_done_event_offers_redirect_not_auto_navigation)."""

    async def fake_search(*args, **kwargs):
        notify = kwargs['query_notify']
        notify.set_total(0)
        return {}

    monkeypatch.setattr(maigret, 'search', fake_search)

    client.get('/')
    start = client.post(
        '/live',
        data={'usernames': 'soxoj', 'csrf_token': _csrf_token(client)},
    )
    assert start.status_code == 302
    assert start.location.startswith('/live/')
    job_id = start.location.rsplit('/', 1)[1]

    page = client.get(start.location)
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert 'id="graph"' in body
    assert 'id="stopBtn"' in body
    assert 'id="reportsBtn"' in body
    assert 'Open reports' in body
    assert job_id in body
    # No unconditional navigation on completion anymore.
    assert 'window.location.href = ev.redirect' not in body

    # Drain the SSE stream so the background thread's queue is consumed and
    # the job entry is cleaned up tidily.
    client.get(f'/api/scan/{job_id}/stream')


def test_live_results_unknown_job_redirects_to_index(client, web_app):
    resp = client.get('/live/does-not-exist')
    assert resp.status_code == 302
    assert resp.location.endswith('/')


def test_live_results_for_finished_job_skips_sse_and_shows_reports(client, web_app):
    """If the job already finished (e.g. the user reloaded the Live Results
    page), the page must offer the Open reports redirect immediately instead of
    trying to reopen a dead SSE stream."""
    web_app.job_results['finishedjob'] = {
        'status': 'completed',
        'session_folder': 'search_finishedjob',
        'graph_file': 'search_finishedjob/combined_graph.html',
        'usernames': ['soxoj'],
        'individual_reports': [],
        'found_count': 0,
    }

    resp = client.get('/live/finishedjob')
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'const doneRedirect = "/results/search_finishedjob";' in body


def test_live_scan_done_event_offers_redirect_not_auto_navigation(
    client, web_app, monkeypatch
):
    """The SSE 'done' payload still carries the redirect URL (consumed by the
    Open reports button), but nothing server- or client-side forces navigation."""

    async def fake_search(*args, **kwargs):
        notify = kwargs['query_notify']
        result = MaigretCheckResult(
            username='soxoj',
            site_name='GitHub',
            site_url_user='https://github.com/soxoj',
            status=MaigretCheckStatus.CLAIMED,
            ids_data={},
        )
        notify.update(result)
        return {'GitHub': {'status': result, 'url_user': result.site_url_user}}

    monkeypatch.setattr(maigret, 'search', fake_search)
    monkeypatch.setattr(maigret.report, 'save_graph_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})

    client.get('/')
    start = client.post(
        '/live',
        data={'usernames': 'soxoj', 'csrf_token': _csrf_token(client)},
    )
    job_id = start.location.rsplit('/', 1)[1]

    body = client.get(f'/api/scan/{job_id}/stream').get_data(as_text=True)
    events = [
        json.loads(line[6:]) for line in body.splitlines() if line.startswith('data: ')
    ]
    done_event = next(e for e in events if e['type'] == 'done')
    assert done_event['redirect'] == f'/results/search_{job_id}'

    result = web_app.job_results[job_id]
    assert result['status'] == 'completed'
    assert result['found_count'] == 1
    assert 'started_at' in result


def test_live_scan_stop_mid_scan_keeps_already_found_results(
    client, web_app, monkeypatch
):
    """Regression: clicking Stop while a username's scan is still in-flight
    used to discard every 'found' result already streamed to the live graph,
    because the cancelled search() task never returns its own results dict —
    general_results stayed empty, build_reports never ran, and the browser
    got 'Completed — nothing to analyze.' despite the graph showing hits.

    StreamNotify now keeps a running copy of what it already streamed, and
    that's what gets reported when the task is cancelled mid-scan.
    """

    async def fake_search(*args, **kwargs):
        notify = kwargs['query_notify']
        assert 'ValidActive' in notify.sites, 'site map not wired into StreamNotify'
        found = MaigretCheckResult(
            username='soxoj',
            site_name='ValidActive',
            site_url_user='https://play.google.com/store/apps/developer?id=soxoj',
            status=MaigretCheckStatus.CLAIMED,
        )
        notify.update(found)
        # Simulate task.cancel() firing mid-scan, after this one site was
        # already checked and streamed to the browser but before the other
        # (still in-flight) sites finished.
        raise asyncio.CancelledError()

    monkeypatch.setattr(maigret, 'search', fake_search)
    monkeypatch.setattr(maigret.report, 'save_graph_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})

    client.get('/')
    start = client.post(
        '/live',
        data={'usernames': 'soxoj', 'csrf_token': _csrf_token(client)},
    )
    job_id = start.location.rsplit('/', 1)[1]

    body = client.get(f'/api/scan/{job_id}/stream').get_data(as_text=True)
    events = [
        json.loads(line[6:]) for line in body.splitlines() if line.startswith('data: ')
    ]
    types_seen = [e['type'] for e in events]
    assert 'stopped' in types_seen
    found = [e for e in events if e['type'] == 'found']
    assert found and found[0]['site'] == 'ValidActive'

    done_event = next(e for e in events if e['type'] == 'done')
    assert (
        done_event.get('redirect') == f'/results/search_{job_id}'
    ), "Stop must not discard already-found results ('nothing to analyze' bug)"

    result = web_app.job_results[job_id]
    assert result['status'] == 'completed'
    assert result['found_count'] == 1
    assert result['individual_reports'][0]['claimed_profiles'][0]['site_name'] == (
        'ValidActive'
    )


def test_real_report_generation_does_not_crash(client, web_app, monkeypatch):
    """End-to-end with mocked maigret.search but REAL report generation.

    This is the regression guard for bugs inside `save_graph_report` and friends
    (e.g. `nt.options.groups = ...` raising AttributeError on a dict). If any of
    the unmocked report functions throws, the task records a failed status and
    this assertion catches it.
    """

    async def fake_search(*args, **kwargs):
        return {}

    monkeypatch.setattr(maigret, 'search', fake_search)
    # Mock the per-username report writers — they are not what we care about here,
    # and pdf/html generation pulls in xhtml2pdf which is slow and brittle.
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})
    monkeypatch.setattr(web_app, 'Thread', _SyncThread)

    post = client.post('/search', data={'usernames': 'testuser'})
    timestamp = post.location.rsplit('/', 1)[1]

    assert timestamp in web_app.job_results, 'background task did not record any result'
    result = web_app.job_results[timestamp]
    assert (
        result['status'] == 'completed'
    ), f"report generation failed: {result.get('error')!r}"

    # Regression guard: pyvis's default cdn_resources="local" writes a lib/
    # folder relative to the process cwd instead of next to the graph HTML,
    # so the browser 404s fetching lib/bindings/utils.js from /reports/...
    graph_path = os.path.join(web_app.app.config['REPORTS_FOLDER'], result['graph_file'])
    with open(graph_path, encoding='utf-8') as f:
        graph_html = f.read()
    assert 'lib/bindings' not in graph_html
    assert not os.path.exists(os.path.join(os.path.dirname(graph_path), 'lib'))


def test_history_empty_state(client, web_app):
    resp = client.get('/history')
    assert resp.status_code == 200
    assert 'No investigations yet' in resp.get_data(as_text=True)


def test_history_link_present_on_every_page(client, web_app):
    resp = client.get('/')
    body = resp.get_data(as_text=True)
    assert 'href="/history"' in body


def test_new_investigation_link_present_on_every_page(client, web_app):
    resp = client.get('/history')
    body = resp.get_data(as_text=True)
    assert 'New investigation' in body
    assert 'href="/"' in body


def test_history_lists_completed_and_failed_runs(client, web_app):
    web_app.job_results['ts_completed'] = {
        'status': 'completed',
        'session_folder': 'search_ts_completed',
        'graph_file': 'search_ts_completed/combined_graph.html',
        'usernames': ['soxoj', 'alice'],
        'individual_reports': [],
        'found_count': 7,
        'started_at': '2026-07-28 10:00:00',
    }
    web_app.job_results['ts_failed'] = {
        'status': 'failed',
        'error': 'boom',
        'usernames': ['bob'],
        'started_at': '2026-07-28 09:00:00',
    }

    resp = client.get('/history')
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)

    assert '2026-07-28 10:00:00' in body
    assert 'soxoj, alice' in body
    assert '>7<' in body
    assert 'completed' in body
    assert '/results/search_ts_completed' in body

    assert '2026-07-28 09:00:00' in body
    assert 'bob' in body
    assert 'Failed' in body

    # Newest run listed first.
    assert body.index('search_ts_completed') < body.index('bob')


def test_history_can_permanently_delete_one_investigation(client, web_app):
    result = {
        'status': 'completed',
        'session_folder': 'search_delete-me',
        'graph_file': 'search_delete-me/combined_graph.html',
        'usernames': ['soxoj'],
        'individual_reports': [],
        'found_count': 2,
        'started_at': '2026-08-27 13:00:00',
    }
    web_app.record_job_result('delete-me', result)
    session_directory = os.path.join(
        web_app.app.config['REPORTS_FOLDER'], 'search_delete-me'
    )
    report_path = os.path.join(session_directory, 'report_soxoj.json')
    with open(report_path, 'w', encoding='utf-8') as report_file:
        report_file.write('{}')

    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'delete-csrf'
    response = client.post(
        '/history/search_delete-me/delete',
        data={'csrf_token': 'delete-csrf'},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert 'permanently deleted' in response.get_data(as_text=True)
    assert not os.path.exists(session_directory)
    assert 'delete-me' not in web_app.job_results
    assert client.get('/results/search_delete-me').status_code == 302


def test_history_deletion_requires_csrf(client, web_app):
    web_app.record_job_result(
        'keep-me',
        {
            'status': 'failed',
            'session_folder': 'search_keep-me',
            'error': 'test failure',
            'usernames': ['alice'],
            'started_at': '2026-08-27 13:01:00',
        },
    )
    session_directory = os.path.join(
        web_app.app.config['REPORTS_FOLDER'], 'search_keep-me'
    )

    response = client.post(
        '/history/search_keep-me/delete',
        data={'csrf_token': 'invalid'},
    )

    assert response.status_code == 302
    assert os.path.isdir(session_directory)
    assert 'keep-me' in web_app.job_results


def test_completed_investigation_survives_process_restart(client, web_app):
    result = {
        'status': 'completed',
        'session_folder': 'search_persisted',
        'graph_file': 'search_persisted/combined_graph.html',
        'usernames': ['soxoj'],
        'individual_reports': [],
        'found_count': 4,
        'started_at': '2026-08-27 12:34:56',
    }
    web_app.record_job_result('persisted', result)

    metadata_path = web_app.get_session_metadata_path('search_persisted')
    assert os.path.exists(metadata_path)
    assert os.stat(metadata_path).st_mode & 0o777 == 0o600

    # Simulate a fresh Gunicorn worker after the container is recreated.
    web_app.job_results.clear()

    history = client.get('/history')
    assert history.status_code == 200
    assert '2026-08-27 12:34:56' in history.get_data(as_text=True)
    assert '/results/search_persisted' in history.get_data(as_text=True)

    results = client.get('/results/search_persisted')
    assert results.status_code == 200
    assert 'soxoj' in results.get_data(as_text=True)


def test_failed_investigation_survives_process_restart(client, web_app):
    web_app.record_job_result(
        'failed-persisted',
        {
            'status': 'failed',
            'error': 'report generation failed',
            'usernames': ['alice'],
            'started_at': '2026-08-27 12:35:00',
        },
    )
    web_app.job_results.clear()

    history = client.get('/history')
    body = history.get_data(as_text=True)
    assert history.status_code == 200
    assert 'alice' in body
    assert 'Failed' in body

    status = client.get('/status/failed-persisted')
    assert status.status_code == 302
    assert status.location.endswith('/history')


def test_invalid_persisted_metadata_is_ignored(client, web_app, tmp_path):
    session_directory = tmp_path / 'search_invalid'
    session_directory.mkdir()
    metadata_path = session_directory / web_app.SESSION_METADATA_FILENAME
    metadata_path.write_text('{not valid json', encoding='utf-8')

    assert web_app.refresh_job_results_from_disk() == 0
    assert 'invalid' not in web_app.job_results
    assert client.get('/history').status_code == 200


def test_internal_session_metadata_cannot_be_downloaded(client, web_app):
    web_app.record_job_result(
        'private-index',
        {
            'status': 'failed',
            'error': 'test',
            'usernames': ['soxoj'],
            'started_at': '2026-08-27 12:36:00',
        },
    )

    response = client.get(
        '/reports/search_private-index/' + web_app.SESSION_METADATA_FILENAME
    )
    assert response.status_code == 404


def test_build_reports_computes_found_count(web_app, monkeypatch):
    """Regression guard: History reads `found_count` off the dict build_reports
    returns, so it must count claimed profiles across all usernames."""
    monkeypatch.setattr(maigret.report, 'save_csv_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_json_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_pdf_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'save_html_report', lambda *a, **kw: None)
    monkeypatch.setattr(maigret.report, 'generate_report_context', lambda *a, **kw: {})

    claimed = MaigretCheckResult(
        username='soxoj',
        site_name='GitHub',
        site_url_user='https://github.com/soxoj',
        status=MaigretCheckStatus.CLAIMED,
        ids_data={
            'fullname': 'Deddy Corbuzier',
            'description': 'Indonesian mentalist and media figure',
            'location': 'Indonesia',
        },
    )
    general_results = [
        (
            'soxoj',
            'username',
            {'GitHub': {'status': claimed, 'url_user': claimed.site_url_user}},
        )
    ]

    report = web_app.build_reports(general_results, ['soxoj'], 'testkey')

    assert report['found_count'] == 1
    profile = report['individual_reports'][0]['claimed_profiles'][0]
    assert profile['site_name'] == 'GitHub'
    assert profile['confidence'] == 'strong'
    assert profile['evidence']['fullname'] == 'Deddy Corbuzier'
    ai_input = web_app.build_ai_markdown(report)
    assert 'Deddy Corbuzier' in ai_input
    assert 'Indonesian mentalist' in ai_input
    assert 'Maigret' not in ai_input


def test_ai_markdown_includes_only_explicitly_approved_operator_context(web_app):
    result = {
        'individual_reports': [],
        'options': {
            'investigation_spec': {
                'allow_ai_context': True,
                'subject_label': 'Jati Pratomo',
                'identifiers': [
                    {'type': 'full_name', 'value': 'Jati Pratomo'},
                    {'type': 'email', 'value': 'jati@example.com'},
                ],
                'include_terms': ['Jakarta'],
                'exclude_terms': ['fan page'],
            }
        },
    }

    approved = web_app.build_ai_markdown(result)
    result['options']['investigation_spec']['allow_ai_context'] = False
    withheld = web_app.build_ai_markdown(result)

    assert 'Operator-provided research context' in approved
    assert 'Jati Pratomo' in approved
    assert 'jati@example.com' in approved
    assert 'fan page' in approved
    assert 'Operator-provided research context' not in withheld
    assert 'jati@example.com' not in withheld


@pytest.mark.parametrize(
    'relative_path',
    [
        '../maigret/web/templates/index.html',
        '../maigret/web/templates/live.html',
        '../maigret/web/templates/settings.html',
        '../maigret/resources/ai_prompt.txt',
        '../maigret/resources/simple_report.tpl',
        '../maigret/resources/simple_report_pdf.tpl',
    ],
)
def test_user_facing_copy_is_openledger_branded(relative_path):
    path = os.path.join(CUR_PATH, relative_path)
    with open(path, encoding='utf-8') as branded_file:
        assert 'maigret' not in branded_file.read().lower()


def test_process_search_task_records_started_at_on_success(web_app, monkeypatch):
    async def fake_search_multi(usernames, options):
        return []

    monkeypatch.setattr(web_app, 'search_multiple_usernames', fake_search_multi)
    monkeypatch.setattr(
        web_app,
        'build_reports',
        lambda *a, **kw: {
            'status': 'completed',
            'session_folder': 'search_ts_ok',
            'graph_file': 'search_ts_ok/combined_graph.html',
            'usernames': [],
            'individual_reports': [],
            'found_count': 0,
        },
    )
    web_app.background_jobs['ts_ok'] = {'completed': False, 'thread': None}

    web_app.process_search_task(['soxoj'], {}, 'ts_ok')

    assert web_app.job_results['ts_ok']['status'] == 'completed'
    assert web_app.job_results['ts_ok']['started_at']
    assert os.path.exists(web_app.get_session_metadata_path('search_ts_ok'))


def test_process_search_task_records_started_at_on_failure(web_app, monkeypatch):
    async def failing_search_multi(usernames, options):
        raise RuntimeError('boom')

    monkeypatch.setattr(web_app, 'search_multiple_usernames', failing_search_multi)
    web_app.background_jobs['ts_fail'] = {'completed': False, 'thread': None}

    web_app.process_search_task(['soxoj'], {}, 'ts_fail')

    assert web_app.job_results['ts_fail']['status'] == 'failed'
    assert web_app.job_results['ts_fail']['started_at']
    assert os.path.exists(web_app.get_session_metadata_path('search_ts_fail'))


def test_load_settings_defaults_when_no_file(web_app):
    settings = web_app.load_settings()
    assert settings['timeout'] == 10
    assert settings['top_sites'] == 500
    assert settings['tags'] == []
    assert settings['proxy'] == ''
    assert settings['openai_model'] == 'gpt-5.6-terra'


def test_save_settings_persists_to_file_and_reloads(web_app):
    web_app.save_settings(
        {**web_app.DEFAULT_SETTINGS, 'timeout': 42, 'proxy': '127.0.0.1:9999'}
    )

    assert os.path.exists(web_app.app.config['SETTINGS_FILE'])
    reloaded = web_app.load_settings()
    assert reloaded['timeout'] == 42
    assert reloaded['proxy'] == '127.0.0.1:9999'


def test_settings_update_saves_and_redirects_to_settings(client, web_app):
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'settings-csrf'
    resp = client.post(
        '/settings',
        data={
            'csrf_token': 'settings-csrf',
            'timeout': '15',
            'top_sites': '250',
            'tags': ['coding', 'tech'],
            'excluded_tags': ['porn'],
            'site': 'GitHub, Reddit',
            'proxy': '127.0.0.1:1080',
            'with_domains': 'on',
        },
    )
    assert resp.status_code == 302
    assert resp.headers['Location'] == '/settings'

    settings = web_app.load_settings()
    assert settings['timeout'] == 15
    assert settings['top_sites'] == 250
    assert settings['tags'] == ['coding', 'tech']
    assert settings['excluded_tags'] == ['porn']
    assert settings['site_list'] == ['GitHub', 'Reddit']
    assert settings['proxy'] == '127.0.0.1:1080'
    assert settings['with_domains'] is True
    assert settings['disable_recursive_search'] is False


def test_settings_update_invalid_timeout_falls_back_to_default(client, web_app):
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'settings-csrf'
    client.post(
        '/settings',
        data={
            'csrf_token': 'settings-csrf',
            'timeout': 'not-a-number',
            'top_sites': 'nope',
        },
    )
    settings = web_app.load_settings()
    assert settings['timeout'] == web_app.DEFAULT_SETTINGS['timeout']
    assert settings['top_sites'] == web_app.DEFAULT_SETTINGS['top_sites']


def test_parse_search_options_uses_saved_settings(web_app):
    web_app.save_settings(
        {
            **web_app.DEFAULT_SETTINGS,
            'timeout': 20,
            'top_sites': 100,
            'proxy': '127.0.0.1:8080',
            'tags': ['gaming'],
            'site_list': ['GitHub'],
            'disable_extracting': True,
        }
    )

    options = web_app.parse_search_options({})

    assert options['timeout'] == 20
    assert options['top_sites'] == 100
    assert options['proxy'] == '127.0.0.1:8080'
    assert options['tags'] == ['gaming']
    assert options['site_list'] == ['GitHub']
    assert options['disable_extracting'] is True
    assert options['all_sites'] is False


def test_parse_search_options_full_mode_ignores_top_sites(web_app):
    options = web_app.parse_search_options({'mode': 'full'})
    assert options['all_sites'] is True


def test_api_sites_returns_site_list(client, web_app):
    resp = client.get('/api/sites')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'sites' in data
    assert isinstance(data['sites'], list)


def test_dashboard_sidebar_and_settings_route_are_available(client, web_app):
    with client.session_transaction() as browser_session:
        browser_session['username'] = 'operator'
    resp = client.get('/')
    body = resp.get_data(as_text=True)
    assert 'id="appSidebar"' in body
    assert 'href="/settings"' in body
    assert 'New investigation' in body
    assert 'nexorus-mark.png' in body
    assert 'Private workspace' not in body
    assert 'OpenLedger by Nexorus' not in body
    assert 'sidebar-status' not in body
    assert 'class="topbar-profile"' in body
    assert '>Logout<' in body

    resp = client.get('/settings')
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'name="timeout"' in body
    assert 'id="connections"' in body
    assert 'name="openai_api_key"' in body
    assert '<select class="form-select" id="openai-model"' in body
    assert 'value="gpt-5.6-sol"' in body
    assert 'value="gpt-5.6-terra"' in body
    assert 'value="gpt-5.6-luna"' in body
    assert 'Username permutations' not in body
    assert 'Security boundary' not in body

    security_body = client.get('/security').get_data(as_text=True)
    assert 'Change password' in security_body
    assert 'Security controls' not in security_body

    font_response = client.get('/static/alliance-no2-regular.otf')
    assert font_response.status_code == 200
    assert font_response.data.startswith(b'OTTO')


def test_openai_settings_rejects_unlisted_model(client, web_app, monkeypatch):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'connection-csrf'

    resp = client.post(
        '/settings/openai',
        data={
            'csrf_token': 'connection-csrf',
            'action': 'connect',
            'openai_api_key': 'sk-must-not-be-stored',
            'openai_model': 'arbitrary-model',
        },
        follow_redirects=True,
    )

    assert 'Select a supported OpenAI analysis model.' in resp.get_data(as_text=True)
    assert not os.path.exists(web_app.app.config['OPENAI_API_KEY_FILE'])


def test_settings_update_requires_csrf(client, web_app):
    resp = client.post(
        '/settings',
        data={'timeout': '99', 'top_sites': '100'},
    )

    assert resp.status_code == 302
    assert resp.headers['Location'] == '/settings'
    assert web_app.load_settings()['timeout'] == web_app.DEFAULT_SETTINGS['timeout']


def test_openai_connection_is_verified_and_saved_server_side(
    client, web_app, monkeypatch
):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    captured = {}

    async def fake_validation(**kwargs):
        captured.update(kwargs)
        return kwargs['model']

    monkeypatch.setattr(web_app, 'validate_openai_connection', fake_validation)
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'connection-csrf'

    api_key = 'sk-proj-browser-submitted-secret'
    resp = client.post(
        '/settings/openai',
        data={
            'csrf_token': 'connection-csrf',
            'action': 'connect',
            'openai_api_key': api_key,
            'openai_model': 'gpt-5.4',
            'ai_web_enrichment': 'on',
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert 'OpenAI connected and verified.' in resp.get_data(as_text=True)
    key_path = web_app.app.config['OPENAI_API_KEY_FILE']
    with open(key_path, encoding='utf-8') as key_file:
        assert key_file.read().strip() == api_key
    assert os.stat(key_path).st_mode & 0o777 == 0o600
    assert api_key not in resp.get_data(as_text=True)
    assert web_app.load_settings()['openai_model'] == 'gpt-5.4'
    assert web_app.load_settings()['ai_web_enrichment'] is True
    assert captured['api_base_url'] == 'https://api.openai.com/v1'


def test_failed_openai_verification_does_not_store_key(
    client, web_app, monkeypatch
):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)

    async def fake_validation(**kwargs):
        raise RuntimeError('invalid key')

    monkeypatch.setattr(web_app, 'validate_openai_connection', fake_validation)
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'connection-csrf'

    resp = client.post(
        '/settings/openai',
        data={
            'csrf_token': 'connection-csrf',
            'action': 'connect',
            'openai_api_key': 'sk-invalid',
            'openai_model': 'gpt-5.4',
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert 'OpenAI verification failed.' in resp.get_data(as_text=True)
    assert not os.path.exists(web_app.app.config['OPENAI_API_KEY_FILE'])

def test_browser_managed_openai_connection_can_be_removed(
    client, web_app, monkeypatch
):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    web_app.save_openai_api_key('sk-proj-existing')
    with client.session_transaction() as browser_session:
        browser_session['csrf_token'] = 'connection-csrf'

    resp = client.post(
        '/settings/openai',
        data={'csrf_token': 'connection-csrf', 'action': 'disconnect'},
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert 'OpenAI connection removed.' in resp.get_data(as_text=True)
    assert not os.path.exists(web_app.app.config['OPENAI_API_KEY_FILE'])
