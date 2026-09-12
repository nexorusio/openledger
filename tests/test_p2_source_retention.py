"""Offline regressions for omitted social checks and supplied profile URLs."""

import asyncio
from pathlib import Path
from queue import Queue
import socket
import types

import pytest

import maigret
import maigret.report
from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.sites import MaigretDatabase, MaigretSite
from maigret.web.case_store import CaseStore
from maigret.web.persona_intelligence import (
    extract_persona_claims,
    extract_supplied_profile_claims,
)
from tests.test_web import client, web_app  # noqa: F401


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('This regression suite must not use sockets or DNS')

    for name in (
        'getaddrinfo',
        'gethostbyname',
        'gethostbyname_ex',
        'create_connection',
    ):
        monkeypatch.setattr(socket, name, blocked)
    for name in ('connect', 'connect_ex', 'sendto'):
        monkeypatch.setattr(socket.socket, name, blocked)


@pytest.fixture
def report_writers(monkeypatch):
    for name in (
        'save_graph_report',
        'save_csv_report',
        'save_json_report',
        'save_pdf_report',
        'save_html_report',
    ):
        monkeypatch.setattr(maigret.report, name, lambda *a, **kw: None)


def _database():
    database = MaigretDatabase()
    for name, rank in (('Instagram', 4), ('TikTok', 24), ('Threads', 409)):
        database.update_site(
            MaigretSite(
                name,
                {
                    'url': f'https://{name.lower()}.example.test/{{username}}',
                    'urlMain': f'https://{name.lower()}.example.test/',
                    'checkType': 'message',
                    'alexaRank': rank,
                    'tags': ['social'],
                },
            )
        )
    return database


def _health():
    return {
        'schema_version': 1,
        'sites': {
            'instagram': {'state': 'degraded'},
            'tiktok': {'state': 'healthy'},
            'threads': {'state': 'healthy'},
        },
    }


def _result(site_name, state=MaigretCheckStatus.CLAIMED, **kwargs):
    profile_url = {
        'Instagram': 'https://www.instagram.com/fixture_account/',
        'TikTok': 'https://www.tiktok.com/@fixture_account',
        'Threads': 'https://www.threads.com/@fixture_account',
    }[site_name]
    status = MaigretCheckResult(
        username='fixture_account',
        site_name=site_name,
        site_url_user=profile_url,
        status=state,
        **kwargs,
    )
    return {
        'status': status,
        'url_user': profile_url,
        'http_status': 200,
        'site': types.SimpleNamespace(check_type='message'),
    }


@pytest.mark.parametrize(
    'cause, expected',
    [
        ('rank', 'outside the source limit'),
        ('filter', 'excluded by case source filters'),
        ('disabled', 'disabled in the source catalog'),
        ('quarantine', 'quarantined after canary failures'),
    ],
)
def test_source_omission_keeps_actual_selection_reason(web_app, cause, expected):
    database = _database()
    health = _health()
    options = {'tags': [], 'excluded_tags': [], 'site_list': []}
    if cause == 'filter':
        options['excluded_tags'] = ['social']
    if cause == 'disabled':
        next(site for site in database.sites if site.name == 'TikTok').disabled = True
    if cause == 'quarantine':
        health['sites']['tiktok']['state'] = 'quarantined'
    selected = web_app.select_sites_for_search(
        database,
        top_sites=1,
        all_sites=False,
        detector_health_registry=health,
        **options,
    )
    coverage = web_app.selected_source_coverage(database, selected, options, health)
    tiktok = next(item for item in coverage if item['site_name'] == 'TikTok')
    assert tiktok['status'] == 'excluded'
    assert expected in tiktok['reason']
    assert tiktok['classification'] is None


def test_shipped_catalog_selects_instagram_and_tiktok_without_p3(web_app):
    root = Path(__file__).resolve().parents[1]
    database = MaigretDatabase().load_from_path(
        str(root / 'maigret/resources/data.json')
    )
    registry = web_app.load_detector_health_registry(
        root / 'maigret/resources/detector_health.json'
    )
    selected = web_app.select_sites_for_search(
        database,
        top_sites=500,
        all_sites=False,
        tags=[],
        excluded_tags=[],
        site_list=[],
        detector_health_registry=registry,
    )
    assert {'Instagram', 'TikTok', 'Threads'} <= selected.keys()
    assert registry['sites']['instagram']['state'] == 'degraded'


def test_candidate_and_inconclusive_checks_stay_visible_without_identity_promotion(
    web_app,
    client,
    monkeypatch,
    report_writers,
):
    monkeypatch.setattr(web_app, 'get_detector_health_registry', _health)
    raw = {
        'Instagram': _result(
            'Instagram', ids_data={'fullname': 'Fixture Account', 'uid': '123'}
        ),
        'TikTok': _result(
            'TikTok', MaigretCheckStatus.UNKNOWN, context='Captcha detected'
        ),
        'Threads': _result('Threads', ids_data={'fullname': 'Fixture Account'}),
    }
    result = web_app.build_reports(
        [('fixture_account', 'username', raw)],
        ['fixture_account'],
        'retention-fixture',
    )
    report = result['individual_reports'][0]
    assert result['found_count'] == 1
    assert result['candidate_count'] == 1
    assert [p['site_name'] for p in report['claimed_profiles']] == ['Threads']
    assert [p['site_name'] for p in report['candidate_profiles']] == ['Instagram']
    assert {
        c['value']['platform']
        for c in extract_persona_claims(report)
        if c['field_name'] == 'social_account'
    } == {'Threads'}
    web_app.job_results['retention-fixture'] = result
    response = client.get('/results/search_retention-fixture')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    section = body.split('id="source-coverage"', 1)[1].split('</section>', 1)[0]
    assert 'https://www.instagram.com/fixture_account/' in section
    assert 'Detector is degraded and requires corroboration.' in section
    assert 'Captcha detected' in section
    assert 'Inconclusive' in section
    assert 'No supported profile leads found' not in section


@pytest.mark.parametrize(
    'state, data, expected',
    [
        (
            MaigretCheckStatus.CLAIMED,
            {'ids_data': {'description': 'Log in or create an account'}},
            'generic or missing-page shell',
        ),
        (MaigretCheckStatus.AVAILABLE, {}, 'does not prove absence'),
    ],
)
def test_shell_and_no_match_are_diagnostic_only(
    web_app, monkeypatch, report_writers, state, data, expected
):
    monkeypatch.setattr(web_app, 'get_detector_health_registry', _health)
    result = web_app.build_reports(
        [('fixture_account', 'username', {'TikTok': _result('TikTok', state, **data)})],
        ['fixture_account'],
        'diagnostic-fixture',
    )
    report = result['individual_reports'][0]
    assert result['found_count'] == 0
    assert extract_persona_claims(report) == []
    assert expected in report['major_platforms'][0]['reason']


def test_provider_failure_retains_selection_without_inventing_check_results(
    web_app,
    monkeypatch,
    report_writers,
):
    database = _database()
    monkeypatch.setattr(
        web_app.MaigretDatabase, 'load_from_path', lambda self, path: database
    )
    monkeypatch.setattr(web_app, 'get_detector_health_registry', _health)
    monkeypatch.setattr(
        web_app.maigret.settings.Settings, 'load', lambda *a, **kw: None
    )

    async def failed_search(**kwargs):
        raise RuntimeError('offline injected collector failure')

    monkeypatch.setattr(maigret, 'search', failed_search)
    notify = web_app.StreamNotify(Queue(), 'fixture_account')
    with pytest.raises(RuntimeError, match='offline injected'):
        asyncio.run(
            web_app.maigret_search('fixture_account', {'top_sites': 500}, notify)
        )
    assert notify.checked == 0
    assert len(notify.source_coverage) == 3
    captured = []
    monkeypatch.setattr(
        web_app,
        'record_job_result',
        lambda job_id, result: captured.append(result) or result,
    )
    web_app.finalize_stream_job(
        'no-response',
        ['fixture_account'],
        [],
        '2026-09-11',
        Queue(),
        source_coverage={'fixture_account': notify.source_coverage},
        budget_exhausted=True,
    )
    report = captured[0]['individual_reports'][0]
    assert captured[0]['collection_status'] == 'budget_exhausted'
    assert captured[0]['found_count'] == 0
    assert report['diagnostics']['unknown'] == 0
    assert all(item['status'] == 'selected' for item in report['major_platforms'])
    assert all(
        'no returned result' in item['reason'] for item in report['major_platforms']
    )


def test_supplied_profiles_remain_exact_analyst_context():
    urls = [
        'https://m.instagram.com/Fixture_Account/?hl=en',
        'https://www.tiktok.com/@fixture_account',
    ]
    spec = {
        'processing_mode': 'same_subject',
        'identifiers': [
            {'type': 'profile_url', 'value': url} for url in [*urls, urls[0]]
        ],
    }
    claims = extract_supplied_profile_claims(spec, usernames=['fixture_account'])
    assert [c['value']['url'] for c in claims] == urls
    assert {c['field_name'] for c in claims} == {'social_account'}
    for claim in claims:
        assert claim['source_engine'] == 'investigation_input'
        details = claim['evidence'][0]['details']
        assert details['independently_corroborated'] is False
        assert details['account_status'] == details['identity_status'] == 'unverified'
        assert details['human_review_required'] is True


def test_supplied_url_does_not_cross_independent_subjects_or_create_login_account():
    spec = {
        'processing_mode': 'independent',
        'identifiers': [
            {
                'type': 'profile_url',
                'value': 'https://www.instagram.com/fixture_account/',
            },
            {'type': 'profile_url', 'value': 'https://www.tiktok.com/@someone_else'},
            {'type': 'profile_url', 'value': 'https://instagram.com/accounts/login/'},
        ],
    }
    claims = extract_supplied_profile_claims(spec, usernames=['fixture_account'])
    assert len(claims) == 1
    assert claims[0]['value']['platform'] == 'instagram'
    spec['processing_mode'] = 'same_subject'
    claims = extract_supplied_profile_claims(spec, usernames=['fixture_account'])
    assert claims[-1]['field_name'] == 'website'
    assert claims[-1]['value'] == 'https://instagram.com/accounts/login/'


def test_unparseable_supplied_profile_retained_only_for_its_bound_target():
    url = 'https://profiles.example.test/person/fixture_account'
    spec = {
        'processing_mode': 'independent',
        'identifiers': [{'type': 'profile_url', 'value': url}],
        'search_targets': [
            {
                'source_type': 'profile_url',
                'source_value': url,
                'value': 'fixture_account',
            }
        ],
    }
    assert extract_supplied_profile_claims(spec, usernames=['someone_else']) == []
    claim = extract_supplied_profile_claims(spec, usernames=['fixture_account'])[0]
    assert claim['field_name'] == 'website'
    assert claim['value'] == url


def test_digital_presence_links_to_retained_case_checks(
    web_app,
    client,
    monkeypatch,
    tmp_path,
    report_writers,
):
    store = CaseStore(f'sqlite:///{tmp_path / "presence.db"}', create_schema=True)
    monkeypatch.setattr(web_app, 'case_store', store)
    monkeypatch.setattr(web_app, 'get_detector_health_registry', _health)
    job_id = store.create_investigation(['fixture_account'], {}, kind='live')
    assert store.claim_next('worker:source-fixture')['job_id'] == job_id
    result = web_app.build_reports(
        [
            (
                'fixture_account',
                'username',
                {
                    'TikTok': _result(
                        'TikTok', MaigretCheckStatus.UNKNOWN, context='Captcha detected'
                    ),
                },
            )
        ],
        ['fixture_account'],
        job_id,
    )
    assert store.finish(job_id, result)
    persona_id = store.get_case(store.get_job(job_id)['case_id'])['personas'][0]['id']
    body = client.get(f'/personas/{persona_id}').get_data(as_text=True)
    assert f'/results/search_{job_id}#source-coverage' in body
    assert 'Review case source checks and candidate leads' in body
    assert 'an omitted account is not proof that it does not exist' in body
    store.engine.dispose()
