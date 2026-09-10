# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
"""Local evidence/cleanup boundaries; every input is an offline fixture."""

import multiprocessing
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest
from sqlalchemy import select, update
from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web import app as web_app
from maigret.web.artifact_execution import enable_subreaper, reap_process
from maigret.web.case_store import CaseStore, affiliation_sites, location_selections, persona_claims
from maigret.web.location_records import propose_site_from_observation, review_site, list_sites


@pytest.fixture
def store(tmp_path, monkeypatch):
    value = CaseStore(f'sqlite:///{tmp_path / "test.db"}', create_schema=True)
    monkeypatch.setattr(web_app, 'case_store', value)
    monkeypatch.setitem(web_app.app.config, 'REPORTS_FOLDER', str(tmp_path / 'reports'))
    yield value
    value.dispose()


def seed(store):
    job_id = store.create_investigation(['fixture'], {})
    store.claim_next('worker:fixture')
    result = {'status': 'completed', 'session_folder': f'search_{job_id}', 'usernames': ['fixture'],
              'found_count': 1, 'individual_reports': [{'username': 'fixture', 'claimed_profiles': [
                  {'site_name': 'GitHub', 'url': 'https://example.test/fixture', 'confidence': 'strong',
                   'evidence': {'company': 'Fixture Organization', 'location': 'Jakarta, Indonesia'}}]}]}
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(store.get_job(job_id)['case_id'])['personas'][0]['id']
    claims = {c['field_name']: c for c in store.get_persona(persona_id)['claims']}
    return persona_id, claims


def test_coordinate_selection_amend_clear_reject_and_legacy(store):
    persona_id, claims = seed(store)
    claim = claims['current_location']
    with store.engine.begin() as connection:
        connection.execute(update(persona_claims).where(persona_claims.c.id == claim['id']).values(latitude=-6.0, longitude=107.0))
    store.review_claim(claim['id'], 'approved', 'reviewer')
    current = next(c for c in store.get_persona(persona_id)['claims'] if c['id'] == claim['id'])
    assert current['latitude'] is None  # text approval does not adopt legacy/AI point
    assert current['legacy_coordinates'] == {'latitude': -6.0, 'longitude': 107.0, 'method': 'legacy_unknown'}
    assert current['coordinate_history'][0]['snapshot']['previous_unreviewed_coordinates']['latitude'] == -6.0
    store.review_claim(claim['id'], 'approved', 'reviewer', 'Verified cited place point', '-6.2', '106.8')
    current = next(c for c in store.get_persona(persona_id)['claims'] if c['id'] == claim['id'])
    assert current['latitude'] == -6.2
    assert current['coordinate_selection']['method'] == 'analyst_selected'
    assert current['coordinate_selection']['source_evidence']
    store.review_claim(claim['id'], 'approved', 'reviewer', 'Clear obsolete point', clear_coordinates=True)
    current = next(c for c in store.get_persona(persona_id)['claims'] if c['id'] == claim['id'])
    assert current['latitude'] is None and len(current['coordinate_history']) == 3
    assert current['coordinate_history'][1]['snapshot']['latitude'] == -6.2
    store.review_claim(claim['id'], 'rejected', 'reviewer', 'Conflicting source')
    assert len(next(c for c in store.get_persona(persona_id)['claims'] if c['id'] == claim['id'])['coordinate_history']) == 4


def test_affiliation_site_rerun_review_is_separate_from_person(store):
    persona_id, claims = seed(store)
    origin = claims['company']
    store.review_claim(origin['id'], 'approved', 'reviewer')
    observation = {'observation_key': 'fixture-address-1', 'address': '1 Public Campus, Jakarta',
                   'source_url': 'https://institution.example/contact',
                   'source_engine': 'official_website_public_content'}
    def run_observation():
        job = store.create_affiliation_investigation('Fixture Organization', source_claim_id=origin['id'],
            source_claim_field='company', target_basis='approved_affiliation_claim')
        store.claim_next('worker:affiliation')
        store.finish(job, {'status': 'completed', 'website_observation': {'location_observations': [observation]}})
        return propose_site_from_observation(store, job, observation['observation_key'])
    site_id = run_observation()
    candidate = {'candidate_id': 'public-campus', 'address': observation['address'], 'display_name': observation['address'],
                 'latitude': -6.2, 'longitude': 106.8, 'precision': 'building', 'provider_id': 'osm:123',
                 'address_type': 'campus', 'source_url': observation['source_url']}
    with store.engine.begin() as connection:
        connection.execute(update(affiliation_sites).where(affiliation_sites.c.id == site_id).values(candidates=[candidate]))
    review_site(store, site_id, 'approved', 'reviewer', 'Source identifies this public campus; branch assignment unknown',
                expected_revision=0, candidate_id='public-campus', address_type='campus')
    assert run_observation() == site_id
    sites = list_sites(store, persona_id)
    assert len(sites) == 1 and sites[0]['review_status'] == 'approved'
    assert sites[0]['resolution']['is_person_location'] is False
    assert sites[0]['evidence']['source_date'] is None
    assert next(c for c in store.get_persona(persona_id)['claims'] if c['id'] == claims['current_location']['id'])['latitude'] is None
    with pytest.raises(ValueError, match='changed'):
        review_site(store, site_id, 'approved', 'reviewer', 'Stale submission', expected_revision=0)
    with store.engine.connect() as connection:
        assert len(list(connection.execute(select(location_selections).where(location_selections.c.site_id == site_id)))) == 1
    store.review_claim(origin['id'], 'rejected', 'reviewer', 'Affiliation source contradicted')
    assert list_sites(store, persona_id)[0]['resolution']['status'] == 'unmapped'
    with pytest.raises(ValueError, match='origin must be approved'):
        review_site(store, site_id, 'approved', 'reviewer', 'Stale origin',
                    expected_revision=1, candidate_id='public-campus', address_type='campus')


def test_site_review_route_export_and_lookup_history(store, monkeypatch):
    from maigret.web import geocoding
    from maigret.web.persona_pdf import build_persona_export_snapshot, build_investigation_report_view
    persona_id, claims = seed(store)
    origin = claims['company']
    store.review_claim(origin['id'], 'approved', 'reviewer')
    observation = {'observation_key': 'route-site', 'address': '12 Public Campus, Jakarta, Indonesia',
                   'source_url': 'https://institution.example/contact',
                   'source_engine': 'official_website_public_content'}
    job_id = store.create_affiliation_investigation('Fixture Organization', source_claim_id=origin['id'],
        source_claim_field='company', target_basis='approved_affiliation_claim')
    store.claim_next('worker:site')
    store.finish(job_id, {'status': 'completed', 'website_observation': {'location_observations': [observation]}})
    monkeypatch.setitem(web_app.app.config, 'AUTH_REQUIRED', False)
    monkeypatch.setitem(web_app.app.config, 'TESTING', True)
    client = web_app.app.test_client()
    with client.session_transaction() as session:
        session['csrf_token'] = 'site-review-csrf'
        session['username'] = 'fixture-reviewer'
    form = {'csrf_token': 'site-review-csrf'}
    workspace = f'/personas/{persona_id}/affiliation-sites'
    assert client.get(workspace).status_code == 200
    assert client.post('/affiliation-sites/propose', data={'job_id': job_id}).status_code == 403
    response = client.post('/affiliation-sites/propose', data=dict(form, job_id=job_id, observation_key='route-site'))
    assert response.status_code == 302
    site_id = list_sites(store, persona_id)[0]['id']
    candidate = {'candidate_id': 'osm:12', 'provider_id': 'osm:12', 'address': observation['address'],
                 'display_name': observation['address'], 'latitude': -6.2, 'longitude': 106.8,
                 'precision': 'building', 'address_type': 'campus', 'method': 'provider_address_candidate'}
    calls = []
    def lookup(evidence, **kwargs):
        calls.append(evidence)
        return [dict(candidate)]
    monkeypatch.setattr(geocoding, 'geocode_public_affiliation_address_candidates', lookup)
    assert client.post(f'/affiliation-sites/{site_id}/lookup', data=form).status_code == 302
    site = list_sites(store, persona_id)[0]
    chosen_id = site['candidates'][0]['candidate_id']
    assert site['resolution']['status'] == 'unmapped'
    review_form = dict(form, revision=site['revision'], candidate_id=chosen_id, decision='approved',
        reason='Published campus address matches institutional identity; date not stated.', address_type='campus')
    assert client.post(f'/affiliation-sites/{site_id}/review', data=review_form).status_code == 302
    page = client.get(workspace).get_data(as_text=True)
    assert 'affiliationSiteMap' in page and 'value="campus" selected' in page
    assert client.post(f'/affiliation-sites/{site_id}/lookup', data=form).status_code == 302
    site = list_sites(store, persona_id)[0]
    assert len(site['history']) == 3 and site['resolution']['candidate']['candidate_id'] == chosen_id
    assert len(calls) == 2 and all(call['address'] == observation['address'] for call in calls)
    snapshot = build_persona_export_snapshot(store.get_persona(persona_id), generated_by='fixture-reviewer',
                                             generated_at=datetime.now(timezone.utc))
    assert len(snapshot['affiliation_sites']) == 1
    assert snapshot['affiliation_sites'][0]['resolution']['is_person_location'] is False
    assert len(build_investigation_report_view(snapshot)['affiliation_sites']) == 1
    store.review_claim(origin['id'], 'rejected', 'reviewer', 'Affiliation withdrawn')
    assert client.post(f'/affiliation-sites/{site_id}/lookup', data=form).status_code == 400
    assert len(calls) == 2
    snapshot = build_persona_export_snapshot(store.get_persona(persona_id), generated_by='fixture-reviewer',
                                             generated_at=datetime.now(timezone.utc))
    assert snapshot['affiliation_sites'] == []


@pytest.mark.parametrize('controls', [
    {'is_public': False}, {'is_official': False}, {'is_private': True},
    {'is_confidential': True}, {'is_sensitive': True}, {'classification': 'private'},
    {'source_engine': 'unreviewed_search_snippet'}, {'is_residential': True},
])
def test_saved_address_restrictions_are_never_promoted_to_public(store, controls):
    _persona_id, claims = seed(store)
    origin = claims['company']
    store.review_claim(origin['id'], 'approved', 'reviewer')
    observation = {'observation_key': 'restricted-source', 'address': '12 Fixture Street, Jakarta',
                   'source_url': 'https://institution.example/contact',
                   'source_engine': 'official_website_public_content', **controls}
    job_id = store.create_affiliation_investigation('Fixture Organization', source_claim_id=origin['id'],
        source_claim_field='company', target_basis='approved_affiliation_claim')
    store.claim_next('worker:restricted')
    store.finish(job_id, {'status': 'completed', 'website_observation': {'location_observations': [observation]}})
    with pytest.raises(ValueError):
        propose_site_from_observation(store, job_id, 'restricted-source')


def test_queued_cancel_has_one_atomic_terminal_event_and_actor(store):
    job_id = store.create_investigation(['never-started-fixture'], {})
    assert store.request_cancel(job_id, requested_by='fixture-reviewer', origin='fixture-stop')
    assert store.request_cancel(job_id)
    events = [row['event'] for row in store.get_events(job_id)]
    terminal = [event for event in events if event['type'] == 'done']
    assert len(terminal) == 1 and terminal[0]['requested_by'] == 'fixture-reviewer'
    assert store.get_job(job_id)['lifecycle']['cleanup_state'] == 'not_required'


def _general_results(users=3, count=3):
    output = []
    for user in range(users):
        username = f'fixture{user}'
        results = {}
        for n in range(count):
            site = f'Fixture{n}'
            results[site] = {'status': MaigretCheckResult(username, site, f'https://example.test/{username}/{n}',
                MaigretCheckStatus.CLAIMED), 'url_user': f'https://example.test/{username}/{n}'}
        output.append((username, 'username', results))
    return output


def test_reports_are_scoped_once_and_checkpoint_classification_is_incremental(monkeypatch, tmp_path):
    import maigret.report
    contexts, graphs, classified = [], [], []
    monkeypatch.setattr(web_app, 'generate_report_context', lambda rows: contexts.append(rows) or {})
    monkeypatch.setattr(maigret.report, 'save_graph_report', lambda *a: graphs.append(a))
    for name in ('save_csv_report', 'save_json_report', 'save_pdf_report', 'save_html_report'):
        monkeypatch.setattr(maigret.report, name, lambda *a, **k: None)
    original = web_app.profile_detection_record
    monkeypatch.setattr(web_app, 'profile_detection_record', lambda *a, **k: classified.append(a[1]) or original(*a, **k))
    general = _general_results(20, 25)
    web_app.build_reports(general, [r[0] for r in general], 'scoped', reports_root=str(tmp_path))
    assert len(contexts) == 20 and all(len(rows) == 1 for rows in contexts) and len(graphs) == 1
    cache = {}
    classified.clear()
    web_app.build_reports(general, [], 'scoped', write_files=False, projection_cache=cache)
    assert len(classified) == 500
    web_app.build_reports(general, [], 'scoped', write_files=False, projection_cache=cache)
    assert len(classified) == 500
    assert len(contexts) == 20 and len(graphs) == 1


@pytest.mark.parametrize('failure', ['timeout', 'exception'])
def test_optional_artifact_failure_retains_terminal_evidence(store, monkeypatch, failure):
    general = _general_results(1, 1)
    job_id = store.create_investigation(['fixture0'], {})
    job = store.claim_next('worker:artifact')
    accounting = {'schema_version': 1, 'revision': 1, 'state': 'completed', 'known': True, 'stages': []}
    def failed(*args, **kwargs):
        if failure == 'exception':
            raise RuntimeError('fixture converter failure')
        return None
    monkeypatch.setattr(web_app, 'build_bounded_artifacts', failed)
    sink = web_app.PersistentEventSink(store, job_id, worker_id=job['worker_id'])
    assert web_app.finalize_stream_job(job_id, ['fixture0'], general, job['started_at'], sink,
        worker_id=job['worker_id'], collection_accounting=accounting,
        lifecycle={'phase': 'finalizing', 'cleanup_state': 'complete', 'stop_cause': None})
    current = store.get_job(job_id)
    assert current['status'] == 'completed'
    assert len(current['individual_reports']) == 1 and current['raw_claimed_count'] == 1
    assert current['graph_file'] is None and current['artifact_status'] == 'unavailable'
    assert sum(row['event']['type'] == 'done' for row in store.get_events(job_id)) == 1


def test_cancel_before_publication_retains_evidence_without_files(store, monkeypatch):
    general = _general_results(1, 1)
    job_id = store.create_investigation(['fixture0'], {})
    job = store.claim_next('worker:publication')
    accounting = {'schema_version': 1, 'revision': 1, 'state': 'completed', 'known': True, 'stages': []}
    def render_then_cancel(*args, kwargs, **options):
        result = web_app.build_reports(general, ['fixture0'], job_id, write_files=False)
        folder = Path(kwargs['reports_root']) / f'search_{job_id}'
        folder.mkdir()
        (folder / 'sentinel.html').write_text('fixture report')
        result['individual_reports'][0]['html_file'] = f'search_{job_id}/sentinel.html'
        store.request_cancel(job_id)
        return result
    monkeypatch.setattr(web_app, 'build_bounded_artifacts', render_then_cancel)
    sink = web_app.PersistentEventSink(store, job_id, worker_id=job['worker_id'])
    assert web_app.finalize_stream_job(job_id, ['fixture0'], general, job['started_at'], sink,
        worker_id=job['worker_id'], collection_accounting=accounting,
        lifecycle={'phase': 'finalizing', 'cleanup_state': 'complete', 'stop_cause': None})
    result = store.get_job(job_id)
    assert result['collection_status'] == 'cancelled' and result['raw_claimed_count'] == 1
    assert result['individual_reports'][0]['html_file'] is None
    assert result['lifecycle']['stop_cause'] == 'operator_cancel'
    assert not list(Path(web_app.app.config['REPORTS_FOLDER']).rglob('*'))
    assert sum(row['event']['type'] == 'done' for row in store.get_events(job_id)) == 1


def _spawn_descendant_and_exit(connection):
    os.setsid()
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    connection.send(child.pid)
    connection.close()


@pytest.mark.skipif(os.name != 'posix' or not Path('/proc').exists(), reason='Linux process cleanup')
def test_reaper_stops_descendant_after_direct_child_exit():
    enable_subreaper()
    context = multiprocessing.get_context('spawn')
    reader, writer = context.Pipe(False)
    process = context.Process(target=_spawn_descendant_and_exit, args=(writer,))
    process.start()
    writer.close()
    assert reader.poll(5)
    descendant = reader.recv()
    process.join(5)
    assert not process.is_alive()
    reap_process(process, whole_session=True)
    with pytest.raises(ProcessLookupError):
        os.kill(descendant, 0)
    reader.close()
