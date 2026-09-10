# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
"""Explicit public-address lookup and independent affiliation-site review."""

import os
import uuid
from datetime import datetime, timezone
from flask import request, session, render_template, redirect, url_for, abort, flash
from sqlalchemy import select, update


def register_location_routes(app):
    def context():
        from maigret.web import app as web_app
        if web_app.case_store is None:
            abort(404)
        return web_app, web_app.case_store

    def require_csrf(web_app):
        if not web_app.is_valid_csrf(request.form.get('csrf_token')):
            abort(403)

    @app.get('/personas/<persona_id>/affiliation-sites')
    def affiliation_sites_workspace(persona_id):
        from maigret.web.case_store import investigation_jobs
        from maigret.web.location_records import list_sites
        web_app, store = context()
        persona = store.get_persona(persona_id)
        if not persona:
            abort(404)
        origins = [c['id'] for c in persona['claims'] if c['field_name'] in {'company', 'occupation'}]
        observations = []
        with store.engine.connect() as connection:
            jobs = connection.execute(select(investigation_jobs).where(
                investigation_jobs.c.kind == 'affiliation', investigation_jobs.c.status == 'completed',
                investigation_jobs.c.options['investigation_spec']['source_claim_id'].as_string().in_(origins)
            ).order_by(investigation_jobs.c.created_at.desc()).limit(100)).mappings()
            for job in jobs:
                website = (job['result'] or {}).get('website_observation') or {}
                for observation in website.get('location_observations') or []:
                    observations.append(dict(observation, job_id=job['id']))
        sites = list_sites(store, persona_id)
        points = []
        for site in sites:
            resolution = site['resolution']
            if site['review_status'] == 'approved' and resolution.get('status') == 'resolved':
                candidate = resolution.get('candidate') or {}
                points.append(dict(candidate, id=site['id'], label=site['evidence']['address']))
        return render_template('affiliation_sites.html', persona=persona, sites=sites,
                               observations=observations, map_locations=points,
                               map_tile_url=os.getenv('OPENLEDGER_MAP_TILE_URL', 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'),
                               map_attribution='© OpenStreetMap contributors')

    @app.post('/affiliation-sites/propose')
    def propose_affiliation_site():
        from maigret.web.location_records import propose_site_from_observation, list_sites
        from maigret.web.case_store import affiliation_sites
        web_app, store = context()
        require_csrf(web_app)
        try:
            identifier = propose_site_from_observation(store, request.form.get('job_id'),
                                                       request.form.get('observation_key'))
        except ValueError as error:
            return {'error': str(error)}, 400
        with store.engine.connect() as connection:
            persona_id = connection.scalar(select(affiliation_sites.c.persona_id).where(affiliation_sites.c.id == identifier))
        return redirect(url_for('affiliation_sites_workspace', persona_id=persona_id))

    @app.post('/affiliation-sites/<site_id>/lookup')
    def lookup_affiliation_site(site_id):
        from maigret.web.case_store import affiliation_sites, persona_claims
        from maigret.web.geocoding import geocode_public_affiliation_address_candidates, GeocodingError
        web_app, store = context()
        require_csrf(web_app)
        with store.engine.connect() as connection:
            row = connection.execute(select(affiliation_sites).where(affiliation_sites.c.id == site_id)).mappings().first()
        if not row:
            abort(404)
        with store.engine.connect() as connection:
            if connection.scalar(select(persona_claims.c.review_status).where(
                    persona_claims.c.id == row['origin_claim_id'])) != 'approved':
                return {'error': 'The affiliation origin requires review before lookup'}, 400
        # A deliberate lookup discloses only the saved public institutional address.
        # Cache/rate state is shared across all application processes on the mount.
        try:
            candidates = geocode_public_affiliation_address_candidates(row['evidence'],
                endpoint=app.config['GEOCODER_URL'], timeout_seconds=app.config['GEOCODER_TIMEOUT_SECONDS'],
                cache_dir=os.path.join(app.config['REPORTS_FOLDER'], '.geocoding-cache'))
        except GeocodingError:
            flash('The public address lookup was unavailable. The saved address and review are unchanged.', 'warning')
        else:
            with store.engine.begin() as connection:
                from maigret.web.case_store import location_selections
                from maigret.web.location_records import append_selection
                lookup_id = str(uuid.uuid4())
                retrieved_at = datetime.now(timezone.utc).isoformat()
                for candidate in candidates:
                    candidate['lookup_id'] = lookup_id
                    candidate['retrieved_at'] = retrieved_at
                    candidate['candidate_id'] = lookup_id + ':' + candidate['candidate_id']
                updated = connection.execute(update(affiliation_sites).where(
                    affiliation_sites.c.id == site_id, affiliation_sites.c.revision == row['revision']).values(
                        candidates=candidates, revision=row['revision'] + 1,
                        updated_at=datetime.now(timezone.utc)))
                if updated.rowcount:
                    append_selection(connection, location_selections, site_id=site_id,
                        decision=row['review_status'], reviewer=session.get('username') or 'local-operator',
                        reason='Explicit lookup of the saved public organizational address',
                        snapshot={'action': 'lookup', 'lookup_id': lookup_id,
                                  'retrieved_at': retrieved_at, 'evidence': row['evidence'],
                                  'candidates': candidates})
                if not updated.rowcount:
                    flash('The site changed while the lookup ran. Reload it before continuing.', 'warning')
        return redirect(url_for('affiliation_sites_workspace', persona_id=row['persona_id']))

    @app.post('/affiliation-sites/<site_id>/review')
    def review_affiliation_site(site_id):
        from maigret.web.location_records import review_site
        web_app, store = context()
        require_csrf(web_app)
        try:
            persona_id = review_site(store, site_id, request.form.get('decision'),
                session.get('username') or 'local-operator', request.form.get('reason', ''),
                expected_revision=int(request.form.get('revision', '-1')),
                candidate_id=request.form.get('candidate_id') or None,
                clear=request.form.get('clear_coordinates') == '1',
                address_type=request.form.get('address_type', 'unknown'))
        except ValueError as error:
            return {'error': str(error)}, 400
        return redirect(url_for('affiliation_sites_workspace', persona_id=persona_id))
