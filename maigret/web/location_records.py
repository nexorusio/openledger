# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
"""Append-only coordinate decisions and case-scoped public affiliation sites."""

from datetime import datetime, timezone
import hashlib
import json
import uuid

from sqlalchemy import (Table, Column, String, Integer, Text, DateTime, ForeignKey,
                        UniqueConstraint, CheckConstraint, select, insert, update)


def declare_tables(metadata, document):
    sites = Table('affiliation_sites', metadata,
        Column('id', String(36), primary_key=True),
        Column('case_id', String(36), ForeignKey('cases.id', ondelete='CASCADE'), nullable=False),
        Column('persona_id', String(36), ForeignKey('personas.id', ondelete='CASCADE'), nullable=False),
        Column('origin_claim_id', String(36), ForeignKey('persona_claims.id', ondelete='CASCADE'), nullable=False),
        Column('source_job_id', String(36), ForeignKey('investigation_jobs.id', ondelete='SET NULL')),
        Column('observation_key', String(64), nullable=False),
        Column('evidence', document, nullable=False),
        Column('candidates', document, nullable=False),
        Column('review_status', String(16), nullable=False),
        Column('revision', Integer, nullable=False),
        Column('created_at', DateTime(timezone=True), nullable=False),
        Column('updated_at', DateTime(timezone=True), nullable=False),
        UniqueConstraint('case_id', 'origin_claim_id', 'observation_key', name='uq_affiliation_site_observation'),
        CheckConstraint("review_status IN ('pending', 'approved', 'rejected', 'uncertain')", name='ck_affiliation_site_review'))
    selections = Table('location_selections', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('claim_id', String(36), ForeignKey('persona_claims.id', ondelete='CASCADE')),
        Column('site_id', String(36), ForeignKey('affiliation_sites.id', ondelete='CASCADE')),
        Column('decision', String(16), nullable=False),
        Column('reviewer', String(200), nullable=False),
        Column('reason', Text, nullable=False),
        Column('snapshot', document, nullable=False),
        Column('created_at', DateTime(timezone=True), nullable=False),
        CheckConstraint('(claim_id IS NULL) <> (site_id IS NULL)', name='ck_location_selection_subject'),
        CheckConstraint("decision IN ('pending', 'approved', 'rejected', 'uncertain')", name='ck_location_selection_review'))
    return sites, selections


def append_selection(connection, table, *, claim_id=None, site_id=None, decision,
                     reviewer, reason, snapshot):
    """Called inside the same locked transaction as the human review."""
    return connection.execute(insert(table).values(
        claim_id=claim_id, site_id=site_id, decision=decision, reviewer=reviewer,
        reason=str(reason or '').strip()[:2000], snapshot=snapshot,
        created_at=datetime.now(timezone.utc),
    ).returning(table.c.id)).scalar_one()


def coordinate_projection(claim, selections):
    """Historical raw coordinates never masquerade as a reviewed selection."""
    history = [dict(row) for row in selections]
    selected = history[0] if history else None
    snapshot = dict(selected['snapshot']) if selected else {}
    mapped = bool(claim['review_status'] == 'approved' and selected
                  and selected['decision'] == 'approved' and snapshot.get('action') == 'select')
    return {
        'latitude': snapshot.get('latitude') if mapped else None,
        'longitude': snapshot.get('longitude') if mapped else None,
        'coordinate_precision': snapshot.get('precision'),
        'coordinate_method': snapshot.get('method', 'legacy_unknown'),
        'coordinate_status': 'mapped' if mapped else 'needs_review' if not selected and claim.get('latitude') is not None else 'unmapped',
        'coordinate_selection': dict(snapshot, selection_id=selected['id'],
            reviewer=selected['reviewer'], reason=selected['reason'],
            created_at=selected['created_at'].isoformat()) if selected else None,
        'coordinate_history': [dict(row, created_at=row['created_at'].isoformat()) for row in history],
        'legacy_coordinates': next((row['snapshot']['previous_unreviewed_coordinates'] for row in history
                                    if row['snapshot'].get('previous_unreviewed_coordinates')), None)
            if selected else {'latitude': claim.get('latitude'), 'longitude': claim.get('longitude'),
                              'method': 'legacy_unknown'},
    }


def list_sites(store, persona_id, *, connection=None):
    from maigret.web.case_store import affiliation_sites, location_selections, persona_claims
    if connection is None:
        with store.engine.connect() as conn:
            return list_sites(store, persona_id, connection=conn)
    rows = connection.execute(select(affiliation_sites).where(
        affiliation_sites.c.persona_id == persona_id).order_by(affiliation_sites.c.created_at,
                                                              affiliation_sites.c.id)).mappings()
    output = []
    for row in rows:
        site = dict(row)
        history = list(connection.execute(select(location_selections).where(
            location_selections.c.site_id == site['id']).order_by(location_selections.c.id.desc())).mappings())
        latest = next((dict(record['snapshot']) for record in history
                       if record['snapshot'].get('action') != 'lookup'), {})
        site['resolution'] = latest.get('resolution') or {'status': 'unmapped', 'reason': 'review_required'}
        origin_status = connection.scalar(select(persona_claims.c.review_status).where(
            persona_claims.c.id == site['origin_claim_id']))
        site['origin_review_status'] = origin_status
        if origin_status != 'approved':
            site['resolution'] = {'status': 'unmapped', 'reason': 'affiliation_origin_requires_review',
                                  'is_person_location': False}
        site['history'] = [dict(r, created_at=r['created_at'].isoformat()) for r in history]
        site['created_at'] = site['created_at'].isoformat()
        site['updated_at'] = site['updated_at'].isoformat()
        output.append(site)
    return output


def propose_site_from_observation(store, source_job_id, observation_key):
    """Resolve immutable address evidence from a saved affiliation run, not form data."""
    from maigret.web.case_store import affiliation_sites, investigation_jobs, persona_claims, personas
    from maigret.web.location_resolution import public_affiliation_evidence_error
    now = datetime.now(timezone.utc)
    with store.engine.begin() as connection:
        job = connection.execute(select(investigation_jobs).where(
            investigation_jobs.c.id == source_job_id)).mappings().first()
        if not job or job['kind'] != 'affiliation' or job['status'] != 'completed':
            raise ValueError('A completed affiliation investigation is required')
        spec = (job['options'] or {}).get('investigation_spec') or {}
        origin_id = spec.get('source_claim_id')
        origin = connection.execute(select(persona_claims, personas.c.case_id).join(
            personas, persona_claims.c.persona_id == personas.c.id).where(
                persona_claims.c.id == origin_id)).mappings().first()
        if not origin or origin['review_status'] != 'approved' or origin['field_name'] not in {'company', 'occupation'}:
            raise ValueError('An approved affiliation origin is required')
        result = job['result'] or {}
        website = result.get('website_observation') or {}
        observations = website.get('location_observations') or (website.get('extra') or {}).get('location_observations') or []
        observation = next((o for o in observations if str(o.get('observation_key')) == observation_key), None)
        if not observation:
            raise ValueError('The saved public address observation was not found')
        # Preserve the source record unchanged and identify the actual affiliation,
        # without asserting employment at a branch or a person's current location.
        evidence = dict(observation)
        if (observation.get('is_public') is False or observation.get('is_official') is False
                or observation.get('source_engine') != 'official_website_public_content'):
            raise ValueError('Only saved official public organizational evidence can be proposed')
        evidence.setdefault('is_public', True)  # This collector retrieves public website content only.
        evidence.update(organization=spec.get('affiliation_name'),
                        organization_name=spec.get('affiliation_name'),
                        origin_claim_id=origin_id, source_job_id=source_job_id,
                        source_snapshot=observation)
        evidence.setdefault('address_type', 'unknown')
        evidence.setdefault('source_date', None)
        evidence['retrieved_at'] = str(job['completed_at'])
        evidence.setdefault('is_current_affiliation', False)
        if public_affiliation_evidence_error(evidence):
            raise ValueError('Only public institutional address evidence can be proposed')
        fingerprint = hashlib.sha256(str(observation['observation_key']).encode()).hexdigest()
        existing = connection.scalar(select(affiliation_sites.c.id).where(
            affiliation_sites.c.case_id == origin['case_id'],
            affiliation_sites.c.origin_claim_id == origin_id,
            affiliation_sites.c.observation_key == fingerprint))
        if existing:
            return existing
        site_id = str(uuid.uuid4())
        connection.execute(insert(affiliation_sites).values(
            id=site_id, case_id=origin['case_id'], persona_id=origin['persona_id'],
            origin_claim_id=origin_id, source_job_id=source_job_id, observation_key=fingerprint,
            evidence=evidence, candidates=[], review_status='pending', revision=0,
            created_at=now, updated_at=now))
        return site_id


def review_site(store, site_id, decision, reviewer, reason, *, expected_revision,
                candidate_id=None, clear=False, address_type='unknown'):
    from maigret.web.case_store import affiliation_sites, location_selections, persona_claims
    from maigret.web.location_resolution import resolve_public_affiliation_site, ADDRESS_TYPES
    if address_type not in ADDRESS_TYPES:
        raise ValueError('Invalid site address type')
    if decision not in {'approved', 'pending', 'uncertain', 'rejected'} or not reviewer or not reason.strip():
        raise ValueError('A review decision, reviewer, and reason are required')
    with store.engine.begin() as connection:
        statement = select(affiliation_sites).where(affiliation_sites.c.id == site_id)
        if store.engine.dialect.name == 'postgresql':
            statement = statement.with_for_update()
        row = connection.execute(statement).mappings().first()
        if not row or row['revision'] != expected_revision:
            raise ValueError('This site changed; reload before reviewing')
        if decision == 'approved':
            origin_query = select(persona_claims.c.review_status).where(
                persona_claims.c.id == row['origin_claim_id'])
            if store.engine.dialect.name == 'postgresql':
                origin_query = origin_query.with_for_update()
            if connection.scalar(origin_query) != 'approved':
                raise ValueError('The affiliation origin must be approved before this site')
        reviewed_evidence = dict(row['evidence'], address_type=address_type, match_basis=reason)
        reviewed_candidates = [dict(candidate) for candidate in row['candidates']]
        for candidate in reviewed_candidates:
            if candidate['candidate_id'] == candidate_id:
                candidate['match_basis'] = {'source_address': row['evidence']['address'],
                    'organization_name': row['evidence'].get('organization_name'), 'site_basis': reason}
        resolution = resolve_public_affiliation_site(reviewed_evidence, reviewed_candidates,
            selected_candidate_id=candidate_id if decision == 'approved' and not clear else None)
        if candidate_id and decision == 'approved' and resolution.get('status') != 'resolved':
            raise ValueError('The selected public address match requires review')
        append_selection(connection, location_selections, site_id=site_id, decision=decision,
                         reviewer=reviewer, reason=reason,
                         snapshot={'action': 'clear' if clear else 'review', 'resolution': resolution,
                                   'evidence': row['evidence'], 'address_type': address_type, 'match_basis': reason, 'is_person_location': False})
        changed = connection.execute(update(affiliation_sites).where(affiliation_sites.c.id == site_id,
            affiliation_sites.c.revision == expected_revision).values(
                review_status=decision, revision=expected_revision + 1,
                updated_at=datetime.now(timezone.utc)))
        if changed.rowcount != 1:
            raise ValueError('This site changed; reload before reviewing')
        return row['persona_id']
