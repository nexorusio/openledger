"""A legacy import task cannot mask a restricted provider's source identity."""

import pytest

from maigret.web.pipeline_evidence import enforce_observation_retention


@pytest.mark.parametrize('engine,source_engine', [
    ('legacy_job_import', 'google_places_live_details'),
    ('google_places_live_details', 'legacy_job_import'),
    ('legacy_job_import', 'google-places-live-details'),
])
def test_source_engine_alias_cannot_weaken_retention(engine, source_engine):
    result = enforce_observation_retention({
        'engine': engine, 'source_engine': source_engine,
        'source_url': 'https://example.test/source',
        'content': 'RESTRICTED_FIXTURE', 'claims': [{'value': 'RESTRICTED_FIXTURE'}],
        'retention': {'mode': 'retained', 'final_eligible': True},
    }, policy='retained')
    assert result['retention']['mode'] == 'metadata_only'
    assert result['retention']['final_eligible'] is False
    assert 'RESTRICTED_FIXTURE' not in str(result)
