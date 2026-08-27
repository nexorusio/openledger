"""Regression tests for the supported OpenLedger production deployment."""

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_web_image_uses_single_process_gunicorn_server():
    dockerfile = (REPOSITORY_ROOT / 'Dockerfile').read_text(encoding='utf-8')
    web_stage = dockerfile.split('FROM base AS web', 1)[1].split(
        'FROM base AS cli', 1
    )[0]

    assert "'gunicorn>=23,<24'" in web_stage
    assert 'exec gunicorn' in web_stage
    assert '--workers 1' in web_stage
    assert '--worker-class gthread' in web_stage
    assert 'maigret.web.app:app' in web_stage
    assert 'maigret --web' not in web_stage
