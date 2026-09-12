"""Collector routing stays explicit without making any external requests."""

import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest

from maigret.web import app as web_app_module


@pytest.fixture
def web_app(tmp_path):
    web_app_module.app.config["TESTING"] = True
    web_app_module.app.config["AUTH_REQUIRED"] = False
    web_app_module.app.config["REPORTS_FOLDER"] = str(tmp_path / "reports")
    web_app_module.app.config["SETTINGS_FILE"] = str(tmp_path / "settings.json")
    yield web_app_module


@pytest.fixture
def client(web_app):
    return web_app.app.test_client()


# ``web_app`` deliberately replaces ``subprocess.Popen`` so a forgotten
# collector fixture cannot launch a production adapter.  Preserve the original
# callable before that fixture is activated and use it only for the explicit,
# local mocked-DOM helper below.
_ORIGINAL_POPEN = subprocess.Popen


def _run_dom_fixture(command, *, timeout):
    process = _ORIGINAL_POPEN(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output=stdout,
            stderr=stderr,
        )
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def test_builder_groups_first_and_accepts_legacy_handle_prefill(client, web_app):
    with web_app.app.test_request_context('/'):
        context = web_app.investigation_builder_context()
        context['initial_identifiers'] = [{'type': 'social_handle', 'value': '@alice'}]
        body = web_app.render_template('index.html', **context)
    assert body.index('Who are you investigating?') < body.index('Known identifiers')
    assert 'value="same_subject" checked' in body
    assert '<option value="username" selected>Username</option>' in body
    assert '<option value="social_handle"' not in body
    assert '<option value="profile_url"' not in body
    assert 'value="@alice"' in body
    assert 'Investigation workspace' in body
    assert 'Username checks (Maigret)' in body
    assert 'Where your identifiers go' in body
    assert 'One username target across eligible social sites' in body
    assert 'generated aliases stay with their source subject' in body
    assert '0 account checks' not in body


@pytest.mark.parametrize(
    'enabled,provider,expected,reason',
    [
        ('', 'brave', False, 'Disabled by server policy.'),
        ('false', 'brave', False, 'Disabled by server policy.'),
        ('true', 'disabled', False, 'No native search provider is enabled.'),
        ('true', 'brave', True, 'Provider configured; credentials are checked at collection.'),
        ('true', 'unrecognized', False, 'Native search server configuration is incomplete.'),
    ],
)
def test_native_search_form_status_is_safe_and_fail_closed(
    web_app, monkeypatch, enabled, provider, expected, reason
):
    monkeypatch.setenv('OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED', enabled)
    monkeypatch.setenv('OPENLEDGER_PROFILE_SEARCH_PROVIDER', provider)
    monkeypatch.setenv('OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE', '/secret/do-not-display-api-key')
    status = web_app.investigation_collector_status()
    assert status['native_search'] == {'enabled': expected, 'reason': reason}
    assert '/secret/' not in json.dumps(status)
    assert 'api_key' not in json.dumps(status)


def test_builder_rendered_script_and_dynamic_routes(client, tmp_path):
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is needed for the local mocked-DOM routing regression test')
    body = client.get('/').get_data(as_text=True)
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', body, re.S)
    script = next(script for script in scripts if "const form = document.getElementById('investigation-builder')" in script)
    source = tmp_path / 'builder.js'
    source.write_text(script)
    result = _run_dom_fixture(
        [node, str(Path(__file__).with_name('investigation_builder_dom.cjs')), str(source)],
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'routing DOM scenarios passed' in result.stdout
