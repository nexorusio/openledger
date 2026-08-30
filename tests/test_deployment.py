"""Regression tests for the supported OpenLedger production deployment."""

from pathlib import Path
import json
import os
import subprocess

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_web_image_uses_single_process_gunicorn_server():
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    web_stage = dockerfile.split("FROM base AS web", 1)[1].split("FROM base AS cli", 1)[
        0
    ]

    assert "'gunicorn>=23,<24'" in web_stage
    assert "exec gunicorn" in web_stage
    assert "--workers 1" in web_stage
    assert "--worker-class gthread" in web_stage
    assert "maigret.web.app:app" in web_stage
    assert "maigret --web" not in web_stage
    assert "USER 10001:10001" in web_stage


def test_container_build_context_excludes_runtime_secrets():
    dockerignore = (REPOSITORY_ROOT / ".dockerignore").read_text(encoding="utf-8")
    ignored = set(dockerignore.splitlines())
    assert "runtime/" in ignored
    assert "reports/" in ignored
    assert "deploy/.env" in ignored
    assert ".env" in ignored
    assert ".venv/" in ignored
    assert "*.log" in ignored


def test_caddy_uses_application_login_instead_of_browser_basic_auth():
    caddyfile = (REPOSITORY_ROOT / "deploy" / "Caddyfile").read_text(encoding="utf-8")
    compose = (REPOSITORY_ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8")

    assert "basic_auth" not in caddyfile
    assert "AUTH_PASSWORD_HASH" not in caddyfile
    assert 'AUTH_REQUIRED: "true"' in compose
    assert 'SESSION_COOKIE_SECURE: "true"' in compose
    assert 'OPENLEDGER_PROXY_HOPS: "1"' in compose
    assert 'OPENLEDGER_TRUSTED_HOSTS: "${DOMAIN},127.0.0.1,localhost,app"' in compose
    assert "OPENLEDGER_ALLOW_CUSTOM_AI_ENDPOINT" in compose
    assert "OPENLEDGER_ALLOW_PRIVATE_AI_ENDPOINT" in compose
    assert "AUTH_FILE: /app/runtime/secrets/auth.json" in compose
    assert "../runtime/secrets:/app/runtime/secrets" in compose
    assert (
        "../runtime/secrets/postgres_password:/app/runtime/secrets/postgres_password:ro"
        in compose
    )
    assert 'POSTGRES_INITDB_ARGS: "--data-checksums"' in compose
    assert "image: openledger-maigret:application-auth" in compose
    # Flask owns the route-specific framing policy. A global proxy header would
    # override the one authenticated evidence graph that is safe to embed.
    assert "\n        X-Frame-Options " not in caddyfile
    assert "Permissions-Policy" in caddyfile
    assert "X-Robots-Tag" in caddyfile


def test_codeql_scans_pull_requests_with_current_actions_and_extended_queries():
    workflow = (
        REPOSITORY_ROOT / ".github" / "workflows" / "codeql-analysis.yml"
    ).read_text(encoding="utf-8")

    assert "pull_request:" in workflow
    assert "actions/checkout@v4" in workflow
    assert "github/codeql-action/init@v3" in workflow
    assert "github/codeql-action/analyze@v3" in workflow
    assert "queries: security-extended" in workflow
    assert "language: [python, javascript-typescript]" in workflow
    assert "languages: ${{ matrix.language }}" in workflow
    assert "config-file: ./.github/codeql-config.yml" in workflow
    assert "github/codeql-action/init@v1" not in workflow

    config = (REPOSITORY_ROOT / ".github" / "codeql-config.yml").read_text(
        encoding="utf-8"
    )
    assert "maigret/web/static/vendor/**" in config


def test_create_auth_script_hashes_password_and_protects_file(tmp_path):
    auth_file = tmp_path / "secrets" / "auth.json"
    password = "correct-horse-battery-staple"
    subprocess.run(
        [
            str(REPOSITORY_ROOT / "deploy" / "create_auth.py"),
            str(auth_file),
            "operator",
        ],
        input=password,
        text=True,
        check=True,
    )

    payload = json.loads(auth_file.read_text(encoding="utf-8"))
    assert payload["username"] == "operator"
    assert payload["password"]["algorithm"] == "pbkdf2_sha256"
    assert password not in auth_file.read_text(encoding="utf-8")
    assert os.stat(auth_file).st_mode & 0o777 == 0o600
    assert len(payload["revision"]) >= 16


def test_create_auth_rejects_short_password_without_echoing_it(tmp_path):
    password = "tiny-secret"
    result = subprocess.run(
        [
            str(REPOSITORY_ROOT / "deploy" / "create_auth.py"),
            str(tmp_path / "auth.json"),
            "operator",
        ],
        input=password,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert password not in result.stdout
    assert password not in result.stderr
    assert "Password must contain at least" in result.stderr


def test_deployment_shell_scripts_pass_syntax_check():
    for script_name in (
        "install.sh",
        "update.sh",
        "configure-auth.sh",
        "reset-password.sh",
    ):
        subprocess.run(
            ["bash", "-n", str(REPOSITORY_ROOT / "deploy" / script_name)],
            check=True,
        )


def test_update_verifies_database_backup_before_migration():
    update_script = (REPOSITORY_ROOT / "deploy" / "update.sh").read_text(
        encoding="utf-8"
    )
    backup_position = update_script.index("pg_dump --format=custom")
    verification_position = update_script.index("pg_restore --list")
    deploy_position = update_script.index("build --pull app")
    assert backup_position < verification_position < deploy_position


def test_deployment_rejects_non_file_database_secret_and_writes_atomically():
    for script_name in ("install.sh", "update.sh"):
        script = (REPOSITORY_ROOT / "deploy" / script_name).read_text(encoding="utf-8")
        assert '! -f "${password_file}"' in script
        assert '-L "${password_file}"' in script
        assert 'mktemp "${password_file}.XXXXXX"' in script
        assert 'mv -f "${temporary_file}" "${password_file}"' in script
