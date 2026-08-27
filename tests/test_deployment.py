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


def test_caddy_uses_application_login_instead_of_browser_basic_auth():
    caddyfile = (REPOSITORY_ROOT / "deploy" / "Caddyfile").read_text(encoding="utf-8")
    compose = (REPOSITORY_ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8")

    assert "basic_auth" not in caddyfile
    assert "AUTH_PASSWORD_HASH" not in caddyfile
    assert 'AUTH_REQUIRED: "true"' in compose
    assert "AUTH_FILE: /app/runtime/secrets/auth.json" in compose
    assert "../runtime/secrets:/app/runtime/secrets" in compose
    assert "image: openledger-maigret:application-auth" in compose


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
