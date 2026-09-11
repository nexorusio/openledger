"""Regression tests for the supported OpenLedger production deployment."""

from pathlib import Path
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _compose_service(name):
    compose = (REPOSITORY_ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8")
    body = compose.split(f"  {name}:\n", 1)[1]
    next_service = re.search(r"(?m)^  [a-z0-9_-]+:\n", body)
    return body if next_service is None else body[: next_service.start()]


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


def test_profile_search_deployment_is_fail_closed_with_runtime_parity():
    expected = (
        "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED: "
        '"${OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED:-false}"',
        "OPENLEDGER_PROFILE_SEARCH_PROVIDER: "
        '"${OPENLEDGER_PROFILE_SEARCH_PROVIDER:-disabled}"',
        "OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE: "
        "/app/runtime/secrets/brave_search_api_key",
        "OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS: "
        '"${OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS:-10}"',
        "OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS: "
        '"${OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS:-5}"',
    )
    app = _compose_service("app")
    worker = _compose_service("worker")

    for setting in expected:
        assert setting in app
        assert setting in worker
    assert "OPENLEDGER_PROFILE_SEARCH_API_KEY:" not in app
    assert "OPENLEDGER_PROFILE_SEARCH_API_KEY:" not in worker
    assert "../runtime/secrets:/app/runtime/secrets" in app
    assert "../runtime/secrets:/app/runtime/secrets:ro" in worker


def test_self_hosted_search_is_private_optional_and_resource_bounded():
    service = _compose_service("searxng")
    settings = (REPOSITORY_ROOT / "deploy" / "searxng-settings.yml").read_text(
        encoding="utf-8"
    )

    assert 'profiles: ["self-hosted-search"]' in service
    assert "image: docker.io/searxng/searxng:2026.9.8-3fdc6d753" in service
    assert ":latest" not in service
    assert "ports:" not in service
    assert 'user: "977:977"' in service
    assert "SEARXNG_SECRET: ${SEARXNG_SECRET:?" in service
    assert "mem_limit: 384m" in service
    assert 'cpus: "0.50"' in service
    assert "pids_limit: 128" in service
    assert "cap_drop:\n      - ALL" in service
    assert "no-new-privileges:true" in service
    assert "./searxng-settings.yml:/etc/searxng/settings.yml:ro" in service
    assert "- openledger" in service

    assert "keep_only:" in settings
    assert "- brave" in settings
    assert "- duckduckgo" in settings
    assert "- json" in settings
    assert "limiter: false" in settings
    assert "public_instance: false" in settings
    assert "image_proxy: false" in settings
    assert "retries: 0" in settings
    assert "secret_key: ultrasecretkey" in settings
    assert "overridden-by-SEARXNG_SECRET" not in settings


def test_install_and_example_keep_profile_search_disabled_by_default():
    install_script = (REPOSITORY_ROOT / "deploy" / "install.sh").read_text(
        encoding="utf-8"
    )
    example = (REPOSITORY_ROOT / "deploy" / ".env.example").read_text(encoding="utf-8")

    for document in (install_script, example):
        assert "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED" in document
        assert "OPENLEDGER_PROFILE_SEARCH_PROVIDER" in document
        assert "OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS" in document
        assert "OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS" in document
    assert "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED=false" in example
    assert "OPENLEDGER_PROFILE_SEARCH_PROVIDER=disabled" in example
    assert "SEARXNG_SECRET=REPLACE_WITH_A_DIFFERENT" in example
    assert 'SEARXNG_SECRET="$(openssl rand -hex 32)"' in install_script
    assert "brave_search_api_key" not in install_script
    assert "OPENLEDGER_PROFILE_SEARCH_API_KEY=" not in example


def test_updater_starts_self_hosted_search_only_for_searxng_provider():
    update_script = (REPOSITORY_ROOT / "deploy" / "update.sh").read_text(
        encoding="utf-8"
    )

    assert "ensure_searxng_secret" in update_script
    assert "OPENLEDGER_PROFILE_SEARCH_PROVIDER" in update_script
    assert "COMPOSE_PROFILE_ARGS=(--profile self-hosted-search)" in update_script
    assert 'docker compose "${COMPOSE_PROFILE_ARGS[@]}"' in update_script


def test_self_hosted_search_operator_flow_is_guarded_and_reversible():
    script_path = REPOSITORY_ROOT / "deploy" / "self-hosted-search.sh"
    script = script_path.read_text(encoding="utf-8")

    assert os.access(script_path, os.X_OK)
    assert "MINIMUM_AVAILABLE_MEMORY_KIB=786432" in script
    assert "MINIMUM_AVAILABLE_DISK_KIB=1048576" in script
    assert "Type CONTINUE to proceed" in script
    assert "site:example.com" in script
    assert "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED false" in script
    assert "OPENLEDGER_PROFILE_SEARCH_PROVIDER searxng" in script
    assert "OPENLEDGER_PROFILE_SEARCH_PROVIDER disabled" in script
    assert "verify_searxng_secret" in script
    assert "hmac.compare_digest" in script
    assert "os.environ['SEARXNG_SECRET']" in script
    assert "trap fail_closed_shutdown ERR" in script
    assert "stop_and_verify_services app worker searxng" in script
    assert 'for service in "$@"' in script
    assert "OPENLEDGER_COMPOSE_PROJECT=openledger" in script
    assert '--project-name "${OPENLEDGER_COMPOSE_PROJECT}"' in script
    assert "label=com.docker.compose.project=${OPENLEDGER_COMPOSE_PROJECT}" in script
    assert "label=com.docker.compose.service=${service}" in script
    assert "running_service_containers" in script
    assert "CRITICAL: ${service} still has a running container" in script
    assert "compose stop app worker searxng || true" not in script
    assert "recreate_runtimes disabled False" in script
    assert "brave_search_api_key" not in script

    enable_block = script.split("    enable)", 1)[1].split("        ;;", 1)[0]
    assert enable_block.index("trap fail_closed_shutdown ERR") < enable_block.index(
        "set_env_value OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED true"
    )
    fail_closed_block = script.split("fail_closed_shutdown()", 1)[1].split(
        "show_status()", 1
    )[0]
    assert "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED false" in fail_closed_block
    assert "OPENLEDGER_PROFILE_SEARCH_PROVIDER disabled" in fail_closed_block
    assert "recreate_runtimes disabled False" in fail_closed_block


def test_compose_validation_supplies_the_local_searxng_secret():
    documents = [
        (REPOSITORY_ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
        for name in (
            "openledger-persistence.yml",
            "upstream-integrity.yml",
            "upstream-sync.yml",
        )
    ]
    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")

    for document in documents:
        assert "SEARXNG_SECRET" in document
    assert "SEARXNG_SECRET=development-only-searxng-secret" in readme


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
    assert payload["schema_version"] == 2
    assert payload["users"][0]["username"] == "operator"
    assert payload["users"][0]["role"] == "admin"
    assert payload["users"][0]["password"]["algorithm"] == "pbkdf2_sha256"
    assert password not in auth_file.read_text(encoding="utf-8")
    assert os.stat(auth_file).st_mode & 0o777 == 0o600
    assert len(payload["revision"]) >= 16
    assert len(payload["users"][0]["revision"]) >= 16


def test_create_auth_password_reset_preserves_existing_analysts(tmp_path):
    auth_file = tmp_path / "secrets" / "auth.json"
    script = str(REPOSITORY_ROOT / "deploy" / "create_auth.py")
    subprocess.run(
        [script, str(auth_file), "admin"],
        input="initial-admin-password",
        text=True,
        check=True,
    )
    payload = json.loads(auth_file.read_text(encoding="utf-8"))
    payload["users"].append(
        {
            "username": "field.analyst",
            "role": "analyst",
            "revision": "analyst-revision-value-1234",
            "password": payload["users"][0]["password"],
        }
    )
    auth_file.write_text(json.dumps(payload), encoding="utf-8")

    subprocess.run(
        [script, str(auth_file), "replacement-admin"],
        input="replacement-admin-password",
        text=True,
        check=True,
    )
    updated = json.loads(auth_file.read_text(encoding="utf-8"))

    assert [user["username"] for user in updated["users"]] == [
        "replacement-admin",
        "field.analyst",
    ]
    assert updated["users"][0]["role"] == "admin"
    assert updated["users"][1]["role"] == "analyst"


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


def _deployment_git(repository, *args):
    return subprocess.check_output(
        [shutil.which("git"), "-C", str(repository), *args], text=True
    ).strip()


@pytest.fixture
def pinned_p2_deployment(tmp_path):
    """Real temporary Git checkout; Docker and host mutations are simulated."""
    repository = tmp_path / "openledger"
    repository.mkdir()
    shutil.copytree(
        REPOSITORY_ROOT / "deploy",
        repository / "deploy",
        ignore=shutil.ignore_patterns(".env", "__pycache__"),
    )
    shutil.copytree(
        REPOSITORY_ROOT / "migrations",
        repository / "migrations",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    (repository / ".gitignore").write_text("runtime/\n.env\n__pycache__/\n")
    update = repository / "deploy" / "update.sh"
    # Test the actual shell program without requiring sudo in a non-root CI
    # process. Only privilege escalation is removed from the isolated fixture.
    escalation = (
        "if [[ ${EUID} -ne 0 ]]; then\n"
        '    exec sudo bash "${BASH_SOURCE[0]}" "$@"\n'
        "fi\n"
    )
    source = update.read_text(encoding="utf-8")
    assert source.count(escalation) == 1
    update.write_text(source.replace(escalation, ""), encoding="utf-8")
    (repository / "deploy" / ".env").write_text(
        "DOMAIN=openledger.example.test\nFLASK_SECRET_KEY=test-only\n"
        "SEARXNG_SECRET=test-only-search\n"
        "OPENLEDGER_PROFILE_SEARCH_PROVIDER=disabled\n"
    )
    for directory in ("secrets", "reports", "backups"):
        (repository / "runtime" / directory).mkdir(parents=True)
    for name, value in (
        ("secrets/auth.json", "{}"),
        ("secrets/postgres_password", "test-only-password"),
        ("web_settings.json", "{}"),
    ):
        (repository / "runtime" / name).write_text(value)
    _deployment_git(repository, "init", "-q")
    _deployment_git(repository, "config", "user.name", "Deployment test")
    _deployment_git(repository, "config", "user.email", "deployment@example.test")
    _deployment_git(repository, "add", ".")
    _deployment_git(repository, "commit", "-qm", "Reviewed P2 fixture")
    commit = _deployment_git(repository, "rev-parse", "HEAD")

    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    trace = tmp_path / "commands.jsonl"
    simulator = fakebin / "simulate"
    simulator.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, subprocess, sys\n"
        "name = pathlib.Path(sys.argv[0]).name\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['P2_TEST_TRACE'], 'a') as stream:\n"
        "    stream.write(json.dumps([name] + args) + '\\n')\n"
        "if name == 'git':\n"
        "    if args[0] == 'status' and os.environ.get('P2_TEST_GIT_STATUS_FAILURE') == '1':\n"
        "        sys.exit(1)\n"
        "    if args[0] not in ('status', 'rev-parse'):\n"
        "        sys.exit('Unexpected Git mutation or network operation')\n"
        "    sys.exit(subprocess.call([os.environ['P2_TEST_REAL_GIT']] + args))\n"
        "if name == 'docker':\n"
        "    if args[0] == 'ps':\n"
        "        print(os.environ.get('P2_TEST_DB_CONTAINERS', 'abcdef012345'))\n"
        "    elif args[0] == 'exec' and 'psql' in args:\n"
        "        if os.environ.get('P2_TEST_QUERY_FAILURE') == '1':\n"
        "            sys.exit(1)\n"
        "        print(os.environ.get('P2_TEST_DB_REVISION', 'b3e9d7c4a610'))\n"
        "    elif args[0] == 'compose':\n"
        "        if 'pg_dump' in args:\n"
        "            print('simulated-custom-format-backup')\n"
        "        elif 'pg_restore' in args:\n"
        "            sys.stdin.read()\n"
        "            sys.exit(int(os.environ.get('P2_TEST_BACKUP_FAILURE', '0')))\n"
        "    else:\n"
        "        sys.exit('Unexpected Docker operation')\n"
        "elif name == 'openssl':\n"
        "    print('0' * 64)\n"
    )
    simulator.chmod(0o755)
    for command in ("docker", "git", "install", "chown", "chmod", "openssl", "apt-get"):
        (fakebin / command).symlink_to(simulator)
    environment = dict(
        os.environ,
        PATH=str(fakebin) + os.pathsep + os.environ["PATH"],
        P2_TEST_TRACE=str(trace),
        P2_TEST_REAL_GIT=shutil.which("git"),
    )
    return repository, commit, environment, trace


def _run_p2_update(deployment, *args, **environment_changes):
    repository, _, environment, trace = deployment
    result = subprocess.run(
        ["bash", str(repository / "deploy" / "update.sh"), *args],
        env=dict(environment, **environment_changes),
        text=True,
        capture_output=True,
        check=False,
    )
    commands = (
        [json.loads(line) for line in trace.read_text().splitlines()]
        if trace.exists()
        else []
    )
    return result, commands


def _assert_no_p2_update_mutation(deployment, commands):
    for command in commands:
        assert (command[0] == "git" and command[1] in ("status", "rev-parse")) or (
            command[:2] == ["docker", "ps"]
            or (command[:2] == ["docker", "exec"] and "psql" in command)
        ), command
    repository = deployment[0]
    assert not list((repository / "runtime" / "backups").iterdir())


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("--commit", "main"),
        ("--commit", "HEAD"),
        ("--commit", "origin/main"),
        ("--commit", "abcdef0"),
        ("--commit", "f" * 40, "--force"),
        ("--commit", "F" * 40),
    ],
)
def test_p2_updater_requires_explicit_full_commit(pinned_p2_deployment, args):
    result, commands = _run_p2_update(pinned_p2_deployment, *args)
    assert result.returncode != 0
    assert "Usage:" in result.stderr
    assert commands == []
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


def test_p2_updater_help_requires_no_docker_or_privileges(pinned_p2_deployment):
    result, commands = _run_p2_update(pinned_p2_deployment, "--help")
    assert result.returncode == 0
    assert "never fetches" in result.stdout
    assert "inspect/start the existing P2 database separately" in result.stdout
    assert commands == []


def test_p2_updater_refuses_unapproved_checkout(pinned_p2_deployment):
    result, commands = _run_p2_update(pinned_p2_deployment, "--commit", "f" * 40)
    assert result.returncode != 0
    assert "HEAD is not the reviewed commit" in result.stderr
    assert all(command[0] == "git" for command in commands)
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


def test_p2_updater_fails_closed_when_git_status_fails(pinned_p2_deployment):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        P2_TEST_GIT_STATUS_FAILURE="1",
    )
    assert result.returncode != 0
    assert "Could not verify that the repository is clean" in result.stderr
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


@pytest.mark.parametrize("dirty_kind", ["tracked", "staged", "untracked"])
def test_p2_updater_refuses_dirty_checkout(pinned_p2_deployment, dirty_kind):
    repository, commit, _, _ = pinned_p2_deployment
    path = (
        repository
        / "deploy"
        / ("unexpected.py" if dirty_kind == "untracked" else "compose.yaml")
    )
    with path.open("a") as stream:
        stream.write("\n# Local change\n")
    if dirty_kind == "staged":
        _deployment_git(repository, "add", str(path))
    result, commands = _run_p2_update(pinned_p2_deployment, "--commit", commit)
    assert result.returncode != 0
    assert "repository has local changes" in result.stderr
    assert all(command[0] == "git" for command in commands)
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


@pytest.mark.parametrize(
    "contamination",
    ["missing-marker", "p3-marker", "p3-module", "p3-migration", "broken-chain"],
)
def test_p2_updater_refuses_incompatible_release_tree(
    pinned_p2_deployment, contamination
):
    repository = pinned_p2_deployment[0]
    if contamination == "missing-marker":
        (repository / "deploy" / "release-channel").unlink()
    elif contamination == "p3-marker":
        (repository / "deploy" / "release-channel").write_text("p3\n")
    elif contamination == "p3-module":
        path = repository / "maigret" / "web" / "evidence_correlation.py"
        path.parent.mkdir(parents=True)
        path.write_text("# P3 module must be rejected even with a P2 marker.\n")
    elif contamination == "p3-migration":
        (repository / "migrations" / "versions" / "c4f8a2d6e901_p3.py").write_text(
            "revision = 'c4f8a2d6e901'\ndown_revision = 'b3e9d7c4a610'\n"
            "branch_labels = None\ndepends_on = None\n"
        )
    else:
        path = (
            repository
            / "migrations"
            / "versions"
            / "b3e9d7c4a610_add_profile_search_candidate_reviews.py"
        )
        path.write_text(
            path.read_text().replace('= "8c4f2a1d9e70"', '= "c4f8a2d6e901"')
        )
    _deployment_git(repository, "add", "-A")
    _deployment_git(repository, "commit", "-qm", "Accidentally contaminated release")
    commit = _deployment_git(repository, "rev-parse", "HEAD")
    result, commands = _run_p2_update(pinned_p2_deployment, "--commit", commit)
    assert result.returncode != 0
    assert "P2 update refused:" in result.stderr
    assert all(command[0] == "git" for command in commands)
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


@pytest.mark.parametrize(
    "revision",
    ["c4f8a2d6e901", "8c4f2a1d9e70", "unknown", "", "b3e9d7c4a610\nc4f8a2d6e901"],
)
def test_p2_updater_refuses_incompatible_database_before_any_mutation(
    pinned_p2_deployment, revision
):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        P2_TEST_DB_REVISION=revision,
    )
    assert result.returncode != 0
    assert "Database must have exactly P2 revision" in result.stderr
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


@pytest.mark.parametrize("containers", ["", "abcdef012345\n012345abcdef"])
def test_p2_updater_refuses_missing_or_ambiguous_running_database(
    pinned_p2_deployment, containers
):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        P2_TEST_DB_CONTAINERS=containers,
    )
    assert result.returncode != 0
    assert "Expected one running openledger database" in result.stderr
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


def test_p2_updater_refuses_unreadable_database_revision(pinned_p2_deployment):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        P2_TEST_QUERY_FAILURE="1",
    )
    assert result.returncode != 0
    assert "Could not read the existing database revision" in result.stderr
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


def test_p2_updater_bad_database_cannot_bootstrap_missing_secrets(pinned_p2_deployment):
    repository = pinned_p2_deployment[0]
    environment_file = repository / "deploy" / ".env"
    old_environment = "DOMAIN=openledger.example.test\n"
    environment_file.write_text(old_environment)
    password = repository / "runtime" / "secrets" / "postgres_password"
    authentication = repository / "runtime" / "secrets" / "auth.json"
    password.unlink()
    authentication.unlink()
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        P2_TEST_DB_REVISION="c4f8a2d6e901",
    )
    assert result.returncode != 0
    assert "Database must have exactly P2 revision" in result.stderr
    assert environment_file.read_text() == old_environment
    assert not password.exists() and not authentication.exists()
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


def test_p2_updater_accepts_forward_rollback_while_remote_main_remains_p3(
    pinned_p2_deployment,
):
    repository = pinned_p2_deployment[0]
    path = repository / "maigret" / "web" / "evidence_correlation.py"
    path.parent.mkdir(parents=True)
    path.write_text("# P3 ancestor\n")
    _deployment_git(repository, "add", ".")
    _deployment_git(repository, "commit", "-qm", "P3 ancestor")
    p3_commit = _deployment_git(repository, "rev-parse", "HEAD")
    _deployment_git(repository, "update-ref", "refs/remotes/origin/main", p3_commit)
    _deployment_git(repository, "rm", str(path))
    _deployment_git(repository, "commit", "-qm", "Forward rollback to P2")
    reviewed_commit = _deployment_git(repository, "rev-parse", "HEAD")
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        reviewed_commit,
        "--check",
    )
    assert result.returncode == 0, result.stderr
    assert "P2 preflight passed" in result.stdout
    assert _deployment_git(repository, "rev-parse", "HEAD") == reviewed_commit
    assert _deployment_git(repository, "rev-parse", "origin/main") == p3_commit
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)


def test_p2_updater_preflight_is_read_only(pinned_p2_deployment):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        "--check",
    )
    assert result.returncode == 0, result.stderr
    assert "No changes made" in result.stdout
    _assert_no_p2_update_mutation(pinned_p2_deployment, commands)
    query = next(command for command in commands if "psql" in command)
    assert (
        "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000"
        in query
    )
    assert (
        "SELECT version_num FROM public.alembic_version ORDER BY version_num;" in query
    )
    assert "-X" in query and "ON_ERROR_STOP=1" in query


def test_p2_updater_accepts_reviewed_checkout_and_preserves_backup_order(
    pinned_p2_deployment,
):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
    )
    assert result.returncode == 0, result.stderr
    assert "P2 update completed from reviewed commit" in result.stdout
    first_query = next(i for i, command in enumerate(commands) if "psql" in command)
    first_runtime_write = next(
        i for i, command in enumerate(commands) if command[0] == "chown"
    )
    backup = next(i for i, command in enumerate(commands) if "pg_dump" in command)
    verify_backup = next(
        i for i, command in enumerate(commands) if "pg_restore" in command
    )
    final_query = max(i for i, command in enumerate(commands) if "psql" in command)
    build = next(i for i, command in enumerate(commands) if "build" in command)
    deploy = next(i for i, command in enumerate(commands) if "up" in command)
    assert (
        first_query
        < first_runtime_write
        < backup
        < verify_backup
        < final_query
        < build
        < deploy
    )
    assert all(
        command[1] in ("status", "rev-parse")
        for command in commands
        if command[0] == "git"
    )
    assert all(
        "--project-name" in command and "openledger" in command
        for command in commands
        if command[:2] == ["docker", "compose"]
    )
    assert (
        len(list((pinned_p2_deployment[0] / "runtime" / "backups").glob("*.dump"))) == 1
    )


def test_p2_updater_backup_failure_never_builds_or_changes_services(
    pinned_p2_deployment,
):
    result, commands = _run_p2_update(
        pinned_p2_deployment,
        "--commit",
        pinned_p2_deployment[1],
        P2_TEST_BACKUP_FAILURE="1",
    )
    assert result.returncode != 0
    assert "Database backup verification failed" in result.stdout
    assert all("build" not in command and "up" not in command for command in commands)
