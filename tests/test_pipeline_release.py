"""Release safety with a real Git checkout and a simulated Docker host.

These tests execute the actual updater; no container, database, registry or host
service is mutated. Source migrations are parsed, never executed by preflight.
"""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "sha256:" + "a" * 64
COMMIT = "c" * 40
TREE = "d" * 40


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


release = load_module(ROOT / "maigret/web/pipeline_release.py", "pipeline_release_test")
manifest_module = load_module(ROOT / "deploy/release-manifest.py", "manifest_test")


def identity(**changes):
    return dict(
        pipeline_id=release.PIPELINE_ID,
        engine_contract=release.ENGINE_CONTRACT,
        schema_revision=release.SCHEMA_REVISION,
        commit=COMMIT,
        tree=TREE,
        source_digest="b" * 64,
        **changes,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("pipeline_id", "p2"),
        ("pipeline_id", "p3"),
        ("engine_contract", "legacy"),
        ("schema_revision", "b3e9d7c4a610"),
        ("commit", "main"),
        ("tree", None),
        ("source_digest", None),
    ],
)
def test_build_rejects_old_missing_and_mixed_contracts(field, value):
    build = identity()
    build[field] = value
    with pytest.raises(RuntimeError):
        release.validate_build(build)


def test_released_runtime_cannot_disable_baked_validation(tmp_path, monkeypatch):
    build = tmp_path / "build.json"
    build.write_text(json.dumps(dict(identity(), pipeline_id="old-pipeline")))
    monkeypatch.setattr(release, "BUILD_PATH", build)
    monkeypatch.setenv("OPENLEDGER_RELEASE_REQUIRED", "false")
    with pytest.raises(RuntimeError, match="pipeline_id"):
        release.build_identity()
    build.unlink()
    monkeypatch.setenv("OPENLEDGER_RELEASE_REQUIRED", "true")
    with pytest.raises(RuntimeError, match="no immutable build"):
        release.build_identity()


def test_source_development_identifies_new_pipeline_without_legacy_switch(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(release, "BUILD_PATH", tmp_path / "absent.json")
    monkeypatch.delenv("OPENLEDGER_RELEASE_REQUIRED", raising=False)
    state = release.runtime_attestation(None)
    assert state["development"] and state["pipeline_id"] == "p2-e2e-v1"


@pytest.mark.parametrize("role", ["app", "worker"])
def test_runtime_startup_validates_registry_before_attestation(monkeypatch, role):
    calls = []
    registry = ModuleType("maigret.web.connectors.registry")

    def validate():
        calls.append("registry")
        raise ValueError("Missing declared connector implementation")

    registry.get_connector_registry = validate
    monkeypatch.setitem(sys.modules, registry.__name__, registry)
    monkeypatch.setattr(
        release, "runtime_attestation", lambda *args, **kwargs: calls.append("ready")
    )
    with pytest.raises(ValueError, match="Missing declared connector"):
        release.assert_runtime_ready(None, role=role)
    assert calls == ["registry"]


@pytest.mark.parametrize(
    "revisions", [[], ["b3e9d7c4a610"], ["e2e1a7c9d401"], ["e2e2b8d0a502"], ["e2e3c9d1f703", "extra"]]
)
def test_released_runtime_refuses_wrong_database(tmp_path, monkeypatch, revisions):
    build = tmp_path / "build.json"
    build.write_text(json.dumps(identity()))
    monkeypatch.setattr(release, "BUILD_PATH", build)
    monkeypatch.setattr(release, "verify_source_content", lambda build: None)
    registry = ModuleType("maigret.web.connectors.registry")
    registry.get_connector_registry = lambda: None
    monkeypatch.setitem(sys.modules, registry.__name__, registry)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query):
            return self

        def scalars(self):
            return revisions

    class Engine:
        def connect(self):
            return Connection()

    class Store:
        engine = Engine()

    with pytest.raises(RuntimeError, match="requires schema"):
        release.assert_runtime_ready(Store(), role="worker")


def test_worker_health_requires_live_fresh_same_build_process(tmp_path, monkeypatch):
    path = tmp_path / "worker.json"
    monkeypatch.setattr(release, "WORKER_PATH", path)
    monkeypatch.setattr(release, "build_identity", identity)
    state = dict(
        identity(),
        role="worker",
        status="ready",
        pid=os.getpid(),
        heartbeat_at=time.time(),
    )
    path.write_text(json.dumps(state))
    assert release.verify_live_worker()["pid"] == os.getpid()
    state["heartbeat_at"] = time.time() - 60
    path.write_text(json.dumps(state))
    with pytest.raises(RuntimeError, match="stale"):
        release.verify_live_worker()
    state.update(heartbeat_at=time.time(), commit="f" * 40)
    path.write_text(json.dumps(state))
    with pytest.raises(RuntimeError, match="different commit"):
        release.verify_live_worker()


@pytest.mark.parametrize(
    "role,field",
    [
        ("app", "pipeline_id"),
        ("worker", "pipeline_id"),
        ("worker", "commit"),
        ("app", "schema_revision"),
        ("worker", "engine_contract"),
        ("worker", "tree"),
    ],
)
def test_mixed_app_worker_runtime_cannot_pass(role, field):
    states = {
        name: dict(identity(), role=name, status="ready") for name in ("app", "worker")
    }
    states[role][field] = "previous"
    with pytest.raises(ValueError, match=f"{role} runtime mismatch"):
        manifest_module.verify_runtime(identity(), states["app"], states["worker"])


def git(repo, *args):
    return subprocess.check_output(
        [shutil.which("git"), "-C", str(repo), *args], text=True
    ).strip()


@pytest.fixture
def deployment(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    for directory in ("deploy", "migrations"):
        shutil.copytree(
            ROOT / directory,
            repo / directory,
            ignore=shutil.ignore_patterns(".env", "__pycache__"),
        )
    # The migration is supplied by the storage work package. A declaration-only
    # fixture allows this isolated delivery package to test its exact allowlist.
    migration = repo / "migrations/versions/e2e1a7c9d401_fixture.py"
    if not any("e2e1a7c9d401" in p.name for p in migration.parent.glob("*.py")):
        migration.write_text(
            "revision='e2e1a7c9d401'\ndown_revision='b3e9d7c4a610'\nbranch_labels=None\ndepends_on=None\n"
        )
    (repo / ".gitignore").write_text("runtime/\n.env\n__pycache__/\n")
    update = repo / "deploy/update.sh"
    escalation = 'if [[ ${EUID} -ne 0 ]]; then\n    exec sudo bash "${BASH_SOURCE[0]}" "$@"\nfi\n'
    assert update.read_text().count(escalation) == 1
    update.write_text(update.read_text().replace(escalation, ""))
    (repo / "deploy/.env").write_text(
        "DOMAIN=example.test\nFLASK_SECRET_KEY=test\nSEARXNG_SECRET=test\n"
    )
    for directory in ("secrets", "reports", "backups"):
        (repo / "runtime" / directory).mkdir(parents=True)
    for name in ("secrets/auth.json", "secrets/postgres_password", "web_settings.json"):
        (repo / "runtime" / name).write_text("{}")
    git(repo, "init", "-q")
    git(repo, "config", "user.name", "Release test")
    git(repo, "config", "user.email", "release@example.test")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "P2 e2e fixture")
    commit, tree = git(repo, "rev-parse", "HEAD"), git(repo, "rev-parse", "HEAD^{tree}")
    manifest = dict(
        identity(),
        manifest_version=1,
        commit=commit,
        tree=tree,
        source_digest=manifest_module.source_digest(repo),
        image=IMAGE,
        image_id=IMAGE,
        accepted_source_schemas=manifest_module.ACCEPTED_SOURCE_SCHEMAS,
        migration_checksums=manifest_module.migration_checksums(repo),
    )
    manifest_path = repo / "runtime/release.json"
    manifest_path.write_text(json.dumps(manifest))
    binary = tmp_path / "bin"
    binary.mkdir()
    simulator = binary / "docker"
    simulator.write_text(f"#!{sys.executable}\n" + r"""
import json, os, pathlib, signal, sys
args = sys.argv[1:]
with open(os.environ['RELEASE_TRACE'], 'a') as stream:
    stream.write(json.dumps(['docker'] + args) + '\n')
manifest = json.loads(pathlib.Path(os.environ['RELEASE_MANIFEST']).read_text())
state_path = pathlib.Path(os.environ['RELEASE_STATE'])
state = json.loads(state_path.read_text()) if state_path.exists() else {}
def save(): state_path.write_text(json.dumps(state))
if args[:2] == ['image', 'inspect']:
    labels = {'org.opencontainers.image.revision':manifest['commit'],
              'io.openledger.tree':manifest['tree'], 'io.openledger.source-digest':os.getenv('BAD_SOURCE_DIGEST', manifest['source_digest']), 'io.openledger.pipeline':os.getenv('BAD_IMAGE_PIPELINE', 'p2-e2e-v1'),
              'io.openledger.schema':'e2e3c9d1f703', 'io.openledger.engine-contract':'p2-e2e-v1'}
    print(json.dumps([{'Id':manifest['image_id'], 'Config': {'Labels':labels}}]))
elif args[0] == 'ps':
    if 'label=com.docker.compose.service=db' in args: print(os.getenv('DB_CONTAINERS', 'abcdef012345'))
    elif os.getenv('DRAIN_FAILURE'): print('ffffeeee1111')
elif args[0] == 'inspect':
    print(os.getenv('BAD_CONTAINER_IMAGE', manifest['image_id']))
elif args[0] == 'exec':
    if 'psql' in args:
        if os.getenv('QUERY_FAILURE') or (state.get('migrated') and os.getenv('POST_MIGRATION_QUERY_FAILURE')): sys.exit(1)
        print('e2e3c9d1f703' if state.get('migrated') else os.getenv('DB_REVISION', 'b3e9d7c4a610'))
    elif 'pg_dump' in args: print('backup')
    elif 'pg_restore' in args:
        sys.stdin.read()
        sys.exit(int(os.getenv('BACKUP_FAILURE', '0')))
    else: sys.exit('Unexpected direct Docker exec')
elif args[0] == 'compose':
    if 'stop' in args: state['stopped'] = True; save()
    elif 'run' in args:
        if os.getenv('MIGRATION_FAILURE'): sys.exit(1)
        state['migrated'] = True; save()
    elif 'exec' in args:
        role = 'app' if 'app' in args else 'worker'
        attestation = {key:manifest[key] for key in ('pipeline_id','engine_contract','schema_revision','commit','tree','source_digest')}
        attestation.update(role=role, status='ready', pid=999)
        if os.getenv('BAD_WORKER') and role=='worker': attestation['pipeline_id']='old-pipeline'
        print(json.dumps({'status':'ok','pipeline':attestation} if role=='app' else attestation))
    elif 'up' in args and 'app' in args and os.getenv('INTERRUPT_AFTER_START'):
        os.kill(os.getppid(), getattr(signal, os.environ['INTERRUPT_AFTER_START']))
    elif 'ps' in args and '-q' in args: print('appcontainer' if 'app' in args else 'workercontainer')
    elif 'config' not in args and 'up' not in args and 'ps' not in args: sys.exit('Unexpected Compose command')
else: sys.exit('Unexpected Docker operation')
""")
    simulator.chmod(0o755)
    environment = dict(
        os.environ,
        PATH=str(binary) + os.pathsep + os.environ["PATH"],
        RELEASE_TRACE=str(tmp_path / "trace.jsonl"),
        RELEASE_STATE=str(tmp_path / "host.json"),
        RELEASE_MANIFEST=str(manifest_path),
    )
    return repo, manifest, manifest_path, environment


def run_update(fixture, *args, **env):
    repo, manifest, path, environment = fixture
    result = subprocess.run(
        ["bash", str(repo / "deploy/update.sh"), *args],
        env=dict(environment, **env),
        text=True,
        capture_output=True,
    )
    trace_path = Path(environment["RELEASE_TRACE"])
    trace = (
        [json.loads(line) for line in trace_path.read_text().splitlines()]
        if trace_path.exists()
        else []
    )
    return result, trace


def pinned_args(fixture, check=False):
    return ["--commit", fixture[1]["commit"], "--manifest", str(fixture[2])] + (
        ["--check"] if check else []
    )


def no_mutations(fixture, trace):
    for command in trace:
        assert (
            command[1:3] == ["image", "inspect"]
            or command[1] == "ps"
            or (command[1] == "exec" and "psql" in command)
        ), command
    assert not list((fixture[0] / "runtime/backups").iterdir())
    assert not list(fixture[0].rglob("__pycache__"))


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--commit", "main"],
        ["--commit", "a" * 40],
        ["--commit", "A" * 40, "--manifest", "x"],
    ],
)
def test_update_requires_both_reviewed_pins(deployment, args):
    result, trace = run_update(deployment, *args)
    assert result.returncode != 0 and "Usage:" in result.stderr
    assert not trace


def test_check_is_read_only_and_target_revision_is_accepted(deployment):
    result, trace = run_update(
        deployment, *pinned_args(deployment, True), DB_REVISION="e2e1a7c9d401"
    )
    assert result.returncode == 0, result.stderr
    assert "No changes made" in result.stdout
    no_mutations(deployment, trace)
    sql = next(command for command in trace if "psql" in command)
    assert (
        "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000" in sql
    )


@pytest.mark.parametrize(
    "env",
    [
        {"DB_REVISION": "p3"},
        {"DB_REVISION": ""},
        {"DB_REVISION": "b3e9d7c4a610\ne2e1a7c9d401"},
        {"DB_REVISION": "8c4f2a1d9e70"},
        {"QUERY_FAILURE": "1"},
        {"DB_CONTAINERS": ""},
        {"DB_CONTAINERS": "abcdef012345\n111111111111"},
        {"BAD_IMAGE_PIPELINE": "p2"},
        {"BAD_SOURCE_DIGEST": "0" * 64},
    ],
)
def test_bad_release_or_database_refuses_before_runtime_writes(deployment, env):
    result, trace = run_update(deployment, *pinned_args(deployment), **env)
    assert result.returncode != 0
    no_mutations(deployment, trace)


@pytest.mark.parametrize(
    "field,value",
    [
        ("commit", "f" * 40),
        ("tree", "f" * 40),
        ("pipeline_id", "p3"),
        ("schema_revision", "b3e9d7c4a610"),
        ("image", "openledger:latest"),
        ("engine_contract", "legacy"),
        ("migration_checksums", {}),
    ],
)
def test_manifest_disagreement_refuses_before_writes(deployment, field, value):
    payload = dict(deployment[1])
    payload[field] = value
    deployment[2].write_text(json.dumps(payload))
    result, trace = run_update(deployment, *pinned_args(deployment))
    assert result.returncode != 0
    no_mutations(deployment, trace)


@pytest.mark.parametrize(
    "change",
    ["tracked", "untracked", "old-schema", "p3-module", "p3-migration", "broken-chain"],
)
def test_checkout_and_explicit_migration_allowlist_guards(deployment, change):
    repo = deployment[0]
    if change in {"tracked", "untracked"}:
        (
            repo
            / "deploy"
            / ("compose.yaml" if change == "tracked" else "unexpected.txt")
        ).write_text("changed")
    elif change == "old-schema":
        for path in (repo / "migrations/versions").glob("*e2e1a7c9d401*"):
            path.unlink()
    elif change == "p3-module":
        path = repo / "maigret/web/evidence_correlation.py"
        path.parent.mkdir(parents=True)
        path.write_text("# forbidden P3 implementation")
    elif change == "p3-migration":
        (repo / "migrations/versions/p3.py").write_text(
            "revision='p3'\ndown_revision='e2e1a7c9d401'\nbranch_labels=None\ndepends_on=None\n"
        )
    else:
        path = next((repo / "migrations/versions").glob("*b3e9d7c4a610*"))
        path.write_text(path.read_text().replace("8c4f2a1d9e70", "wrong"))
    if change not in {"tracked", "untracked"}:
        git(repo, "add", "-A")
        git(repo, "commit", "-qm", "Bad release")
        deployment[1].update(
            commit=git(repo, "rev-parse", "HEAD"),
            tree=git(repo, "rev-parse", "HEAD^{tree}"),
        )
        deployment[2].write_text(json.dumps(deployment[1]))
    result, trace = run_update(deployment, *pinned_args(deployment))
    assert result.returncode != 0
    no_mutations(deployment, trace)


@pytest.mark.parametrize(
    "source_schema", ["b3e9d7c4a610", "e2e1a7c9d401", "e2e2b8d0a502", "e2e3c9d1f703"]
)
def test_success_drains_backs_up_migrates_verifies_both_and_pins_image(
    deployment, source_schema
):
    result, trace = run_update(
        deployment, *pinned_args(deployment), DB_REVISION=source_schema
    )
    assert result.returncode == 0, result.stderr

    def pos(token):
        return next(index for index, command in enumerate(trace) if token in command)

    assert (
        pos("stop")
        < pos("pg_dump")
        < pos("pg_restore")
        < pos("run")
        < pos("up")
        < pos("worker-health")
    )
    assert all("pull" not in cmd and "build" not in cmd for cmd in trace)
    assert (
        "OPENLEDGER_RELEASE_IMAGE='" + IMAGE + "'"
        in (deployment[0] / "deploy/.env").read_text()
    )
    records = list((deployment[0] / "runtime/backups").glob("release-*"))
    assert len(records) == 1
    for name in (
        "manifest.json",
        "database.dump",
        "evidence-settings.tar.gz",
        "app.json",
        "worker.json",
    ):
        assert (records[0] / name).stat().st_size > 0


@pytest.mark.parametrize(
    "env",
    [
        {"BACKUP_FAILURE": "1"},
        {"MIGRATION_FAILURE": "1"},
        {"BAD_WORKER": "1"},
        {"BAD_CONTAINER_IMAGE": "sha256:" + "b" * 64},
        {"DRAIN_FAILURE": "1"},
        {"POST_MIGRATION_QUERY_FAILURE": "1"},
        {"INTERRUPT_AFTER_START": "SIGINT"},
        {"INTERRUPT_AFTER_START": "SIGTERM"},
    ],
)
def test_failure_stops_both_and_never_falls_back_or_drops_evidence(deployment, env):
    original = (deployment[0] / "deploy/.env").read_bytes()
    result, trace = run_update(deployment, *pinned_args(deployment), **env)
    assert result.returncode != 0
    assert "No automatic legacy rollback" in result.stderr
    if "INTERRUPT_AFTER_START" in env:
        assert result.returncode == (
            130 if env["INTERRUPT_AFTER_START"] == "SIGINT" else 143
        )
    last_stop = next(cmd for cmd in reversed(trace) if "stop" in cmd)
    assert all(service in last_stop for service in ("app", "worker", "caddy"))
    assert (deployment[0] / "deploy/.env").read_bytes() == original
    assert all(
        "downgrade" not in cmd and "down" not in cmd and "pull" not in cmd
        for cmd in trace
    )
    if "BACKUP_FAILURE" in env or "DRAIN_FAILURE" in env:
        assert all("run" not in cmd and "up" not in cmd for cmd in trace)


def test_source_digest_binds_code_while_excluding_runtime_secrets_and_caches(tmp_path):
    root = tmp_path / "source"
    (root / "maigret").mkdir(parents=True)
    (root / "deploy").mkdir()
    code = root / "maigret/app.py"
    code.write_text('PIPELINE="p2-e2e-v1"\n')
    shutil.copy(
        ROOT / "deploy/source-fingerprint.py", root / "deploy/source-fingerprint.py"
    )
    expected = manifest_module.source_digest(root)
    (root / "deploy/.env").write_text("PRIVATE_SECRET=never-copy\n")
    (root / "maigret/__pycache__").mkdir()
    (root / "maigret/__pycache__/app.pyc").write_bytes(b"generated")
    assert manifest_module.source_digest(root) == expected
    release.verify_source_content(dict(identity(), source_digest=expected), root=root)
    code.write_text('PIPELINE="previous-pipeline"\n')
    with pytest.raises(RuntimeError, match="source differs"):
        release.verify_source_content(
            dict(identity(), source_digest=expected), root=root
        )


def test_docker_build_identity_rejects_mismatched_source_before_writing(tmp_path):
    root = tmp_path / "build"
    (root / "deploy").mkdir(parents=True)
    (root / "maigret/web").mkdir(parents=True)
    for name in ("source-fingerprint.py", "write-build-identity.py"):
        shutil.copy(ROOT / "deploy" / name, root / "deploy" / name)
    shutil.copy(
        ROOT / "maigret/web/pipeline_release.py",
        root / "maigret/web/pipeline_release.py",
    )
    env = dict(
        os.environ,
        OPENLEDGER_RELEASE_COMMIT=COMMIT,
        OPENLEDGER_RELEASE_TREE=TREE,
        OPENLEDGER_SOURCE_DIGEST="0" * 64,
    )
    result = subprocess.run(
        [sys.executable, str(root / "deploy/write-build-identity.py")],
        env=env,
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0 and "source differs" in result.stderr
    assert not (root / "openledger-build.json").exists()
    env["OPENLEDGER_SOURCE_DIGEST"] = manifest_module.source_digest(root)
    subprocess.run(
        [sys.executable, str(root / "deploy/write-build-identity.py")],
        env=env,
        check=True,
    )
    built = json.loads((root / "openledger-build.json").read_text())
    assert built["source_digest"] == env["OPENLEDGER_SOURCE_DIGEST"]
    assert (root / "openledger-build.json").stat().st_mode & 0o777 == 0o444
