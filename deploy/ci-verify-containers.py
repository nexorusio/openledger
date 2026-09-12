#!/usr/bin/env python3
"""Mandatory disposable-container acceptance; absence of Docker/PG is an error.

CI supplies a PostgreSQL administrator URL for ephemeral named databases. No
production volume or case is touched. Both real entrypoints must attest the same
new pipeline; both must refuse the old schema even if an environment flag asks
to disable release checking.
"""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]


def command(*args, **kwargs):
    completed = subprocess.run(
        args, check=False, text=True, capture_output=True, **kwargs
    )
    if completed.returncode:
        print(
            f"command failed with exit code {completed.returncode}: {args!r}",
            file=sys.stderr,
        )
        if completed.stdout.strip():
            print("child stdout:\n" + completed.stdout.strip(), file=sys.stderr)
        if completed.stderr.strip():
            print("child stderr:\n" + completed.stderr.strip(), file=sys.stderr)
        raise subprocess.CalledProcessError(
            completed.returncode,
            args,
            output=completed.stdout,
            stderr=completed.stderr,
        )
    return completed.stdout.strip()


def main(image):
    url = os.environ["OPENLEDGER_TEST_POSTGRES_URL"]
    administrator = create_engine(url, isolation_level="AUTOCOMMIT")
    suffix = uuid.uuid4().hex[:12]
    databases = [
        "pipeline_container_" + suffix,
        "pipeline_old_schema_" + suffix,
        "pipeline_previous_e2e_" + suffix,
    ]
    containers = [
        "pipeline-app-" + suffix,
        "pipeline-worker-" + suffix,
        "pipeline-old-app-" + suffix,
        "pipeline-old-worker-" + suffix,
        "pipeline-previous-app-" + suffix,
        "pipeline-previous-worker-" + suffix,
    ]
    created = []
    spec = importlib.util.spec_from_file_location(
        "manifest", ROOT / "deploy/release-manifest.py"
    )
    manifest_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manifest_module)
    expected = {
        "pipeline_id": "p2-e2e-v1",
        "engine_contract": "p2-e2e-v1",
        "schema_revision": "e2e2b8d0a502",
        "commit": command("git", "rev-parse", "HEAD"),
        "tree": command("git", "rev-parse", "HEAD^{tree}"),
        "source_digest": manifest_module.source_digest(ROOT),
    }
    image_id = json.loads(command("docker", "image", "inspect", image))[0]["Id"]

    def options(database, *, port=5089):
        database_url = administrator.url.set(database=database).render_as_string(
            hide_password=False
        )
        return [
            "--network",
            "host",
            "--env",
            "DATABASE_URL=" + database_url,
            "--env",
            "FLASK_SECRET_KEY=container-acceptance-only-secret",
            "--env",
            "AUTH_REQUIRED=false",
            "--env",
            f"PORT={port}",
            "--env",
            "OPENLEDGER_TRUSTED_HOSTS=127.0.0.1,localhost",
            "--env",
            "OPENLEDGER_PROFILE_DISCOVERY_ENABLED=false",
            "--env",
            "OPENLEDGER_MAIGRET_DISCOVERY_ENABLED=false",
            "--env",
            "OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED=false",
            "--env",
            "OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED=false",
        ]

    try:
        for database, target in zip(
            databases, ("e2e2b8d0a502", "b3e9d7c4a610", "e2e1a7c9d401")
        ):
            with administrator.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE "{database}"')
            created.append(database)
            command(
                "docker",
                "run",
                "--rm",
                *options(database),
                "--entrypoint",
                "alembic",
                image,
                "upgrade",
                target,
                timeout=120,
            )
        # Seed/complete conformance jobs before starting the real worker, so its
        # queue claim cannot race the explicit synthetic attempt lease.
        scanner_conformance = []
        for import_order in ("before", "after"):
            for client_kind in ("sync", "async"):
                output = command(
                    "docker",
                    "run",
                    "--rm",
                    *options(databases[0]),
                    "--entrypoint",
                    "python",
                    image,
                    "deploy/ci-scanner-transport.py",
                    "--import-order",
                    import_order,
                    "--client",
                    client_kind,
                    timeout=45,
                )
                scanner_conformance.append(json.loads(output.splitlines()[-1]))
        for role, name in zip(("app", "worker"), containers[:2]):
            extra = [] if role == "app" else ["--entrypoint", "python"]
            trailing = [] if role == "app" else ["-m", "maigret.web.worker"]
            command(
                "docker",
                "run",
                "-d",
                "--name",
                name,
                *options(databases[0]),
                *extra,
                image,
                *trailing,
            )
        app = worker = None
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                app = json.loads(
                    command(
                        "docker",
                        "exec",
                        containers[0],
                        "python",
                        "-c",
                        "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:5089/healthz',timeout=3).read().decode())",
                        timeout=5,
                    )
                )
                worker = json.loads(
                    command(
                        "docker",
                        "exec",
                        containers[1],
                        "python",
                        "-m",
                        "maigret.web.pipeline_release",
                        "worker-health",
                        timeout=5,
                    )
                )
                if app.get("status") != "ok":
                    raise ValueError("Application is not ready")
                manifest_module.verify_runtime(expected, app["pipeline"], worker)
                break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            raise RuntimeError("Real app and worker failed mandatory readiness")
        for container in containers[:2]:
            assert (
                command("docker", "inspect", "--format", "{{.Image}}", container)
                == image_id
            )
        for role, name, old_database in (
            ("app", containers[2], databases[1]),
            ("worker", containers[3], databases[1]),
            ("app", containers[4], databases[2]),
            ("worker", containers[5], databases[2]),
        ):
            extra = [] if role == "app" else ["--entrypoint", "python"]
            trailing = [] if role == "app" else ["-m", "maigret.web.worker"]
            refused = subprocess.run(
                [
                    "docker",
                    "run",
                    "--name",
                    name,
                    # The verified app remains bound to 5089. Use a distinct
                    # port so the old-schema app reaches its schema guard
                    # instead of failing earlier with an address collision.
                    *options(old_database, port=5090),
                    "--env",
                    "OPENLEDGER_RELEASE_REQUIRED=false",
                    *extra,
                    image,
                    *trailing,
                ],
                text=True,
                capture_output=True,
                timeout=40,
            )
            assert refused.returncode != 0, f"{role} accepted the previous schema"
            assert (
                "Pipeline requires schema e2e2b8d0a502" in refused.stderr
            ), f"{role} failed for an unrelated reason"
        record = ROOT / "runtime/ci/container-attestation.json"
        record.parent.mkdir(parents=True, exist_ok=True)
        record.write_text(
            json.dumps(
                {
                    "image_id": image_id,
                    "app": app,
                    "worker": worker,
                    "old_schema_refused_by": ["app", "worker"],
                    "refused_source_schemas": ["b3e9d7c4a610", "e2e1a7c9d401"],
                    "installed_scanner_transport_conformance": scanner_conformance,
                },
                indent=2,
            )
            + "\n"
        )
        print(
            "Real app/worker have the same p2-e2e-v1 image and schema; both refuse the previous schema"
        )
    finally:
        for container in containers:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        for database in created:
            with administrator.connect() as connection:
                connection.exec_driver_sql(f'DROP DATABASE "{database}" WITH (FORCE)')
        administrator.dispose()


if __name__ == "__main__":
    main(sys.argv[1])
