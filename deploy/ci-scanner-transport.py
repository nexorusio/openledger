#!/usr/bin/env python3
"""Exercise the installed pinned scanner's httpx patches without provider I/O.

The container acceptance runner invokes each import order/client combination in
a fresh process, against its own disposable migrated PostgreSQL database. Real
scanner imports and the real durable meter are required; MockTransport replaces
only the final provider response. A missing package is an acceptance failure.
"""

import argparse
import asyncio
import importlib
import json
import os
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import update

from maigret.web.case_store import CaseStore, investigation_jobs, utcnow
from maigret.web.pipeline_http import TransportGuard
from maigret.web.pipeline_runtime import PipelineRuntimeStore, RequestBudgetExceeded
from maigret.web.pipeline_store import PipelineStore

SCANNER_MODULES = (
    "user_scanner.core.helpers",
    "user_scanner.core.email_orchestrator",
    "user_scanner.core.orchestrator",
)
SCANNER_CONFORMANCE_JOB_KIND = "scanner_conformance"


def import_scanner():
    for name in SCANNER_MODULES:
        importlib.import_module(name)


def main(import_order, client_kind):
    store = CaseStore(os.environ["DATABASE_URL"], create_schema=False)
    pipeline = PipelineStore(store)
    # This fixture exercises the P2 transport guard, not profile discovery.
    # Keep it outside the governed discovery job kinds so the container can
    # (and must) run with every outbound discovery capability disabled.
    job_id = store.create_investigation(
        ["scanner-ci-" + uuid.uuid4().hex],
        {},
        kind=SCANNER_CONFORMANCE_JOB_KIND,
    )
    worker = "scanner-conformance:" + uuid.uuid4().hex
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(status="running", worker_id=worker, heartbeat_at=utcnow())
        )
    case_id = store.get_job(job_id)["case_id"]
    persona_id = store.get_case(case_id)["personas"][0]["id"]
    request = pipeline.create_request(
        case_id,
        persona_id,
        [{"type": "username", "value": "offline-scanner-fixture"}],
        {
            "pipeline_id": "p2-e2e-v1",
            "budgets": {"max_requests": 1},
            "tasks": [
                {
                    "task_id": "scanner-ci",
                    "engine_id": "fixture",
                    "route_state": "active",
                    "retry_ceiling": 1,
                }
            ],
        },
        actor="container-conformance",
        job_id=job_id,
    )
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], worker)
    if import_order == "before":
        import_scanner()
    calls = []
    try:
        with TransportGuard(
            store, request["id"], attempt["id"], worker
        ).install() as guard:
            if import_order == "after":
                import_scanner()
            import httpx

            def provider(http_request):
                calls.append(str(http_request.url))
                return httpx.Response(200, json={"synthetic": True})

            def verify_blocked(send):
                assert send().status_code == 200
                try:
                    send()
                except RequestBudgetExceeded:
                    return
                raise AssertionError(
                    "Installed scanner patches bypassed the saved allowance"
                )

            if client_kind == "sync":
                with httpx.Client(
                    transport=httpx.MockTransport(provider), trust_env=False
                ) as client:
                    verify_blocked(
                        lambda: client.get("https://scanner-fixture.invalid/record")
                    )
            else:

                async def exercise_async():
                    async with httpx.AsyncClient(
                        transport=httpx.MockTransport(provider), trust_env=False
                    ) as client:
                        assert (
                            await client.get("https://scanner-fixture.invalid/record")
                        ).status_code == 200
                        try:
                            await client.get("https://scanner-fixture.invalid/record")
                        except RequestBudgetExceeded:
                            return
                        raise AssertionError(
                            "Installed scanner patches bypassed the saved allowance"
                        )

                asyncio.run(exercise_async())
            result = guard.finish({"outcome": "found"})
        assert len(calls) == 1, "Second request reached the provider transport"
        assert (
            PipelineRuntimeStore(store).budget_snapshot(request["id"])["consumed"] == 1
        )
        assert result["error_code"] == "request_budget_exhausted"
        assert result["completeness"] == "partial" and result["retryable"] is False
        pipeline.finish_attempt(attempt["id"], "inconclusive", worker_id=worker)
        with store.engine.begin() as connection:
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.id == job_id)
                .values(
                    status="completed",
                    worker_id=None,
                    heartbeat_at=None,
                    updated_at=utcnow(),
                )
            )
        print(
            json.dumps(
                {
                    "import_order": import_order,
                    "client": client_kind,
                    "scanner_modules": list(SCANNER_MODULES),
                    "provider_calls": len(calls),
                    "consumed": 1,
                    "second_send_refused": True,
                    "external_provider_calls": 0,
                },
                sort_keys=True,
            )
        )
    finally:
        store.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--import-order", choices=("before", "after"), required=True)
    parser.add_argument("--client", choices=("sync", "async"), required=True)
    args = parser.parse_args()
    main(args.import_order, args.client)
