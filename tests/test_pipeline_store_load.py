"""Required deterministic ledger reconciliation gate, opt-in for release CI.

Run OPENLEDGER_TEST_PIPELINE_LOAD=1 pytest -q tests/test_pipeline_store_load.py.
This measures engineered persistence integrity, never internet accuracy.
"""

import os
import time

import pytest
from sqlalchemy import func, select, update

from maigret.web.case_store import CaseStore, investigation_jobs, utcnow
from maigret.web.pipeline_store import PipelineStore


@pytest.mark.skipif(
    os.getenv("OPENLEDGER_TEST_PIPELINE_LOAD") != "1",
    reason="Release load gate; set OPENLEDGER_TEST_PIPELINE_LOAD=1",
)
def test_fifty_thousand_returned_observations_reconcile(tmp_path):
    started = time.monotonic()
    store = CaseStore(f"sqlite:///{tmp_path}/pipeline-load.db", create_schema=True)
    pipeline = PipelineStore(store)
    case_ids = []
    returned_count, unique_count = 0, 0
    try:
        for case_number in range(2):
            # Historical subject fixture avoids executing or bypassing the mandatory
            # new-submission planner; this gate measures the explicit ledger below.
            job_id = f"synthetic-load-history-{case_number}"
            assert store.import_legacy_result(
                job_id,
                {"status": "completed", "usernames": [f"synthetic-load-{case_number}"]},
            )
            case_id = store.get_job(job_id)["case_id"]
            persona_id = store.get_case(case_id)["personas"][0]["id"]
            case_ids.append(case_id)
            with store.engine.begin() as connection:
                connection.execute(
                    update(investigation_jobs)
                    .where(investigation_jobs.c.id == job_id)
                    .values(
                        status="running", worker_id="worker:load", heartbeat_at=utcnow()
                    )
                )
            request = pipeline.create_request(
                case_id,
                persona_id,
                [{"type": "username", "value": f"synthetic-load-{case_number}"}],
                {
                    "tasks": [
                        {
                            "task_id": f"source-{task_number:02d}",
                            "engine_id": f"fixture-engine-{task_number % 3}",
                            "route_state": "active",
                            "retry_ceiling": 0,
                        }
                        for task_number in range(50)
                    ]
                },
                actor="release-fixture",
                job_id=job_id,
            )
            for task_number, task in enumerate(request["tasks"]):
                with store.engine.begin() as connection:
                    connection.execute(
                        update(investigation_jobs)
                        .where(investigation_jobs.c.id == job_id)
                        .values(heartbeat_at=utcnow())
                    )
                attempt = pipeline.start_attempt(task["id"], "worker:load")
                outcome = "blocked" if task_number % 10 == 0 else "found"
                observations = [
                    {
                        "id": f"load:{case_number}:{task_number}:{index}",
                        "case_id": case_id,
                        "subject_id": persona_id,
                        "request_id": request["id"],
                        "task_id": task["id"],
                        "attempt_id": attempt["id"],
                        "status": outcome,
                        "source_url": f"https://example.org/synthetic/{case_number}/{index}",
                        "origin_family_id": f"origin:{case_number}:{index}",
                        "payload": {"fixture": True, "reported_value": index},
                    }
                    for index in range(450)
                ]
                delivered = observations + observations[:50]
                pipeline.record_observations(
                    attempt["id"], delivered, outcome=outcome, worker_id="worker:load"
                )
                returned_count += len(delivered)
                unique_count += len(observations)
            assert len(list(pipeline.iter_observations(case_id, persona_id))) == 22500
        observations, tasks, attempts = (
            pipeline._table("observations"),
            pipeline._table("tasks"),
            pipeline._table("attempts"),
        )
        with store.engine.connect() as connection:
            assert (
                connection.scalar(select(func.count()).select_from(observations))
                == unique_count
                == 45000
            )
            assert returned_count == 50000
            assert connection.scalar(select(func.count()).select_from(tasks)) == 100
            assert connection.scalar(select(func.count()).select_from(attempts)) == 100
            assert (
                connection.scalar(
                    select(func.count())
                    .select_from(tasks)
                    .where(tasks.c.status != "completed")
                )
                == 0
            )
            assert (
                connection.scalar(
                    select(func.count())
                    .select_from(tasks)
                    .where(tasks.c.outcome == "blocked")
                )
                == 10
            )
            assert (
                connection.scalar(
                    select(func.count())
                    .select_from(tasks)
                    .where(tasks.c.outcome == "found")
                )
                == 90
            )
            assert (
                connection.scalar(
                    select(func.count())
                    .select_from(
                        observations.join(tasks, observations.c.task_id == tasks.c.id)
                    )
                    .where(
                        (observations.c.case_id != tasks.c.case_id)
                        | (observations.c.persona_id != tasks.c.persona_id)
                    )
                )
                == 0
            )
            groups = connection.execute(
                select(observations.c.case_id, func.count()).group_by(
                    observations.c.case_id
                )
            ).all()
            assert dict(groups) == {case_id: 22500 for case_id in case_ids}
        print(
            f"ledger_load returned=50000 unique=45000 duplicates=5000 tasks=100 blocked=10 lost=0 cross_case_errors=0 elapsed_seconds={time.monotonic() - started:.2f}"
        )
    finally:
        store.dispose()
