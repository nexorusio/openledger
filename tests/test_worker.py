import threading
import time

from maigret.web import worker


class _WorkerLock:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _LaneStore:
    def __init__(self):
        self.lock = _WorkerLock()
        self.ai_job: dict | None = {
            "job_id": "ai-job",
            "kind": "case_fusion_ai",
            "usernames": [],
        }
        self.normal_job: dict | None = {
            "job_id": "normal-job",
            "kind": "live",
            "usernames": ["alice"],
        }

    def ping(self):
        return True

    def try_acquire_worker_lock(self):
        return self.lock

    def mark_stale_running(self, _seconds):
        return 0

    def claim_next_matching(
        self, _worker_id, *, include_kinds=None, exclude_kinds=None
    ):
        if include_kinds and self.ai_job:
            job, self.ai_job = self.ai_job, None
            return job
        if exclude_kinds and self.normal_job:
            job, self.normal_job = self.normal_job, None
            return job
        return None


def test_worker_runs_normal_jobs_while_combined_ai_is_waiting(monkeypatch):
    store = _LaneStore()
    ai_started = threading.Event()
    release_ai = threading.Event()
    normal_ran = threading.Event()

    def fake_execute(_store, job, *, shutdown_check):
        if job["kind"] == "case_fusion_ai":
            ai_started.set()
            release_ai.wait(timeout=2)
            return
        assert ai_started.wait(timeout=1)
        assert not release_ai.is_set()
        normal_ran.set()
        release_ai.set()
        worker.stopping.set()

    monkeypatch.setattr(worker, "case_store", store)
    monkeypatch.setattr(worker, "execute_job", fake_execute)
    monkeypatch.setattr(worker.signal, "signal", lambda *_args: None)
    monkeypatch.setenv("WORKER_POLL_SECONDS", "0.01")
    worker.stopping.clear()
    try:
        assert worker.run() == 0
    finally:
        release_ai.set()
        worker.stopping.clear()

    assert normal_ran.is_set()
    assert store.lock.closed is True


def test_execute_job_heartbeats_during_silent_work(monkeypatch):
    class Store:
        def __init__(self):
            self.heartbeats = 0

        def heartbeat(self, job_id, worker_id):
            assert (job_id, worker_id) == ("job-1", "worker:one")
            self.heartbeats += 1
            return True

    store = Store()

    def silent_job(_store, _job, *, shutdown_check):
        time.sleep(0.045)
        assert shutdown_check() is False

    monkeypatch.setattr(worker, "WORKER_HEARTBEAT_SECONDS", 0.01)
    monkeypatch.setattr(worker, "run_persistent_job", silent_job)
    worker.execute_job(
        store,
        {
            "job_id": "job-1",
            "worker_id": "worker:one",
            "kind": "live",
            "usernames": ["alice"],
        },
        shutdown_check=lambda: False,
    )

    assert store.heartbeats >= 3


def test_heartbeat_rejection_stops_the_expired_execution(monkeypatch):
    class Store:
        def heartbeat(self, _job_id, _worker_id):
            return False

    observed_stop = threading.Event()

    def wait_for_lease_loss(_store, _job, *, shutdown_check):
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            if shutdown_check():
                observed_stop.set()
                return
            time.sleep(0.002)
        raise AssertionError("lease loss was not propagated to the execution")

    monkeypatch.setattr(worker, "WORKER_HEARTBEAT_SECONDS", 0.01)
    monkeypatch.setattr(worker, "run_persistent_job", wait_for_lease_loss)
    worker.execute_job(
        Store(),
        {
            "job_id": "job-2",
            "worker_id": "worker:expired",
            "kind": "live",
            "usernames": ["alice"],
        },
        shutdown_check=lambda: False,
    )

    assert observed_stop.is_set()


def test_watchdog_uses_thirty_second_stale_threshold(monkeypatch):
    calls = []
    stop = threading.Event()

    class Store:
        def mark_stale_running(self, seconds):
            calls.append(seconds)
            stop.set()
            return 0

    monkeypatch.setattr(worker, "WORKER_HEARTBEAT_SECONDS", 0.01)
    worker.monitor_stale_jobs(Store(), stop)

    assert calls == [30]
