import threading

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
