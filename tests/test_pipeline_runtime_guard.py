"""Fail closed when a released worker loses its required runtime contract."""

import json
import threading
import time

import pytest

from maigret.web import pipeline_release as release, worker


def _identity(**changes):
    return dict(
        pipeline_id=release.PIPELINE_ID,
        engine_contract=release.ENGINE_CONTRACT,
        schema_revision=release.SCHEMA_REVISION,
        commit="c" * 40,
        tree="d" * 40,
        source_digest="e" * 64,
        **changes,
    )


@pytest.mark.parametrize("development", [True, False, "true", 1, None])
def test_baked_identity_cannot_claim_development_mode(tmp_path, monkeypatch, development):
    path = tmp_path / "build.json"
    path.write_text(json.dumps(_identity(development=development)))
    monkeypatch.setattr(release, "BUILD_PATH", path)
    with pytest.raises(RuntimeError, match="cannot declare development"):
        release.runtime_attestation(None)


@pytest.mark.parametrize("pid", [0, -1, True, "1", None])
def test_worker_health_rejects_process_group_and_invalid_pids(tmp_path, monkeypatch, pid):
    path = tmp_path / "worker.json"
    path.write_text(json.dumps(dict(
        _identity(), role="worker", status="ready", pid=pid,
        heartbeat_at=time.time(),
    )))
    monkeypatch.setattr(release, "WORKER_PATH", path)
    monkeypatch.setattr(release, "build_identity", _identity)
    with pytest.raises(RuntimeError, match="positive process ID"):
        release.verify_live_worker()


def test_attestation_failure_stops_worker_and_removes_ready_heartbeat(monkeypatch):
    removed = []

    def failed_attestation(_store):
        raise RuntimeError("Pipeline requires schema")

    class Store:
        def mark_stale_running(self, _seconds):
            pytest.fail("Must stop before additional store operations")

    monkeypatch.setattr(worker, "publish_worker_attestation", failed_attestation)
    monkeypatch.setattr(worker, "remove_worker_attestation", lambda: removed.append(True))
    monkeypatch.setattr(worker, "record_internal_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "WORKER_HEARTBEAT_SECONDS", 0.001)
    monkeypatch.setattr(worker, "stopping", threading.Event())
    worker.monitor_stale_jobs(Store(), threading.Event())
    assert worker.stopping.is_set()
    assert removed == [True]


def test_initial_attestation_failure_releases_acquired_worker_lock(monkeypatch):
    class Lock:
        closed = False

        def close(self):
            self.closed = True

    lock = Lock()

    class Store:
        def ping(self):
            pass

        def try_acquire_worker_lock(self):
            return lock

    def failed_attestation(_store):
        raise RuntimeError("Runtime source differs")

    monkeypatch.setattr(worker, "case_store", Store())
    monkeypatch.setattr(worker, "assert_runtime_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "publish_worker_attestation", failed_attestation)
    monkeypatch.setattr(worker, "remove_worker_attestation", lambda: None)
    monkeypatch.setattr(worker.signal, "signal", lambda *args: None)
    with pytest.raises(RuntimeError, match="Runtime source differs"):
        worker.run()
    assert lock.closed
