import queue
from pathlib import Path

import maigret.report
from maigret.web import app as web_app


def test_collector_observations_are_reported_without_maigret_results(
    monkeypatch, tmp_path
):
    observations = [
        {
            "source_engine": "user_scanner_username",
            "subject_type": "username",
            "subject_value": "alice",
            "status": "found",
            "detector_status": "operational",
            "account_status": "exists",
            "identity_status": "unverified",
            "site_name": "Instagram",
        }
    ]
    recorded = {}
    monkeypatch.setitem(web_app.app.config, "REPORTS_FOLDER", str(tmp_path))
    monkeypatch.setitem(
        web_app.app.config,
        "MAIGRET_DB_FILE",
        str(Path(__file__).with_name("db.json")),
    )
    monkeypatch.setattr(maigret.report, "save_graph_report", lambda *a, **kw: None)

    def record_result(job_id, result):
        recorded.update(job_id=job_id, result=result)
        return result

    monkeypatch.setattr(
        web_app,
        "record_job_result",
        record_result,
    )
    event_sink = queue.Queue()

    web_app.finalize_stream_job(
        "job-with-independent-evidence",
        ["alice"],
        [],
        "2026-09-04 17:00:00",
        event_sink,
        collector_observations=observations,
    )

    assert recorded["job_id"] == "job-with-independent-evidence"
    assert recorded["result"]["status"] == "completed"
    assert recorded["result"]["collector_observations"] == observations
    assert recorded["result"]["individual_reports"] == []
    assert recorded["result"]["username_verification_found_count"] == 1
    assert event_sink.get_nowait() == {
        "type": "done",
        "status": "completed",
        "redirect": "/results/search_job-with-independent-evidence",
    }


def test_adapter_error_only_observations_do_not_complete_job(monkeypatch):
    observations = [
        {
            "source_engine": "user_scanner_username",
            "subject_type": "username",
            "subject_value": "alice",
            "status": "error",
            "detector_status": "degraded",
            "scan_stage": "adapter",
            "reason": "Target collection timed out before completion",
        }
    ]
    recorded = {}

    def record_result(job_id, result):
        recorded.update(job_id=job_id, result=result)
        return result

    monkeypatch.setattr(
        web_app,
        "record_job_result",
        record_result,
    )
    event_sink = queue.Queue()

    web_app.finalize_stream_job(
        "job-with-adapter-errors-only",
        ["alice"],
        [],
        "2026-09-06 14:00:00",
        event_sink,
        collector_observations=observations,
    )

    assert recorded["job_id"] == "job-with-adapter-errors-only"
    assert recorded["result"]["status"] == "failed"
    assert event_sink.get_nowait() == {"type": "done", "status": "failed"}


def test_nonmatch_collector_diagnostic_is_reportable():
    assert web_app.has_reportable_collector_observations(
        [
            {
                "source_engine": "user_scanner_username",
                "status": "not_found",
                "scan_stage": "direct",
            }
        ]
    )
