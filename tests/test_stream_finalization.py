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
    monkeypatch.setattr(
        web_app,
        "record_job_result",
        lambda job_id, result: recorded.update(job_id=job_id, result=result),
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
