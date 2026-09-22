from maigret.web.connectors.builtin import _emit, _timeout


class _Context:
    def __init__(self):
        self.raw_collector_observations = []
        self.emitted = []

    def emit_observations(self, rows):
        self.emitted.extend(rows)


def test_connector_deadline_uses_the_reviewed_engine_timeout():
    assert _timeout({"timeout_seconds": 300, "request_timeout_seconds": 30}) == 300
    assert _timeout({"timeout_seconds": 900}) == 420


def test_positive_result_survives_independent_provider_error_as_warning():
    context = _Context()

    result = _emit(
        [
            {"status": "Found", "site_name": "Working module"},
            {"status": "Error", "site_name": "Broken module"},
        ],
        context,
    )

    assert result["outcome"] == "found"
    assert result["display_status"] == "completed_with_warnings"
    assert result["warning_count"] == 1
    assert context.emitted == context.raw_collector_observations


def test_negative_result_with_provider_error_remains_partial():
    result = _emit(
        [
            {"status": "Not found", "site_name": "Completed module"},
            {"status": "Error", "site_name": "Broken module"},
        ],
        _Context(),
    )

    assert result["outcome"] == "partial"
    assert result["retryable"] is False
