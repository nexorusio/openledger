from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROFILE_FLAGS = (
    "OPENLEDGER_PROFILE_DISCOVERY_ENABLED",
    "OPENLEDGER_FOCUSED_DISCOVERY_ENABLED",
    "OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED",
    "OPENLEDGER_MAIGRET_DISCOVERY_ENABLED",
    "OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED",
    "OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED",
    "OPENLEDGER_PROVIDER_CIRCUIT_BREAKERS_ENABLED",
)


def test_production_compose_passes_default_on_flags_to_app_and_worker():
    compose = (ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8")
    app_section = compose.split("\n  app:\n", 1)[1].split("\n  worker:\n", 1)[0]
    worker_section = compose.split("\n  worker:\n", 1)[1].split(
        "\n  caddy:\n", 1
    )[0]

    for flag in PROFILE_FLAGS:
        expected = f'{flag}: "${{{flag}:-true}}"'
        assert expected in app_section
        assert expected in worker_section
        assert compose.count(expected) == 2


def test_investigation_builder_uses_canonical_mode_names_and_fixed_budgets():
    template = (
        ROOT / "maigret" / "web" / "templates" / "index.html"
    ).read_text(encoding="utf-8")

    assert 'name="mode" id="mode-focused" value="focused"' in template
    assert 'name="mode" id="mode-exhaustive" value="exhaustive"' in template
    assert "Up to 10 minutes" in template
    assert "Up to 30 minutes" in template
    assert "queue time does not count" in template
    assert "Fast check" not in template
    assert "Full check" not in template


def test_live_ux_exposes_runtime_and_recovery_states_without_auto_approval():
    template = (
        ROOT / "maigret" / "web" / "templates" / "live.html"
    ).read_text(encoding="utf-8")

    for required in (
        "Discovery mode",
        "Runtime budget",
        "Time remaining",
        "Worker heartbeat",
        "provider_circuit_open",
        "Cancellation confirmed",
        "no automatic retry",
        "partial findings were saved",
    ):
        assert required in template
    assert "no identity decision was made automatically" in (
        ROOT / "maigret" / "web" / "app.py"
    ).read_text(encoding="utf-8")


def test_live_progress_and_pipeline_workspace_keep_collection_separate_from_approval():
    live = (ROOT / "maigret" / "web" / "templates" / "live.html").read_text(
        encoding="utf-8"
    )
    pipeline = (
        ROOT / "maigret" / "web" / "templates" / "pipeline_workspace.html"
    ).read_text(encoding="utf-8")

    # Discovery shows source execution without drawing premature relationships;
    # operator action is limited to the evidence-ranked shortlist.
    for required in ("Live engine progress", "updateCollector"):
        assert required in live
    for forbidden in ('id="graph"', "addCandidate(ev)", "vis.Network"):
        assert forbidden not in live
    assert "shortlist_sections" in pipeline
    assert 'section["items"]' in pipeline
    assert "section.items" not in pipeline
    assert "No raw-item QC is required" in pipeline
    assert "Digital presence" not in live
    assert "Approve finding" in pipeline
    assert "Record decision" not in pipeline


def test_active_case_has_a_safe_stop_then_archive_path():
    case_template = (ROOT / "maigret" / "web" / "templates" / "case.html").read_text(
        encoding="utf-8"
    )
    app = (ROOT / "maigret" / "web" / "app.py").read_text(encoding="utf-8")
    assert "Stop collection" in case_template
    assert "stop_and_delete_case_workspace" in case_template
    assert '"/cases/<case_id>/stop-and-delete"' in app


def test_partial_outcomes_are_distinct_in_history_and_results():
    history = (
        ROOT / "maigret" / "web" / "templates" / "history.html"
    ).read_text(encoding="utf-8")
    results = (
        ROOT / "maigret" / "web" / "templates" / "results.html"
    ).read_text(encoding="utf-8")

    assert "Partial · budget reached" in history
    assert "Interrupted · no auto-retry" in history
    assert "Budget reached · no findings" in history
    assert "This is a partial evidence set" in results
    assert "not a completed coverage claim or an identity approval" in results
