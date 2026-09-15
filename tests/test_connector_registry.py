"""Offline conformance exercises execution, not function-name inventory alone."""

import asyncio
import copy
import json
import sys
import types
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from maigret.web.connectors.registry import (
    ConnectorRegistry,
    collect_registered,
    get_connector_registry,
    normalize_connector_result,
    validate_default_configuration,
)
from maigret.web.pipeline_contract import ENGINE_REGISTRY, PIPELINE_ID
from maigret.web.pipeline_query import _catalog, _source_snapshot


class Context:
    def __init__(self):
        self.context = {
            "legal_jurisdiction": "FR",
            "official_website_organizations": {"test_value": "Example"},
            "organization_name": "Example",
            "operator_observations": [{"status": "observed"}],
        }
        self.options = {}
        self.job = {"kind": "connector_ingestion"}
        self.raw_collector_observations = []
        self.emitted = []
        self.cancelled = lambda: False

    def emit_observations(self, rows):
        self.emitted.extend(rows)


def task_for(spec):
    capability = spec.capability
    return dict(
        pipeline_id=PIPELINE_ID,
        engine_id=capability.engine_id,
        execution_key=capability.execution_key,
        route_state="active",
        input_type=capability.input_types[0],
        input_value="test_value",
        platform=next(iter(capability.platforms), ""),
        timeout_seconds=30,
        request_budget=10,
        retry_ceiling=1,
        **spec.task_metadata(),
    )


@pytest.mark.parametrize(
    "spec",
    get_connector_registry().values(),
    ids=lambda spec: spec.capability.engine_id,
)
def test_every_registered_builtin_dispatches_through_its_callable(monkeypatch, spec):
    from maigret.web import (
        collector_adapters,
        pipeline_execution,
        pipeline_public_search,
        pipeline_case_fusion,
    )
    from maigret.web.connectors import builtin

    calls = []

    async def rows(*args, **kwargs):
        calls.append((args, kwargs))
        return [{"status": "observed", "source_url": "https://example.test/profile"}]

    async def envelope(*args, **kwargs):
        calls.append((args, kwargs))
        return {"outcome": "candidate", "observations": [{"status": "observed"}]}

    async def approved_envelope(task, context):
        calls.append(((task, context), {}))
        observation = {
            "status": "observed",
            "source_url": "https://example.test/profile",
        }
        context.raw_collector_observations.append(observation)
        context.emit_observations([observation])
        return {"outcome": "candidate"}

    # Stub only the lower-level provider boundary. Registry selection, wrappers,
    # argument adaptation and observation emission remain real.
    capability = spec.capability
    if capability.module.endswith("collector_adapters"):
        monkeypatch.setattr(collector_adapters, capability.execution_key, rows)
    monkeypatch.setattr(pipeline_execution, "_maigret_adapter", envelope)
    monkeypatch.setattr(pipeline_execution, "_native_adapter", envelope)
    monkeypatch.setattr(pipeline_execution, "_cited_research_adapter", envelope)
    monkeypatch.setattr(builtin, "collect_approved_source", approved_envelope)
    monkeypatch.setattr(
        pipeline_public_search, "collect_public_exact_matches", envelope
    )
    monkeypatch.setattr(pipeline_case_fusion, "collect_case_fusion_snapshot", envelope)
    monkeypatch.setattr(pipeline_case_fusion, "collect_case_fusion_synthesis", envelope)
    monkeypatch.setattr(
        pipeline_execution,
        "_app",
        lambda: SimpleNamespace(get_google_maps_api_key=lambda: "fixture-only-key"),
    )
    feed = types.ModuleType("maigret.web.pipeline_connector_ingestion")
    feed.process_feed = envelope
    monkeypatch.setitem(sys.modules, feed.__name__, feed)
    context = Context()
    result = asyncio.run(
        collect_registered(
            task_for(spec), context, operator=spec.execution_mode == "operator"
        )
    )
    assert result is not None
    assert calls or capability.engine_id in {"external_evidence", "case_chat_proposal"}
    if (
        capability.module.endswith("collector_adapters")
        and spec.execution_mode != "operator"
    ):
        assert context.emitted and context.raw_collector_observations


def test_new_connector_needs_only_package_manifest_and_fixtures(monkeypatch):
    module = types.ModuleType("maigret.web.connectors.fixture_connector")
    seen = []

    async def collect(task, context):
        context.emit_observations(
            [{"status": "observed", "source_record_id": "fixture:1"}]
        )
        return {"outcome": "candidate", "request_count": 1}

    def normalize(result, **scope):
        seen.append(scope)
        return iter(result["source_observations"])

    module.collect, module.normalize = collect, normalize
    monkeypatch.setitem(sys.modules, module.__name__, module)
    base = get_connector_registry().get("github_public_profile")
    capability = replace(
        base.capability,
        engine_id="fixture_registry",
        execution_key="fixture_collect",
        module=module.__name__,
    )
    source = copy.deepcopy(base.source)
    source["id"] = capability.engine_id
    source["connector"].update(
        capability=capability.as_dict(),
        collector=module.__name__ + ":collect",
        normalizer=module.__name__ + ":normalize",
    )
    registry = ConnectorRegistry([source], {capability.engine_id: capability})
    context = Context()
    result = asyncio.run(
        collect_registered(
            task_for(registry.get(capability.engine_id)), context, registry=registry
        )
    )
    assert result["outcome"] == "candidate"
    assert context.emitted[0]["source_record_id"] == "fixture:1"
    monkeypatch.setattr(
        "maigret.web.connectors.registry.get_connector_registry", lambda: registry
    )
    list(
        normalize_connector_result(
            task_for(registry.get(capability.engine_id)),
            {"source_observations": context.emitted},
        )
    )
    assert seen[0]["engine_version"] == "openledger-adapter-1"
    assert seen[0]["parser_version"] == "pipeline-evidence-2"
    assert seen[0]["retention_policy"] == [capability.retention, None]


def test_exact_profile_url_collector_preserves_linkedin_slug(monkeypatch):
    from maigret.web.connectors import builtin

    captured = {}

    async def analyze(target, *, timeout_seconds):
        captured.update(target)
        return {
            "status": "candidate",
            "source_url": target["profile_url"],
            "subject_value": target["investigated_username"],
        }

    monkeypatch.setattr(
        builtin,
        "_adapters",
        lambda: SimpleNamespace(run_unfurl_url_analysis=analyze),
    )
    context = Context()
    asyncio.run(
        builtin.collect_url(
            {
                "input_value": "https://linkedin.com/in/jati-pratomo",
                "execution_key": "run_unfurl_url_analysis",
                "timeout_seconds": 20,
            },
            context,
        )
    )

    assert captured == {
        "profile_url": "https://linkedin.com/in/jati-pratomo",
        "investigated_username": "jati-pratomo",
        "site_name": "linkedin.com",
    }


def test_approved_maigret_target_requires_the_exact_profile_path():
    from maigret.web.connectors.builtin import _approved_maigret_target

    sites = [
        SimpleNamespace(
            name="LinkedIn",
            url="https://linkedin.com/in/{username}",
            disabled=False,
            type="username",
        ),
        SimpleNamespace(
            name="Threads mirror",
            url="https://mirror.example/@{username}",
            disabled=False,
            type="username",
        ),
    ]

    assert _approved_maigret_target(
        "https://id.linkedin.com/in/jati-pratomo/", sites
    ) == ("LinkedIn", "jati-pratomo")
    assert _approved_maigret_target(
        "https://id.linkedin.com/in/another-person/", sites
    ) == ("LinkedIn", "another-person")
    assert _approved_maigret_target(
        "https://mirror.example/not-the-profile/jati-pratomo", sites
    ) is None


def test_approved_source_runs_exact_cited_fallback_when_parsers_lack_fields(
    monkeypatch,
):
    import maigret.sites as site_module
    from maigret.web.connectors import builtin

    source_url = "https://www.linkedin.com/in/jati-pratomo/"
    site = SimpleNamespace(
        name="LinkedIn",
        url="https://linkedin.com/in/{username}",
        disabled=False,
        type="username",
        headers={"User-Agent": "Maigret LinkedIn browser"},
    )

    class Database:
        sites = [site]

        def load_from_path(self, _path):
            return self

    direct_target = {}

    async def direct_fetch(target, *, timeout_seconds):
        direct_target.update(target)
        return {
            "source_engine": "approved_public_source_fetch",
            "source_record_id": "direct",
            "source_url": source_url,
            "status": "observed",
            "claims": [{"predicate": "summary", "value": "Indexed profile"}],
            "extra": {"maigret_parser_fields": []},
        }

    fallback_tasks = []

    async def cited_fallback(task, _context):
        fallback_tasks.append(task)
        return {"outcome": "candidate", "citation_count": 1, "proposal_count": 2}

    app = SimpleNamespace(
        app=SimpleNamespace(config={"MAIGRET_DB_FILE": "fixture.json"}),
        record_internal_error=lambda *_args, **_kwargs: "fixture-error",
    )
    runtime = SimpleNamespace(
        _app=lambda: app,
        _cited_research_adapter=cited_fallback,
        _aggregate_outcomes=lambda values: next(iter(values), "inconclusive"),
    )
    monkeypatch.setattr(site_module, "MaigretDatabase", Database)
    monkeypatch.setattr(builtin, "_runtime", lambda: runtime)
    monkeypatch.setattr(
        builtin,
        "_adapters",
        lambda: SimpleNamespace(run_approved_public_source_fetch=direct_fetch),
    )

    emitted, events = [], []

    class Pipeline:
        def iter_observations(self, _case_id, _persona_id):
            return iter(())

    class Store:
        def get_persona(self, _persona_id):
            return {"display_name": "Jati Pratomo"}

    context = SimpleNamespace(
        task={"id": "task-1"},
        options={},
        job={"job_id": "job-1", "case_id": "case-1"},
        request={"persona_id": "persona-1"},
        pipeline=Pipeline(),
        store=Store(),
        sink=SimpleNamespace(put=events.append),
        raw_collector_observations=[],
        normalize=lambda _envelope, **_kwargs: [
            {"claims": [{"predicate": "social_account", "value": source_url}]}
        ],
        emit=lambda envelope, **kwargs: emitted.append((envelope, kwargs)),
        emit_observations=lambda rows, **kwargs: emitted.append((rows, kwargs)),
    )
    result = asyncio.run(
        builtin.collect_approved_source(
            {
                "id": "task-1",
                "input_value": source_url,
                "timeout_seconds": 180,
            },
            context,
        )
    )

    assert result["outcome"] == "candidate"
    assert direct_target["investigated_username"] == "jati-pratomo"
    assert direct_target["maigret_site_name"] == "LinkedIn"
    assert direct_target["maigret_public_headers"] == {
        "User-Agent": "Maigret LinkedIn browser"
    }
    assert fallback_tasks[0]["approved_source_url"] == source_url
    assert [event["stage"] for event in events] == [
        "maigret_catalogue",
        "maigret_catalogue",
        "literal_page",
        "literal_page",
        "maigret_parser",
        "cited_fallback",
        "cited_fallback",
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("collector", "maigret.web.connectors.builtin:missing"),
        ("normalizer", "maigret.web.connectors.builtin:missing"),
        ("adapter_version", "unknown"),
        ("policy_group", "wrong"),
        ("execution_mode", "automatic"),
    ],
)
def test_registry_fails_closed_for_invalid_implementation_or_contract(field, value):
    spec = get_connector_registry().get("maigret")
    source = copy.deepcopy(spec.source)
    source["connector"][field] = value
    with pytest.raises(ValueError):
        ConnectorRegistry([source], {"maigret": spec.capability})


@pytest.mark.parametrize(
    "engine",
    [
        "maigret",
        "native_profile_search",
        "public_exact_match",
        "user_scanner_email",
        "user_scanner_username",
        "github_public_profile",
    ],
)
def test_disabled_catalog_cannot_be_reenabled_by_provider_flags(engine):
    catalog = _catalog()
    catalog[engine]["status"] = "disabled"
    sources = {
        "discovery_enabled": True,
        "maigret_enabled": True,
        "scanner_enabled": True,
        "scanner_available": True,
        "enrichment_enabled": True,
        "native_search": {"enabled": True},
        "public_search": {"enabled": True},
        "engines": {engine: {"enabled": True}},
    }
    snapshot = _source_snapshot(ENGINE_REGISTRY[engine], "", sources, catalog)
    assert snapshot["enabled"] is False
    assert snapshot["catalog_status"] == "disabled"
    assert "catalog" in snapshot["reason"]


def test_enrichment_policy_is_independent_of_connector_module_name():
    capability = replace(
        ENGINE_REGISTRY["github_public_profile"],
        module="maigret.web.connectors.future_package",
    )
    snapshot = _source_snapshot(
        capability, "", {"enrichment_enabled": False}, _catalog()
    )
    assert not snapshot["enabled"]
    assert "Enrichment" in snapshot["reason"]


def test_authenticated_paid_source_requires_both_availability_flags():
    source = {"access": {"credentials_required": True, "genuinely_free": False}}
    for configuration in (
        {},
        {"credentials_configured": True},
        {"paid_access_enabled": True},
    ):
        with pytest.raises(ValueError):
            validate_default_configuration(source, configuration)
    validate_default_configuration(
        source, {"credentials_configured": True, "paid_access_enabled": True}
    )


def test_operator_and_machine_connectors_are_not_query_worker_fallbacks():
    registry = get_connector_registry()
    for engine in (
        "external_evidence",
        "case_chat_proposal",
        "google_places_live_details",
    ):
        with pytest.raises(ValueError, match="explicit operator"):
            asyncio.run(collect_registered(task_for(registry.get(engine)), Context()))
    context = Context()
    context.job = {"kind": "search"}
    with pytest.raises(ValueError, match="authenticated ingestion"):
        asyncio.run(
            collect_registered(task_for(registry.get("connector_feed")), context)
        )


def test_saved_versions_cannot_silently_use_a_different_parser():
    spec = get_connector_registry().get("maigret")
    task = task_for(spec)
    task["parser_version"] = "different-parser"
    with pytest.raises(ValueError, match="replan"):
        asyncio.run(collect_registered(task, Context()))


def test_registered_wrapper_keeps_positive_result_and_surfaces_retry_warning(monkeypatch):
    from maigret.web import collector_adapters

    async def rows(*args, **kwargs):
        return [
            {"status": "observed"},
            {"status": "timeout", "retryable": True, "retry_after_seconds": 12},
        ]

    monkeypatch.setattr(collector_adapters, "run_github_public_profile", rows)
    context = Context()
    result = asyncio.run(
        collect_registered(
            task_for(get_connector_registry().get("github_public_profile")), context
        )
    )
    assert result["outcome"] == "found"
    assert result["display_status"] == "completed_with_warnings"
    assert result["warning_count"] == 1
    assert result["retryable"] is True
    assert result["retry_after_seconds"] == 12
    assert len(context.emitted) == 2


def test_registered_wrapper_propagates_provider_exceptions(monkeypatch):
    from maigret.web import collector_adapters

    async def failure(*args, **kwargs):
        raise TimeoutError("offline provider timeout fixture")

    monkeypatch.setattr(collector_adapters, "run_github_public_profile", failure)
    with pytest.raises(TimeoutError):
        asyncio.run(
            collect_registered(
                task_for(get_connector_registry().get("github_public_profile")),
                Context(),
            )
        )


def test_disabled_manifest_blocks_direct_dispatch_before_provider(monkeypatch):
    spec = get_connector_registry().get("maigret")
    source = copy.deepcopy(spec.source)
    source["status"] = "disabled"
    registry = ConnectorRegistry([source], {"maigret": spec.capability})
    with pytest.raises(ValueError, match="not active"):
        asyncio.run(collect_registered(task_for(spec), Context(), registry=registry))


def test_source_audit_accepts_honest_disabled_authenticated_paid_catalog(
    tmp_path, monkeypatch
):
    import importlib.util

    root = Path(__file__).resolve().parents[1]
    module_spec = importlib.util.spec_from_file_location(
        "fixture_source_audit", root / ".github/scripts/check_osint_sources.py"
    )
    audit = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(audit)
    document = json.loads((root / "config/osint-sources.json").read_text())
    source = document["sources"][0]
    source["status"] = "disabled"
    source["integration_mode"] = "authenticated_api"
    source["access"].update(
        genuinely_free=False, registration_required=True, credentials_required=True
    )
    path = tmp_path / "sources.json"
    path.write_text(json.dumps(document))
    monkeypatch.setattr(audit, "REGISTRY_PATH", path)
    assert (
        audit.load_and_validate_registry()["sources"][0]["access"][
            "credentials_required"
        ]
        is True
    )


def test_manifest_environment_configuration_needs_no_central_application_edit(
    monkeypatch,
):
    base = get_connector_registry().get("github_public_profile")
    source = copy.deepcopy(base.source)
    source["access"].update(credentials_required=True, genuinely_free=False)
    source["connector"]["configuration_env"] = {
        "enabled": "OPENLEDGER_FIXTURE_ENABLED",
        "credentials_configured": ["OPENLEDGER_FIXTURE_KEY"],
        "paid_access_enabled": "OPENLEDGER_FIXTURE_PAID",
    }
    spec = ConnectorRegistry([source], {"github_public_profile": base.capability}).get(
        "github_public_profile"
    )
    for name in (
        "OPENLEDGER_FIXTURE_ENABLED",
        "OPENLEDGER_FIXTURE_KEY",
        "OPENLEDGER_FIXTURE_PAID",
    ):
        monkeypatch.delenv(name, raising=False)
    configuration = spec.configuration_snapshot()
    assert configuration == {
        "enabled": False,
        "credentials_configured": False,
        "paid_access_enabled": False,
    }
    with pytest.raises(ValueError):
        spec.validate_configuration(source, configuration)
    monkeypatch.setenv("OPENLEDGER_FIXTURE_ENABLED", "true")
    monkeypatch.setenv("OPENLEDGER_FIXTURE_KEY", "fixture-secret-must-never-be-in-plan")
    monkeypatch.setenv("OPENLEDGER_FIXTURE_PAID", "true")
    configuration = spec.configuration_snapshot()
    spec.validate_configuration(source, configuration)
    assert all(value is True for value in configuration.values())
    assert "fixture-secret" not in json.dumps(configuration)
    assert spec.configuration_snapshot({"enabled": False})["enabled"] is False


@pytest.mark.parametrize(
    "environment",
    [
        {"endpoint": "OPENLEDGER_ENDPOINT"},
        {"enabled": "HOME"},
        {"credentials_configured": []},
    ],
)
def test_manifest_environment_configuration_rejects_unbounded_fields(environment):
    base = get_connector_registry().get("maigret")
    source = copy.deepcopy(base.source)
    source["connector"]["configuration_env"] = environment
    with pytest.raises(ValueError, match="configuration_env"):
        ConnectorRegistry([source], {"maigret": base.capability})
