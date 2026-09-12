# Adding an OpenLedger connector

The pipeline has one reviewed executable source manifest: `config/osint-sources.json`.
Each source entry binds its input capability, collector callable, normalizer callable,
configuration validator, retention policy, adapter/parser versions, provider identity,
and execution mode. `EngineCapability` is loaded from that same manifest. There is no
second list in the executor to keep synchronized.

For an ordinary connector using existing inputs and generic account/claim evidence,
add **one Python package, one manifest entry, and one fixture suite**. Persona tables,
QC rules, routes, templates, and central executor dispatch do not need edits. A new
kind of operator input or domain-specific claim semantics may require additional
product work; registration alone does not invent those semantics.

## Runtime contract

A collector has the signature `async def collect(task, context)`. Its responsibilities:

1. Read only the task's approved input and bound context. Never infer another case or
   subject, expand to unrelated targets, or create a Persona from a finding.
2. Check `context.cancelled()` before new work. Use bounded request timeouts and
   response/page limits. Return explicit completeness and unresolved failures.
3. Emit every permitted observation through `context.emit(...)` or
   `context.emit_observations(...)`, including negative and unavailable outcomes.
   Committed evidence is retained independently of task completion.
4. Return outcome metadata separately from the evidence. A valid outcome is `found`,
   `candidate`, `not_found`, `partial`, `inconclusive`, `blocked`, `timeout`, `error`,
   `cancelled`, or `not_executed`. Finding one record does not make a batch complete.
5. Declare `retryable` when provider semantics establish it, and retain
   `retry_after_seconds` when supplied. Do not retry a whole successful batch to
   recover one failed page if the connector can checkpoint failed subrequests.

`context.emit` invokes the registered normalizer. The standard normalizer accepts
`source_observations` (preferred for new connectors), plus compatibility envelopes
for existing adapters. It supplies case/subject/request/task/attempt scope, the
manifest retention policy, and explicit adapter/parser versions. Never persist by
calling application tables from a connector: the scoped ledger is the common sink.

Ordinary `aiohttp` and `httpx` requests run under the production isolated collector's
`pipeline_http.TransportGuard`. Physical sends, redirects, retries and pages consume
the same durable request allowance. The standard requests/urllib3 and supported
curl paths are guarded for compatibility. New network connectors should use
`aiohttp` or `httpx`. Do **not** manually reserve those requests a second time.
`context.reserve_request(count=1, provider="provider-key")` is reserved for non-HTTP
transports that cannot use the standard guard. These are trusted deployment-owned
connectors, not a sandbox for arbitrary uploaded Python.

Provider health is shared across processes by provider host/key. A source-specific
engine name must not create an independent allowance for the same provider. The
planner records estimates separately from physical request consumption.

## Minimal collector example

```python
# maigret/web/connectors/example_registry/__init__.py
import httpx


async def collect(task, context):
    if context.cancelled():
        return {"outcome": "cancelled", "retryable": False}
    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        response = await client.get(
            "https://api.example.test/entities",
            params={"name": task["input_value"], "limit": 20},
        )
    if response.status_code == 429:
        # Parse provider Retry-After (seconds or date) under its documented rules.
        return {"outcome": "blocked", "retryable": False}
    if response.status_code in {401, 403}:
        return {"outcome": "blocked", "retryable": False}
    response.raise_for_status()
    document = response.json()
    # The real connector must validate and bound the provider response schema.
    rows = document["entities"]
    for row in rows:
        context.emit({"source_observations": [{
            "source_engine": task["engine_id"],
            "source_record_id": str(row["id"]),
            "source_url": row["public_record_url"],
            "status": "candidate",
            "claims": [{"predicate": "organization", "value": row["name"]}],
        }]})
    return {
        "outcome": "partial" if document.get("next_cursor") else
                   "candidate" if rows else "not_found",
        "retryable": False,
    }
```

The example demonstrates the calling boundary and generic evidence shape. The
reserved `.test` endpoint is a placeholder, not an added live provider. A production
connector must validate public locators, cap response bytes before loading JSON,
include time/qualifier fields for qualified claims, implement pagination via durable
checkpoints, and report specific unresolved work. Fixture tests must exercise those
behaviors before its manifest status becomes active.

## Manifest entry

Copy the closest existing source entry and replace the provider's governance
metadata honestly. Keep human review required and automatic approval disabled.
Inside that single entry, the `connector` section contains:

```json
{
  "capability": {
    "engine_id": "example_registry",
    "execution_key": "example_registry",
    "module": "maigret.web.connectors.example_registry",
    "input_types": ["organization"],
    "label": "Example registry",
    "platforms": [],
    "prerequisites": ["approved_organization"],
    "option": "",
    "timeout_seconds": 30,
    "retry_ceiling": 1,
    "request_budget": 1,
    "retention": "bounded_source_evidence",
    "trigger": "query"
  },
  "collector": "maigret.web.connectors.example_registry:collect",
  "normalizer": "maigret.web.pipeline_evidence:iter_result_observations",
  "configuration_validator": "maigret.web.connectors.registry:validate_default_configuration",
  "adapter_version": "example-registry-adapter-1",
  "parser_version": "example-registry-parser-1",
  "policy_group": "connector",
  "provider_key": "api.example.test",
  "execution_mode": "worker"
}
```

The outer source `id` must equal `capability.engine_id`. Existing source fields
record official documentation, terms, access requirements, caps, review ownership,
and retention. Status accepts `active`, `disabled`, `quarantined`, or `retired`.
Source status, global policy, provider readiness, and per-source controls intersect:
none can re-enable a disabled catalog source. `policy_group="enrichment"` applies
existing enrichment controls regardless of where the connector's Python module lives.

Authenticated and paid APIs are permitted to declare their actual requirements;
registration does not supply permission, credentials, or funding. The default
configuration validator checks server-owned `credentials_configured` and
`paid_access_enabled` availability booleans. A connector with additional settings
can supply its own `validate_configuration(source, configuration)` function. Never
put secrets into source manifests, saved plans, fixtures, logs, or configuration
revision hashes. Actual keys stay in the server's credential configuration. To use the default
validator without changing central application code, add optional manifest variable
names (never variable values):

```json
"configuration_env": {
  "enabled": "OPENLEDGER_EXAMPLE_ENABLED",
  "credentials_configured": ["OPENLEDGER_EXAMPLE_API_KEY"],
  "paid_access_enabled": "OPENLEDGER_EXAMPLE_PAID_ENABLED"
}
```

Only availability booleans enter planning. Missing credentials disable the source;
missing or false explicit enablement/paid flags remain disabled. A per-source or
catalog restriction still takes precedence. The package reads its own server key
at collection time without placing it in task metadata.

Use `execution_mode="operator"` with `trigger="operator"` for an explicit operator
submission, and `execution_mode="machine"` with `trigger="machine"` for an
authenticated feed. Neither is a fallback for query collection. The existing
`connector_feed` gateway uses a machine-bound receipt and is excluded from normal
interactive planning. New feeds should use that gateway's authentication, scope,
receipt, replay and checkpoint contract rather than add public ingestion routes.

## Evidence and version rules

- Use stable provider record IDs and explicit record versions where available.
  Observation identity preserves attempt lineage; source identity supports
  deduplication. Never silently overwrite an earlier source assertion.
- Supply source origins separately from account URLs. Mirrors of one origin do not
  count as independent corroboration. An evidence page URL is not an account URL.
- Preserve negative/blocked/timeout/error observations. They describe attempted
  collection, not verified absence or evidence supporting a claim.
- Retention resolves the strictest source/task/observation rule. Supported rules are
  retained, metadata-only, transient/live-only, and prohibited. Legacy aliases are
  mapped centrally. A public URL never upgrades a restricted observation to final
  eligibility. Google Places live details remain transient.
- `adapter_version` is the OpenLedger wrapper implementation version, not a claim
  about the underlying provider or scanner release. Parser version changes when
  normalization semantics change. Preserve upstream versions independently when
  returned. Saved tasks with different explicit versions require revalidation.
- Account existence, subject attribution, claim correctness, operator inclusion,
  and QC approval are distinct. A connector can propose evidence, not approve it.

## Conformance and delivery

`tests/test_connector_registry.py` executes every registered built-in wrapper using
offline provider fixtures. It also registers a synthetic new package solely through
its manifest, verifies custom normalization/version/policy binding, disabled-source
intersection, operator/machine isolation, partial outcomes, and exception propagation.
The older adapter inventory check remains a compatibility aid; it is not the
execution acceptance gate.

Each new connector fixture suite should cover:

| Scenario | Required observation/execution behavior |
|---|---|
| Found and empty | Valid evidence versus explicit absence; no automatic identity claim |
| Found plus timeout | Retained positive and failure observations; partial completeness |
| 429, invalid credentials | Shared cooldown and correct retry eligibility |
| Invalid schema, oversized response | Bounded error with no malformed final evidence |
| Duplicate records/pages | Idempotent logical records with intact attempt lineage |
| Cancellation, interruption, resume | Committed pages survive; no cursor ahead of evidence |
| Retention and dependencies | Restricted payload cannot persist/finalize; mirrors share origin |
| Curation, QC, graph/export | Generic evidence remains traceable through the approved version |

Run the static source audit and actual connector fixtures:

```bash
python .github/scripts/check_osint_sources.py
python -m pytest tests/test_connector_registry.py tests/test_pipeline_query.py tests/test_osint_source_registry.py
```

Optional live probes are declared in `maintenance.live_probe`; the audit iterates
registered active sources instead of maintaining another source-name list. New
probes may be deployment-owned `module:function` callables. Live checking is an
explicit `--live` action, never part of offline fixture execution. The runtime
registry imports and validates every declared callable before plans are saved;
missing implementations fail closed. Adding a source cannot revive the previous
pipeline or bypass reviewed P2 release identity and schema gates.
