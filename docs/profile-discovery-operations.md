# Profile discovery operations

This runbook defines the governed execution contract for OpenLedger profile
discovery. It covers live and Persona-refresh investigations that use Maigret,
User Scanner, and optional enrichment providers. It does not change the
evidence model: collectors produce observations and pending proposals; only a
human analyst can approve identity evidence or merge identity conclusions.

## Execution modes

The server owns the mode, duration, and deadline. Client-supplied durations or
policy documents are ignored.

| UI mode | Legacy API alias | Runtime budget | Maigret coverage |
|---|---|---:|---|
| Focused | `fast` | 10 minutes | The configured top-ranked eligible sites; the supported deployment currently defaults to 500. |
| Exhaustive | `full` | 30 minutes | Every eligible enabled site matching the case filters. |

`focused` and `exhaustive` are the canonical request values. The legacy
`fast` and `full` values remain accepted so existing integrations do not
break.

The runtime budget begins when the durable worker claims the job. Time waiting
in the queue is excluded. Every bounded collector receives no more than the
remaining deadline. If the deadline arrives first, OpenLedger cancels active
work, retains evidence already collected, and records a distinct
`budget_exhausted` outcome.

Eligible coverage still respects source enablement, detector-health controls,
case category/country filters, and provider-specific policy. Exhaustive means
all eligible sources, not disabled or quarantined sources.

## Durable lifecycle

| Job status | Meaning | Operator action |
|---|---|---|
| `queued` | Stored in PostgreSQL and waiting for a worker. No runtime budget has been consumed. | Wait, or cancel from the live page. |
| `running` | Claimed by one worker with an absolute deadline and active lease. | Monitor countdown, heartbeat, and findings. |
| `cancel_requested` | The stop request is durable and the worker is saving what it can. | Do not repeatedly restart services; wait for the terminal state. |
| `completed` | The worker finished. Check `collection_status` before treating coverage as complete. | Review evidence; never infer identity from completion alone. |
| `cancelled` | Operator cancellation ended before reportable evidence was retained. | Review History before deciding whether to rerun. |
| `interrupted` | Worker ownership or process continuity was lost. No automatic retry is created. | Inspect logs and retained evidence, then manually authorize any rerun. |
| `budget_exhausted` | The deadline ended before reportable evidence was retained. | Choose whether a new governed run is justified. |
| `failed` | A non-budget, non-cancellation failure prevented completion. | Inspect the public error and server logs. |

A report can have database status `completed` while its
`collection_status` is `budget_exhausted`, `cancelled`, or
`interrupted`. That combination means partial evidence was safely retained;
it is not a completed coverage claim.

## Cancellation and worker recovery

- The worker polls durable cancellation every 0.25 seconds.
- After observing cancellation, terminal cleanup is bounded to 15 seconds.
- Managed subprocess cleanup uses terminate, then kill if necessary, within a
  shared five-second cleanup window.
- Cancellation is idempotent. Repeating the same stop request does not create a
  second cancellation event.
- The worker writes its lease heartbeat every five seconds.
- A heartbeat older than 30 seconds is stale. The watchdog marks that execution
  interrupted and clears ownership.
- An expired or replaced worker cannot publish late events, evidence snapshots,
  or terminal results.
- Interrupted jobs are never automatically retried. This prevents duplicated
  collection and unintended identity correlation.

The live page polls bounded runtime metadata and displays queue state, mode,
budget, remaining time, heartbeat health, stop acknowledgement, and partial
outcomes. History remains the durable place to inspect the final state.

## Provider circuit breakers

Circuit breakers are isolated by provider within each worker process:

- three consecutive transient failures open that provider's circuit;
- the cooldown is 60 seconds;
- after cooldown, only one request is admitted as the half-open probe;
- a successful probe closes the circuit;
- a failed probe starts a new 60-second cooldown;
- invalid operator input, cancellation, not-found results, and partial evidence
  do not count as transient provider failures; and
- the breaker never retries a request or a job.

An open circuit skips only that provider boundary and creates an explicit live
operational notice. Previously collected evidence remains available. Circuit
state is process-local and is cleared by a worker restart, but restarting the
worker solely to bypass a cooldown is not recommended.

## Server-owned feature flags

Every flag defaults to enabled. Only the exact case-insensitive values `0`,
`false`, `no`, or `off` disable a flag. Missing, empty, or malformed
values fail open to the documented default so a typo cannot silently invent a
new policy state.

| Environment variable | When disabled |
|---|---|
| `OPENLEDGER_PROFILE_DISCOVERY_ENABLED` | Refuses all new live and refresh profile-discovery jobs. |
| `OPENLEDGER_FOCUSED_DISCOVERY_ENABLED` | Refuses new Focused jobs. |
| `OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED` | Refuses new Exhaustive jobs. |
| `OPENLEDGER_MAIGRET_DISCOVERY_ENABLED` | Refuses profile discovery because the required long-tail engine is unavailable. |
| `OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED` | Refuses plans that request User Scanner; plans without it remain eligible. |
| `OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED` | Blocks governed enrichment adapters, including shared organization and public-record adapters. |
| `OPENLEDGER_PROVIDER_CIRCUIT_BREAKERS_ENABLED` | Bypasses breaker state while retaining one bounded call with no retry. |

The web application checks policy when a job is submitted. The worker checks
again when claiming queued work. If policy changed while a job waited, the job
fails before its first attempt and is not retried. Each accepted job stores the
server policy snapshot used for its execution.

## Change flags on the supported Docker deployment

These commands run on the Ubuntu production server after connecting over SSH.
They are for a flag-only operational change; they do not pull or rebuild code.

1. Open the repository and back up the current environment file:

   ```bash
   cd /opt/openledger
   sudo cp deploy/.env "deploy/.env.backup.$(date -u +%Y%m%dT%H%M%SZ)"
   ```

2. Edit `deploy/.env` and set only the required flag to `false` or
   `true`:

   ```bash
   cd /opt/openledger
   sudo nano deploy/.env
   ```

3. Validate the complete Compose configuration before changing containers:

   ```bash
   cd /opt/openledger
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
   ```

4. Recreate only the app and worker so both receive the same flag snapshot:

   ```bash
   cd /opt/openledger
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --no-deps --force-recreate app worker
   ```

5. Verify container health and the injected flag values:

   ```bash
   cd /opt/openledger
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml ps
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T app env | grep '^OPENLEDGER_.*DISCOVERY\|^OPENLEDGER_.*PROVIDER' | sort
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T worker env | grep '^OPENLEDGER_.*DISCOVERY\|^OPENLEDGER_.*PROVIDER' | sort
   curl -fsS https://openledger.nexorus.io/healthz
   ```

If validation or health checks fail, restore the timestamped backup by replacing
`BACKUP_FILE` with its exact filename:

```bash
cd /opt/openledger
sudo cp deploy/BACKUP_FILE deploy/.env
sudo docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
sudo docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --no-deps --force-recreate app worker
sudo docker compose --env-file deploy/.env -f deploy/compose.yaml ps
```

Do not use a flag change to approve evidence, merge identities, erase retained
observations, or force an interrupted job to resume.

## Verification

The focused P1 regression suite is:

```bash
poetry run pytest +  tests/test_execution_budget.py +  tests/test_profile_discovery_policy.py +  tests/test_provider_circuit_breaker.py +  tests/test_profile_discovery_ux.py +  tests/test_worker.py +  tests/test_case_store.py +  tests/test_persistent_jobs.py
```

The full repository test and CI gates remain mandatory before merge.
