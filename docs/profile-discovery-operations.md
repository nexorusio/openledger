# Profile discovery operations

This runbook defines the governed execution contract for OpenLedger profile
discovery. It covers live and Persona-refresh investigations that use Maigret,
User Scanner, optional enrichment providers, and the native major-platform
search stage. It does not change the evidence model: collectors produce
observations and pending proposals; only a human analyst can approve identity
evidence or merge identity conclusions.

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

## Native search-first discovery

Native search runs before Maigret and User Scanner only when both the
default-off search-first flag and the server-owned search provider are enabled.
The supported provider is Brave Search. A disabled, incomplete, or failed
search stage emits a bounded operational notice and safely continues to the
legacy collectors; it does not retry provider requests.

The planner uses at most five approved or analyst-supplied username, handle,
profile-URL, alias, or full-name seeds. It never pivots from email addresses or
phone numbers. It creates no more than 25 exact-phrase, platform-scoped queries
across Facebook, Instagram, Threads, TikTok, and X. Each query returns five
results by default and can be configured from one to ten. The request timeout
defaults to ten seconds and can be configured from one to 30 seconds. Every
request remains inside the investigation's Focused or Exhaustive deadline.

Only canonical public HTTPS profile URLs for the requested platform become
candidates. The case page displays the immutable audit identifier, retained
source evidence, provider lineage, and a discovery score for triage. Raw search
queries and provider errors are withheld from the case UI. The discovery score
is not identity confidence. An analyst can mark a candidate proposed,
uncertain, or rejected:

- **Proposed** creates or reuses a pending `social_account` Persona claim with
  neutral confidence 50. Normal Persona approval is still required.
- **Uncertain** and **rejected** remain in the append-only candidate-review
  history and do not create or change a Persona claim.
- Repeating a proposal cannot clear an existing Persona decision.

The API key is read at request time from an owner-only regular file. It is never
placed in Compose environment values, job options, database records, events,
logs, HTML, or browser JavaScript. Search queries, returned public evidence,
candidate scores, decisions, and provider lineage are retained in the case
audit. Enabling the provider therefore authorizes those bounded queries to be
sent to Brave Search under the operator's provider agreement and retention
policy.

### Search-first configuration

| Environment variable | Supported value and default |
|---|---|
| `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED` | Defaults to `false`. Only `1`, `true`, `yes`, or `on` enables the outbound capability; empty or malformed values remain off. |
| `OPENLEDGER_PROFILE_SEARCH_PROVIDER` | `disabled` by default; set to `brave` after the credential is installed. Other values fail closed. |
| `OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE` | Fixed by the supported Compose deployment at `/app/runtime/secrets/brave_search_api_key`. |
| `OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS` | Integer from 1 to 30; defaults to `10`. |
| `OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS` | Integer from 1 to 10 per query; defaults to `5`. |

All five values must be identical in the app and worker containers. The app
captures the server policy when a job is submitted, and the worker refreshes it
when claiming the job. This prevents a queued job from silently retaining an
older search-first decision.

## Server-owned feature flags

The seven P1 flags default to enabled. Only the exact case-insensitive values
`0`, `false`, `no`, or `off` disable one of those flags. Missing, empty, or
malformed P1 values retain the documented default. The P2 search-first flag is
different: it defaults off and only an explicit true value enables new outbound
search access.

| Environment variable | When disabled |
|---|---|
| `OPENLEDGER_PROFILE_DISCOVERY_ENABLED` | Refuses all new live and refresh profile-discovery jobs. |
| `OPENLEDGER_FOCUSED_DISCOVERY_ENABLED` | Refuses new Focused jobs. |
| `OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED` | Refuses new Exhaustive jobs. |
| `OPENLEDGER_MAIGRET_DISCOVERY_ENABLED` | Refuses profile discovery because the required long-tail engine is unavailable. |
| `OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED` | Refuses plans that request User Scanner; plans without it remain eligible. |
| `OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED` | Blocks governed enrichment adapters, including shared organization and public-record adapters. |
| `OPENLEDGER_PROVIDER_CIRCUIT_BREAKERS_ENABLED` | Bypasses breaker state while retaining one bounded call with no retry. |
| `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED` | Skips native major-platform search; Maigret and any authorized User Scanner work continue. |

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
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T app env | grep -E '^OPENLEDGER_(.*DISCOVERY|.*PROVIDER|PROFILE_SEARCH_)' | sort
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T worker env | grep -E '^OPENLEDGER_(.*DISCOVERY|.*PROVIDER|PROFILE_SEARCH_)' | sort
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

## Enable native search on the supported Docker deployment

Deploy the P2 image and migrations with search still disabled before granting
new outbound access. Wait for active investigations to finish before recreating
the worker.

1. Confirm `deploy/.env` contains the fail-closed values, then apply the normal
   update:

   ```bash
   cd /opt/openledger
   sudo nano deploy/.env
   ```

   ```dotenv
   OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED=false
   OPENLEDGER_PROFILE_SEARCH_PROVIDER=disabled
   OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS=10
   OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS=5
   ```

   ```bash
   cd /opt/openledger
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
   sudo bash deploy/update.sh
   curl -fsS https://openledger.nexorus.io/healthz
   ```

2. Create the credential file without placing the key in shell history or
   `deploy/.env`:

   ```bash
   cd /opt/openledger
   sudo install -m 0600 -o 10001 -g 10001 /dev/null runtime/secrets/brave_search_api_key
   sudo bash -c 'read -rsp "Brave Search API key: " key; printf "\n"; printf "%s\n" "$key" > runtime/secrets/brave_search_api_key; unset key'
   sudo chown 10001:10001 runtime/secrets/brave_search_api_key
   sudo chmod 0600 runtime/secrets/brave_search_api_key
   ```

3. Set the provider to `brave` while leaving the search-first flag `false`.
   Validate, recreate both runtimes, and verify that each can read the protected
   key without printing it:

   ```bash
   cd /opt/openledger
   sudo nano deploy/.env
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --no-deps --force-recreate app worker
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T app python -c "from maigret.web.profile_search_backend import load_profile_search_config, read_profile_search_api_key; c=load_profile_search_config(); read_profile_search_api_key(c); print('app profile-search configuration valid')"
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T worker python -c "from maigret.web.profile_search_backend import load_profile_search_config, read_profile_search_api_key; c=load_profile_search_config(); read_profile_search_api_key(c); print('worker profile-search configuration valid')"
   ```

4. Change only `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED` to `true`, then
   validate and recreate the app and worker together:

   ```bash
   cd /opt/openledger
   sudo nano deploy/.env
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --no-deps --force-recreate app worker
   sudo docker compose --env-file deploy/.env -f deploy/compose.yaml ps
   curl -fsS https://openledger.nexorus.io/healthz
   ```

5. Run one authorized Focused smoke investigation. Confirm that native search
   starts before Maigret, the case shows only canonical candidates for the five
   supported platforms, proposed candidates remain pending in Persona, and no
   credential or raw query appears in container logs. Confirm the app and worker
   display identical `OPENLEDGER_` search/discovery settings using the parity
   commands above.

To roll back the capability, set
`OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED=false`, validate Compose, and recreate
the app and worker together. Existing audits and analyst decisions remain
available, while new investigations continue through the legacy collectors.
Setting `OPENLEDGER_PROFILE_SEARCH_PROVIDER=disabled` as a second step also
prevents provider use if the feature flag is later changed accidentally. Do not
delete the protected key or audit data as part of an operational rollback.

## Verification

The focused P1 regression suite is:

```bash
poetry run pytest -q \
  tests/test_execution_budget.py \
  tests/test_profile_discovery_policy.py \
  tests/test_provider_circuit_breaker.py \
  tests/test_profile_discovery_ux.py \
  tests/test_worker.py \
  tests/test_case_store.py \
  tests/test_persistent_jobs.py
```

The full repository test and CI gates remain mandatory before merge.

The focused P2 regression suite is:

```bash
poetry run pytest -q \
  tests/test_profile_search_contract.py \
  tests/test_profile_discovery_policy.py \
  tests/test_profile_search_planner.py \
  tests/test_profile_search_backend.py \
  tests/test_profile_search_facebook.py \
  tests/test_profile_search_instagram.py \
  tests/test_profile_search_threads.py \
  tests/test_profile_search_tiktok.py \
  tests/test_profile_search_x.py \
  tests/test_profile_search_candidates.py \
  tests/test_profile_search_ranking.py \
  tests/test_profile_search_orchestrator.py \
  tests/test_profile_search_persistence.py \
  tests/test_profile_search_runtime.py \
  tests/test_profile_search_review.py \
  tests/test_deployment.py
```
