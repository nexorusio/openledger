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

| UI mode | Canonical value | Legacy API alias | Runtime budget | Maigret coverage |
|---|---|---|---:|---|
| Quick Scan | `focused` | `fast` | 10 minutes | The configured top-ranked eligible sites; the supported deployment currently defaults to 500. |
| Full Scan | `exhaustive` | `full` | 30 minutes | Every eligible enabled site matching the case filters. |

`focused` and `exhaustive` are the canonical request values. The legacy
`fast` and `full` values remain accepted so existing integrations do not
break.

The runtime budget begins when the durable worker claims the job. Time waiting
in the queue is excluded. Every bounded collector receives no more than the
remaining deadline. If the deadline arrives first, OpenLedger cancels active
work, retains evidence already collected, and records a distinct
`budget_exhausted` outcome.

Eligible coverage still respects source enablement, detector-health controls,
case category/country filters, and provider-specific policy. Full Scan means
all eligible sources, not disabled or quarantined sources. In the unified
builder, Full Scan also requires at least one explicit username, social handle,
supported profile URL, or selected server-ranked username alias. The server
refuses the submission if no such Maigret target exists or if Maigret is
disabled; it never reports a native-name or email-only run as a Full Scan.
Quick Scan may proceed through enabled native search when Maigret is disabled,
with the unavailable Maigret route shown explicitly in the plan.

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
The supported no-subscription deployment uses a private SearXNG container on
the existing Docker network. It has no host port and aggregates the standard
Brave Web and DuckDuckGo engines without a paid API account. The legacy direct
Brave API adapter remains optional for operators who separately approve its
billing relationship. A disabled, incomplete, throttled, or failed search stage
emits a bounded operational notice and safely continues to the legacy
collectors; it does not retry provider requests.

The planner uses at most five approved or analyst-supplied username, handle,
profile-URL, alias, or full-name seeds. It never pivots from email addresses or
phone numbers. It creates no more than 25 exact-phrase, platform-scoped queries
across Facebook, Instagram, Threads, TikTok, and X. Each query returns five
results by default and can be configured from one to ten. The request timeout
defaults to ten seconds and can be configured from one to 30 seconds. Every
request remains inside the investigation's Quick Scan or Full Scan deadline.

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

The self-hosted provider needs no external credential. SearXNG is a
metasearch proxy, not a local Web index: enabling it authorizes bounded queries
to be sent from the Droplet to its configured upstream search engines. Those
engines may throttle or change their Web interfaces, which is why OpenLedger's
timeouts, circuit breaker, and legacy fallback remain mandatory. Search
queries, returned public evidence, candidate scores, decisions, and provider
lineage are retained in the case audit.

If the optional direct Brave API adapter is deliberately selected, its API key
is read at request time from an owner-only regular file. It is never placed in
Compose environment values, job options, database records, events, logs, HTML,
or browser JavaScript.

### Search-first configuration

| Environment variable | Supported value and default |
|---|---|
| `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED` | Defaults to `false`. Only `1`, `true`, `yes`, or `on` enables the outbound capability; empty or malformed values remain off. |
| `OPENLEDGER_PROFILE_SEARCH_PROVIDER` | `disabled` by default. `searxng` selects the private no-subscription container. `brave` is an optional paid external API integration. Other values fail closed. |
| `OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE` | Used only by the optional direct Brave adapter and fixed at `/app/runtime/secrets/brave_search_api_key`. SearXNG never reads it. |
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
search access. The P3 unified-input presentation flag also defaults off and is
app-only; it does not alter worker policy.

| Environment variable | When disabled |
|---|---|
| `OPENLEDGER_PROFILE_DISCOVERY_ENABLED` | Refuses all new live and refresh profile-discovery jobs. |
| `OPENLEDGER_FOCUSED_DISCOVERY_ENABLED` | Refuses new Quick Scan jobs. |
| `OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED` | Refuses new Full Scan jobs. |
| `OPENLEDGER_MAIGRET_DISCOVERY_ENABLED` | Refuses Full Scan and marks Maigret unavailable. Quick Scan may proceed only when another effective collection route, such as native search, remains available. |
| `OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED` | Refuses plans that request User Scanner; plans without it remain eligible. |
| `OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED` | Blocks governed enrichment adapters, including shared organization and public-record adapters. |
| `OPENLEDGER_PROVIDER_CIRCUIT_BREAKERS_ENABLED` | Bypasses breaker state while retaining one bounded call with no retry. |
| `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED` | Skips native major-platform search; Maigret and any authorized User Scanner work continue. |
| `OPENLEDGER_UNIFIED_INVESTIGATION_INPUT_ENABLED` | Restores the legacy typed investigation builder. Existing jobs, stored plans, worker execution, evidence, reviews, and reports are unchanged. |

The web application checks policy when a job is submitted. The worker checks
again when claiming queued work. If policy changed while a job waited, the job
fails before its first attempt and is not retried. Each accepted job stores the
server policy snapshot used for its execution.

### Unified investigation builder rollout and rollback

`OPENLEDGER_UNIFIED_INVESTIGATION_INPUT_ENABLED` controls only the web
application's new-investigation and Persona-rerun builder. It is absent from
the worker environment by design. Only `1`, `true`, `yes`, or `on`, matched
case-insensitively, enables the unified token editor and its preview endpoint.
A missing, false, empty, or malformed value fails closed to the legacy typed
builder; direct unified submissions and preview requests are refused.

Both builders create the same bounded persisted investigation contracts. The
flag does not cancel or reinterpret queued or completed jobs, change Quick
Scan or Full Scan coverage, alter graph projection, approve evidence, or hide
retained state.

The unified builder enables server-ranked likely username aliases by default,
matching the legacy builder's default name-variant behavior. The analyst can
clear individual aliases or disable alias planning, but a Full Scan cannot start
after every Maigret target has been removed. Phone numbers and generic public
URLs remain context only. Email discovery remains conditional on explicit
confirmation. Established username verification, GitHub enrichment, and
archive-evidence collectors remain optional and off by default under
`Additional existing checks`; selected routes and their policy state appear in
the authoritative preview before submission.

To stage the unified builder, set the flag to `true`, validate Compose, and
recreate only `app`:

```bash
cd /opt/openledger
sudo docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
sudo docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --no-deps --force-recreate app
sudo docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T app env | grep '^OPENLEDGER_UNIFIED_INVESTIGATION_INPUT_ENABLED='
curl -fsS https://openledger.nexorus.io/healthz
```

For immediate presentation containment, set the flag to `false`, repeat those
app-only validation and recreation commands, and confirm the typed builder is
shown. This restores the legacy input presentation but is not a remediation for
a unified-builder defect. Do not recreate the worker for this flag. Existing
jobs continue under their persisted server-owned route plans.

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

4. For execution-policy flags, recreate the app and worker so both receive the
   same flag snapshot. For the unified-input presentation flag, follow the
   preceding app-only procedure instead:

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
the worker. The supported path below creates no external API account or
per-request billing relationship.

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

2. Inspect the current disabled state:

   ```bash
   cd /opt/openledger
   sudo bash deploy/self-hosted-search.sh status
   ```

3. Prepare the private provider. The script refuses hosts with less than 768
   MiB available memory or 1 GiB free disk, saves the protected environment,
   pulls the pinned image, starts only the private SearXNG profile, sends one
   non-personal `example.com` probe, and recreates app and worker with discovery
   still disabled:

   ```bash
   cd /opt/openledger
   sudo bash deploy/self-hosted-search.sh prepare
   ```

   Type `CONTINUE` only after confirming that no investigation is running.
   SearXNG remains internal, runs as UID/GID 977, and is limited to 384 MiB
   memory, half a CPU, and 128 processes.

4. Review the preparation output. It must show provider `searxng`, identical
   app/worker configuration, a healthy private container, a healthy OpenLedger
   application, and `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED=false`. Then
   enable the capability:

   ```bash
   cd /opt/openledger
   sudo bash deploy/self-hosted-search.sh enable
   ```

5. Run one authorized Quick Scan smoke investigation. Confirm that native search
   starts before Maigret, the case shows only canonical candidates for the five
   supported platforms, proposed candidates remain pending in Persona, and no
   credential or raw query appears in container logs. Confirm the app and worker
   display identical `OPENLEDGER_` search/discovery settings using the parity
   commands above.

To roll back the capability after active investigations finish:

```bash
cd /opt/openledger
sudo bash deploy/self-hosted-search.sh disable
```

This sets the flag and provider to their fail-closed values, recreates app and
worker together, and stops the private SearXNG container. Existing audits and
analyst decisions remain available while new investigations continue through
the legacy collectors. It does not delete audit data or the local SearXNG
volume.

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
