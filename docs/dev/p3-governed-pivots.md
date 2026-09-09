# P3 governed pivots: operations and release gate

This document defines the original P3c operating contract. It covers safe
analyst-directed pivots from evidence that OpenLedger already retains. It does
not authorize automatic expansion, a new source, a new service, or the separate
P3 usability extension.

## Safety invariants

A governed pivot is allowed only when all of these conditions are true:

1. An authenticated analyst explicitly requests the pivot through the existing
   case and Persona workflow, with the existing CSRF and authorization checks.
2. The case retains its declared lawful investigation purpose and every
   applicable external-use consent is present.
3. The origin is either an analyst-approved full-name claim or an
   analyst-approved, supported public social/profile link. A pending, rejected,
   uncertain, malformed, private, local, or unsupported origin is ineligible.
4. The origin belongs to the Persona and case named by the request. The store
   re-resolves that relationship; identifiers supplied by a client are not
   trusted as proof of ownership.
5. `OPENLEDGER_GOVERNED_PIVOTS_ENABLED` and every capability-specific flag
   needed by the selected route are enabled in the server environment.
6. The server-generated plan satisfies the source, request, depth, and
   execution budgets below.

The case boundary is absolute. A pivot may add observations and reviewable
assertions only to its origin case and Persona. It cannot read another case,
create a cross-case relationship, or use output from another case as a seed.
The origin is depth zero and its governed work is depth one. A result produced
at depth one cannot recursively start another pivot; a later pivot requires a
new explicit analyst action against an independently eligible approved origin.

## Eligible origins and routing

| Origin | Required state | Permitted routing | Prohibited routing |
|---|---|---|---|
| Full name | `full_name` claim, approved by a human, non-empty, and in the current Persona and case | Existing bounded Wikipedia public-biography and ICIJ Offshore Leaks name-match adapters | Alias spray, email or phone expansion, organization activation, or use of a pending name |
| Public social/profile link | Approved `social_account` claim whose HTTPS URL has a canonical identity supported by the current profile parsers | Existing Facebook, Instagram, Threads, TikTok, and X planner routes and enabled public-profile/search adapters | Arbitrary URL fetching, private/local destinations, unsupported hosts, or treating the link as identity proof |

An approved origin is permission to execute a bounded lead-generation step,
not proof that returned material describes the subject. Adapter output is
evidence or a proposal for review. It is never an approval.

## Server-owned budgets

The client may identify the eligible origin but cannot select sources, increase
depth, increase result counts, supply timeouts, or replace the policy snapshot.
The server must persist the applied policy version and numeric limits with the
job so a rerun and an audit can explain what actually ran.

| Control | Operational rule |
|---|---|
| Depth | Exactly one step beyond the approved origin; recursive expansion is refused. |
| Confirmed-name source and request budget | Exactly two allowlisted sources: Wikipedia public biography and ICIJ Offshore Leaks. Each may be attempted once, for a maximum of two source requests. Failed, throttled, and cancelled attempts still consume that budget. |
| Confirmed-name execution budget | A fixed 120-second overall deadline covers both source attempts. No client timeout or stored value may extend it. |
| Verified-link source and request budget | At most the existing five supported profile-platform routes: Facebook, Instagram, Threads, TikTok, and X. Planning remains capped at 25 native-search queries from at most five seeds. The existing per-query result cap remains authoritative: the contract permits no more than ten and the production provider setting of five is the tighter limit. Every attempted query, including a failure or retry, consumes the budget. |
| Verified-link execution budget | The existing focused-mode ceiling of 600 seconds applies. Exhaustive mode retains its 1,800-second ceiling for existing manual scans but is never selected by an automatic P3c pivot. The earlier of the persisted deadline and policy deadline wins. |
| Case concurrency | Preserve the existing one-active-investigation-per-case guard and durable worker lease. |
| Output | Existing correlation, evidence, candidate, citation, document-size, and confidence bounds remain authoritative. A pivot cannot enlarge them. |

Retries consume the same request and time budgets. Changing a browser field,
stored JSON document, or API payload cannot increase any limit. If stored data
contains forged or obsolete limits, the current server policy derives a safe
plan and applies the tighter deadline.

## Feature flags and kill switch

`OPENLEDGER_GOVERNED_PIVOTS_ENABLED` is the P3c kill switch. It must be
interpreted fail-closed: only an explicit true value enables creation of a new
governed pivot; a missing, false, or unrecognized value keeps the capability
off. The application and worker must load the same value.

This flag never overrides an existing capability flag. The following existing
controls remain authoritative where applicable:

- `OPENLEDGER_PROFILE_DISCOVERY_ENABLED`
- `OPENLEDGER_FOCUSED_DISCOVERY_ENABLED`
- `OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED`
- `OPENLEDGER_MAIGRET_DISCOVERY_ENABLED`
- `OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED`
- `OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED`
- `OPENLEDGER_PROVIDER_CIRCUIT_BREAKERS_ENABLED`
- `OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED`

Both the P3c flag and every required downstream flag must allow an operation.
Disabling a provider or search-first route therefore narrows the pivot even
when governed pivots remain enabled. A disabled flag returns a clear policy
outcome and must not be recorded as `absent` or `not found`.

## Audit and human review

The durable audit for each pivot must make the following reconstructable from
existing case, job, event, evidence, observation, candidate-review, and
claim-review records:

- case, Persona, job, and source-claim identifiers;
- canonical origin kind and value, without secrets or credentials;
- requesting actor, declared purpose, consent state, and request time;
- policy version, feature-flag snapshot, pivot depth, source allowlist, request
  limit, execution limit, start time, deadline, and completion state;
- every attempted source and its native outcome, request/citation lineage,
  retrieval time, immutable snapshot locator and digest where applicable;
- cancellation, interruption, throttling, parser/provider error, partial, and
  budget-exhaustion events; and
- every review decision with actor, timestamp, and bounded reason or note.

New assertions enter Persona review as `pending`. Profile-search candidate
review may propose a pending claim, but proposal is not approval. Approval,
rejection, and deferral are human decisions. Deferral uses the existing
`uncertain` review state. Each decision appends a review-history row; current
claim state may change, but prior decisions are not overwritten or removed.

The following components have no approval authority:

- AI analysis or chat;
- public-record and profile-search adapters;
- the correlation contract and engine;
- the planner, worker, and persistence synchronizer; and
- confidence, ranking, or exact-name matching logic.

They may create a pending assertion only with valid provenance. No adapter or
correlation payload may smuggle `approved`, an approval actor, or an equivalent
automatic-approval field. Existing reviewed states survive synchronization and
reruns; in particular, a rerun cannot clear or silently reopen a rejection.

## Cancellation, partial results, and failures

Queued cancellation completes without making an upstream call. In-flight
cancellation becomes a durable cancel request, stops further source scheduling,
and allows the worker to save already received, valid observations before it
records `cancelled`. Worker shutdown records `interrupted`. Repeating a stop
request is idempotent.

Valid partial evidence remains reviewable and retains its provenance. Work not
attempted after cancellation or budget exhaustion remains unknown. One source
failure does not erase valid output from another source, and it does not cause
the whole pivot to report `not found`.

The P3 evidence outcomes remain distinct:

- observed;
- absent;
- private;
- blocked;
- rate-limited;
- parser error;
- provider error; and
- indeterminate.

Only an authoritative, successfully parsed source response can support an
`absent` outcome. Timeouts, transport failures, unexpected response formats,
provider degradation, disabled capabilities, and ambiguous identity matches
remain their specific failure or indeterminate outcome.

## Reruns and idempotency

A rerun creates a new bounded execution audit but does not create a second
logical claim or duplicate evidence card for the same canonical observation.
Existing P2/P3 fingerprints, supported-platform aliases, immutable snapshot
digests, and source-record identities remain the deduplication authority.

Repeated observations may append genuinely new retrieval times, queries,
citations, or immutable locators to lineage. Replaying the same logical source
does not increase evidence totals or correlation confidence. Independently
attributable sources remain separate; contradictions remain visible. Human
review state and review history always take precedence over a fresh adapter
proposal.

## P2 compatibility and architecture impact

P3c is additive to the production-verified P2 workflow:

- Existing investigation creation, focused and exhaustive modes, aliases,
  Maigret, User Scanner, search-first discovery, candidate review, Personas,
  reports, history, cancellation, and reruns continue to use their current
  contracts.
- Existing stored jobs and audits remain readable. P3c does not rewrite or
  backfill prior evidence and does not reinterpret a historical failure as a
  finding.
- The new kill switch controls only new governed-pivot creation. Disabling it
  does not disable ordinary P2 discovery or hide retained evidence.
- P3c adds no Alembic migration and requires no schema change. Its policy and
  audit context use existing bounded job options, events, observations,
  evidence, and review-history records.
- P3c adds no dependency, lockfile, container, database, queue, registry,
  managed service, server, or paid API.

The deployment remains on the existing single Droplet. The application,
PostgreSQL-backed worker queue, PostgreSQL database, Caddy, and private SearXNG
container are reused. SearXNG retains its existing 384 MiB memory, 0.5 CPU, and
128 PID limits and has no host-published port.

Governed pivots can make only bounded calls to existing public upstreams and
public-record endpoints. Those services may observe, throttle, block, or
change responses, and normal Droplet bandwidth and compute usage still apply.
No Brave Search key, payment card, subscription, or new service is requested;
the existing private SearXNG route remains the search provider when its
capability flag and provider configuration permit it.

## Rollback

The first rollback action is to set
`OPENLEDGER_GOVERNED_PIVOTS_ENABLED=false` for both app and worker and recreate
the affected services through the normal deployment mechanism. This blocks new
pivot creation. It does not cancel an already persisted job automatically;
operators should use the existing durable cancellation control for active
work, then verify that no governed-pivot job remains active.

Capability-specific incidents can be contained more narrowly with the existing
provider, search-first, User Scanner, enrichment, or circuit-breaker flags. A
code rollback may return to the production-verified P2 release because P3c has
no migration or new runtime dependency. Do not delete cases, jobs, events,
audits, evidence, observations, claims, or review history during rollback.
Older code may ignore new bounded JSON audit fields; retained evidence remains
available when P3 is restored.

After rollback, verify app/worker flag parity, active-job count, worker and
database health, public HTTP health, P2 focused discovery, Persona review, and
report export. A rollback is incomplete if it removes evidence or resets human
review decisions.

## Verification matrix

Focused verification must cover:

| Area | Required assertions |
|---|---|
| Origin policy | Approved same-case full name and supported public link are accepted; pending/rejected/uncertain, cross-case, malformed, credential-bearing, private, local, and unsupported origins fail closed. |
| Budgets | Depth is one; source and request caps cannot be widened; retries count; focused/exhaustive deadlines use server policy; forged stored limits are ignored or tightened. |
| Flags | The P3c switch defaults off and rejects unknown values; each existing capability flag can independently narrow execution; app and worker snapshots agree. |
| Review | Every derived assertion is pending; approve/reject/defer append actor, timestamp, and reason; previous decisions survive rerun; AI, adapters, planners, and correlation cannot approve. |
| Outcomes | Cancellation, interruption, partial completion, blocked, rate-limited, parser error, provider error, indeterminate, and true absence remain distinguishable. |
| Idempotency | Repeated requests and source snapshots do not duplicate claims/evidence or inflate confidence; new retrieval lineage and independent corroboration are retained. |
| Boundaries | No pivot escapes its case or Persona, exceeds one depth, follows an output recursively, activates an unsupported source, or bypasses purpose/consent. |
| Rollback | Kill switch blocks new work without hiding or deleting retained evidence; ordinary P2 discovery and reporting still work. |

The release regression must include the P3 correlation contract, engine,
profile-search adapter, persistence/review, profile-search backend/planning/
ranking/orchestration/runtime/platform/UX suites, case store, Persona
intelligence, external evidence, reports, worker and deployment tests. It must
also run formatting, critical lint, Python compilation, schema validation,
`git diff --check`, and the dependency-complete relevant test suite without a
repository dependency workaround.

## Pull request, CI, and pre-merge gate

The coordinator integrates the P3c policy, acceptance tests, this document, and
shared application/store/worker changes into
`codex/p3-evidence-correlation-pivots`. One consolidated P3 pull request must
contain P3a, P3b, and original P3c. Before the original-P3 pre-merge checkpoint:

1. Review every worker diff and resolve ownership or security findings.
2. Run focused P3c tests and the full relevant regression.
3. Confirm no migration, dependency, infrastructure, or paid-service change.
4. Push the exact tested head and open or update the single P3 pull request.
5. Record its remote head and tree, and require all applicable GitHub CI and
   independent review checks to pass.
6. Verify the kill switch, cancellation, rollback, P2 compatibility, and
   administrator/analyst controls in a production-equivalent environment.

Passing this gate does **not** authorize squash, merge, deployment, or
production modification. It establishes only the original P3c pre-merge
checkpoint.

## Mandatory separate P3 extension

The required P3 usability extension is separate from original P3c. After the
checkpoint above, and before any P3 merge, the coordinator must:

1. stop at the pre-merge gate;
2. perform the mandated read-only programme-wide extension impact assessment;
3. propose distinct extension activity codes, ownership, dependencies, risks,
   tests, and estimates;
4. wait for the user's explicit authorization for each extension activity;
5. implement and test only authorized extension work;
6. test original P3 and the extension together in the same consolidated P3
   pull request; and
7. resolve CI and review before asking the user to squash-merge.

The extension includes the unified token-entry investigation input, Quick Scan
and Full Scan ordinary modes, a minimal alias option, deterministic server-side
planning, and clear no-relationship/degraded states. None of those changes is
authorized by P3c. The extension assessment itself does not authorize
implementation. Original P3 must not be merged first, and P4 must not begin
until the combined P3 release is human-authorized, deployed, and
production-verified.
