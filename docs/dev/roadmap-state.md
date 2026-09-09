<!--
SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
-->

# OpenLedger programme roadmap and state

This document is the durable source of truth for the approved P3--P18
programme and its current release state. Update it at every authorized activity,
major integration, phase release, and chat-transfer checkpoint. Do not record a
phase or test as complete until its evidence exists.

## Programme summary

- Scope: P3 through P18, 42 consolidated authorization activities.
- Conservative planned effort: approximately 54.5 hours.
- Expected critical path with safe dynamic concurrency: approximately 42--50
  hours, excluding time waiting for human approval.
- Typical activity: 45--90 minutes. An activity may finish earlier; do not
  invent work to consume its estimate. If it materially exceeds 90 minutes,
  stop at a safe checkpoint and propose a justified split.
- One consolidated pull request is produced per phase. Pull requests,
  migrations, merges, deployments, and production verification are serialized
  in numerical phase order.

The foundation sequence is:

```text
P3 -> P4 -> P5 -> P6 -> P7 -> P8 -> P9 -> P10 -> P11
```

After P11, the dependency lanes are:

```text
Organization: P11 -> P12 -> P13 -> P14 --+
                                               +-> P17 -> P18
Dark web:     P11 -------------> P15 -> P16 --+
                                  ^
                                  +-- P16 also requires the P13 adapter contract
```

P12--P14 and P15 may be developed in parallel only when contracts and ownership
are isolated. P15 still merges and deploys after P14. P17 requires
production-verified P14 and P16. P18 requires production-verified P17.

## Production-verified baseline

P0, P1, and P2 are complete and production-verified. No P2 activity remains;
do not repeat P0--P2 work.

| Phase | Release record |
|---|---|
| P0 | PR #46, squash commit `1a97398` |
| P1 | PR #47, squash commit `33106f3` |
| P2 search provider | PR #49; branch `codex/self-hosted-profile-search`; final remote head `19c45c3fef01c20c9f70a0a43b63b49b87d8544e`; tested tree `894c552548dd2636bd0c088c708d4230a5f17ecd`; squash commit `3c5b2a9e818d9c64ca7f74d440f91060ce5b3445` |
| P2 report update | PR #50; branch `codex/human-centric-investigation-report`; final remote head `62c045f8e36644ada0b155a9a5154fd84114ddef`; final tree `d771486cfbd9683f97bd59b76829d2eac495f72a`; squash commit `288fdd216c1a107113719cc1c850d9fc0dd47808` |

Production currently runs Git SHA
`288fdd216c1a107113719cc1c850d9fc0dd47808`, tree
`d771486cfbd9683f97bd59b76829d2eac495f72a`, and Alembic head
`b3e9d7c4a610`. The P2 search-first discovery baseline, private SearXNG
provider, governed analyst review, and human-centric investigation report have
passed their release and production gates.

The verified PostgreSQL backup is
`/opt/openledger/runtime/backups/openledger-20260909T013742Z.dump` with SHA-256
`a0d6f8dfa986a20c74f389975ae21d133c4acabeca6a9fb69d1ca8df4cf862d1`.
Report-update source recovery material is in
`/opt/openledger/runtime/backups/report-update-20260909T013733Z`, including
`source-before.bundle`. The report update introduced no migration; restoring an
older database dump is not part of normal code rollback because it could
discard newer production data.

The application, database, and private SearXNG containers are healthy; the
worker and proxy are running; the migration service exited successfully; the
public health endpoint returns HTTP 200 with a connected database; and no
investigation remained active after the production verification. SearXNG has
no published host port. App and worker share the enabled SearXNG search-first
configuration, five-result maximum, and ten-second timeout.

## Current P3 checkpoint

P3a, P3b, and original P3c completed local integration checkpoints on
2026-09-09. The user authorized original P3c; that authorization did not
authorize the separate mandatory P3 usability extension, squash, merge,
deployment, or production modification. All P3 work started from exact
production-verified commit
`288fdd216c1a107113719cc1c850d9fc0dd47808` and tree
`d771486cfbd9683f97bd59b76829d2eac495f72a`; none was built on the old P2
checkout.

The phase integration branch is `codex/p3-evidence-correlation-pivots` in
`/workspace/scratch/a7cc4d9992b8/openledger-p3`. Its independently reviewed
original-P3c checkpoint is `aae6891a7883905424a1405bcfc672cbbad19c4e`,
tree `e85501c0d2588722b763838b900469955d351636`. The consolidated remote
branch was reconstructed without squashing as 19 commits with head
`c062a9eb97bfb5e8edd10c8efab0f537d317c120`; its final tree exactly matches
the locally tested tree `f053bb88509ce6fb38bcea9fe96c3cae1577e5c3`.
Consolidated P3 pull request #51 targets unchanged P2 `main` at
`288fdd216c1a107113719cc1c850d9fc0dd47808`.

| Owner | Branch and worktree | Exclusive responsibility | Checkpoint state |
|---|---|---|---|
| Coordinator | Phase branch and worktree above | Architecture, shared app/store/worker integration, review fixes, test gate, and durable checkpoint | P3a, P3b, and original P3c integrated locally; independently reviewed P3c checkpoint `aae6891a7883905424a1405bcfc672cbbad19c4e` |
| P3a contract worker | `codex/p3a-correlation-contract` in `/workspace/scratch/a7cc4d9992b8/openledger-p3a-contract` | Correlation contract module and schema | Source `9586e626420817dae5a068f36119f389a264f14e`; integrated as `6eacc3539fdf9c3e972c897d1bafb61b9778ec03` |
| P3a acceptance worker | `codex/p3a-acceptance-fixtures` in `/workspace/scratch/a7cc4d9992b8/openledger-p3a-tests` | Contract fixtures and acceptance tests | Source `18dd0f84cc96de847f6500ae77938f8a9f811c3b`; integrated as `b560f61c24e1cf9877b341abf5c872b65a255380` |
| P3a continuity worker | `codex/p3a-roadmap-docs` in `/workspace/scratch/a7cc4d9992b8/openledger-p3a-docs` | Multi-agent runbook and roadmap state | Source `cd719ea850820243b1e74ffb5d3e66d807d9eb46`; integrated as `2c0afd83fb41618162c12202c5fcace919d732c1`; final P3a checkpoint `3191b860cc334ff8c45fd20d56ef47d3847c2bc8` |
| P3b engine worker | `codex/p3b-correlation-engine` in `/workspace/scratch/a7cc4d9992b8/openledger-p3b-engine` | Pure deterministic clustering, relationships, confidence, and unit tests | Source `fd1d6e7c34b90e2c1fb81b0fc990f25c63a244b5`; integrated as `c8c4e63bea44ab1bc611c40c64c84400dcdb7975` |
| P3b acceptance worker | `codex/p3b-correlation-acceptance` in `/workspace/scratch/a7cc4d9992b8/openledger-p3b-tests` | Black-box correlation and Persona acceptance coverage | Source `46eff6754d6c42e67832273ee7b38087e4b6365f`; integrated as `a96145aa3d311e86c615effe46fa4398930dc5c6` |
| P3b adapter workers | `codex/p3b-profile-search-adapter` in `/workspace/scratch/a7cc4d9992b8/openledger-p3b-adapter` | Proposed isolated adapter ownership | Both attempts stopped without changes; coordinator completed the shared integration in `fb670bb`, `53afafd`, and `bd27e5c` |
| P3b reviewer | Read-only review of the phase worktree | Correlation, provenance, compatibility, idempotency, and human-control review | No remaining actionable findings at `bd27e5c`; 102 focused tests passed; no edits made |
| P3c policy worker | `codex/p3c-governed-pivot-policy` in `/workspace/scratch/a7cc4d9992b8/openledger-p3c-policy` | Pure deterministic governed-pivot policy and unit tests | Source `8c8d985501d23dbd48b7557a235fad83bb25a13b`; integrated as `0634ca5` |
| P3c acceptance worker | `codex/p3c-governed-pivot-acceptance` in `/workspace/scratch/a7cc4d9992b8/openledger-p3c-acceptance` | Black-box origin, scope, budget, audit, review, cancellation, rerun, and failure acceptance | Source `297363aea6d44d378e67c92de16172577cd00f3e`; integrated as `2db5a2e` |
| P3c operations worker | `codex/p3c-governed-pivot-ops` in `/workspace/scratch/a7cc4d9992b8/openledger-p3c-ops` | Governed-pivot policy, operations, rollback, cost, and release documentation | Source `e3666ca062f0ab3cb8b2c06d6ddfd4b5733b6b4d`; integrated as `5233a75` |
| P3c reviewer | Fresh read-only review of exact committed phase tree | Policy, security, runtime, persistence, compatibility, tests, operations, and cost boundaries | Review of `6bcef56` found two blockers, resolved at `4428161`; re-review of `9ee5c2f` found one actor-attribution blocker, resolved at `e2ff104`; final review of `aae6891` found no actionable findings and passed 386 focused tests; no edit authority |

P3a froze the provider-neutral, case-scoped evidence contract. P3b now adds a
bounded deterministic correlation engine and a strict adapter for immutable P2
profile-search audits. Supported-platform aliases share one canonical claim;
same-snapshot observations are duplicates; independently attributable source
signals remain separate; explicit conflicting or unrelated classifications
suppress incompatible inference; and correlation confidence is deterministic,
bounded, and never an approval decision. All eight outcome classes remain
distinct, including ambiguous provider and parser failures.

Repeated observation identities retain every unique retrieval time, query,
citation set, and immutable snapshot locator. Profile-search snapshots use a
deterministic normalized digest that excludes query-relative rank and provider
identity; the locator remains audit/source-record-specific and embeds the exact
digest. The overall audit ID and SHA remain separately retained. Source errors
use their native occurrence time. Legacy P2 profile URLs with credential-like
tracking-key names remain readable because citations use the safe canonical
profile URL while the immutable raw audit remains referenced.

Persona proposals reuse canonical social-account claims. Reruns from the same
logical source do not add evidence cards or increase confidence. Existing P2
evidence rows are reused without changing their payload, fingerprint, or
timestamp; new correlation context is recorded in append-only claim lineage.
Alias-only observations cannot replace the curated representation or
confidence of a reviewed claim. Correlation remains visible before review but
creates no claim automatically, and no AI, adapter, or engine can approve a
claim.

Original P3c adds deterministic, analyst-directed pivots from only an approved
full-name claim or a supported approved public-profile claim in the same
Persona and case. The server owns the plan, source allowlist, request ceiling,
depth-one limit, focused execution mode, deadline, feature snapshot, and audit
event. Confirmed-name enrichment is restricted to the existing Wikipedia and
ICIJ adapters with at most two source attempts and 120 seconds. Verified-link
discovery is restricted to the existing Facebook, Instagram, Threads, TikTok,
and X routes, at most 25 planned native-search requests, and the existing
600-second focused ceiling. The worker revalidates the source claim, case,
Persona, review decision, plan, and current kill switch immediately before
execution. Pivots cannot request AI, User Scanner, recursive expansion,
arbitrary URLs, private/local destinations, or exhaustive mode.

Every derived assertion continues through the existing pending analyst-review
workflow. Existing approvals and review history survive reruns; repeated
completed link pivots from the same origin are refused; output from a pivot
cannot become another pivot origin; ambiguous failures and budget exhaustion
remain indeterminate rather than negative evidence; and queued cancellation is
explicitly auditable. Each request now requires a bounded lawful purpose and an
affirmative declaration that the public-source follow-up is permitted by the
applicable authorization or consent and remains within that purpose. The plan
and queued event retain the purpose, confirmation, actor, and authorization
basis. Approving a full name does not itself queue enrichment.
`OPENLEDGER_GOVERNED_PIVOTS_ENABLED` fails closed when missing, false, or
malformed. Compose, the install script, and the deployment environment template
all ship it disabled for both app and worker; an operator must explicitly
enable it after the release controls are satisfied. Disabling it blocks new and
not-yet-executed pivots without deleting retained evidence or disabling
ordinary P2 discovery.

After the review fixes, the P3c-focused and shared integration set passed 284
tests. The dependency-complete relevant release regression passed 993 tests
with one intentional skip across correlation, governed pivots, profile discovery,
profile-search backends/planning/ranking/orchestration/platforms/runtime,
persistence/review, case store, Persona intelligence, external evidence,
reports, workers, deployment, budgets, inputs, collectors, stream
finalization, User Scanner, and alias UX. The initial complete repository run
passed 1,562 tests with 11 skips; its only three failures were unchanged slow
tests that require live Reddit, Google Play, and Cloudflare access, which the
current workspace proxy/DNS could not reach. After the review fixes, the
offline repository sweep passed 1,575 tests with 11 skips and those three
network probes explicitly deselected. Critical Flake8 checks, Python
compilation, `git diff --check`, and Black checks for new P3-owned policy and
acceptance files passed. No test compatibility plugin or repository workaround
was required in the dependency-complete environment. The final independent
review of exact commit `aae6891` passed 386 focused tests, found no actionable
issues, and left the worktree unchanged.

P3c adds no migration, dependency, lockfile, service, paid API, managed
infrastructure, or production-data change. It reuses the single Droplet,
PostgreSQL worker queue, existing public-source adapters, and private SearXNG.
No Brave credential or subscription is required. At this checkpoint nothing
has been merged or deployed, and no production action has occurred. The
consolidated branch and PR #51 are open; the next original-P3 actions are CI
resolution and pre-merge checkpoint verification. Only after that checkpoint
may the mandated read-only programme-wide P3-extension assessment begin. Extension
implementation still requires separate activity-code authorization; squash,
merge, deployment, and production modification remain unauthorized.

## P3 -- Evidence correlation and governed pivots

**Objective:** transform profile-discovery output into deduplicated,
provenance-preserving evidence clusters and allow safe analyst-directed pivots
without automatic approval or uncontrolled expansion.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P3a -- Correlation and evidence contract | 60 min | Inspect exact P2 code and tests; freeze source identity, canonical URL, supported-platform alias equivalence, supporting/duplicate/conflicting/unrelated relationships, failure taxonomy, and stable idempotency; preserve citations, retrieval timestamps, source snapshots, and originating queries; establish fixtures and focused acceptance tests; create the repository-owned multi-agent and roadmap documents. |
| P3b -- Cross-source correlation engine | 75 min | Cluster cross-source evidence, reuse equivalent claims instead of duplicate cards, and correlate normalized handles and aliases; combine support without hiding individual provenance; retain conflicts; calculate bounded explainable confidence; prevent rerun inflation; keep stable case, Persona, report, and evidence totals; add focused unit and integration tests. |
| P3c -- Governed pivots, analyst review, and release | 75 min | Permit confirmed-name and verified-link pivots under lawful-purpose, consent, source, request, depth, execution, feature-flag, and case budgets; send assertions to pending review; preserve approve/reject/defer actor, timestamp, reason, and history; test rollback and regression; complete the consolidated PR and human-controlled production release gate. |

Acceptance requires equivalent claims not to duplicate evidence records,
independent corroboration to remain separately attributable, conflicts to remain
visible, and every retained claim to preserve provenance and retrieval context.
Absent, private, blocked, rate-limited, parser-error, provider-error, and
indeterminate outcomes remain distinct. Pivots originate only from confirmed
names or verified links and cannot escape consent, budgets, flags, or case
boundaries. No AI or external adapter may approve Persona or organization
assertions. Reruns are idempotent, rollback disables behavior without deleting
evidence or resetting the UX, and all P2 workflows and reports remain
functional. Release requires a tested, merged, deployed, and
production-verified P3.

Dependency: production-verified P2.

### Mandatory P3 extension gate

| Extension state | Current record |
|---|---|
| Programme-wide impact assessment | Mandated after the original-P3 pre-merge checkpoint; not started |
| Proposed implementation activity codes | None yet; they must be produced by the assessment |
| Authorized implementation activity codes | None |
| Implementation | Not authorized and not started |
| Merge effect | Original P3 must not be squash-merged before the assessed extension activities are separately authorized, completed, tested with P3, and added to the same PR |

## P4 -- Benchmarking, telemetry, and controlled rollout

**Objective:** measure P3 against an authorized corpus, add privacy-safe
operational signals, and release behind controlled rollout mechanisms.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P4a -- Benchmark corpus, metrics, and baseline | 60 min | Create an authorized representative corpus; define precision, useful recall, duplicate, indeterminate, provider-failure, latency, and resource metrics plus release thresholds; build a reproducible harness; record the production-equivalent P3 baseline. |
| P4b -- Privacy-safe telemetry and regression gates | 60 min | Add telemetry without unnecessary subjects, secrets, or evidence content; use the approved failure taxonomy; gate duplicates, conflicts, precision, and latency; keep telemetry within case and authorization boundaries. |
| P4c -- Controlled rollout and release | 60 min | Add shadow, canary, staged-ramp, kill-switch, and per-platform rollback controls; test blocks, throttling, parser changes, and partial failures; document operations; run benchmarks and full regression; complete the consolidated PR and production release. |

Release dependency: production-verified P3. Isolated corpus and metric
preparation may begin during P3, but the final P4 deployment may not.

## P5 -- User Scanner upstream synchronization

**Objective:** assess and selectively synchronize the existing pinned Nexorus
User Scanner fork without duplicating OpenLedger logic or weakening its adapter
boundary.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P5a -- Upstream assessment and target selection | 75 min | Compare the fork with current upstream across code, security, dependencies, license, behavior, and generated artifacts; select an exact target; document accepted, rejected, and manually adapted changes; confirm incremental value. |
| P5b -- Controlled integration and release | 75 min | Integrate selected changes while preserving adapters, classifications, budgets, provenance, and isolation; update immutable pins and notices; run compatibility, security, focused, and full regression tests; document the fork; complete the PR and runtime-version production gate. |

Release dependency: production-verified P4. Any assessment prepared earlier
must be refreshed before integration.

## P6 -- Reproducible runtime, dependency, and deployment automation

**Objective:** make runtime inputs reproducible and add protected, fail-closed
deployment automation without new paid infrastructure or loss of human control.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P6a -- Immutable runtime and update automation | 75 min | Inventory base, app, database, proxy, worker, CI, and auxiliary images; replace floating references with reviewed immutable digests where appropriate; record versions and updates; create reviewable dependency proposals; validate license, compatibility, rollback, and supply-chain integrity. |
| P6b -- Protected deployment automation and release | 75 min | Retain `deploy/update.sh`; add a protected GitHub production environment, human approval, least-privileged deployment identity, pinned Droplet identity, expected-SHA check, PostgreSQL backup, Alembic migration service, service rebuild/recreation, and Git/migration/container/worker/HTTP verification; fail closed on any failed verification; document manual deployment and rollback; add no paid registry, managed database, queue, or server. |

Release dependency: final pins use production-verified P5 versions.

## P7 -- Social-discovery programme acceptance

**Objective:** close the complete social-discovery programme with functional,
security, operational, and production-equivalent acceptance.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P7a -- Full regression and security acceptance | 60 min | Validate focused/exhaustive discovery, search-first, User Scanner, Maigret, correlation, review, Personas, AI proposals, reports, history, cancellation, and reruns; measure precision, recall, duplicates, stable counts, isolation, rate limits, privacy, egress, secrets, rollback, and all distinct failure outcomes. |
| P7b -- Operational closure and release | 60 min | Confirm repository SHA, dependency versions, digests, Alembic head, flags, and deployment configuration; validate backup, update, rollback, and recovery; run final CI, security, and production-equivalent tests; complete programme documentation, PR, and end-to-end production acceptance. |

Release dependency: production-verified P3--P6.

## P8 -- Project and database identity foundation

**Objective:** place Project above Case, move users and sessions to PostgreSQL,
and migrate legacy data without exposing projects across analysts.

Binding model: projects contain isolated cases; project merging and
cross-project case combination are unsupported; administrators see all
projects; analysts see only active memberships; users may belong to multiple
projects.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P8a -- Project invariants and schema | 75 min | Add database users, projects, memberships, audit records, case project/creator ownership, constraints, indexes, non-destructive archive behavior, and administrator/analyst access invariants. |
| P8b -- Authentication migration and legacy backfill | 90 min | Replace JSON-file authentication with database users/sessions; preserve passwords or require safe reset; create a legacy/default project; backfill cases, creators, and memberships without additional analyst exposure; retain session invalidation, password management, reversibility, and rollback. |
| P8c -- Stores, migration tests, and release | 75 min | Implement project, membership, user, and audit stores; test migrations, constraints, authentication, sessions, and legacy data; preserve all existing cases, evidence, reports, and jobs; complete the PR and production data/auth verification. |

Release dependency: production-verified P7.

## P9 -- Project authorization boundary

**Objective:** enforce a single canonical project policy across every resource
and eliminate cross-project access paths.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P9a -- Central project policy | 90 min | Enforce administrator implicit access and active analyst membership; define create, view, edit, administration, and archive permissions; avoid duplicating rules in routes. |
| P9b -- Complete resource scoping | 90 min | Scope cases, jobs, Personas, claims, external evidence, history/search, reports, AI/chat, relationships/timelines, combined cases, organization assets, downloads, and generated artifacts; enforce same-project combinations and immediate revocation. |
| P9c -- Security regression and release | 60 min | Test IDOR, guessed IDs, stale sessions, revocation, cross-project queries, workers, reports, and artifacts; preserve administrator visibility without weakening analyst isolation; complete production RBAC and isolation verification. |

Release dependency: production-verified P8.

## P10 -- Project landing and user administration

**Objective:** make project selection the authenticated landing journey and
move project membership administration into that context.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P10a -- Project dashboard and selection | 90 min | Display accessible project cards; support selection, switching, and administrator creation/edit/archive; design archived, unavailable, loading, empty, error, and permission states; safely maintain active-project context. |
| P10b -- User-management relocation and release | 90 min | Move user and membership management to the landing dashboard; support multi-project assignment; keep personal password management under Security; add responsive/accessibility behavior; test administrator and analyst journeys; verify production. |

Release dependency: production-verified P9.

## P11 -- Project-scoped investigation workflows

**Objective:** require active-project context throughout investigation,
derived-case, combination, relationship, and legacy navigation workflows.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P11a -- Active-project enforcement | 90 min | Require an active project for case and investigation creation; scope navigation, lists, history, search, jobs, evidence, reports, and AI; ensure derived person, organization, entity, and affiliation cases inherit the project; prohibit global fallback. |
| P11b -- Combination, relationships, and release | 90 min | Allow same-project combinations only; scope combined analysis, graphs, relationships, and timelines; preserve the current graph; stop legacy URLs/bookmarks leaking data; test revocation during work; verify production isolation. |

Release dependency: production-verified P10.

## P12 -- First-class organization cases

**Objective:** add organization subjects and governed organization assets as
first-class, project-scoped evidence and review objects.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P12a -- Organization subject and asset model | 90 min | Support person, organization, and combined subjects; safely migrate existing subjects; add organization identity, header, verified domain, metadata, and reviewable domains, emails, hosts, IPs, URLs, ASNs, and public-people references linked to canonical evidence. |
| P12b -- Organization workflows and release | 90 min | Support pending/approved/rejected/deferred assets with provenance and audit; create same-project person cases from findings; combine approved organization findings only within the project; test migrations, stores, routes, workflow, review, and isolation; verify production. |

Release dependency: production-verified P11.

## P13 -- theHarvester integration

**Objective:** integrate a passive, organization-scoped theHarvester adapter in
a pinned isolated runtime with explicit license and safety boundaries.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P13a -- Licensing, runtime, and policy boundary | 90 min | Keep GPL theHarvester in a separately pinned process/container; do not copy it into proprietary OpenLedger; record notices/distribution duties; require analyst opt-in and verified organization domain; allowlist passive sources; disable DNS brute force, reverse lookup, virtual-host discovery, takeover checks, screenshots, Shodan, breach/infostealer collection, active probing, and arbitrary arguments. |
| P13b -- Scoped execution and ingestion | 90 min | Add project/case-scoped jobs with resource, concurrency, time, and result limits; support progress, cancellation, partial completion, cleanup, and temporary storage; parse JSONL; preserve source/action provenance; normalize organization assets; prevent raw output bypassing evidence controls. |
| P13c -- Evidence, security, and release | 90 min | Store canonical evidence and pending assets; test parsing, command injection, timeout, cleanup, malformed output, isolation, licensing, project isolation, and end-to-end behavior; prove active functionality is unreachable; document update/rollback/sources; verify production. |

Release dependency: production-verified P12.

## P14 -- Organization intelligence workspace

**Objective:** provide a minimal functional organization discovery and review
journey before the P18 visual revamp.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P14a -- Scan and review journey | 90 min | Add scan controls and source selection; show progress, cancellation, partial completion, failures, type/source grouping, and provenance; support approve/reject/defer/history; make only necessary pre-P18 UI changes. |
| P14b -- Differences, correlation, AI, and release | 90 min | Show new, unchanged, removed, and changed rerun findings; feed approved assets into same-project combinations; perform exact correlation only in authorized combined cases; add cited AI interpretation and export; test end-to-end and regression; verify production. |

Release dependency: production-verified P13.

## P15 -- Minimized Robin manual discovery

**Objective:** provide bounded, manual public-onion retrieval through a
minimized, Tor-only Robin adapter while OpenLedger remains authoritative.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P15a -- Fork minimization and Tor isolation | 90 min | Fork and pin Robin; remove/disable its UI, LLM, storage, and reporting; run isolated and unprivileged; permit only Tor-routed public v3 onion access with no clearnet fallback; retain OpenLedger ownership of projects, cases, evidence, review, and reports. |
| P15b -- Bounded retrieval | 75 min | Require manual initiation and recorded purpose; permit bounded GET/HEAD only; limit redirects, media types, sizes, time, result counts, and retained text; block SSRF, private networks, local services, unsafe schemes, and arbitrary destinations; preserve cancellation and cleanup. |
| P15c -- Evidence, manifests, and release | 75 min | Create hashed manifests; store project/case evidence; route assertions to analyst review; test Tor isolation, no-clearnet, redirects, SSRF, malformed content, timeout, cleanup, audit, and end-to-end behavior; document operations/rollback; verify production. |

Development dependency: production-verified P11. This isolated lane may run
beside P12--P14, but its PR merges and deploys after P14 in numerical order.

## P16 -- Monitoring, deltas, and alerts

**Objective:** add project-scoped recurring monitoring on the existing
PostgreSQL worker, producing deduplicated deltas, alerts, and pending evidence.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P16a -- Monitoring subscription model | 75 min | Store project, case, query, purpose, creator, cadence, status, timestamps, and ownership; enforce minimum cadence and project/user limits; define active, paused, failed, retired, and archived states; preserve audit history. |
| P16b -- PostgreSQL scheduler, deltas, and alerts | 75 min | Use the current PostgreSQL-backed worker without Redis or another queue; add concurrency, retries, backoff, circuit breaking, and limits; detect new, changed, disappeared, and unchanged findings; create in-app alerts and pending evidence without duplication or inflation. |
| P16c -- Lifecycle, isolation, and release | 60 min | Support pause, resume, retire, retention, and cleanup; test isolation, revoked membership, retries, concurrency, duplicates, retention, and end-to-end behavior; document monitoring/rollback; verify production alerts. |

Development and release dependency: production-verified P13 and P15 adapter
contracts.

## P17 -- Benchmarking, security, and functional stabilization

**Objective:** close functional and security risk across the expanded product,
decide whether Subfinder merits a later P19, and establish the P18 baseline.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P17a -- Authorized-domain benchmark and P19 decision | 90 min | Compare current sources, theHarvester, and Subfinder on an authorized corpus for incremental useful recall, precision, duplicates, overlap, latency, resource use, and reliability; firmly authorize conditional P19 or reject/defer Subfinder; do not integrate it. |
| P17b -- Security, operations, and functional closure | 90 min | Test IDOR, command injection, SSRF, Tor isolation, secrets, authorization, revocation, and project leakage; validate cleanup, cancellation, backup, migration, recovery, licensing, notices, and SBOM; run full regression; confirm journeys; inventory P18 routes/roles/states/components; record performance; verify stabilization in production. |

Release dependency: production-verified P14 and P16. P19 is not approved by
this roadmap; P17 may only produce its explicit decision.

## P18 -- Selective UI/UX revamp and final validation

**Objective:** selectively adapt the complete production-verified product to
the approved OpenLedger design system without indiscriminate rewrites or
functional regression.

| Activity | Estimate | Approved outcome |
|---|---:|---|
| P18a -- Preservation audit and design contract | 75 min | Audit all routes, roles, workflows, components, loading/error/permission/degraded states, and responsive behavior; classify each component retain/reskin/refactor/replace; retain working components by default; require separate approval to replace one; lock Design DNA and O/ identity; record performance. |
| P18b -- Shared system and application shell | 75 min | Implement Alliance No. 2, black/near-black/near-white/gray/purple tokens, restrained glass rules, shared components, responsive sidebar/rail/drawer/top bar, authentication, project selection/switching, cards, and dashboard user administration. |
| P18c -- Core investigation journeys | 75 min | Selectively adapt new investigation, planning, progress, cancellation/partial completion, cases/history, person Personas, claims/evidence, analyst review, affiliations, and reports while preserving logic, APIs, contracts, and loading behavior. |
| P18d -- Extended intelligence, relationships, and AI | 75 min | Selectively adapt organization discovery/evidence/differences, theHarvester, Robin, monitoring/alerts, combined cases, relationships, timelines, AI chat/assessments/citations/proposals, and exports; preserve the relationship graph's library, model, layout, controls, navigation, and interactions except for separately approved defect fixes. |
| P18e -- Responsive, accessibility, performance, and release | 60 min | Validate desktop/tablet/mobile, keyboard, focus, semantics, contrast, tables, touch graphs, loading/empty/error/permission/refresh/degraded states, LCP <=2.5 s, INP <=200 ms, and CLS <=0.1 under agreed conditions; run parity, visual, security, accessibility, and performance tests; complete human-approved production validation. |

Release dependency: production-verified P17. No piecemeal redesign is permitted
before P18 except essential usability corrections and reusable infrastructure
required by an earlier approved function.

### P18 Design DNA and preservation guardrails

Preserve the minimalist `O/` OpenLedger identity, `OPENLEDGER` wordmark, and
`OPEN-GATE · OSINT WORKSPACE` descriptor. Use Alliance No. 2 typography, true
black `#000000`, near-black structural surfaces, near-white primary text,
neutral-gray secondary text, and Nexorus purple near `#7C4DFF` as the normal
accent. Primary actions are solid purple.

Panes, forms, tables, evidence records, side panels, and list items remain
opaque. Smoked transparency is restrained and belongs only on appropriate
floating surfaces; backdrop blur is used only when genuinely necessary and is
normally no more than approximately 8 px. Preserve responsive desktop, tablet,
and mobile behavior; the collapsible desktop sidebar, tablet rail, and mobile
drawer; keyboard and semantic accessibility; progressive table disclosure;
touch-usable graphs; refresh content preservation; and designed loading, empty,
error, permission-denied, and degraded-service states.

Prohibit cyan/tosca, neon glow, holographic styling, decorative gradients,
luminous shadows, excessive status colors, unnecessary cards, unnecessary
labels or explanatory copy, decorative metrics, generic AI-dashboard styling,
and indiscriminate rewrites. Performance targets are LCP at most 2.5 seconds,
INP at most 200 milliseconds, and CLS at most 0.1 under agreed conditions.

## Cost and external-dependency boundaries

State the applicable boundaries before requesting any deployment or external
integration action:

- Do not request or use a Brave Search API key, Brave subscription, payment
  card, or paid search plan.
- SearXNG uses existing Droplet capacity, has no per-request API bill, and is
  capped at 384 MiB RAM, 0.5 CPU, and 128 PIDs.
- SearXNG still queries external upstream engines. They may observe, throttle,
  block, or change responses; normal outbound traffic and Droplet bandwidth and
  compute usage remain.
- Report export may make bounded outbound requests to the host of an approved
  public photograph and to the fixed OpenStreetMap tile service. No paid map
  API, key, card, or subscription is required.
- Photograph and OpenStreetMap providers may observe, throttle, refuse, or
  become unavailable. Cache map tiles for at least seven days. Media failures
  must render explicit safe fallbacks instead of failing a report.
- Do not add paid infrastructure, managed databases, registries, queues, or
  recurring-cost services without explicit approval.
- P13 adds a separately pinned theHarvester runtime and its licensing and
  distribution obligations.
- P15 adds isolated Tor-routed minimized Robin and dependence on Tor and public
  onion availability; clearnet fallback is prohibited.
- P19 is not approved. Subfinder remains a possible later phase only if P17
  demonstrates meaningful incremental value and the user explicitly approves
  it.
