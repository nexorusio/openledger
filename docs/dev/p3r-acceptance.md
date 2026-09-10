<!--
SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
-->

# P3R reliability remediation and acceptance

P3R is authorized through a tested and independently reviewed pull request.
The user performs squash-and-merge and the Docker update. Implementation starts
from merged PR #52, commit `77408090946015064ed8df99e9d7b44365d54838`, tree
`3380ef19af74050d29462b195e0694584b050737`. Earlier pre-merge roadmap entries are
historical. Production has not been independently reinspected in this audit.

## A. Actual incident closure

The exact production count gap is not explained by a synthetic reproduction.
Its minimum saved diagnostics are: the affected job ID and UTC time window;
the stored selected inputs/routes and source filters with secrets removed;
the job's status, deadline, heartbeat/stop timestamps, progress and terminal
result; its ordered saved events; per-source report/diagnostic summaries; and
worker exception/timeout/stop logs for that same interval. Include the deployed
image/commit identity and detector database/health selection counts. Separate
Maigret supported findings, candidates, User Scanner usernames and registrations.
Do not collect new production scans to substitute for this saved evidence.

Reconciliation must explain the actual planned/admitted/completed/failed/
timed-out/cancelled/unknown/unattempted counts and connect them to saved outputs.
Missing historical diagnostics remain explicitly unknown. PR readiness does not
close the production incident; deployment and human production acceptance are
separate user-controlled milestones.

## B. Reliability and larger-ingestion readiness

| Area and current behavior | Defect or risk | Minimal P3R change and dependency | Test workload | Required threshold and evidence |
| --- | --- | --- | --- | --- |
| Maigret native checks feed legacy notifier results | Unexpected exceptions disappear from returned outcomes; terminal output does not establish absence | Preserve native UNKNOWN failures and a separate opaque task/attempt ledger; persist plans before admission | 2,645 checks with 245 exceptions; 50,000 with 5,000; healthy controls; retries and duplicate-ID rejection | Every planned task has exactly one disposition or explicit interrupted unknown; no error/timeout becomes absence; exact returned-ID and persisted-ledger reconciliation |
| Concurrent queries can remain alive during cancellation cleanup | Timeouts release capacity before cleanup finishes; source self-cancel may abort siblings | Bound active plus cleaning queries, drain cleanup within five seconds, freeze late notifier updates, retain incomplete-cleanup state | 1,000 timeouts; resistant cleanup; early iterator close; durable sink failure; healthy siblings alongside self-cancel | Outstanding work never exceeds configured cap; bounded return; zero untracked late writes; clean process exit and cleanup transition evidence |
| Existing profile stages execute sequentially under one deadline | An upstream stall can exhaust downstream time | Injectable standalone stage scheduler; selected-stage reservations with real dependency readiness; reuse existing absolute job deadline | Existing adapters replaced only at I/O boundaries; source stalls/failures, disabled/consent-withheld routes, 1/4/16 targets | Every eligible route starts or has an explicit admission reason; 600/1,800-second policy and bounded cleanup retained; no sibling cancellation cascade |
| Jobs, SKIP LOCKED claiming, heartbeats and single collector lock already exist | Event replay, lease loss and interrupted finalization can produce misleading results | Add versioned accounting/checkpoints in existing JSON storage; owner-guarded idempotent events; commit terminal result, pending claims and done event together | Disposable PostgreSQL: 1/10/50 queued jobs; stop, stale owner, concurrent finalizers, checkpoint and publication fault boundaries | One authoritative terminal result/done; committed permitted evidence survives; no old-owner event/audit/claim/publication; transaction rollback evidence |
| Native search retains typed query/run/candidate audits | Saving every running snapshot would exhaust the ten-audit limit; in-flight cancellation differs from unattempted work | One bounded mutable running checkpoint plus immutable terminal audit; explicit active/interrupted counts; existing retention validator | Up to 25 queries, per-query checkpoint, failure before/after persistence, interrupted current query | No audit-cap exhaustion; exact planned = attempted + unattempted; completed/error/interrupted outcomes preserved without raw provider payloads |
| Supported profile URL extraction uses database templates | A trailing slash loses routing; blindly stripping it admits reserved platform pages | Resolve slash-compatible existing templates and apply existing reserved-route parsers | 32 supported URL fixtures, generic/reserved negatives, persisted source/alias/consent parity | 100% expected routes; zero widened unsupported targets |
| Correlation already clusters with union-find and expands pairs | A dense cluster of 101 observations exceeds 5,000 materialized relationships | Preserve small v1 envelopes; compact v2 memberships for large automatic projections; preserve explicit relationships and lineage | Dense 100/101/1,000; dispersed and duplicate inputs; conflicts, unrelated overrides, permutations; oversized 1,001 | All allowed observations/provenance and confidence semantics retained; deterministic equality; explicit oversize rejection; no quadratic UI expansion |
| Live/results use different source counters; Streaming label was static | Last-target counters and replay can misrepresent whole-job coverage | Persist per-stage/unit totals and truthful terminal/partial states; keep findings, candidates and registration counts distinct | Real Chromium with PostgreSQL, actual submission, SSE replay/reconnect, refresh, results and review forms | Database/API/DOM agree; no replay inflation; null stays unknown; zero terminal Streaming labels |
| Approved graph uses shared-attribute hubs; edge source lists are bounded | More than ten citations can be hidden by a bounded projection | Preserve graph library/model and bounded edges; link to complete claim provenance | At least 12 permitted citations; pending-to-approved browser review; duplicate ingestion and deterministic graph fixtures | 100% expected memberships and provenance reachable; pending visible but noncanonical; zero review-boundary violations |
| Existing adapters have different transport, retention and consent policies | A generic adapter abstraction could erase restrictions or count copied evidence twice | Document and test source-specific contract, preserving transient Google Places details and same-origin evidence semantics | Existing adapter/persistence regressions and source-family checklist | Zero prohibited retained details or unauthorized exposure; no automatic approval; no false independence/confidence inflation |

Supported-finding precision >=95%, first Full Scan recovery >=90%, cross-case
shared-attribute precision >=99% and conditional recall >=95% are requirements,
not measured production results. Report end-to-end cross-case recovery separately,
including ingestion losses. Shared attributes do not establish personal relationships.

Future passive collection, Tor-only retrieval, monitoring and project isolation
remain backlog work. The [source contract](p3r-source-contract.md) lists their
required gates. P3R adds no source, service, multi-worker collection or automatic
source replay, and does not begin P4. Admin/analyst roles are not full project
isolation. No new observability, object-storage, queue or graph service is required.

## Execution and release evidence

The integration branch is `codex/p3r-reliability-foundation`. Isolated worker
branches cover executor, orchestration, correlation, presentation, acceptance
and native-query checkpoints. The coordinator alone edits application/store/
worker integration, central policy and CI. Independent review owns no code.
Integration order is contracts and executor/correlation, then application/store
wiring and presentation, followed by PostgreSQL/browser gates and final review.

Focused passing tests are component evidence only. Final readiness requires
applicable CI and fresh independent review on the exact published PR head/tree,
including an enabled browser gate and real PostgreSQL tests. Skipped mandatory
gates leave the PR unaccepted. This document is updated with final evidence before
the PR is marked ready for the user's merge.
