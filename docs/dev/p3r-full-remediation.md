# P3R full remediation acceptance register

This follow-up starts from merged PR #53 (`d59c24988691b63727cab51aaa12123c8c93240d`).
It is an implementation checkpoint, not a release or incident-closure claim.
The user retains squash-and-merge, deployment, and production migration control.

| Defect | Repair and required evidence | Current gate |
|---|---|---|
| R1 Executor saturation | Preserve occupied capacity until bounded cancellation cleanup finishes; cooperative workers resume queued checks. Resistant workers cannot free a live slot. | Focused executor/task tests pass; full offline CI pending. |
| R2 Budgets and stop causes | Distinguish operator, job/stage deadline, cleanup, lease and worker shutdown. Reserve per-alias time and source opportunities. | Orchestration fixtures and six explicit 1/4/16 alias/stop tests pass; full pipeline CI pending. |
| R3 Worker lifecycle | Existing leased worker supervises one spawned collection process; kill/reap precedes recovery. Preserve checkpoints without replay. Cancellation records actor, origin, time and cause; queued cancellation writes one terminal event atomically. | Local process/store tests pass; PostgreSQL resistant/crash/stop fixtures pending. |
| R4 Report work | One context per subject, one combined graph; checkpoint projection caches unchanged records and invokes zero artifact writers. Filename collisions are disambiguated. | 20 subjects × 25 records work-count fixture passes. |
| R5 Optional artifacts | PDF resource callback uses a bundled placeholder. A bounded child renders optional reports; conversion failure retains evidence and unavailable descriptors. | Report/export and failure-retention fixtures pass; real worker export gate pending. |
| R6 Artifact access | Only a terminal job's declared CSV/JSON/PDF/HTML/graph is downloadable. Staging, metadata, undeclared files and symlink escapes are rejected. | Focused download tests pass; PostgreSQL publication races pending. |
| R7 Runtime presentation | Persist collection/stopping/finalizing/terminal lifecycle, typed stop reason and cleanup state. Source notices do not change the whole-job lifecycle. | Local API/DOM fixtures pass; real worker → PostgreSQL → SSE → Chromium gate pending. |
| R8 Maps | Initialize hidden persona maps only at nonzero visible size; preserve later viewport, group duplicate points, reject invalid coordinates, fit the shortest longitude span. | JavaScript boundary tests pass; actual Leaflet 0/1/2/10/100, mobile and dateline gate pending. |
| R9 Affiliation sites | Review saved public organizational addresses separately from person locations. Require exact address/organization basis; preserve headquarters/registered/operating/branch/campus/mailing/area meaning. Never default to a city centroid. | Resolver/route fixtures pass; live provider coverage unmeasured. Area text remains unmapped without a configured validated land dataset. |
| R10 Coordinate history | Additive site/selection tables; append-only actor/time/reason/source snapshots, lookup history and optimistic revision guards. Legacy/AI coordinates cannot silently become reviewed points. Revoked affiliation origins stop map projection. | SQLite review/rerun/clear/reject fixtures pass; PostgreSQL migration/review gates pending. |
| R11 Export coverage | Include all approved location facts and all approved affiliation sites, including unmapped text, with sources and review provenance. A single image is explicitly a map excerpt. | Persona/report fixtures pass; browser/download gate pending. |
| R12 Existing invariants | Preserve exact-match citation access, cross-case/review exclusions, compact clustering, immutable source task audit and transient provider restrictions. | Full existing offline and PostgreSQL regression gates pending. |

## Validation record

Local targeted runs include 94 executor/orchestration/task/worker tests, 47
Persona/report/location tests, 156 download/web tests, and 166 passing
store/worker/route tests in a run that also exposed one new test fixture's missing
timestamp argument. That fixture was corrected. These overlapping counts are
not a single suite total and do not substitute for the gates below.

A subsequent store/worker/fairness/route run passed 183 tests after the review fixes.
The corrected process cleanup checks verify process disappearance with
`kill(pid, 0)`, including a converter descendant in a separate process group.
The supervisor also handles procfs mounted from an outer PID namespace.
Independent review found and prompted repairs for operator-cause propagation,
cancellation during artifact publication, public-address controls, revoked
affiliation origins and preservation of legacy coordinate provenance.

The full SQLite migration chain could not run because an earlier migration
adds foreign keys using a PostgreSQL-specific operation. This is not evidence
of a successful migration; the required PostgreSQL migration gate remains pending.

The first PR CI run passed PostgreSQL migrations/check, 24 PostgreSQL integrity
tests and two existing Chromium journeys. Subsequent runs exposed and verified
repairs for fixture registration and flattened-result contracts, compact JSON
export metadata, premature notifier closure during cancellation, per-stage
timeout labeling, and retained-observation accounting during cooperative stop.
The corrected offline suite passed on Python 3.10–3.14 and in the minimal-install
job. The latest isolated worker/browser gate passed all five real-worker journeys
and two of three browser/map journeys, then found that its resize assertion
sampled a popup auto-pan before it settled. The corrected journey establishes an
analyst viewport before resizing and inspects actual rendered marker coordinates;
the map now places antimeridian markers in the same world copy as their fitted
bounds. A fresh CI run is required; prior passing substeps are not final-tree
acceptance.

The mandatory full acceptance command is
`.github/scripts/run_p3r_full_acceptance.sh <absolute-python-path>`.
It starts a disposable PostgreSQL instance inside a loopback-only network
namespace and runs real worker, SSE, report and Chromium journeys. Spawn-safe
fixture adapters are supplied only through an explicit Python argument under
TESTING; production HTTP/configuration cannot select them. Pinned Leaflet assets
are fetched and hash-verified before entering the namespace. No public collector
or geocoder is exercised by these acceptance fixtures.

Required before PR-ready: green offline Python matrix, PostgreSQL migrations and
integrity tests, full isolated worker/browser/map journeys, container build and
fresh independent review of the final committed tree. Skips are not passes.

## Data and provider boundaries

Migration `c4f8a2d6e901` adds tables without backfilling or changing existing
approvals, evidence or coordinates. A code rollback retains those tables;
destructive schema downgrade deliberately refuses to discard human review
history. Production application is a separate user-controlled step.

Address lookup is explicit and restricted to saved public institutional evidence.
Public Nominatim requests share a cache/rate lock on the existing application
reports volume, with one request per second across web/worker processes. This
assumes the current single-host shared volume; it does not claim a distributed
multi-host limiter. See the [Nominatim policy](https://operations.osmfoundation.org/policies/nominatim/).
Personal/confidential inputs and Google Places endpoints are rejected by this
durable lookup path. Existing Google transient behavior remains separate.

No land dataset is bundled or represented as building-level validation. Area
fallback requires declared geometry, a validated land dataset/version/resolution
and actual containment; absent these inputs, the record stays unmapped. The pure
policy includes coastline, holes, island, antimeridian and ambiguous/no-match
fixtures. This makes no universal real-world land guarantee.

The historical incident remains open: saved-run diagnostics have not been
supplied for reconciliation, and no new production scan or replay is authorized
by this checkpoint. Production coverage, precision/recall, and later human
deployment acceptance remain unmeasured.
