# P2 complete pipeline — implementation state

The later comprehensive reliability review and remediation supersede the readiness interpretation of the initial test counts below. See `p2-reliability-state.md` for corrected semantics, connector contracts, and remaining validation boundaries.

The user approved the complete plan in `p2-pipeline-approved-plan.md` on 11 September 2026 and explicitly requested multi-agent implementation. The release must run the new pipeline after the supported Docker update from GitHub. No merge or production deployment is authorized.

Base: PR #55 head `f004e719d6bd23e79850379fba67956355736ce2`, tree `f84724316c3dea6d91a05dd64b6cf20cca45ad95`.

Required runtime identity: `p2-e2e-v1`. Required additive schema: `e2e3c9d1f703`, descending through `e2e2b8d0a502` and `e2e1a7c9d401` from P2 `b3e9d7c4a610`.

The later user instruction supersedes the plan's temporary default-off release toggle: the released app and worker must require this pipeline and schema, with no automatic old-pipeline fallback. Source selection flags still control individual providers.

Parallel work: persistence/QC, routing, consolidation, assessment, interface, release safety. Coordinator owns execution, integration and full acceptance. New code is written on the reviewed P2 base; P3 is not revived.

Empirical probability readiness requires real independently labelled data. A working evaluation harness and synthetic fixtures do not count as measured production calibration.

## Implemented target and review record

The complete diagram is implemented as one mandatory P2 workflow. The primary
query request and engine compatibility decisions commit with the case/job;
workers use bounded process groups and leased attempts; all permitted source
observations feed account and qualified-claim groups. Account attribution and
claim correctness assessments are separate. Operators can review evidence
without a probability model, record inclusion/exclusion/corrections, and submit
an immutable curated version. Explicit QC alone publishes a final version.
Rejection creates structured research requirements; the launch action queues
only their typed targets in the same case and Persona. Existing evidence,
decisions, failed versions and request ancestry remain retained.

Existing combined-case snapshot and synthesis operations also use the new
attempt service. They retain exact source QC-final versions and preserve their
own subject attribution. Working historical views and collector helpers remain
available for evidence inspection and regression testing; normal launches and
Persona navigation use the new workflow. They cannot create a Final Persona.

The released runtime has no old-pipeline activation switch. App, worker and
migration must share the manifest's immutable image, exact source fingerprint,
`p2-e2e-v1` contract and `e2e3c9d1f703` schema. The supported updater refuses a
floating image, stale source, mismatched commit/tree, unknown migration, or
incompatible app/worker identity. It stops on failed validation instead of
silently starting the earlier pipeline. Preparation/build is separate from
merge/deployment authorization; see `deploy/README.md`.

### Explicit refinements and operational limits

- The later user request removes the originally proposed temporary default-off
  cutover. Individual provider permissions and availability still apply.
- Ordinary deletion is blocked for any case with pipeline research history,
  including pre-final cases. This is stricter than protecting only final-linked
  evidence. An approved version may be withdrawn without erasing its lineage.
- A changed active source configuration stops that saved route with an auditable
  outcome; a newly enabled route requires a new reviewed request. Old saved
  evidence and excluded/conditional source decisions remain inspectable.
- No production probability model or human-labelled validation set was supplied.
  Numeric output stays unavailable with a reason. The entire evidence/operator/
  QC path is operational. Synthetic model tests establish serving and gating
  correctness, not empirical accuracy. See `p2-probability-assessment.md`.
- Source-retention exceptions, stable uncurated Persona shells, explicit QC
  permission and optional same-human QC remain as disclosed in the approved plan.
  No diagram stage or research-feedback arrow was removed.

### Acceptance evidence and outstanding release gates

Real application HTTP tests execute all four input types and the complete
reject → targeted worker research → resolve → successor → approve loop using
real store, query, consolidation, operator and QC code; only external collector
responses are synthetic. Final HTML/API/graph/PDF assert the same version/hash
and retained evidence. A separate Poppler check verifies all 137 included fact
and observation identifiers appear in a dense export. Linux process tests kill
stubborn descendants, including on worker death. The ledger integrity fixture
reconciles 50,000 returns into 45,000 unique records and 5,000 duplicate deliveries
across two cases, with ten blocked source tasks and no lost committed records.

Independent integration review added explicit regression coverage for approval
with new open requirements, material QC findings, corrections erasing conflict
constraints, stale/bound-away research leads, structured mandatory objectives,
and queued search-provider changes.

Acceptance mappings:

| Approved tests | Implementation/test evidence |
| --- | --- |
| A01–A06 | `test_pipeline_query`, `test_pipeline_job_context`, `test_pipeline_execution`, `test_pipeline_app_journey`, input/web regressions |
| A07–A11 | `test_pipeline_consolidation`, `test_pipeline_store`, `test_pipeline_ingestion`, deterministic 50k ledger reconciliation |
| A12–A13 | `test_pipeline_assessment`, `test_pipeline_probability`, `test_pipeline_assessment_runtime`, real operator/QC journey |
| A14 | Evaluation/serving harness complete; human reference data and empirical calibration remain unmeasured, so production numbers are disabled |
| A15–A23 | Store/route/app-journey tests, immutable projection, scoped evidence reuse, dense PDF extraction |
| A24 | Execution/process/worker tests; PostgreSQL concurrency and restart coverage remains a mandatory CI gate |
| A25–A27 | Historical import/store tests, fresh/legacy migration rehearsal in CI, immutable release/updater refusal tests |
| A28 | Actual PostgreSQL migration/container startup and browser acceptance remain pending until their environments pass |

This workspace has no PostgreSQL server, Docker runtime or Chromium. Chromium
installation failed with download timeouts/502. The repository's broad offline
runner could not start because this environment cannot perform its required
sudo/UID operations. An earlier broad test attempted an external Trello contact
and automatic approval review blocked that run; it was not repeated unisolated.
Known-mocked web regressions now explicitly deny network transports and child
process execution. CI runs every regression inside the unchanged private-network
runner, exposes its disposable PostgreSQL via a Unix socket, rejects skips in
the required engineering suite, and starts real containers against correct and
incorrect schemas. A candidate is not approved for deployment until these gates
pass. No merge, production deployment, or P3 restart is authorized by this record.

Local verification on the assembled implementation (11 September 2026):
**573 passed, 37 environment/explicit-load skips** in the focused pipeline,
store, guarded web, worker, policy, UX and deployment suite. The separately
enabled ledger load passed: **50,000 returns / 45,000 unique / 5,000 duplicate
replays / 100 tasks / 10 blocked / 0 lost / 0 cross-case errors**, 21.71 seconds
on local SQLite. These counts are engineering checks, not PostgreSQL production
latency or empirical source/model accuracy. Final counter/projection adjustments
were rechecked with the real application and execution acceptance tests.

### Additional multi-agent delivery audit

The follow-up audit closed three concrete delivery risks: updater interruption
or explicit failure after starting containers now runs fail-closed cleanup;
baked build metadata cannot opt into development mode and bypass schema/source
validation; and a worker stops collection if runtime attestation fails. Invalid
heartbeat process IDs are rejected, and failed initial readiness releases its
worker lock.

The inherited DockerHub workflow no longer publishes moving `web`/`latest` tags
on a branch push. It is an explicit candidate-build workflow using the exact
GitHub event commit and the reviewed build script. Its runner-local manifest is
build evidence, not an installable release or deployment authorization. Release
host image preparation and the supported manifest-pinned updater remain required.

Independent store/routes/application workflow recheck: 39 passed, 22 PostgreSQL
variants skipped because no test database was available. No complete production
readiness claim follows from these local results. Shell cleanup handles normal
failures, SIGINT and SIGTERM; SIGKILL or host loss requires recovery and identity
verification before reopening ingress. Arbitrary old Docker commands remain
outside the supported updater's protection. No merge or deployment occurred.


### Reliability review continuation (pending integrated acceptance)

The critical review identified incomplete adapter execution/normalization
contracts, process-local provider cooldowns, unmetered attempts, missing durable
connector checkpoints/receipts, and expensive write-on-read projections. The
user authorized parallel remediation of those findings. The schema advances
additively to `e2e3c9d1f703`; the original `e2e1a7c9d401` definition is frozen.
Both earlier schema versions remain accepted sources for the reviewed updater,
and both are refused by the new app/worker until migration completes.

This continuation adds mandatory CI discovery of pipeline/connector test
modules, named failure/conformance gates, real Chromium review/QC interaction,
and a disposable PostgreSQL dump/restore with evidence, receipts, checkpoints
and Persona-version reconciliation. Tests that require absent local tools stay
explicitly unverified until CI executes them. These additions do not imply that
new browser or PostgreSQL/Docker acceptance has already passed. Merge and
production deployment remain separately authorized actions.
