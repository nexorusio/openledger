# Maigret, User Scanner, and OGI integration decision

## Decision

OpenLedger remains the investigation orchestrator and the only system of record.
Maigret remains the primary username collector. User Scanner is introduced only
for explicitly enabled, silent email-registration checks. OpenGraph Intel (OGI)
does not replace OpenLedger's evidence or relationship model; it may be added
later as a disposable, read-only visualization projection.

This preserves the current journey: start one investigation, watch one job,
review pending Persona evidence, approve or reject claims, and analyze the same
case. It does not add a second login, project picker, database, or review queue.

## Canonical ownership

| Responsibility | Owner |
|---|---|
| Case, Persona, and investigation lifecycle | OpenLedger PostgreSQL |
| Username collection | Maigret adapter |
| Opt-in email-registration collection | User Scanner subprocess adapter |
| Native collector result artifact | `investigation_jobs.result` |
| Normalized claims and source provenance | `persona_claims`, `claim_evidence`, and `claim_observations` |
| Analyst decisions and audit history | `claim_reviews` |
| AI evidence input | OpenLedger projection of the same job evidence |
| Persona and shared-attribute graphs | OpenLedger read-only projections |
| Future OGI graph | Rebuildable projection; never canonical |

Collector output must not be written independently to an OGI project or a
separate User Scanner store. Every source reaches analysis through the same job,
claim, evidence, and observation-lineage records.

## Why User Scanner is additive only

User Scanner adds useful email registration coverage and richer native metadata.
Its username sweep overlaps Maigret, while recursive cross-scan can turn a common
handle or user-authored link into many weak candidates. OpenLedger already has a
bounded full-name variant planner and reviewable profile evidence, so replacing
those paths would add cost and change confidence semantics without a clear user
benefit.

The initial integration therefore:

- is off by default and available only in **One subject** mode;
- requires an email identifier and explicit enablement;
- accepts one email per investigation so collection cost stays bounded;
- disables notification-producing and adult modules;
- runs User Scanner in a subprocess because its email orchestrator patches
  `httpx.AsyncClient` and `httpx.Client` globally when imported;
- retains bounded positive, negative, skipped, and error observations on the
  OpenLedger job result;
- creates pending `account_registration` claims only for positive registrations;
- assigns confidence 55 because the probe supports registration, not Persona
  ownership;
- sends the email value to AI only when the separate AI-context consent is on;
- leaves User Scanner cross-scan, arbitrary patterns, Hudson Rock, and its MCP
  server out of the OpenLedger runtime.

## Why OGI is not a safe replacement today

OGI is a complete application, not a graph component. It introduces a FastAPI
backend requiring Python 3.14, a React frontend, its own project/auth model,
asynchronous workers, Redis/RQ, and its own entity/edge persistence. Running it
as OpenLedger's relationship system would create a second operational stack and
a second place where investigators can edit identity data.

Its schema also does not preserve OpenLedger's evidence contract:

| OpenLedger invariant | OGI behavior relevant to replacement |
|---|---|
| Claim and evidence are separate, fingerprinted records | Entity properties are generic JSON |
| Every claim has pending/approved/rejected/uncertain state | No equivalent entity review state or review audit |
| Every evidence item has source, type, details, and observed time | Edges expose a transform name but no evidence record |
| Analyst decisions survive refresh | Entity upsert merges properties and updates source values |
| Database uniqueness protects claim/evidence fingerprints | Entity deduplication is application-level type/value lookup |
| Import failures must be explicit | OGI import catches row exceptions and reports aggregate skips |
| Edge provenance must survive import | The JSON importer does not map incoming edge properties |

OGI is AGPL-3.0-or-later while OpenLedger and User Scanner are MIT-licensed.
Keeping OGI as a separately operated service with a documented projection API
avoids copying AGPL code into OpenLedger, but operators must still comply with
OGI's network-use source obligations when they deploy or modify OGI.

## Safe OGI introduction criteria

Do not deploy OGI in the default OpenLedger journey until all of these are true:

1. OpenLedger exposes a versioned, read-only graph projection containing stable
   case, Persona, claim, evidence, and review identifiers.
2. The OGI adapter imports edge properties and reports every rejected/skipped
   record with a reason; aggregate silent skips are not acceptable.
3. OGI projects created from OpenLedger are visibly read-only or changes are
   discarded on rebuild.
4. Projection access uses a case-scoped, read-only service credential.
5. Deleting OGI data and rebuilding from OpenLedger produces an equivalent
   graph; this is tested automatically.
6. Relationship labels continue to mean shared approved attributes, not proven
   personal relationships.
7. Load tests demonstrate that PostgreSQL projections are the actual bottleneck
   before a separate graph persistence layer is considered.

The first OGI slice should be an export/import contract test, not a replacement
UI. OpenLedger's existing Relationships route remains the fallback throughout.

## Runtime orchestration

There is intentionally no function that treats all three projects as equivalent
collection engines.

The current durable lifecycle is:

1. `CaseStore.create_investigation` creates the case, Persona, and queued job.
2. `maigret.web.worker.run` claims one durable job.
3. `run_persistent_job` owns the event loop, cancellation, and terminal state.
4. `_stream_search` orchestrates collector adapters: `maigret_search` for each
   username, then the opt-in `run_user_scanner_email` adapter.
5. `finalize_stream_job` builds one result artifact and persists it.
6. `CaseStore.sync_persona_claims` normalizes Maigret and User Scanner evidence
   into the same canonical tables and records every source-labelled observation
   against its investigation job without changing prior analyst decisions.
7. `CaseStore.build_persona_graph` and `CaseStore.build_relationship_graph`
   create read-only evidence and relationship projections.

If OGI is later enabled, it belongs after step 7 as a projection consumer. It
must not be called from `_stream_search`, because visualization is not evidence
collection.

## Fork synchronization policy

The User Scanner fork tracks `kaifcodec/user-scanner:main`; the OGI fork tracks
`khashashin/ogi:main`. Each fork has a scheduled/manual workflow that:

1. fetches the original `main` branch;
2. merges it with a merge commit on an automation branch;
3. opens or updates a pull request;
4. runs the repository's native backend and/or frontend gates against the exact
   merge candidate; and
5. always leaves the pull request open for human review.

Upstream sync is never deployed automatically. OpenLedger pins User Scanner to
an immutable reviewed commit archive, so a second OpenLedger pull request must
update that pin and pass regression and container-build checks. Squash or rebase
must not be used for upstream synchronization because it discards ancestry and
causes later syncs to replay old commits.

## Revisit triggers

- Add selected User Scanner email categories only after false-positive and
  notification telemetry is measured per module.
- Consider verified-link-only cross-pivots only after OpenLedger has an explicit
  pivot observation type, budgets, and analyst confirmation before the next hop.
- Add an OGI projection only after its importer preserves edge provenance and
  exposes record-level failures.
- Keep Maigret as username fallback until a measured, labeled comparison shows
  User Scanner improves precision or metadata coverage without regression.
