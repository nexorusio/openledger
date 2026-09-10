<!--
SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
-->

# P3R source-adapter contract

This document records the contract that P3R preserves for existing collection
adapters and the entry gate for later adapter families. It does not activate a
source, add a transport, change a dependency, or authorize collection. A future
implementation needs its own approved change, source-specific contract, and
acceptance evidence before it can run.

## Existing contract boundary

Existing profile-search and evidence-correlation modules establish these
versioned, adapter-neutral seams:

| Seam | Current versioned contract | Preserved meaning |
|---|---|---|
| Query | `ProfileSearchQuery` version 1 records a stable query ID and fingerprint, supported platform, approved seed kind and value, and a bounded result count. | The server derives and bounds a query. A caller does not turn an arbitrary URL or identifier into a new collection route. |
| Run | A profile-search run binds that query to provider provenance, bounded result evidence, or one bounded error diagnostic. | A run is attributable to one provider request. A failed request is a recorded outcome, not an empty successful response. |
| Candidate | A candidate records a supported public profile URL, canonical handle, result evidence, and query/provider lineage. Its contract state is candidate, unverified, and pending review. | A candidate is a lead. It is never an identity determination or an approval. |
| Observation | Evidence-correlation observation version 1 carries case-scoped stable observation and cluster IDs, source identity/version/record ID, native outcome/status, citations, retrieval time, query fingerprint, and immutable snapshot digest and locator. | The same source record can be deduplicated while genuinely distinct retrieval contexts remain attributable. |
| Relationship and confidence | Relationship version 1 distinguishes supporting, duplicate, conflicting, and unrelated records. Correlation keeps explicit relationships and avoids counting duplicate snapshots as independent support. | Correlation has no review authority. Contradictions remain visible, and confidence is not an approval or a source of new evidence. |

The existing profile-search contract accepts only public HTTPS result and profile
URLs, validates bounded text and timestamps, and limits one query to at most ten
results. Current collection adapters own their individual fixed-origin,
timeout, response-size, result, and redirect limits; there is no universal
network policy that may replace those source-specific controls.

## Required adapter record

Before a future adapter can be enabled, its implementation and source-specific
documentation must define an `adapter_contract_version` and all fields below.
The record may be stored in a future configuration or code contract, but it must
be validated by the adapter and captured with the execution that used it.

| Field group | Required declaration |
|---|---|
| Identity | Stable adapter ID, adapter contract version, source name and version or immutable release reference, source-record identity rule, and the claim/evidence types it can emit. |
| Authority and scope | Eligible case/project and Persona scope, permitted input types, purpose/consent or other authorization requirement, explicit feature gate, and the conditions that refuse work. |
| Transport | Approved origin or isolated transport boundary, DNS and redirect policy, authentication handling, egress restrictions, user-agent policy where applicable, and whether the adapter can follow any source-supplied URL. |
| Limits | Maximum requests, concurrency, retries, total execution time, per-request time, response bytes, parsed records, retained records, and persistent output bytes. Retries and failures consume declared budgets. |
| Native outcomes | Exact mapping for observed, authoritative absence, private, blocked, rate-limited, parser error, provider error, indeterminate, cancellation, interruption, and unattempted work. An error or timeout cannot become absence. |
| Normalization | Canonicalization rules; deterministic IDs; provenance, citation, retrieval-time, query/target, source-snapshot, and source-record fields; duplicate-origin behavior; and any claim proposal mapping. |
| Retention and review | Which raw values, immutable locators, derived fields, and diagnostics may be retained; prohibited fields; redaction; expiry/deletion rules where applicable; and the mandatory pending-review path. |
| Lifecycle | Admission, cancellation, cleanup, retry/replay, partial-result, idempotency, and finalization behavior. Existing durable worker claiming and one collector worker remain the baseline unless separately changed. |
| Verification | Offline fixtures for the contract, source-specific boundary tests, cancellation and failure tests, retention/consent assertions, and a documented rollback/kill-switch path. |

An adapter contract must name the originating evidence. Wrapping the same page,
snapshot, or provider record in another engine does not create independent
support. An explicit duplicate or conflicting relationship remains authoritative
for correlation and review even when a future adapter emits a different native
status.

## Retention, consent, and review invariants

- Collection starts only from inputs permitted by the adapter's declared
  authorization and scope. A previous claim approval does not by itself grant a
  broader source query.
- Secrets, credentials, session material, and credential-shaped fields do not
  enter plans, observations, citations, logs, browser payloads, or provenance.
- Each retained observation identifies its actual source, record, retrieval
  context, and immutable snapshot or locator where the source permits one.
  Source-specific retention can be narrower than this contract, never broader.
- A successful source response can support an `absent` outcome only when that
  source contract makes the response authoritative for the exact bounded query.
  Transport, parser, provider, rate-limit, and cancellation outcomes remain
  distinct.
- Adapter output may produce evidence or a pending proposal only. It cannot
  approve a claim, change a reviewer, merge identities, or overwrite review
  history.
- Repeated delivery preserves a single logical observation and all unique
  retrieval contexts. It cannot inflate evidence totals or confidence.

## Future family gates

The following are requirements for later authorized work. They are not claims
that an adapter exists, is safe to run, or has passed a test.

| Family | Required contract additions | Required acceptance evidence before enablement |
|---|---|---|
| P13 passive collection | Declare an isolated, pinned runtime; an explicit passive-only source allowlist; approved organization/domain inputs; argument, egress, stdout/stderr, byte, result, and process-time limits; and terminate/kill cleanup. Active probing, broad target expansion, and an unrestricted subprocess are prohibited. | Fixtures prove passive arguments only, source/egress allowlist enforcement, bounded output parsing, cancellation cleanup, native failure accounting, and no retained secrets or prohibited result classes. Source license and maintenance status are reviewed separately. |
| P15 public onion retrieval | Declare a Tor-only isolated transport for public version-3 addresses, with no clearnet fallback and no direct DNS path. Limit method, redirects, content type, response bytes, time, and retained result classes. Retrieved scripts and documents are data, never executable input. | Disposable transport tests prove Tor-only routing, rejected clearnet and non-v3 addresses, no direct-DNS fallback, bounded redirect/content handling, cancellation cleanup, and source-specific retention/review labels. |
| P16 recurring monitoring | Declare an authorized cadence, case/project admission rule, stable monitored-target/check identity, overlap prevention, per-target and total budgets, cursor/checkpoint semantics, retry/backoff, change/delta rules, revocation, and stop behavior. | Repeated scheduled and replay fixtures prove no concurrent duplicate work, durable checkpoint behavior, explicit missed/failed/unknown states, cancellation and revocation, retained provenance for deltas, and no duplicated claims or confidence inflation. |
| P8–P11 project-scoped ingestion | Declare project membership and authorization checks for planning, worker claim, source input, persistence, API, UI, reports, and every derived graph or export view. Define revocation behavior for queued, active, and retained work. | Boundary tests prove no cross-project read, write, event, citation, report, graph, or cache exposure; revocation blocks later writes and is visible during active work; and recovery/replay remains scoped. |

## Compatibility and change control

Existing saved observations, relationships, audits, review records, and compact
correlation envelopes remain readable. A new adapter may add a versioned record;
it must not rewrite historical evidence to claim a newer source version,
authorization, outcome, or confidence basis.

A change that needs a new endpoint, credential, service, queue, runtime,
database model, broad retention rule, or cross-project access is outside this
contract alone. It requires a concrete approved design and source-specific test
plan. Passing synthetic contract fixtures does not establish real-world source
precision, recall, availability, or legal suitability.
