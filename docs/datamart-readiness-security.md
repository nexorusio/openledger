# Datamart readiness and security design

## Decision and boundary

OpenLedger should consume authorized, case-relevant evidence from a client-owned
datamart. It should not become the warehouse for millions of raw structured and
unstructured records. The client pipeline remains responsible for acquisition,
ETL, retention, classification, and enterprise-wide access control. OpenLedger
stores only the bounded result selected for an investigation, its immutable
source locator and version, the declared query purpose, and the claims an
analyst may review.

This change establishes that contract and its database foundation. It does not
enable an ingestion HTTP route, direct SQL connection, MCP server, background
datamart query, probabilistic entity merge, or graph database. Those remain
disabled until authentication, authorization, audit, connector isolation, and
load behavior can be tested against a real client contract.

## Ownership and data flow

| Layer | Owns | Must not own |
|---|---|---|
| Client lake/datamart | Raw records, ETL, classification, retention, source ACLs | OpenLedger analyst decisions |
| Connector process | Authorized query execution and envelope translation | Browser sessions, Persona writes, unrestricted credentials in results |
| OpenLedger PostgreSQL | Cases, source registry, query receipts, attached evidence, claims, observation lineage, reviews | Bulk copies of the client corpus |
| Maigret | Public username checks and native run artifacts | Canonical identity or reviewed relationships |
| User Scanner adapter | Additional public-account observations after its own safety gate | A parallel Persona or evidence store |
| OpenAI enrichment | Cited proposals and assessments | Approval, canonical identity, or hidden source records |
| Relationship UI | Bounded read-only projections of reviewed data | A second graph system of record |

The intended flow is:

1. An authenticated analyst opens a case and declares a specific investigative
   purpose.
2. OpenLedger creates a `query_receipt` tied to the case, principal, registered
   source, policy context, and a fingerprint of the bounded query.
3. A future isolated connector executes only that authorized query.
4. Results cross the boundary through the versioned external-evidence envelope.
   OpenLedger rejects unknown schema versions, oversized documents, credentials,
   unsafe locators, non-finite numbers, invalid timestamps, and authority
   mismatches.
5. The connector attaches selected immutable record versions to the case. A
  stable locator points back to the authoritative record; OpenLedger retains
  only bounded attributes and a redacted preview. OpenLedger does not
  dereference that locator during attachment.
6. A source-specific normalizer may propose typed claims. Every proposal remains
   source-labeled and pending until analyst review.
7. Maps, Persona pages, AI, and relationship graphs consume the shared claim and
   evidence model instead of reading engine-specific silos.

## Database contract

The foundation adds five tables without changing existing Persona or review
semantics.

| Table | Purpose | Security property |
|---|---|---|
| `data_sources` | Stable identity, schema version, authority, classification default, enable switch | Contains no credentials; registration identity is immutable |
| `query_receipts` | Case, principal, purpose, query fingerprint/document, policy context, result status/count | Makes access intent auditable before evidence is attached |
| `external_evidence_records` | Immutable source record/version, content hash, safe locator, handling, bounded preview/attributes | Always case-scoped and tied to a completed receipt |
| `external_evidence_receipts` | Many-to-many audit links between immutable evidence and every receipt that returned it | Retains repeated discovery without duplicating or overwriting the evidence record |
| `claim_observations` | Append-only link from a claim to every investigation job or external record that observed it | Prevents the current `source_job_id` pointer from erasing earlier provenance |

`persona_claims` remains the normalized review unit. `claim_evidence` remains the
human-readable support shown with a claim. `claim_observations` is the complete
machine lineage: Maigret, User Scanner, AI enrichment, and a future datamart
normalizer write observations through the same mechanism. The provenance type,
stable provenance identifier, engine, source record, confidence, native status,
and bounded details remain distinct. Matching values can therefore be analyzed
together without laundering one source into another.

Foreign keys to a job or external record use `SET NULL` while retaining the
stable `provenance_id`. This preserves a minimal lineage receipt when an
authorized retention workflow removes a larger artifact. Case deletion still
cascades through case-owned data.

## External evidence envelope v1

The machine-readable contract is
[`schemas/external-evidence-envelope.v1.schema.json`](../schemas/external-evidence-envelope.v1.schema.json).
Required fields are:

- schema version, registered source ID, source record ID, and immutable source
  version;
- record type, SHA-256 content hash, and timezone-qualified observation time;
- classification and source authority;
- one stable `datamart:`, `evidence:`, `urn:`, or HTTPS locator with no embedded
  credentials, query string, or fragment;
- optional bounded, redacted preview and attributes.

The Python validator is intentionally stricter than generic JSON parsing. It
caps total document bytes, string size, collection size, and nesting depth;
rejects control characters and credential-shaped keys at any depth; and accepts
only explicitly supported top-level fields. A new schema requires a new
validator and migration decision rather than silent forward compatibility.

## Engine orchestration and Maigret's extent

The OpenLedger job service and `CaseStore` are the orchestration boundary. Each
collector keeps its native execution logic, but it must return bounded
observations to the shared normalization and lineage path.

Maigret remains the default engine for exact public username discovery because
it is mature, broad, and already integrated with OpenLedger's job lifecycle. It
is suitable for finding public accounts, extracting public profile attributes,
and producing initial leads. It is not an identity-resolution authority, a
client-data ETL system, a communications/transaction collector, or proof that
same usernames belong to the same person.

User Scanner should be enabled feature-by-feature only where it adds a measured
source or workflow advantage. If its output duplicates or conflicts with
Maigret, keep Maigret as the active implementation and retain User Scanner
behind an adapter/feature flag. It must never write its own Persona, evidence,
or relationship store.

Open Graph Intel is best used as interaction and bounded graph-presentation
inspiration. The canonical data should remain in PostgreSQL. An OGI-style
workspace can request a case root, limited depth, relationship type filters,
review status, and time window, then render the returned projection. It should
not import OGI's storage model wholesale or load every approved claim into
browser memory.

## Relationship and knowledge-graph evolution

The current relationship view is a safe exact-value projection over approved
claims. It should evolve in two steps:

1. Add explicit entity and relationship observation contracts only for source
   types with defensible semantics, such as communication, transaction,
   ownership, mention, or co-location. Preserve direction, time interval,
   source record, classification, confidence, and review state.
2. Replace whole-dataset graph assembly with bounded server-side expansion:
   case scope, root node, depth, node/edge caps, filters, deterministic cursor,
   and aggregate-only fallbacks for high-degree nodes.

PostgreSQL remains canonical. A dedicated graph index may be introduced later
only as a rebuildable projection when measured traversal latency justifies it.
It must not contain relationships unavailable in PostgreSQL or bypass case and
handling policies.

## MCP boundary

MCP can later expose narrow read tools over the same authorization service, for
example `get_case_summary`, `search_case_evidence`, or `expand_case_graph`.
Tool inputs must include the case scope implicitly from the authenticated
session, enforce result and time limits, redact by classification, return
citations/record IDs, and write an audit receipt. An MCP client must not receive
database credentials, arbitrary SQL, a bulk export tool, or authority to approve
claims. No MCP surface is added by this change.

## Security hardening in this change

- Forwarded headers are trusted only when `OPENLEDGER_PROXY_HOPS` explicitly
  names the deployed proxy depth; direct/local execution trusts none.
- Production supplies an allowlist through `OPENLEDGER_TRUSTED_HOSTS` to reject
  Host-header attacks.
- Authentication-enabled startup fails if the Flask signing key or secure
  session cookies are missing.
- Request body, in-memory form data, and multipart part counts are bounded.
- Flask adds CSP, anti-framing, MIME-sniffing, referrer, permissions,
  cross-origin isolation, no-index, and HTTPS HSTS headers even if Caddy is
  bypassed. Caddy mirrors the critical edge headers.
- CodeQL runs current action versions with extended security queries on pull
  requests, main-branch pushes, and the weekly schedule.

The initial CSP still permits inline script/style because the existing templates
contain inline behavior. Moving these into versioned static assets and replacing
the external CDN dependencies with locally pinned assets is the next browser
hardening step.

## Safe rollout sequence

1. Merge this schema, lineage, validation, and deployment hardening change.
2. Deploy migrations before application startup using the existing migration
   service; confirm backups and health checks.
3. Roll out User Scanner capabilities independently: keep email-registration
   checks opt-in, bound username verification to selected major-platform
   aliases, and require explicit approval for X's third-party API path.
4. Implement one read-only connector in a separate process with client identity,
   least privilege, secret-file delivery, egress allowlisting, timeouts, row and
   byte limits, cancellation, and audit logs.
5. Pilot on synthetic/declassified data. Measure authorization failures,
   duplicate rates, false merges, query latency, evidence volume, and graph
   expansion limits.
6. Add source-specific claim normalization and analyst binding review. Do not
   auto-merge persons from names, cities, occupations, or usernames alone.
7. Add bounded OGI-style graph presentation, then read-only MCP tools. Consider
   a graph projection database only after production measurements require it.

## Explicitly deferred controls

Before real law-enforcement data is connected, the deployment still needs the
client's identity provider and role/case authorization model, classification
ordering and downgrade rules, record-level redaction policy, audit export and
tamper-evidence requirements, retention/legal-hold workflows, connector network
policy, backup encryption/restore tests, and a documented incident-response
procedure. This PR creates the enforcement points; it does not invent those
client policies.
