# OpenLedger case intelligence architecture

## Decision

PostgreSQL is the canonical record for cases, personas, investigation jobs,
structured claims, evidence, coordinates, and analyst decisions. Collectors,
AI, report files, maps, and graphs are consumers or producers of evidence; none
of them owns the canonical Persona profile.

The governed client-datamart boundary, immutable external-evidence envelope,
complete claim observation lineage, engine roles, MCP constraints, graph-scale
plan, and deployment threat controls are specified in
[Datamart readiness and security design](datamart-readiness-security.md).

The user journey is deliberately explicit:

1. **Collect.** A source adapter emits observations. The current public-profile
   discovery engine is one adapter, not a privileged path.
2. **Normalize.** OpenLedger converts each observation into a typed claim,
   normalizes its value, deduplicates by fingerprint, and attaches provenance.
3. **Review.** New claims are pending. An analyst approves, rejects, or marks a
   claim uncertain. Every decision is appended to the review audit.
4. **Publish inside the workspace.** Approved claims form the default Persona
   view and may appear on maps and in cross-person Relationships. Rejected
   claims stay in PostgreSQL for audit and reversal but are suppressed from the
   default profile, map, and relationship projection.
5. **Analyze and propose.** AI may summarize selected evidence, perform
   public-web research, and submit schema-constrained suggestions backed by URLs
   returned in that same cited research run. Valid suggestions become pending
   claims. AI cannot approve, reject, or make a record canonical.

Re-running an investigation updates the observation time and evidence attached
to an existing fingerprint without overwriting its analyst decision. A newly
observed different value creates a new pending claim.

## Investigation input and query planning

OpenLedger stores a typed investigation specification beside each collection
job. The input type determines which adapter may use it; values must never be
silently coerced into a username:

| Input type | Initial behavior |
|---|---|
| Username or social handle | Exact public-account check |
| Supported profile URL | Parse the account identifier locally, then run an exact check; do not fetch the URL during planning |
| Full name | Preserve as one phrase and, when selected, generate a bounded preview of likely account handles |
| Email or phone | Normalize and retain as subject context; never send to the username collector or permutation logic |
| Source categories and countries | Persist include/exclude selections on this case and apply them only to its account checks |

Every generated handle is still checked independently. “One subject” mode
organizes resulting claims under one Persona for review; it does not make the
underlying account matches verified identity evidence. “Independent subjects”
mode creates one Persona candidate per account identifier.

Variant generation is deliberately bounded and deterministic. It uses common
first/last-name orderings and separators, caps the query plan, and never creates
phone or email permutations. Arbitrary Cartesian combinations are prohibited
because they increase collection cost and false-positive risk without adding
provenance.

Operator-provided names, emails, phones, and research terms remain inside
OpenLedger by default. They are added to an external AI research request only
when the analyst explicitly enables that use for the investigation. Even then,
they remain unverified targeting context and cannot become canonical claims
without cited evidence and human review.

## Canonical ownership

| Responsibility | Canonical owner |
|---|---|
| Raw collector output | Collector run/result artifact |
| Normalized observation contract | OpenLedger ingestion layer |
| Claims, evidence, and provenance | PostgreSQL |
| Analyst decision and audit history | PostgreSQL |
| Canonical Persona view | Projection of approved claims |
| Cross-person relationship leads | Projection of approved normalized claims |
| AI assessment/chat | Separate case analysis record with claim/evidence citations |
| AI evidence suggestions | Pending claims after server validation; PostgreSQL |
| Maps and graph visualizations | Read-only presentation projections |

## Source adapter contract

Every public OSINT tool or authorized internal system must emit the same
observation envelope. Adapters must not write directly to Persona templates,
AI summaries, maps, or relationship nodes.

```json
{
  "case_id": "uuid",
  "subject_key": "adapter-specific-subject-id",
  "field_name": "current_location",
  "value": "Jakarta, Indonesia",
  "normalized_value": "jakarta indonesia",
  "source_engine": "authorized_source_adapter",
  "source_record_id": "record-123",
  "source_name": "Source system",
  "source_url": "https://evidence.example/record/123",
  "evidence_type": "observed_record",
  "confidence": 90,
  "observed_at": "2026-08-28T12:00:00Z",
  "coordinates": {"latitude": -6.1754, "longitude": 106.8272},
  "handling": {"classification": "internal", "access_policy": "case-team"}
}
```

The adapter pipeline performs:

1. schema and field validation;
2. value normalization and exact fingerprinting;
3. source-record and evidence provenance validation;
4. deterministic deduplication inside one Persona;
5. confidence-policy mapping for that source;
6. pending claim creation or last-seen/evidence update;
7. an entity-resolution proposal when the source subject is not already bound
   to a Persona;
8. explicit analyst review before canonical publication.

## Integrating additional sources

### Public OSINT collector

Wrap the tool in an adapter that maps its native fields into OpenLedger field
names. Preserve the tool name, record URL, record identifier, collection time,
and original value in evidence. Tool confidence must be translated through a
documented source-specific policy; it must not be treated as identity certainty.

### Internal data source

Use a service account with least-privilege, read-only access. The adapter should
run server-side, emit the same observation envelope, and copy only fields the
case is authorized to use. Store an internal record identifier and handling
policy even when no browser-accessible source URL exists. A connector must not
expose internal credentials or raw unrestricted records to the web process.

### Combining results into one Persona

If the adapter already has a trusted binding to an OpenLedger Persona, its
observations are synchronized there. Otherwise, an entity-resolution service
proposes candidates using stable identifiers such as verified email, phone, or
internal employee ID. Names, city, occupation, and usernames alone remain weak
signals. A human must approve the binding before claims from two source subjects
are presented as one person.

Claims remain separate records even when their displayed values match. The
Persona view groups them, preserves all source evidence, and calculates a
display confidence from documented evidence rules. This prevents one source
from erasing disagreement or laundering an unsupported assertion through a
second source.

## AI evidence proposal boundary

AI analysis uses two server-side Responses API stages. The first stage analyzes
the normalized investigation evidence and, when enrichment is enabled, requires
hosted public web search plus URL citations. An uncited model-only response is
rejected and is not saved as a successful assessment. The
second stage receives the assessment and its citation catalogue without browsing
again, then returns a strict JSON Schema payload. OpenLedger treats that payload
as untrusted and validates it again before storage.

The initial allowlist is intentionally limited to public-biographical fields:
summary, full name, coarse current location, occupation, company, public social
account, website, and photograph. Every accepted suggestion must identify an
investigated username, cite an exact URL returned by the research stage, contain
a review rationale, and use confidence between 40 and 85. Email, phone, private
address, finances, vehicles, criminal records, sensitive traits, and inferred
relationships are rejected at the server boundary even if the model emits them.

Accepted suggestions use `source_engine=openai_web_research` and
`evidence_type=cited_public_web`. They enter the same pending review queue as
collector observations. Re-analysis is idempotent by claim and evidence
fingerprint: it can add provenance or refresh last-seen time, but it does not
erase a rejection or any other analyst decision. Only approved claims reach the
default Persona, map, or Relationships projection.

For a cited coarse `current_location`, AI may also propose an approximate city
or region map center. The validator rejects partial coordinates, out-of-range
values, precise-position labels, and coordinates attached to any other field.
The proposal remains pending and is visibly labelled approximate; it reaches
the map only with the analyst's location decision. The UI exposes assessment,
citation, structured-proposal, and validation-rejection counts so a narrative
assessment cannot be mistaken for successful Persona extraction.

## Relationship projection

The Relationships workspace separates two graph contracts. **Persona evidence**
shows one Persona, every non-rejected claim, its review status, and provenance
sources. This is a review network and may display pending or uncertain nodes.
**Shared relationship leads** applies the stricter canonical projection below.

The first safe relationship workspace uses exact normalized values from
approved claims. It creates a bipartite graph:

- Persona nodes represent reviewed subjects.
- Attribute nodes represent approved shared values such as a location,
  company, email, phone, occupation, website, vehicle, or public account.
- Edges mean only “this Persona has an approved claim with this value.”

Two people connected to the same city or company have a shared-attribute lead,
not a confirmed personal or social relationship. Approximate matching,
directional social interactions, ownership, and temporal co-location require
separate evidence types and review policies before they become graph edges.

Rejected, pending, and uncertain claims do not enter the default relationship
projection. Rejected claims are retained so an analyst can reverse a decision
without losing provenance.

## Location privacy

Leaflet renders only approved location claims with coordinates. When an analyst
approves an address or current-location claim without coordinates, OpenLedger
sends that approved place label to the configured HTTPS geocoder and stores the
returned bounding-box centroid. This is an explicit review-time action rather
than background disclosure of every extracted place. Coordinates can also be
supplied by an authorized adapter, entered by an analyst, or proposed as a
visibly approximate city/region map center by the cited AI pipeline. These map
centers must never be interpreted as the person's precise position.

The default geocoder and map use configurable external endpoints. Isolated or
sensitive deployments should set `OPENLEDGER_GEOCODER_URL` and
`OPENLEDGER_MAP_TILE_URL` to approved internal services because geocoding and
map tile requests reveal query or viewed-area information to their providers.

## Deferred scope and revisit triggers

- Add a formal connector SDK when a second source adapter is ready.
- Add source-level access policies before mixing data with different
  classifications or case permissions.
- Add probabilistic entity resolution only after labeled analyst decisions can
  be used to measure false merges and false splits.
- Persist AI conversations with claim/evidence citations as a separate case
  artifact; never use chat text as a canonical claim.
- Add follower, mention, reply, transaction, communication, or co-location
  edges only when their source-specific observation and review contracts exist.
- Consider a dedicated graph database only when PostgreSQL projections cannot
  meet measured query or scale requirements. It must not become a second system
  of record.
