# OpenLedger case intelligence architecture

OSINT collector admission, runtime limits, and update governance are documented in [Governed OSINT source maintenance](osint-source-maintenance.md). Curated sources reuse the existing worker, PostgreSQL evidence lineage, and pending Persona review boundary; catalogs such as OSINT Framework are not runtime platforms.

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

The existing Maigret path uses the public, MIT-licensed `socid-extractor`
library only against profile responses Maigret has already fetched. OpenLedger
does not enable its optional AI fallback, private parser pack, hosted API, or an
authenticated-cookie requirement. Normalized biography, contact, and location
values follow the ordinary pending-claim path. A reviewed allowlist of stable
account identifiers becomes pending `platform_identifier` claims; arbitrary
fields ending in `_id` are not promoted. Public links extracted from a profile
become capped-confidence `linked_profile_lead` claims and require independent
review. Neither field participates in the shared-relationship projection.

Volatile account state such as creation date, verification/private flags, and
follower/following counts remains time-stamped evidence on the corresponding
`social_account` claim. A changed count creates a new evidence observation, not
a new account claim, and never changes an existing analyst decision. This keeps
account continuity, identity attribution, and changing platform metadata as
three distinct propositions.

The initial User Scanner integration is deliberately narrower than the tool's
full capability. It runs email-registration checks only after explicit case
opt-in, only in one-subject mode, and always with notification-producing and
adult modules disabled. It executes in a subprocess because User Scanner
patches shared HTTP client classes when imported. Positive registrations become
pending `account_registration` claims; all bounded native outcomes remain on
the investigation result for diagnostics and AI analysis. User Scanner's
username sweep and recursive cross-scan are deferred because they overlap
Maigret and can turn handle collisions into unsupported identity links.

OpenGraph Intel (OGI) may consume a read-only graph projection of approved
OpenLedger claims and their evidence. Its project, entity, and edge tables are
not authoritative and must not receive collector writes independently. An OGI
adapter must preserve OpenLedger claim/evidence identifiers and rebuild from
PostgreSQL, so deleting the projection never deletes case evidence.

The production image installs User Scanner from an immutable, reviewed commit
archive. A fork synchronization does not change the running collector until a
separate OpenLedger pull request updates that pin and passes the OpenLedger
regression and image-build gates.

The optional claimed-URL evidence slice follows the same ownership boundary.
Unfurl runs offline in a dependency-isolated subprocess and emits bounded URL
structure. Wayback receives only the same exact public claimed URL and returns
bounded CDX capture metadata; OpenLedger never downloads the archived page in
this slice. Both sources attach labelled evidence to the already-discovered
`social_account` claim. URL structure and historical capture presence do not
create a new identity fact or increase confidence in Persona ownership.

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

The allowlist is intentionally limited to public-biographical and explicit public
contact fields: summary, full name, coarse current location, occupation,
affiliation, public social account, website, photograph, email, phone, and a
published institutional or business contact address. `company` remains the
backward-compatible storage key for affiliations such as employers, education
institutions, associations, and organizations. Every accepted web suggestion
must identify an investigated username, cite an exact URL returned by the
research stage, contain a review rationale, and use confidence between 40 and
85. Contact values must be exact, never derived from usernames or naming
patterns. Private or residential addresses, finances, vehicles, criminal
records, sensitive traits, and inferred relationships remain rejected.

In One subject mode, exact email and phone identifiers supplied in the
investigation form also become 50-confidence pending claims with
`source_engine=investigation_input`. They remain unverified until an analyst
reviews them. They are not attached in Independent subjects mode because the
owning Persona would be ambiguous. Email registration observations can support
account discovery, but do not establish an address or current location.

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

## Persistent case chat

Case chat is stored in PostgreSQL as an append-only conversation owned by one
case. OpenLedger does not rely on provider-side conversation state: each request
receives a bounded projection of the case record and recent retained messages,
while the complete message history remains in the case database. This preserves
durability without introducing another memory service or vector database.

An analyst may use case evidence only, explicitly enable hosted public-web
research, and optionally target one Persona for proposed updates. Research mode
requires URL citations in the same response. The answer, model, sources, actor,
target Persona, and research choice are stored together. The assistant must
distinguish approved case evidence, pending or uncertain records, user-supplied
context, cited public sources, and analytical inference.

Persona updates use a separate strict extraction pass and server validation:

- cited public-web findings must use an exact URL returned by that chat turn;
- user-statement findings must be explicit text from the analyst's message,
  remain unverified, and are capped at 50 confidence;
- extrapolations, hypotheses, and model-only values never become claims;
- the existing public-biographical allowlist and sensitive-field exclusions
  remain in force;
- every accepted proposal is pending and links its claim observation to the
  originating chat message; and
- chat can never approve, reject, or otherwise make a claim canonical.

## Affiliation-led cases

An approved `company` claim can open a separate affiliation-led case and may
include an optional legal jurisdiction. The workflow uses the existing
PostgreSQL queue and worker. The governed `wikidata_affiliation` adapter
resolves up to five label or alias candidates,
reads the selected entity's official website (`P856`), and runs one bounded
SPARQL query for at most fifty people connected by explicit employment,
education, membership, affiliation, founder, executive, chair, director, or
board-member statements. Ambiguous names pause for operator selection.

Both endpoints are fixed Wikimedia origins with a thirty-second timeout,
one-megabyte response cap, redirects disabled, no credentials, and an
identifiable user agent. A source error degrades only this adapter. Every person
enters as pending `full_name`, backward-compatible `company`, and Wikidata
identifier claims retaining entity and relation lineage. Only analyst-approved
shared affiliation claims can enter the existing Relationships projection, and
they do not establish a personal relationship.

When a jurisdiction is supplied, the independent GLEIF adapter searches the
global LEI index by organization name and country and retains up to five
jurisdiction-matched entity candidates. A missing LEI result is never presented
as proof that the business is unregistered. Country-level France searches also
query the French National Enterprise Directory. Only one exact legal-name
match can propose public natural-person leaders as pending `full_name`,
backward-compatible `company`, and `occupation` claims. The adapter deliberately
discards upstream birth and nationality data. Each proposal retains its SIREN,
public role, source URL, and review restrictions; registry evidence never enters
canonical Persona or relationship views automatically.

An optional domain-context pass accepts one explicitly supplied official website,
or the selected organization's `P856` website when available. The worker queries
only Cloudflare's fixed DNS-over-HTTPS endpoint for bounded `A`, `AAAA`, `MX`, and
`NS` results. These records remain technical observations with no claim mapping.
The case workspace presents registered location, registry activity, website
association, and DNS routing as separate evidence classes; every displayed
statement includes its source basis and limitation. In particular, hosting,
nameserver, mail-provider, registrar, and domain-registration geography never
become evidence that the organization operates in that place.

For organization-published addresses and broader operating context, the case
workspace opens the existing persistent chat with a bounded research brief and
public-web citations enabled. The brief requires direct citations and explicitly
separates legal records, self-published statements, and infrastructure. It does
not create an organization address claim on a Persona automatically.

## Confirmed-name public-record enrichment

Approving a `full_name` claim queues two independent, credential-free checks in
the existing worker. The Wikipedia adapter uses the English MediaWiki Action API
to propose a bounded introductory summary, page identifier, and available lead
image. An exact, non-disambiguation title may proceed directly to the review
queue; otherwise an analyst must select one of at most five stored candidates.

The ICIJ Offshore Leaks adapter uses the public W3C Reconciliation API and keeps
only exact-name `Officer` candidates. A match creates a prominent pending risk
alert linked to the ICIJ node. It never confirms that the OpenLedger Persona and
the ICIJ record are the same person, never automatically establishes an offshore
affiliation, and never implies illegal or improper conduct. The analyst must
compare independent identifiers before approving the record. Fuzzy candidates
do not create alerts.

Both sources use fixed HTTPS origins, redirects disabled, response and result
limits, explicit user agents, and source-scoped failure handling. Wikipedia and
ICIJ proposals enter the existing claim, evidence, observation, review, and
timeline model without a schema migration or separate service.

OpenCorporates was evaluated but is not active because its normal API path
requires an API key. OpenData.org was also evaluated but is not active because
its current terms prohibit automated retrieval and use in a third-party service
without a separate agreement. Neither source is scraped or called by OpenLedger.

## Relationship projection

The Relationships workspace separates two graph contracts. **Persona evidence**
shows one Persona, every non-rejected claim, its review status, and provenance
sources. This is a review network and may display pending or uncertain nodes.
**Shared relationship leads** applies the stricter canonical projection below.

The first safe relationship workspace uses exact normalized values from
approved claims. It creates a bipartite graph:

- Persona nodes represent reviewed subjects.
- Attribute nodes represent approved shared values such as a location,
  affiliation, email, phone, occupation, website, vehicle, or public account.
- Edges mean only “this Persona has an approved claim with this value.”

Two people connected to the same city or affiliation have a shared-attribute lead,
not a confirmed personal or social relationship. Approximate matching,
directional social interactions, ownership, and temporal co-location require
separate evidence types and review policies before they become graph edges.

Rejected, pending, and uncertain claims do not enter the default relationship
projection. Rejected claims are retained so an analyst can reverse a decision
without losing provenance.

## Case timeline projection

The case evidence timeline is a bounded, read-only projection of records already
owned by PostgreSQL. It merges three existing event classes without adding a
timeline table or a second audit system:

- investigation queue/start and terminal timestamps from investigation jobs;
- immutable claim-observation timestamps and provenance;
- append-only analyst review decisions and notes.

Persona filtering includes only claim observations and reviews tied directly to
that Persona. It excludes case-level investigation lifecycle events because a
multi-subject collection job cannot be attributed to one Persona without an
additional reviewed binding. The projection is capped and supports only exact
event-type, Persona, and sort-order filters.

The primary event timestamp means when OpenLedger recorded the lifecycle event,
observation, or decision. Dates supplied by a public profile parser—such as
account creation, profile update, or latest activity—remain labelled metadata
inside that observation. They are not promoted into the primary chronology and
must not be interpreted as verified real-world behavior.

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
- Add follower, mention, reply, transaction, communication, or co-location
  edges only when their source-specific observation and review contracts exist.
- Consider a dedicated graph database only when PostgreSQL projections cannot
  meet measured query or scale requirements. It must not become a second system
  of record.
