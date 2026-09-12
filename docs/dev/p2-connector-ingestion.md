# Connector continuation and machine delivery

The P2 evidence ledger supports both collector-driven pagination and durable
incoming batches. Both paths use a leased task, source observation normalization,
consolidation, operator curation and explicit QC. A machine identity cannot
perform operator/QC actions. No new scanner or external source is enabled here.

## Paginated collectors

An executable connector reads `context.checkpoint`, whose initial value has a
null cursor/watermark, unknown completeness and zero page/record counts. Before
each outbound call it reserves the existing governed request budget. After
parsing one bounded page (maximum 500 observations), it calls:

```python
context.checkpoint_page(
    source_rows,
    expected_cursor=checkpoint["cursor"],
    next_cursor=response.next_cursor,
    watermark=response.watermark,
    completeness="partial" if response.next_cursor is not None else "complete",
    source_versions=[
        {
            "record_id": record.id,
            "source_version": record.version,
            "supersedes_version": record.previous_version,
            "operation": "upsert",
        }
        for record in response.records
    ],
)
```

`source_rows` are the same raw observation envelope used by `context.emit`.
There must be exactly one source-version descriptor per normalized observation.
Providers without version IDs can use a deterministic content hash as the
version, while retaining their stable native record ID. The adapter must supply
the last committed version when changing a record; timestamps alone do not
establish ordering for opaque provider versions.

Evidence, source version/head, cursor, watermark and counters commit together
under the active worker lease. Retries read the durable cursor. Replaying an
identical consumed page returns its original observation IDs; changing content
under that page identity fails. A late conflict rolls back the whole page.
Complete pages have no next cursor. Partial pages must advance their cursor.
Truncated means a deliberate bound stopped collection; unknown means source
coverage cannot be established. Neither is equivalent to complete. Cursor and
watermark objects are bounded to 16 KiB each and cannot contain credential keys.

Native versions deduplicate across requests within the same case, Persona and
connector. A new version explicitly supersedes the current version. A withdrawal
requires an existing version and introduces no claims/accounts. Previous
observations and final versions remain retained; outdated/withdrawn observations
cease to qualify as current QC support and the existing final requires review.
Independent current sources may continue to support the same fact.

## Incoming feeds

Configure a distinct machine identity using the server environment variable
`OPENLEDGER_CONNECTOR_IDENTITIES_JSON`. The value is a JSON mapping from connector
identity to `enabled: true`, a SHA-256 digest `token_sha256` of a random bearer
token of at least 32 characters, and explicit `scopes` entries containing both
`case_id` and `persona_id`. Optional `retention` defaults to retained bounded
evidence. Configure the same identity policy for the web and worker services.
The actual token belongs in the producer's secret configuration, never its
payload, URL, case options or source observations. There is no wildcard scope.

The producer sends `POST /api/connectors/{connector_id}/batches` with
`Authorization: Bearer ...`, `Idempotency-Key: ...` and JSON:

```json
{
  "case_id": "existing-case-uuid",
  "persona_id": "existing-persona-uuid",
  "records": [
    {
      "record_id": "provider-record-123",
      "source_version": "provider-version-1",
      "operation": "upsert",
      "data": {
        "status": "candidate",
        "source_url": "https://example.org/public/123",
        "claims": [{"predicate": "occupation", "value": "Researcher"}]
      }
    }
  ]
}
```

The endpoint requires its bearer identity even when browser authentication is
disabled locally. Browser cookies are never machine credentials. The receiving
service validates exact case/Persona membership and scope, maximum 1 MB/500
records, forbidden credential fields, source identity and record lifecycle.
One batch contains at most one version of a native record. Source data cannot
override pipeline scope, engine identity or evidence lineage.

The inbox rejects transient/prohibited/live-only source policies before storing
payloads. Metadata-only sources accept only source URL/name, outcome, published
time and retention fields, with final eligibility disabled. This avoids using
the durable inbox to bypass the evidence retention policy.

A successful 202 response means the immutable payload receipt and ordinary
worker job/request committed in one transaction. It does not mean collection,
operator review or QC is complete. The response includes the receipt, job and
request IDs. Repeating the same scoped idempotency key/content returns 200 and
the same job; changed content returns 409. If the connection is lost before the
response, retry the same key rather than creating a new one.

`GET /api/connectors/{connector_id}/batches/{receipt_id}` returns scoped delivery
status. The standard worker executes `connector_feed`; its durable page commit
can replay safely after interruption. Invalid update ordering produces a failed
receipt with a nonretryable contract error; submit a corrected new batch with a
new idempotency key. An interrupted job uses the normal explicit recovery path.
No receipt has a path to automatic finalization.

Before processing, the worker rechecks the configured identity's enabled state,
exact subject scope and retention policy. Revocation or policy change blocks the
queued delivery with an explicit receipt error; rotating only the bearer token
does not discard previously authorized receipts. Metadata fields must be bounded
strings; nested objects cannot disguise source payloads as metadata.

## Delivery and remaining provider-specific work

The additive reliability migration creates five connector ledger tables.
Page/version history and receipt content are protected against database mutation;
checkpoints, source-head pointers and processing status remain mutable.

Each actual new provider still needs a reviewed manifest, parser/normalizer and
contract fixtures. Authentication protocols, cursor expiry, provider-specific
watermark ordering and producer scheduling are adapter responsibilities. No
particular external paginated API or machine producer was supplied or contacted
in this change. Local fixtures prove transactional behavior; PostgreSQL migration,
concurrent delivery and normal worker execution are separate CI gates.
