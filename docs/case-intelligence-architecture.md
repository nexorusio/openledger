# OpenLedger case intelligence architecture

## Decision

PostgreSQL is the canonical record for cases, personas, investigation jobs,
structured claims, claim evidence, and analyst review decisions. Generated
report files remain compatibility artifacts and are not the source of truth for
the Persona workspace.

The first delivered slice is deliberately narrow:

1. A completed investigation is synchronized into its case personas.
2. Only directly observed profiles and extracted source fields become claims.
3. Every claim retains confidence, provenance, first/last observation times,
   and a source job.
4. Analysts can approve, reject, or mark claims uncertain. Each decision is
   appended to an audit history.
5. **Run investigation again** queues fresh collection in the same case. Claim
   synchronization is idempotent and never overwrites analyst decisions.
6. The relationship graph is derived from non-rejected claims and evidence.

Unsupported fields remain empty. In particular, financial, vehicle, ownership,
and criminal-record fields are never populated merely because a model can make
an inference.

## Extension boundary

Additional collection engines and authorized internal sources should be added
as adapters that emit the same normalized observation contract:

```json
{
  "case_id": "uuid",
  "subject_key": "source subject identifier",
  "field_name": "email",
  "value": "person@example.org",
  "source_engine": "authorized_source_adapter",
  "source_name": "Source system",
  "source_url": "https://evidence.example/record/123",
  "evidence_type": "observed_record",
  "confidence": 90,
  "observed_at": "2026-08-28T12:00:00Z"
}
```

Adapters must not write directly to templates or the relationship graph. They
pass observations through normalization, deduplication, provenance validation,
and claim synchronization. This prevents one source from bypassing review and
allows authorized internal data to coexist with public-source findings without
losing origin or access policy.

## Next safe phase

- Add an adapter interface and connector-level access policy.
- Add entity resolution that proposes, but never auto-approves, cross-source
  identity links.
- Store ongoing AI conversations by case with citations to claim and evidence
  identifiers.
- Rebuild an assessment when accepted evidence changes while retaining the
  conversation and prior analyst decisions.
- Add case-level relationship types only after their evidence and review model
  is defined.

The current graph is an evidence graph, not a follower or interaction network.
Influence metrics must not be calculated until directed social-interaction data
is collected from an authorized source.
