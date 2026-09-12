# P2 pipeline reliability and connector remediation

11 September 2026. The user authorized comprehensive correction of the critical pipeline review and requested multi-agent implementation. Branch upload and PR creation/update are already authorized. Merge and production deployment remain separate decisions. This continues PR #56 on the reviewed P2 rollback base; it does not revive P3.

Six implementation tracks covered evidence/QC, execution/recovery, executable connectors, page/feed ingestion, bounded workspace reads, and release acceptance. The coordinator implemented durable transport budgets/provider cooldowns and integrated the tracks. Independent follow-up reviews exercised transport redirects/retries, machine ingestion restrictions, and migration/schema parity.

## Corrected behavior

| Review problem | Implemented correction | Decisive acceptance coverage |
|---|---|---|
| Contradictory facts across attributed accounts can finalize | Subject-level date consistency after attribution, preserving account-scoped hypotheses and compatible temporal facts; explicit contradictory-evidence dispositions with immutable human reasons and restore controls | Cross-account dates, predicate aliases, compatible date precision, distinct temporal affiliations, resolution/reopening |
| Hard-crash job recovery leaves attempts running | Atomic job/request/task/attempt reconciliation, stale-writer fencing and authenticated same-job continuation; original case, evidence, deadlines and allowances retained | Transaction rollback, stale writes, preserved completed tasks, cancelled-job refusal, scoped CSRF resume |
| Found results hide unresolved sources; another subject inherits success | Partial completeness is distinct from findings; request status derives from that request's own tasks | Found-plus-timeout and independent username/email readiness |
| Source retention can be weakened | Strictest manifest/task/observation policy applies before persistence, including the raw feed inbox; restricted nested metadata is rejected or removed | Normalization, direct ledger entry, durable inbox, QC and export across restricted modes |
| Retry/request ceiling and provider circuit are not durable | Transactional pre-send allowance reservations, cross-process provider cooldowns and one leased probe after cooldown | Concurrent reservations, restart/retry, HTTP redirects, native retries, 429, User Scanner subprocess |
| Graph calls negative evidence support | Provenance is distinct from support, absence, contradiction, failure and human exclusion; graph paging preserves source endpoints | API roles, executed JavaScript viewer with 240 source nodes, rendered HTML and extracted PDF |
| Workspace reads rebuild all observations | Persisted compact summaries, explicit projection revisions, write fencing and read-only paginated GETs | 50,001-observation API reads, five simultaneous readers, dirty/stale projection refusal, failed rebuild rollback |
| Connector registration is centrally coupled | One executable manifest binds collection, normalization, compatible inputs, configuration, retention and versions; startup refuses invalid declarations | A fixture connector added through package + manifest + fixtures, saved parser mismatch, configuration/lifecycle controls |
| Pagination and machine-feed infrastructure absent | Atomic page/evidence/cursor checkpoints, stable source versions, scoped authenticated batch receipts and durable queued processing | Crash/page replay, lost acknowledgement, duplicate concurrency, version conflicts, revocation, source withdrawal and successor QC |

DNS collection also distinguishes authoritative absence from transient resolver errors. A timeout or SERVFAIL cannot establish that an account/domain is absent.

## Connector integration contract

An ordinary new source requires a reviewed connector package, one entry in `config/osint-sources.json`, and a conformance fixture suite. It does not require central executor or Persona/QC template edits. The registry preserves all 22 prior capabilities and adds a machine-feed gateway; no new scanner was added.

`docs/dev/connector-guide.md` describes callable/configuration contracts. `docs/dev/p2-connector-ingestion.md` describes authenticated feeds, exact case/Persona scopes, page checkpoints and source-version semantics. Source-specific parsing, jurisdiction rules, cursor expiry and watermark meaning remain the connector author's responsibility. Scheduling may remain with the external producer.

Standard requests/urllib3, aiohttp, httpx (when installed), and curl transports are governed only inside supervised collection processes, including the User Scanner subprocess. Physical client HTTP sends, redirects and retries consume the saved query request allowance. Explicit DNS collection also consumes a permit. Other reviewed non-HTTP adapters must use `CollectorContext.reserve_request`. Counts do not claim visibility into work performed internally by an external API or search provider. Executable connectors are trusted, reviewed application code; the compatibility guard is not a sandbox for arbitrary third-party programs.

Reservations are consumed before sending and are not refunded after ambiguous failures. A cancelled or dead attempt cannot reserve again. Per-host provider cooldowns survive worker/process restart. Curl cannot downgrade HTTPS redirects; cross-host redirects with session-wide credentials or query defaults require explicit connector handling. Per-request credentials and original query/body data are not forwarded to another host.

Machine identities can submit scoped evidence and inspect their receipts. They cannot curate claims, make human evidence dispositions, approve QC, or choose another case's subject. Updates and withdrawals preserve prior source versions and final Persona versions while preventing obsolete support from passing successor QC.

## Projection and probability limits

Ordinary workspace GETs read bounded summaries and paged history. Legacy import and recovery preparation are explicit authenticated mutations. Changed evidence invalidates the projection, and freezing a version requires a complete current projection. Write-side consolidation still rebuilds the subject under its lock; this implementation does not claim constant-time writes or an incremental inference algorithm. Read-side benchmark results are engineering measurements, not a production latency SLA.

No independently labelled probability reference dataset has been supplied or validated. Numeric probability remains unavailable unless a reviewed empirical calibration artifact passes its gates. Software fixtures do not establish source accuracy or model calibration.

## Release and validation

The runtime identity remains `p2-e2e-v1`. The required schema is now `e2e2b8d0a502`, descending from `e2e1a7c9d401`. The historical 16-table migration is frozen; the new migration adds nine runtime, ingestion and projection tables. Populated schema downgrades are refused to preserve history. The updater accepts only the explicitly supported source schemas and the exact reviewed application/image identity. It never switches to the old pipeline or a floating image.

The broad Python PR workflow now includes the stacked rollback base. The mandatory engineering gate discovers pipeline/connector tests and requires named failure regressions without skips. CI also requires real PostgreSQL migration/concurrency, the 50,000-record ledger workload, a Chromium operator/QC/research journey, disposable database backup/restore, and same-image app/worker/scanner validation. Scanner probes import the actual pinned library before and after the transport guard, exercising both synchronous and asynchronous clients without contacting providers.

Final local and GitHub results are recorded in the PR against the exact uploaded head. Local SQLite and mocked-source results must not be represented as PostgreSQL, real-provider, browser, or production acceptance. This change does not alter the running production application.
