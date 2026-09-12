# P2 working evidence read projections

The pipeline workspace HTML/API and its case/subject identity lookup are read-only.
They do not import legacy cases, run collection, consolidate observations, or evaluate
probabilities. A page reads at most 100 group summaries (the UI uses 25), with three
sample observations and three audit-history entries per group. Queries, tasks,
versions and research requirements have independent 25-record workspace windows.
`/history/<kind>?offset=...&limit=...&parent_id=...` exposes complete scoped history;
limits are capped at 100. Exact recorded query plans and source observation pages
remain accessible. Request rows expose consumed, maximum and remaining requests
from the shared budget ledger, when registered, with unused planned allowances
before the first runtime reservation.

`pipeline_group_summaries` is a mutable projection of immutable group assessments,
source memberships and grouping revisions. It omits per-observation assessment
arrays and stores only three sample observations. Evidence counts remain exact.
Normal pages never deserialize an assessment containing thousands of source IDs.
Operator decisions are read live, including probability abstention after evidence
exclusions; grouping writes refresh the source and split-target summaries in their
existing transaction. Full source and frozen assessment history is retained.

`pipeline_projection_state` tracks committed evidence and projected revisions.
Each nonduplicate evidence append increments the input revision in its transaction.
The rebuild coordinator takes the same subject lock as evidence writers, reads
inputs, writes assessments/summaries and advances its watermark in one transaction.
A failed rebuild rolls back everything, leaving the evidence visibly pending. A
concurrent append waits for that snapshot and makes the next revision pending.
SQLite rebuild and append reserve the writer explicitly; PostgreSQL uses the
existing subject row lock.

Partial `upsert_groups` calls never acknowledge a full projection. Trusted internal
full-snapshot writers may supply the revision captured before reading their input;
the store rejects stale revisions while holding the subject lock. This parameter
is not accepted by HTTP clients. Freezing a Persona version requires the current
projection, so unprocessed evidence cannot silently disappear from a successor.
Existing final versions stay immutable throughout.

Committed collection/manual evidence already invokes consolidation. Historical
imports and recovery of pending projections also have an explicit authenticated,
CSRF-protected **Prepare working evidence** action. Historical record counts and
the reason preparation is needed appear in the workspace; GET never performs
that work. Successful legacy import records its durable completion marker.

## Verification and limits

`tests/test_pipeline_projections.py` exercises a real SQLite store and Flask API
with 50,001 observations in one dense group. Three measured reads use bounded SQL,
less than 8 MB of traced Python allocation, no writes, and no reads of the full
assessment table. Five separate clients also read concurrently. The final source
observation remains available on page 501. The test reports actual elapsed time;
its generous 15-second regression ceiling is not a production latency promise.

The suite also covers complete independent history pagination, scope/CSRF checks,
explicit preparation, duplicate replay, failed rebuild rollback, partial and stale
projection rejection, and concurrent append/rebuild ordering. Transaction tests
run on both SQLite and PostgreSQL when OPENLEDGER_TEST_POSTGRES_URL is configured.

A rebuild still consolidates the full subject on an ingestion boundary or explicit
preparation request. Its memory and write-lock duration scale with subject evidence.
Membership persistence now uses batched lookups/inserts instead of two database
queries per observation. The bounded-read correction does not claim an incremental
assessment algorithm or a measured production SLA. Very large source exports,
exact frozen manifests and full assessment detail are intentionally explicit,
complete operations rather than ordinary workspace page loads.
