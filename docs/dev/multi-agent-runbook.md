# Multi-agent development runbook

This runbook is the authoritative operating protocol for coordinated
OpenLedger development. It supplements the repository's technical and
operational documentation; it does not grant product scope, production access,
or authority to weaken an existing safety boundary.

## Control model

- One coordinator remains the only user-facing agent. The user does not create
  or manage worker chats.
- The coordinator owns architecture, dependencies, task assignment,
  integration, testing, pull-request preparation, release reporting, and
  handovers.
- A worker may start only after the user authorizes the activity containing its
  assignment. Authorization of one activity does not authorize a later
  activity or phase.
- Every worker receives a bounded task, explicit ownership, acceptance
  criteria, focused tests, and instructions to preserve unrelated work.
- Workers may inspect shared interfaces needed for their task but must not edit
  outside their assigned ownership.
- The coordinator alone integrates worker commits into the phase branch.

The user remains the mandatory approval gate for material scope expansion,
architecture outside the approved roadmap, squash-and-merge, production
deployment, and destructive or irreversible operations. No worker may merge,
deploy, modify production data, bypass a release gate, or expand its own scope.

## Dynamic concurrency

Use only as much concurrency as the dependency graph can safely support.
Normally this means the coordinator plus two or three workers. Up to seven
total active agents is permitted only when tasks have clearly isolated
contracts, files, tests, and acceptance criteria. Do not create workers merely
to fill capacity. Reduce concurrency when coordination and integration cost is
likely to outweigh the saved time.

Parallel work is permitted within an authorized activity. During P3--P7,
isolated implementation, tests, fixtures, benchmarking, review, inventory, and
future preparation may run concurrently, but dependent future code must not be
merged early. P8--P11 are mostly sequential. Before P11 is production-verified,
cross-phase work should remain preparation rather than feature implementation.
After P11, P12--P14 and the independent P15 lane may proceed concurrently when
their contracts and ownership do not overlap. P16 waits for the required P13
and P15 adapter contracts. P17 waits for P14 and P16. P18 starts only after
production-verified P17; its page groups may run concurrently only after the
preservation audit, Design DNA, tokens, and shell contracts are locked.

## Worktree, branch, and ownership rules

Every coding worker uses a separate Git worktree and temporary branch created
from the coordinator-declared base commit. The coordinator uses a separate
phase integration worktree and phase branch. Before work begins, record:

1. the exact base commit and tree;
2. every worktree and branch;
3. the worker's exclusive files, component, or responsibility;
4. interfaces the worker may inspect but not modify;
5. focused tests and acceptance criteria; and
6. the intended integration order.

Never assign concurrent independent ownership of:

- Alembic migration ordering;
- database schema foundations;
- central authorization or RBAC;
- shared application contracts;
- `maigret/web/case_store.py`;
- `maigret/web/app.py`;
- `maigret/web/worker.py`;
- Docker or deployment configuration;
- dependency lockfiles;
- the same template, component, or other tightly coupled shared file.

If an unforeseen overlap appears, stop the affected workers at safe
checkpoints and let the coordinator serialize the work. Workers commit only
their owned changes. They do not push to `main`, merge directly into `main`, or
integrate another worker's branch.

Existing changes and untracked artifacts belong to the user unless explicitly
identified otherwise. Preserve them. Do not use destructive Git commands to
discard work. Resolve exact targets before any deletion and obtain user
approval for destructive or irreversible operations.

## Integration and pull requests

The coordinator reviews every worker diff and test result before integration.
Only completed, relevant worker commits are integrated into the phase branch.
There is one consolidated pull request per phase. Phase pull requests are
merged and deployed in numerical order, even when isolated preparation for a
future phase was performed earlier.

Future-phase work must be rebased onto the latest production-verified `main`
and fully retested before it can be integrated. A worker result is not a phase
release, and a green focused test is not permission to merge or deploy.

## Required end-of-phase release gate

A phase is complete only when all applicable items below have been recorded:

1. All planned phase activities are complete.
2. Focused tests pass.
3. The full relevant regression passes.
4. Worker work is integrated into one phase branch.
5. One phase pull request is created or updated.
6. The final remote-head SHA is reported.
7. Applicable continuous-integration checks are green.
8. Review findings are resolved.
9. The coordinator explicitly states that squash-merge is safe.
10. The user performs the squash-and-merge.
11. Complete deployment instructions are supplied.
12. Production is updated from merged `main`.
13. The application and affected services are rebuilt or recreated.
14. Alembic runs when applicable.
15. The production Git SHA is verified.
16. The production Alembic head is verified.
17. The database backup is verified.
18. The app, worker, database, migration service, and proxy are verified.
19. Public HTTP health is verified.
20. The actual new functionality is tested in production.
21. Applicable administrator and analyst RBAC is tested.
22. Rollback readiness is confirmed.
23. Production verification is recorded before the next phase begins.

Production deployment remains human-controlled. Use `deploy/update.sh` as the
canonical mechanism. P3--P5 may use the established manual post-merge process.
P6 may add protected GitHub-to-production automation, but it must retain a
protected production environment, human approval, expected-SHA verification,
PostgreSQL backup, Alembic migration, Docker and HTTP verification, and
fail-closed behavior.

When production access is through an existing SSH/Linux session, provide a
complete Linux command block. If access must begin in Windows PowerShell,
provide the complete PowerShell SSH invocation and a separate complete Linux
script. Do not provide fragments or place commands outside their designated
code blocks.

## Mid-phase chat transfer

A coordinator chat may move at a safe checkpoint. Before producing a resume
prompt, the coordinator must:

1. stop assigning work;
2. collect every active worker result;
3. ensure workers stop modifying files;
4. run the appropriate focused tests;
5. commit completed work;
6. integrate safe completed commits;
7. push the phase or checkpoint branches;
8. update `docs/dev/roadmap-state.md`;
9. record worktrees, branches, commits, tests, migrations, pull-request and CI
   state, completed and incomplete work, and the exact next action;
10. generate one complete resume prompt; and
11. stop the old child agents.

The new coordinator uses fresh worker sessions and durable branches, not old
agent sessions. It verifies the recorded Git state before resuming and does not
repeat completed work.

## Chat-health reporting

After every authorized activity, major integration, and phase completion, the
coordinator reports all three fields:

```text
Chat health: GREEN / AMBER / RED
New chat recommended: YES / NO
Safe checkpoint available: YES / NO
```

- **GREEN** means context, responsiveness, logs, and branch tracking remain
  clear.
- **AMBER** means context or operational state is becoming heavy. Finish the
  safe activity, persist its state, and prepare a transfer.
- **RED** means do not start another activity. Stop assignment, persist and
  integrate safe work, update roadmap state, and hand over.

Signals include degraded responsiveness, context reconstruction or compaction,
truncated output, excessive logs, unclear branches, uncertainty about
completed work, and repeated restatement.

## Cost and dependency guardrails

Do not add paid infrastructure, managed databases, registries, queues, API
plans, subscriptions, or any new recurring-cost service without explicit user
approval. The current PostgreSQL-backed worker remains the queue unless the
approved roadmap explicitly changes that decision. External integrations must
be bounded, documented, feature-controlled, and fail safely without converting
an ambiguous failure into a negative finding.

Specific approved external-dependency boundaries and phase decisions are
maintained in [Roadmap state](roadmap-state.md). Before requesting deployment
or an external-integration action, restate the applicable cost, traffic,
licensing, privacy, and availability consequences.
