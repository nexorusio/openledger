# Maigret upstream maintenance

OpenLedger is a customized downstream distribution of Maigret. Upstream changes
must therefore be integrated without replacing OpenLedger's product, security,
deployment, AI, or interface work.

## Automated workflow

`Check Maigret upstream` runs daily at 02:17 UTC and can also be started from
**Actions → Check Maigret upstream → Run workflow**. It:

1. fetches `soxoj/maigret:main`;
2. stops when the upstream commit is already contained in OpenLedger `main`;
3. attempts a merge on `automation/maigret-upstream-sync`;
4. opens or updates one synchronization pull request; and
5. creates or updates an issue when Git cannot resolve the merge.

The scheduled workflow runs synchronization, read-only integrity testing, and
guarded merge as separate jobs. This is intentional: GitHub suppresses recursive
workflow events created with `GITHUB_TOKEN`, so the safety decision does not rely
on the generated PR starting another workflow. The optional PR integrity workflow
provides the same checks when a person updates the synchronization branch.

Both paths use the guard script from OpenLedger `main`, not the candidate branch,
so an upstream change cannot weaken its own checks.

## Automatic-merge boundary

Only these files are eligible for unattended merge:

- `maigret/resources/data.json`
- `maigret/resources/db_meta.json`
- `sites.md`

The database and metadata must change together. The guard checks JSON structure,
site count, SHA-256, download host, database format, and minimum compatible
version. It then runs the offline regression suite, validates the production
Compose configuration and shell scripts, and builds the web image.

Any other changed file causes the integrity job to fail intentionally. The PR
remains open for review even if all ordinary tests pass. Resolve conflicts and
review code changes on that PR or a replacement maintenance branch.

## Required repository setting

Keep **Settings → General → Pull Requests → Allow merge commits** enabled.
Upstream synchronization PRs must use merge commits. Squashing or rebasing loses
the recorded upstream ancestry and makes later synchronization attempt to replay
previous upstream commits.

The workflow uses only the repository `GITHUB_TOKEN`; it needs no personal access
token or third-party secret. Repository Actions permissions must allow GitHub
Actions to create pull requests. If organization policy disables that permission,
the synchronization check will report a permission error without changing main.

## Deployment

Successful synchronization updates only the GitHub repository. The production
Droplet is not modified automatically. After reviewing the merged commit, update
the deployment using the existing `deploy/update.sh` procedure.
