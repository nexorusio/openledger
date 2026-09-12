# OpenLedger DigitalOcean deployment

This directory provides the supported single-Droplet deployment for the
Nexorus OpenLedger fork.

It runs:

- the OpenLedger Flask app behind a supervised Gunicorn WSGI server;
- Caddy as the only public service on ports 80 and 443;
- automatic HTTPS for the configured domain;
- a branded OpenLedger login with protected password management;
- persistent reports, PostgreSQL case state, investigation history, and web
  settings;
- optional server-side OpenAI analysis and Google Places business leads configured from the protected Settings page.

## Prerequisites

- Ubuntu or Debian DigitalOcean Droplet;
- at least 2 GB RAM, with 4 GB recommended for report generation;
- the domain A record resolving directly to the Droplet;
- inbound TCP 22, 80, and 443 permitted by the DigitalOcean firewall;
- inbound UDP 443 permitted if HTTP/3 is desired.

## P2 end-to-end pipeline: approved Docker delivery

The supported updater selects exactly `p2-e2e-v1`. There is no legacy execution
flag. A GitHub pull or a Docker restart is not release selection. Updating Docker
Engine does not change application code. Use the approved release's instructions
and its literal full commit, Git tree and immutable image ID below.

**No production command is authorized merely because this guide exists.** Merge
and deployment require their separate approvals after review and acceptance.

### What prevents the previous pipeline returning

- `deploy/update.sh` requires `--commit` and `--manifest`; missing pins stop it.
- `app`, `worker` and `migrate` require the same immutable SHA256 image reference.
  Compose has no app build directive, old application tag, or automatic pull.
- The manifest binds full commit/tree, pipeline, engine contract, exact image ID,
  and a source fingerprint checked on the checkout, inside Docker RUN, and at
  runtime. Supplying a correct commit label on different source cannot pass.
  The manifest also binds source/target schema and every migration SHA256. Its Git tree must be clean.
- The exact allowed chain is `b3e9d7c4a610` → `e2e1a7c9d401` → `e2e2b8d0a502` → `e2e3c9d1f703`.
  The reviewed updater accepts either predecessor or the current target as the
  existing database revision. The released app and worker require `e2e3c9d1f703`.
  Multiple/unknown/P3 schemas and forbidden P3 implementation artifacts stop preflight.
- A released web image contains an immutable build file. Both runtime startup and
  health checks require its matching schema. Environment flags cannot bypass it.
- Before success the updater compares the actual app and live worker heartbeat
  with the manifest and checks both Docker container image IDs. Mixed versions
  stop the app, worker and ingress; no automatic old-app rollback occurs.
- After success the SHA256 pin is stored in the existing protected deployment
  environment, so ordinary Compose restarts and search-provider maintenance use
  that same reviewed image.

These are accident guards, not a signature system. They do not control arbitrary
old scripts/source deliberately checked out or Docker commands run outside this
supported process. Never invoke an updater still present in an older restored
snapshot: first obtain and inspect the approved new updater and exact checkout.

### Prepare one reviewable candidate (release engineer)

After the candidate is committed, use its reviewed **literal** 40-character SHA.
Do not derive the approved commit from `main`, a branch tip, `HEAD`, or `latest`.
The manifest is external to tracked source to avoid a self-referential Git hash.
No container registry or paid service is required; an immutable local image ID is
supported. Docker build is preparation, not deployment.

```bash
cd /opt/openledger
P2_RELEASE_SHA='REPLACE_WITH_REVIEWED_40_CHARACTER_COMMIT'
P2_RELEASE_TREE='REPLACE_WITH_REVIEWED_40_CHARACTER_TREE'
test "$(git rev-parse HEAD)" = "$P2_RELEASE_SHA"
test "$(git rev-parse HEAD^{tree})" = "$P2_RELEASE_TREE"
test -z "$(git --no-optional-locks status --porcelain --untracked-files=all)"
python3 deploy/check-p2-release.py
docker build --target web \
  --build-arg OPENLEDGER_RELEASE_COMMIT="$P2_RELEASE_SHA" \
  --build-arg OPENLEDGER_RELEASE_TREE="$P2_RELEASE_TREE" \
  --build-arg OPENLEDGER_SOURCE_DIGEST="$(python3 deploy/source-fingerprint.py)" \
  --iidfile /tmp/openledger-reviewed-image.id .
```

Alternatively, the preparation script performs those source checks, builds the
image, captures its immutable ID, and creates the candidate manifest in one call:

```bash
bash deploy/build-reviewed-release.sh --commit "$P2_RELEASE_SHA" \
  --manifest /tmp/openledger-reviewed-release.json
```

Inspect the image and full acceptance evidence. Substitute its exact `sha256:…`
ID into this command; a tag is refused. Preserve the manifest alongside release
acceptance evidence. It is not a cryptographic signature or deployment approval.

```bash
python3 deploy/release-manifest.py create \
  --commit "$P2_RELEASE_SHA" \
  --image 'REPLACE_WITH_REVIEWED_SHA256_IMAGE_ID' \
  --manifest /tmp/openledger-reviewed-release.json
```

The final release handoff must provide actual values; placeholders are not a
request to deploy. If squash merge changes commit identity, verify tree equality,
rebuild with the final approved commit and review its new image/manifest.

### Activate only after separate deployment authorization

If the host has the old source, fetch the exact approved release, inspect local
changes and switch to that full SHA first. Do not run the old `update.sh`, use an
unqualified pull, reset local changes, or switch to `main`. If the checked-out
source differs from the release manifest, stop and reconcile it.

First run the read-only command with the supplied real release values:

```bash
bash deploy/update.sh --commit "$P2_RELEASE_SHA" \
  --manifest /tmp/openledger-reviewed-release.json --check
```

Only after approval for this same release and successful preflight:

```bash
bash deploy/update.sh --commit "$P2_RELEASE_SHA" \
  --manifest /tmp/openledger-reviewed-release.json
```

The updater rechecks everything, locks concurrent supported updates, stops
public ingress and drains app/worker, verifies they are stopped, creates a
PostgreSQL custom dump and an evidence/settings/secret archive, validates archive
indexes, rechecks source/schema, migrates to the exact target and starts the
matching app/worker. It verifies both process identities and actual container
images before reopening ingress. Records are under `runtime/backups/release-*`.
No passwords, old evidence or database volumes are recreated to fix a failure.

Archive index validation detects corrupt/empty backups; it is not a substitute
for the release acceptance restore rehearsal on disposable PostgreSQL and file
storage. Keep the restore rehearsal record with the candidate. Preserve an
appropriately protected off-host copy of the recovery set.

`--check` never creates files, credentials or backups, starts containers, builds,
pulls images, or changes database state. It requires one already-running database
and an already-loaded reviewed image. Inspect/start a stopped existing database
or load the approved image separately; a preflight cannot choose those for you.

### Failure and compatibility rollback

A failed migration or mismatched runtime leaves app and worker stopped. Inspect
the saved release record. Fix forward using a separately reviewed `p2-e2e-v1`
compatibility release that understands `e2e3c9d1f703`. Never start the prior
pipeline, run Alembic downgrade/stamp, remove volumes or automatically restore a
backup. New evidence/decisions/versions must be retained. Recovery by database
restore requires a separate post-backup data reconciliation plan and approval.

### First installation

Prepare Docker, the reviewed checkout, immutable image and manifest first. The
installer now also requires both pins and refuses an existing configuration:

```bash
bash deploy/install.sh --commit "$P2_RELEASE_SHA" \
  --manifest /tmp/openledger-reviewed-release.json
```

The installer sets up protected runtime credentials and applies the exact schema
before starting the mandatory pipeline. Existing installations use the updater.

## Connect or change provider keys

Sign in to OpenLedger, open **Settings**, and use **Provider connections**. For
OpenAI, the server verifies the key and selected model without generating
content, then stores the key in `runtime/secrets/openai_api_key` with mode 600.
The key is never returned to the browser or written to the ordinary web
settings file.

Never put the API key in GitHub, screenshots, or support messages.

The optional Google Places connection stores its key separately in
`runtime/secrets/google_maps_api_key` with mode 600. Enable Places API (New),
billing and quota in the Google Cloud project, then restrict the key to the
production server and the Places API before connecting it. OpenLedger uses
Places Text Search for organization names; the Geocoding API is not a substitute
for that search. The worker retains only Place IDs, while the case page fetches
business details live without persisting Google content.

The web deployment accepts only the fixed OpenAI HTTPS endpoint by default. To
use another operator-controlled OpenAI-compatible endpoint, set
`OPENLEDGER_ALLOW_CUSTOM_AI_ENDPOINT=true` with `OPENAI_API_BASE_URL` in
`deploy/.env`. Private or special-purpose IP endpoints require the additional
`OPENLEDGER_ALLOW_PRIVATE_AI_ENDPOINT=true` opt-in. Keep both controls disabled
unless the destination is deliberately administered and trusted: requests carry
the API key and investigation evidence. Plain HTTP is accepted only for a
loopback endpoint.

AI assessments use extracted public-profile fields rather than only site names
and URLs. When **cited public-web research** is enabled, OpenLedger uses the
OpenAI Responses web-search tool to corroborate the strongest identity cluster
and displays the returned public sources as clickable links. This is an OpenAI
hosted tool, not a separately deployed MCP server. Disable it in Settings when
an investigation must remain limited to collected evidence.

When cited research is enabled, a second schema-constrained model pass may
propose supported public-biographical fields for Persona. The server accepts
only allowlisted fields and exact URLs returned by the cited research response,
caps confidence, and rejects sensitive or malformed suggestions. Accepted
suggestions enter the Persona review queue as pending. AI cannot approve a
record, and repeating analysis never clears an analyst rejection.

Each case also has a persistent chat workspace. All messages remain in
PostgreSQL with their actor, optional Persona target, citations, model, and
proposal result. Chat reuses the configured OpenAI key and model; it does not add
another service. Public-web research is enabled per message. When an analyst
asks to propose supported facts, cited findings and explicit user statements
enter the existing Persona queue as pending records. Inferences and
extrapolations stay in the conversation and are never stored as Persona facts.

Category and country filters are selected in the new-investigation form and are
stored with that case rather than applied globally. Country codes describe where
sources are focused; they do not assert or filter the subject's location.
Selecting `ID` keeps broadly available and global platforms while excluding
sources explicitly focused only on other countries. Language filtering is not
offered because the source database has no reliable per-source language field.

When an analyst approves a place without coordinates, OpenLedger sends that
approved label to the configured HTTPS geocoder and stores the returned
bounding-box centroid. Cited AI research may also prefill a visibly approximate
city or region map center. The defaults use Nominatim and OpenStreetMap tiles;
set `OPENLEDGER_GEOCODER_URL` and `OPENLEDGER_MAP_TILE_URL` in `deploy/.env` to
approved internal endpoints for an isolated or sensitive deployment. Set
`OPENLEDGER_GEOCODER_TIMEOUT_SECONDS` to change the default 10-second timeout.

### Investigation-report media and cost boundary

The Persona export is a self-contained investigation PDF: it embeds the first
approved public photograph that passes validation, renders an approved city or
region map center, and places the complete approved provenance register in the
appendix. Missing or unreachable media never blocks the export; the report uses
an explicit placeholder instead. A certainty badge accompanies each displayed
fact. That score is evidence confidence, not a probability of identity, guilt,
ownership, or legal responsibility.

This export does not use Brave Search, a paid map API, a card, or a new API key.
When an analyst requests the PDF, the app may make bounded outbound requests to
the approved photograph host and to the fixed OpenStreetMap tile service. Those
providers can see the request and may throttle or refuse it, and the deployment
still bears its ordinary host bandwidth and compute usage. Map tiles are cached
for at least seven days in `runtime/reports/.map-tile-cache`; the export uses a
recognizable user agent and visible OpenStreetMap attribution. Prevent outbound
access in an isolated deployment to force the safe no-photo/no-map fallback.

Photograph retrieval accepts only bounded public HTTP(S) image responses,
rejects private or mixed public/private DNS answers, pins the validated address,
revalidates every redirect, and verifies the decoded image before embedding it.
The source URL remains available in the provenance appendix; the report profile
shows the actual embedded photograph rather than substituting the URL as the
photograph.

## Security model

This setup keeps port 5000 private and supports two application roles in the
existing protected authentication file. The initial account is an
administrator. Administrators can manage analyst accounts and system settings;
analysts can use every investigation, case, Persona, relationship, timeline,
history, and chat workspace but cannot open Settings or manage users. Every user
may change their own password. Removing a user or changing their password
invalidates that user's existing sessions.

The application login uses protected salted password hashes, 12-hour sessions,
CSRF protection, sign-in rate limiting, password change, and logout. This
application-level RBAC does not replace a client's identity provider, case-level
authorization, classification policy, or audit export. Add those controls before
connecting multi-team production data. Gunicorn intentionally runs one web
worker with multiple threads. Investigation execution belongs to the separate
worker service; job state and replayable progress events are stored in
PostgreSQL, while report files remain in the mounted runtime directory for
compatibility.

The application and worker run as unprivileged UID/GID 10001. Docker excludes
the entire `runtime/` directory and deployment environment files from the image
build context so credentials, reports, and database dumps cannot be copied into
an image layer.

The generated discovery graph is the only report permitted to render in a
same-origin iframe. Its path is strictly allowlisted and the iframe is sandboxed
to scripts without same-origin DOM access. All other application pages and
reports retain `DENY`/`frame-ancestors 'none'` anti-framing controls. Flask owns
this route-specific policy so the reverse proxy must not replace it globally.

Deleting an investigation from History permanently removes its metadata,
reports, graph, and cached AI assessment from the mounted runtime directory.

Use OpenLedger only for lawful, authorized investigations. AI summaries are
analytical assistance and must be verified against the underlying profiles.

## Required acceptance before a release is approved

`OpenLedger persistence safety / postgres-and-container` must pass on the exact
candidate commit. It checks a fresh PostgreSQL migration, migrates a separate
legacy database with existing cases and evidence, checks the repeated backfill,
exercises immutable QC on PostgreSQL, runs the complete `test_pipeline*` suite
with the 50,000-observation load gate enabled, requires the real application
journey through research and final projection, independently extracts PDF text
to check every curated fact and observation,
and rejects any skipped or missing mandatory test. It then builds the real image,
starts both real app/worker entrypoints, compares their source/build/schema
attestations, and verifies that both refuse the old schema even when an environment
flag requests disabling release checks. The saved CI artifact contains test and
container attestation evidence, not real cases or production secrets.

All regression pytest jobs use the existing private loopback-only network
namespace, which also isolates native transports and child processes. The
PostgreSQL test job connects through its disposable service's filesystem socket
and passes its required test environment explicitly after the privilege drop.
Dependency installation, migrations and disposable-container checks remain
separate from the offline regression process. A failed namespace setup fails the
job; it cannot fall back to host-network tests.

The workflow executes the exact PR head, including stacked P2 review branches.
The continuation's verified starting commit is PR #55 head
`f004e719d6bd23e79850379fba67956355736ce2`, whose tree
`f84724316c3dea6d91a05dd64b6cf20cca45ad95` matches the audited P2 content. A later
reviewed squash commit must have its tree reconciled explicitly before rebuilding
and approving its new image/manifest; release selection never follows `main`.

The workflow is mandatory in this release procedure. Configuring GitHub branch
protection to require its check is a separate repository setting; this source
change does not claim to have changed that setting or to have run remote CI.
A machine without PostgreSQL or Docker cannot satisfy these gates with SQLite or
mocked-host results. The current implementation environment lacked a Docker
executable and could not install its PostgreSQL service; those local limitations
must remain visible until the real CI/staging runs pass.

An old `docker compose pull` command cannot obtain this new pipeline. Before any
update, obtain the separately approved new source checkout, review the actual
CI result and image, and use `build-reviewed-release.sh` plus the pinned manifest
sequence above. A plain pull, old updater, or a restart of the existing image
continues using that existing release. The new supported updater refuses missing
pins and never substitutes the old pipeline if migration or readiness fails.


### Reliability acceptance and disposable recovery

The P2 reliability continuation adds nine tables for shared request/provider
state, connector page/checkpoint/version/receipt records, and read projections.
Both migration revisions embed their frozen schema. Existing evidence and
curated manifests are preserved byte-for-byte; existing projection scopes are
marked dirty for an explicit rebuild. A populated pipeline refuses downgrade.

`openledger-persistence.yml` requires PostgreSQL engineering results without
skips, real Chromium interaction with the Flask operator/QC screens, a disposable
PostgreSQL 17 dump/restore rehearsal, and app/worker container identity checks.
The container checks reject both earlier database revisions. The general Python
regression workflow also runs when the PR targets `codex/rollback-to-p2`.

`deploy/ci-rehearse-recovery.py` creates two fresh named CI databases, upgrades
synthetic historical evidence, seeds all reliability ledger families, and
compares every restored row and count. It verifies restored immutable guards and
refuses a populated downgrade without changing data. It does not authorize a
production restore, prove production data completeness, or run an old pipeline.
CI retains JUnit, screenshots, database reconciliation and container attestations
under `runtime/ci/`. A missing acceptance case or skip is a release failure.
