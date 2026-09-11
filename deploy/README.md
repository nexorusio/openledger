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

## First installation

Run these commands from the DigitalOcean browser console or an SSH session:

    (
        set -e
        P2_RELEASE_SHA='REPLACE_WITH_REVIEWED_40_CHARACTER_P2_COMMIT'
        [[ "$P2_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]
        apt-get update
        apt-get install -y git
        git clone --no-checkout https://github.com/nexorusio/openledger.git /opt/openledger
        cd /opt/openledger
        git switch --detach "$P2_RELEASE_SHA"
        bash deploy/install.sh
    )

Check that the detached checkout succeeded before running the installer. P2
operators must use the reviewed P2 commit; the current `main` branch may contain
P3. The installer itself does not select or approve a release.

The installer asks for the domain, application username, application password,
and default OpenAI model. Deployment secrets are written only to protected
runtime files with mode 600. The plaintext password is discarded after a
salted PBKDF2 hash is generated.

Do not commit deploy/.env or runtime data.

The `db` service is private to the Compose network, publishes no host port, and
initializes with PostgreSQL data checksums. Its password is generated into
`runtime/secrets/postgres_password` with mode 600.
The `migrate` service applies explicit Alembic migrations before the application
and worker start. The worker owns investigation execution, so closing or changing
the browser page cannot terminate a running collection. PostgreSQL enforces a
singleton worker lock to prevent two collectors from claiming the same queue.
Stopping a collection preserves already-collected findings as a clearly marked
partial result; a stopped job with no findings is retained as cancelled.

Every successful `deploy/update.sh --commit <full-P2-commit>` deployment writes
and validates a mode-600, UTC-stamped PostgreSQL custom-format dump under
`runtime/backups` before rebuilding or applying migrations. A refused update or
`--check` preflight makes no runtime changes and does not create a backup.
Copy these backups to encrypted off-Droplet storage under the applicable
retention policy; a backup kept only on the same Droplet is not disaster
recovery.

A complete recovery set also needs `runtime/reports` and the protected settings
and secret files. Back those up separately to encrypted, access-controlled
storage; do not commit them to Git or package them into a container image.

## Routine commands

Show status:

    cd /opt/openledger/deploy
    docker compose ps

Inspect logs:

    cd /opt/openledger/deploy
    docker compose logs --tail=200

### Delivering an approved P2 update

The P2 updater requires a full, explicitly reviewed 40-character commit SHA and
an already clean checkout at that exact commit. It never fetches, pulls, or
checks out `main`, a release branch, or a tag. Running it without a commit is an
error. The supplied SHA is the operator's approval boundary; the `p2` marker in
`deploy/release-channel`, known P3 artifact checks, and exact P2 migration-chain
check are extra safeguards against a mistaken release, not cryptographic release
authentication. P3 commits in Git history are permitted because a forward
rollback can restore the P2 tree while retaining its P3 ancestors.

**First activation after restoring a Droplet snapshot:** the updater in that
snapshot is still the old script, which pulls `main`. Do not run it. Obtain the
reviewed P2 release branch and its full SHA from the release instructions. Fetch
that branch, verify the exact SHA, and switch to it before invoking the new
updater. Review local changes first; do not reset or discard them automatically.

The following preparation block stops on any error. Replace both placeholders
with the reviewed release values before running it:

```bash
cd /opt/openledger
P2_RELEASE_BRANCH='REPLACE_WITH_REVIEWED_P2_RELEASE_BRANCH'
P2_RELEASE_SHA='REPLACE_WITH_REVIEWED_40_CHARACTER_P2_COMMIT'
(
    set -e
    [[ "$P2_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]
    P2_CHECKOUT_STATUS=$(GIT_OPTIONAL_LOCKS=0 git status --porcelain --untracked-files=all)
    test -z "$P2_CHECKOUT_STATUS"
    git fetch origin "$P2_RELEASE_BRANCH"
    test "$(git rev-parse FETCH_HEAD)" = "$P2_RELEASE_SHA"
    git switch --detach "$P2_RELEASE_SHA"
    test "$(git rev-parse HEAD)" = "$P2_RELEASE_SHA"
)
```

Only after that block succeeds, inspect the pinned release and run its preflight:

```bash
bash deploy/update.sh --commit "$P2_RELEASE_SHA" --check
```

Only after the preflight succeeds, apply that same reviewed release:

```bash
bash deploy/update.sh --commit "$P2_RELEASE_SHA"
```

Use the same sequence with a newly reviewed SHA for each later P2 release. Do
not substitute `main`, `HEAD`, a short SHA, a branch name, or an automatically
resolved latest commit for the reviewed literal SHA. `docker compose pull`,
restarts, and Docker Engine upgrades do not select application source code; the
supported app update is the pinned command above. Direct `git pull` or Compose
build/deploy commands bypass these updater guards.

Before any runtime writes, package installs, backups, builds, migrations, or
service changes, the updater reads the existing running `openledger` database
using a read-only SQL session. Its Alembic revision must be exactly the P2 head
`b3e9d7c4a610`. A missing or stopped database, missing/empty version table,
multiple heads, an older revision, or a later/P3 revision such as `c4f8a2d6e901`
stops the update. If the database is stopped, inspect and start the existing P2
database separately; the preflight never starts or creates a database for you.
Older databases need a separately reviewed migration plan. A P3 database needs
a separately reviewed recovery plan; this updater never downgrades, stamps, or
deletes its data. The restored P2 snapshot should already be at the required
revision, so this release needs no schema upgrade.

Do not change the checkout or run another deployment during the update. The
updater checks the commit, tree, and database revision again after verifying the
backup and before building. It then builds the local application image and
starts the existing Compose deployment; the ordinary migration service runs
against the already verified P2 head.

Profile discovery has server-owned Focused/Exhaustive budgets, durable
cancellation and worker leases, provider circuit breakers, seven default-on P1
operational flags, and a separate default-off native search-first capability.
The same flag and provider settings must reach both the app and worker. The
supported no-subscription path runs a pinned private SearXNG container only
when explicitly enabled; it has no public port or paid API credential. For the
exact capacity checks, staged enablement, validation, restart, and rollback
commands, use the
[profile discovery operations runbook](../docs/profile-discovery-operations.md).

The first update from the original Basic Authentication deployment prompts for
an application username and password before removing the proxy login. Existing
reports, settings, and protected provider keys are preserved.

Reset a forgotten application password from the Droplet console:

    cd /opt/openledger
    bash deploy/reset-password.sh

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
