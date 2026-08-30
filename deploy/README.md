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
- optional server-side OpenAI analysis configured from the protected Settings page.

## Prerequisites

- Ubuntu or Debian DigitalOcean Droplet;
- at least 2 GB RAM, with 4 GB recommended for report generation;
- the domain A record resolving directly to the Droplet;
- inbound TCP 22, 80, and 443 permitted by the DigitalOcean firewall;
- inbound UDP 443 permitted if HTTP/3 is desired.

## First installation

Run these commands from the DigitalOcean browser console or an SSH session:

    apt-get update
    apt-get install -y git
    git clone https://github.com/nexorusio/openledger.git /opt/openledger
    cd /opt/openledger
    bash deploy/install.sh

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

Every `deploy/update.sh` run writes and validates a mode-600, UTC-stamped
PostgreSQL custom-format dump under `runtime/backups` before applying migrations.
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

Apply normal updates after changes have been merged to main:

    cd /opt/openledger
    bash deploy/update.sh

The first update from the original Basic Authentication deployment prompts for
an application username and password before removing the proxy login. Existing
reports, settings, and the protected OpenAI key are preserved.

Reset a forgotten application password from the Droplet console:

    cd /opt/openledger
    bash deploy/reset-password.sh

## Connect or change OpenAI

Sign in to OpenLedger, open **Settings**, and use **AI connections**. The
server verifies the key and selected model without generating content, then
stores the key in `runtime/secrets/openai_api_key` with mode 600. The key is
never returned to the browser or written to the ordinary web settings file.

Never put the API key in GitHub, screenshots, or support messages.

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

## Security model

This setup is intended for one operator account and keeps port 5000 private.
The application login uses a protected salted password hash, 12-hour sessions,
CSRF protection, sign-in rate limiting, password change, and logout. Use a
database-backed identity provider, per-user authorization, and audit logs
before exposing OpenLedger as a true multi-user service. Gunicorn intentionally
runs one web worker with multiple threads. Investigation execution belongs to
the separate worker service; job state and replayable progress events are stored
in PostgreSQL, while report files remain in the mounted runtime directory for
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
