# OpenLedger DigitalOcean deployment

This directory provides the supported single-Droplet deployment for the
Nexorus OpenLedger fork.

It runs:

- the OpenLedger Flask app behind a supervised Gunicorn WSGI server;
- Caddy as the only public service on ports 80 and 443;
- automatic HTTPS for the configured domain;
- a branded OpenLedger login with protected password management;
- persistent reports, investigation history, and web settings under ../runtime;
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

AI assessments use extracted Maigret profile fields rather than only site names
and URLs. When **cited public-web research** is enabled, OpenLedger uses the
OpenAI Responses web-search tool to corroborate the strongest identity cluster
and displays the returned public sources as clickable links. This is an OpenAI
hosted tool, not a separately deployed MCP server. Disable it in Settings when
an investigation must remain limited to the collected Maigret evidence.

Country codes under **Source coverage filters** describe where sources are
focused; they do not assert or filter the subject's location. Selecting `ID`
keeps broadly available and global platforms while excluding sources explicitly
focused only on other countries. Language filtering is not offered because the
Maigret site database does not provide reliable per-source language metadata.

## Security model

This setup is intended for one operator account and keeps port 5000 private.
The application login uses a protected salted password hash, 12-hour sessions,
CSRF protection, sign-in rate limiting, password change, and logout. Use a
database-backed identity provider, per-user authorization, and audit logs
before exposing OpenLedger as a true multi-user service. Gunicorn intentionally
runs one worker with multiple threads because live scan coordination is
process-local; completed and failed investigation metadata is written
atomically beside the mounted report files and is rebuilt when the application
restarts.

Deleting an investigation from History permanently removes its metadata,
reports, graph, and cached AI assessment from the mounted runtime directory.

Use OpenLedger only for lawful, authorized investigations. AI summaries are
analytical assistance and must be verified against the underlying profiles.
