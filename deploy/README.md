# OpenLedger DigitalOcean deployment

This directory provides the supported single-Droplet deployment for the
Nexorus OpenLedger fork.

It runs:

- the Maigret web target as the private application container;
- Caddy as the only public service on ports 80 and 443;
- automatic HTTPS for the configured domain;
- Caddy Basic Authentication for a small operator team;
- persistent reports and web settings under ../runtime;
- optional server-side OpenAI analysis from the results page.

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

The installer asks for the domain, browser username, browser password, and an
optional OpenAI API key. Secrets are written only to deploy/.env with mode 600.
The password itself is discarded after Caddy's one-way hash is generated.

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

## Change the OpenAI key

Edit /opt/openledger/deploy/.env, replace OPENAI_API_KEY, and then run:

    cd /opt/openledger/deploy
    docker compose up -d --force-recreate app

Never put the API key in browser JavaScript, GitHub, screenshots, or support
messages.

## Security model

This setup is intended for one operator or a small fixed team. It uses one
Caddy Basic Authentication account and keeps port 5000 private. Use
application-level accounts, audit logs, and durable database-backed job state
before exposing OpenLedger as a multi-user service.

Use OpenLedger only for lawful, authorized investigations. AI summaries are
analytical assistance and must be verified against the underlying profiles.
