# Contributing to OpenLedger

OpenLedger contains proprietary Nexorus material and third-party components
under their own licenses. A pull request must not blur those ownership and
license boundaries.

## Contribution authorization

PT Daya Prana Inovasi does not accept external code contributions without a
written contributor agreement or other written authorization that defines the
license and intellectual-property terms. Opening a pull request does not
guarantee that it can be merged.

Employees and contractors must contribute through an account and agreement
that assigns, or otherwise grants PT Daya Prana Inovasi sufficient rights to,
the submitted work. Do not submit confidential material, copied code, data, or
assets that you are not authorized to provide.

Dependency updates, generated files, and AI-assisted changes must retain the
same provenance review as human-authored changes. Record any third-party source
and its exact license in the pull request.

## Maigret changes

Maigret-derived files remain governed by Maigret's MIT License. Preserve the
upstream copyright and permission notice in
[`LICENSES/MAIGRET-MIT.txt`](LICENSES/MAIGRET-MIT.txt) and do not relabel
Maigret-derived material as exclusively owned by Nexorus.

Changes that belong in the generic Maigret engine or site catalogue should
normally be proposed to [Maigret upstream](https://github.com/soxoj/maigret).
OpenLedger's guarded upstream workflow may integrate reviewed site-database
updates without changing proprietary product files.

## Engineering requirements

1. Branch from the latest `main`.
2. Preserve evidence provenance, pending human review, privacy boundaries, and
   source-scoped failure behavior.
3. Keep changes backward-compatible with existing PostgreSQL data and runtime
   secrets whenever practical.
4. Add regression tests and run the relevant local checks.
5. Never commit deployment secrets, reports, backups, or real investigation
   data.
6. Identify the applicable license for every new file. New proprietary files
   should carry `SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary` and a
   PT Daya Prana Inovasi copyright notice.

Useful checks:

```bash
poetry run pytest tests
poetry run python .github/scripts/check_osint_sources.py
DOMAIN=openledger.example.test \
FLASK_SECRET_KEY=development-only-secret \
docker compose -f deploy/compose.yaml config --quiet
```

See [the repository licensing notice](LICENSE) and
[third-party notices](THIRD_PARTY_NOTICES.md) before submitting a change.
