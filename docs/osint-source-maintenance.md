# Governed OSINT source maintenance

OpenLedger treats OSINT Framework as a discovery catalog, not as a runtime dependency. Each selected source must have an explicit contract in `config/osint-sources.json` before its data can enter the case and Persona workflow.

## Admission rules

A source is eligible only when the entire normal collection path is free, needs no registration or credential, remains relevant to a case or Persona claim, and can run inside the existing worker without adding another service. Repository-backed tools must also show maintenance activity within the preceding 365 days. Native public APIs are reviewed against their official version, access, and rate-limit documentation instead.

Every integration must declare its trigger, accepted target, network boundary, timeout, result cap, claim mapping, observation-only metadata, and review deadline. Source-derived claims remain pending until a human approves them.

## Update flow

1. The pull-request audit validates the source contract and its review deadline.
2. The scheduled audit runs the same validation plus a non-sensitive live contract check against a fixed public fixture.
3. A failed audit creates a maintenance signal; it does not update code or dependencies automatically.
4. A maintainer reviews upstream changes, updates fixtures and mappings, runs the focused and Docker-compatible test suites, and submits a normal pull request.
5. Runtime collector failures degrade only that collector. Existing discovery results still finalize and remain reviewable.

## GitHub public-profile adapter

The initial adapter is disabled by default. When enabled, it receives only exact GitHub profile URLs that native OpenLedger discovery has already marked as claimed. Requests use the fixed `https://api.github.com` origin, the pinned REST API version, no redirects, no credential, a 15-second timeout, a 1 MB response cap, and at most 20 unique profiles per investigation.

Numeric account ID, public name, company, location, biography, website, avatar, and linked X handle may become pending claim candidates. Activity timestamps and counts are retained only as observation metadata. Organization records, missing profiles, and rate-limited responses do not produce Persona claims.
