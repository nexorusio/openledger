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

## Unfurl URL-analysis adapter

Unfurl is disabled by default and receives only exact public profile URLs already marked claimed by native OpenLedger discovery. It runs with remote lookups explicitly disabled, no inherited API-key environment variables, an 80-node limit, a 20-second timeout, a 1 MB output cap, and the shared 20-profile investigation cap. Secret-shaped query parameters are excluded before execution and secret-shaped output nodes are redacted.

The production image pins Unfurl commit `a21ef7ce1896bd8db17aeeb990911877ab839dbe` and version `20260405`. Unfurl requires Python 3.11 and NetworkX 3 while Maigret retains Python 3.10 and NetworkX 2 compatibility, so the Docker build installs it in `/opt/openledger-unfurl`. A small JSON subprocess boundary prevents its dependencies and configuration from changing the OpenLedger worker.

URL components and decoded nodes are deterministic structural evidence only. They attach to the already-pending `social_account` claim at confidence 25, never create a separate identity fact, never establish ownership, and never change a human review decision.

## Wayback CDX adapter

The same opt-in queries only `https://web.archive.org/cdx/search/cdx`. Each request uses the exact claimed URL, `matchType=exact`, HTML/HTTP-200 filters, digest collapse, no redirects, a 20-second timeout, a 1 MB response cap, and at most the latest 10 unique capture rows. Domain, host-prefix, and wildcard searches are not allowed. OpenLedger stores capture metadata and replay locators but does not download archived page content.

An exact capture supports historical presence of the URL, not ownership by the Persona and not the truth of the archived page. It therefore attaches low-confidence evidence to the existing pending `social_account` claim. Empty, mismatched, malformed, unavailable, and rate-limited responses remain diagnostics and do not create claims.

The scheduled source audit performs a small exact-URL CDX contract check. Because the API is unversioned, any response-shape or availability change fails the maintenance signal and requires a reviewed pull request; runtime failures still degrade only the Wayback collector.

## Jurisdiction-scoped legal-entity adapters

An analyst may add a country name, ISO 3166-1 alpha-2 code, or ISO 3166-2 subdivision code when opening a case from an approved affiliation. OpenLedger normalizes that value before collection and retains it in the investigation specification. Registry results are candidates, not confirmed organization claims.

The global adapter queries the credential-free GLEIF API by exact operator-approved name and country, then applies the requested country or subdivision filter locally. It retains at most five bounded Legal Entity Identifier candidates. Because GLEIF covers entities issued an LEI rather than every registered business, a zero result is explicitly not treated as evidence that the business is unregistered.

For country-level `FR` searches, a second independent adapter queries the French National Enterprise Directory. One unique exact legal-name match may propose public natural-person leaders as pending `full_name`, backward-compatible `company`, and `occupation` claims. Every proposal retains the SIREN record and public role as evidence. Dates of birth and nationality returned by the upstream source are deliberately excluded from normalization and storage.

Both adapters use fixed HTTPS origins, redirects disabled, a thirty-second timeout, a one-megabyte response cap, no credential, at most twenty upstream rows, and no more than five retained entity candidates. A registry failure cannot fail Wikidata or another registry, and no result enters canonical Persona or relationship views without analyst approval.

## Official-domain technical context

An analyst may explicitly opt in to domain context and supply one official website URL. If no URL is supplied, a uniquely resolved Wikidata organization's `P856` website may provide the single target. OpenLedger does not guess domain variants and does not download the website. It queries only the fixed Cloudflare DNS-over-HTTPS origin for current `A`, `AAAA`, `MX`, and `NS` records, with redirects disabled, a twenty-second timeout, a 128 KB response cap per query, twenty rows per type, and forty retained rows overall.

DNS records are observation-only. They may describe current web routing, mail handling, or delegated nameservers, but they cannot create Persona claims and cannot establish incorporation, ownership, staff location, or operating jurisdiction. The case workspace states this limitation beside every result. Automatic registrar/RDAP collection remains deferred because authoritative endpoints vary by top-level-domain registry; following dynamic endpoints would expand the governed network boundary, while registrar geography would still not prove business operations. The workspace provides a bounded ICANN lookup link for manual corroboration.

Exact addresses published by an official website are handled through the existing cited case-chat research path. The generated research brief requires direct citations and separates legal registry records, organization-published statements, and technical infrastructure. Any supported Persona proposal remains pending human review; an organization address is not silently attached to a person.
