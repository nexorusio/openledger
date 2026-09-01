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

Country-specific adapters remain independent additions to the global contract. The first active country adapter queries the French National Enterprise Directory only when the requested jurisdiction is `FR`; it is not used as a fallback or authority for another country. Organization selection and public-officer normalization are source-neutral: any governed registry adapter that returns the shared legal-entity and named-officer structure can propose pending `full_name`, backward-compatible `company`, and `occupation` claims. Every proposal retains the actual registry engine, identifier, jurisdiction, public role, and record URL. Dates of birth, nationality, and other unnecessary personal fields are excluded from normalization and storage.

Both adapters use fixed HTTPS origins, redirects disabled, a thirty-second timeout, a one-megabyte response cap, no credential, at most twenty upstream rows, and no more than five retained entity candidates. A registry failure cannot fail Wikidata or another registry, and no result enters canonical Persona or relationship views without analyst approval.

## Official-domain technical context

An analyst may explicitly opt in to domain context and supply one official website URL. If no URL is supplied, a uniquely resolved Wikidata organization's `P856` website may provide the single target. OpenLedger does not guess domain variants and does not download the website. It queries only the fixed Cloudflare DNS-over-HTTPS origin for current `A`, `AAAA`, `MX`, and `NS` records, with redirects disabled, a twenty-second timeout, a 128 KB response cap per query, twenty rows per type, and forty retained rows overall.

DNS records are observation-only. They may describe current web routing, mail handling, or delegated nameservers, but they cannot create Persona claims and cannot establish incorporation, ownership, staff location, or operating jurisdiction. The case workspace states this limitation beside every result. Automatic registrar/RDAP collection remains deferred because authoritative endpoints vary by top-level-domain registry; following dynamic endpoints would expand the governed network boundary, while registrar geography would still not prove business operations. The workspace provides a bounded ICANN lookup link for manual corroboration.

The supplied organization page may expose up to three same-domain organization-context links, for a maximum of four pages total. Every page repeats DNS resolution, public-IP enforcement, resolved-IP pinning, response limits, same-domain redirect controls, and the shared timeout. Structured addresses and exact address-like text in organization, contact, office, location, or footer contexts become pending organization-location observations with the exact page URL. The extractor supports language-neutral HTML/Schema.org signals plus bounded multilingual address markers. Residential or explicitly private address labels are blocked. An organization address is never silently attached to a person, treated as a registered office, or converted into an operating-footprint conclusion.

## Cited external company-profile research

When the existing server-side OpenAI key and AI web-enrichment setting are available, an affiliation investigation may also run one bounded cited public-web research turn for the exact organization. This is an extension of the existing case-chat research boundary, not a direct LinkedIn, Google Maps, or public-directory scraper. It uses the already configured model and key; it adds no credential, service, or source-specific session.

The model response is followed by a strict non-web extraction pass and server-side validation. Every retained finding must use an exact citation URL returned by the research turn, state its identity-match basis, and remain a pending organization observation. Professional-profile, map-listing, and directory evidence is capped below authoritative sources and explicitly described as third-party and potentially stale. A headquarters label is retained only when the cited source explicitly uses it; an address otherwise remains a business-address observation. Ambiguous, uncited, private, residential, and malformed findings are discarded. Coordinates are optional observation metadata and never establish a complete operating footprint.

This path never creates an officer Persona, copies an organization address onto a person, or approves a canonical claim. LinkedIn and consumer Google Maps pages are not directly scraped. A failure of either the research turn or the structured extraction degrades only this optional source; deterministic website, registry, Wikidata, DNS, and configured provider results still finalize.

## Credentialed Google Places provider exception

An administrator may connect an operator-owned Google Maps Platform key when Places API (New), billing, quota, a server application restriction, and a Places API restriction are enabled. This explicit exception is not listed in the credential-free governed-source registry and does not weaken that registry's admission rules. Geocoding is not used for organization-name discovery: the worker calls the fixed Places Text Search (New) endpoint once for the exact organization name plus the operator-supplied jurisdiction when available.

The request uses a fixed HTTPS origin, redirects disabled, a fifteen-second shared timeout, a 256 KB response cap, and at most five candidates. The key is sent only in the `X-Goog-Api-Key` header and is excluded from job options, database results, provenance URLs, logs, page HTML, and browser JavaScript. Search results durably retain only stable Place IDs and generated Google Maps provenance links. The case workspace may use those IDs to fetch bounded Place Details live; the returned business name, formatted address, status, types, URI, and any other Google content are never written to PostgreSQL or report artifacts.

Every displayed result is labelled as a pending third-party business-listing lead. It is not a legal-registry record, first-party statement, verified headquarters, or complete operating footprint. Explicitly private or residential labels and non-business geocoding results are blocked. Google data never creates a Persona, person location, organization selection, approved claim, or relationship projection. A provider failure degrades only this source, and a zero result is an evidence gap rather than proof that the organization has no operating location.

The evaluated HarvestAPI LinkedIn Profile Search actor on Apify remains excluded. It is a credentialed, paid third-party people-profile scraper rather than a company-location API; it expands personal-data collection, depends on an opaque community actor and platform scraping compliance, and does not supply a stable first-party provider contract suitable for automatic OpenLedger collection. LinkedIn company pages may still appear as cited public-web research or explicit manual evidence, always pending review.
