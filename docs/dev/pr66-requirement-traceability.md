# PR #66 requirement traceability and development acceptance gate

Status: repair-branch audit, 14 September 2026. This document records evidence;
it does not authorize merge, deployment, or promotion to `main`.

## Release identity and decision rule

| Item | Exact value or rule |
| --- | --- |
| Repair branch | `codex/fix-pr66-p1-traceability` |
| Branch base | `dev` commit `9266a38c7aab203799e0ad2aa28a7a361ebc4fdc`, tree `04ab8faaa9ff1a2f99fdfbe069ad6bcd68e67ced` |
| Included feature change | PR #66, squash commit `b79368390392ead9e443ad4cb37bd0c3dca38481` |
| Included registry repair | PR #67, squash commit `9266a38c7aab203799e0ad2aa28a7a361ebc4fdc` |
| Development URL | `https://dev-openledger.nexorus.io` |
| Previous deployed development image | `sha256:be0f3c4545a227428cf58e38ac4dd01836d45d9550ded5f57643ed2c88188185` |
| Expected schema before this code-only repair | `e2e3c9d1f703` |
| Repair release identity | The PR head commit/tree, CI image digest and schema attestation must be recorded after CI. This document cannot self-record the commit containing itself. |

The previous deployment is baseline evidence only. It is not evidence that this
repair branch passed, and the repair must not be deployed under the current
authorization.

The only supported sequence is repair PR to `dev`, required checks, review,
separate merge authorization, exact merged-`dev` staging deployment, human
acceptance, and only then a separate `dev` to `main` promotion PR. See
[`development-promotion.md`](development-promotion.md).

## Status vocabulary

| Status | Meaning |
| --- | --- |
| `Linked` | Implementation and an automated regression are identified. The repair PR checks still decide whether that evidence passes on the exact head. |
| `Repair` | A confirmed PR #66 P1 defect is fixed on this branch and has a new regression; CI is still required. |
| `Human` | Source/tests can support the behavior, but the exact deployed development build needs operator or visual acceptance. |
| `Configured` | The result depends on development-only provider configuration; disabled/unavailable must be shown truthfully. |
| `Conditional` | A claim is allowed only when its named empirical or policy prerequisite is met; otherwise the product must abstain. |

No row marked `Linked` means “observed on staging.” All mandatory automated
checks and all mandatory `Human`/`Configured` checks below must be recorded
against the same development commit, tree, image and schema before promotion.

## Confirmed P1 repairs

| ID | Requirement and defect | Enforcement point | Regression | Status |
| --- | --- | --- | --- | --- |
| P1-01 | “Cross-check all approved evidence” must work when the Persona has approved affiliation/location evidence but no username, URL, email, phone or full-name identifier. PR #66 built cited questions and then sent an empty identifier form through the public parser, which rejected it. | `maigret/web/app.py::_launch_approved_pipeline_discovery`; `maigret/web/investigation_input.py::build_approved_research_plan`; the explicit internal gate in `maigret/web/case_store.py::CaseStore.repeat_persona_investigation`; question validation in `maigret/web/pipeline_enqueue.py::approved_research_questions` | `tests/test_pipeline_routes.py::test_approved_discovery_uses_cited_research_for_affiliation_without_identifier`; `tests/test_case_store.py::test_identifier_free_repeat_requires_server_approved_research`; existing `tests/test_pipeline_query.py::test_approved_persona_research_questions_route_as_active_cited_ai_tasks` | `Repair` |
| P1-02 | Persona map popups must not insert attacker-controlled `precision` or `label` strings as HTML. PR #66 escaped the label but interpolated precision into popup HTML. | `maigret/web/templates/pipeline_persona.html` creates DOM nodes and assigns untrusted values through `textContent`/`createTextNode` before `bindPopup` | `tests/test_pipeline_routes.py::test_persona_map_popup_uses_text_nodes_for_untrusted_precision`; complementary HTML escaping coverage in `tests/test_report.py` and `tests/test_chat_presentation.py` | `Repair` |

Repair review also identified and fixed a P2 limit mismatch: the approved
workflow and validator permit 100 questions of up to 10,000 characters, while
the generic query-context path accepted only 24 and truncated each to 2,000.
`maigret/web/pipeline_query.py` now applies the approved-research bounds to both
planning and prerequisite matching. The regression
`tests/test_pipeline_query.py::test_approved_research_preserves_25_full_length_batches`
proves that more than 24 complete batches reach active cited-research tasks.

The identifier-free path is intentionally not a general bypass. Public intake
still requires validated identifiers. An empty-identifier rerun is accepted only
when the internal caller sets the explicit gate and the specification contains
bounded, server-generated questions marked `approved_pipeline_findings` with AI
context enabled.

## Approved P2 acceptance matrix (A01–A28)

This is the requirement-to-current-evidence crosswalk for the acceptance matrix
in [`p2-pipeline-approved-plan.md`](p2-pipeline-approved-plan.md). Test names are
the stable evidence identifiers; CI/JUnit supplies their executed result.

| ID | Requirement | Implementation | Automated evidence | Remaining development acceptance | Status |
| --- | --- | --- | --- | --- | --- |
| A01 | Username, full name, email and phone work independently and in combinations without a username dependency. | `investigation_input.py`, `pipeline_query.py`, `pipeline_execution.py`, `pipeline_store.py` | `test_email_and_phone_submission_creates_one_subject_without_username`; `test_each_input_routes_independently_without_spurious_username`; `test_real_app_accepts_and_executes_each_input_without_username_dependency` | Run one synthetic input of each type and one mixed case. | `Linked` + `Human` |
| A02 | Name, handle, `@handle` and exact profile URL retain raw provenance while avoiding duplicate tasks. | `investigation_input.py`, `pipeline_query.py`, `pipeline_job_context.py` | `test_username_handle_and_profile_url_are_one_canonical_account_target`; `test_same_handle_and_url_preserve_all_three_raw_inputs_without_triplicate_tasks`; `test_exact_profile_url_does_not_fan_out_to_cross_platform_username_tasks` | Inspect the exact plan in the development UI. | `Linked` + `Human` |
| A03 | Type overrides, ambiguous phone country, unsupported URLs and selected aliases fail safely without guesses. | `investigation_input.py`, `pipeline_query.py`, investigation builder UI | `test_numeric_typed_correction_is_honored_and_ambiguous_phone_needs_country`; `test_analyst_can_edit_and_deselect_ranked_aliases`; `test_unsupported_or_nonprofile_url_is_retained_without_guessed_username` | Exercise operator correction and alias selection. | `Linked` + `Human` |
| A04 | Every executable/configured adapter has an explicit route and state. | `pipeline_query.py`, `connector_registry.py`, `config/osint-sources.json` and its static-governance test | `test_every_existing_adapter_and_catalog_source_has_a_route_contract`; `test_osint_source_registry_passes_static_governance_audit`; `test_registry_people_contract_is_source_neutral_for_governed_adapters` | Compare the Settings/source inventory to actual development configuration. | `Linked` + `Configured` |
| A05 | Instagram/TikTok and other platform outcomes distinguish found, blocked, timeout and excluded; failure is not absence. | platform adapters, `pipeline_execution.py`, `pipeline_graph_viewer.py` | adapter suites in `test_profile_search_instagram.py`, `test_profile_search_tiktok.py`, `test_profile_search_facebook.py`, `test_profile_search_threads.py`, `test_profile_search_x.py`; `test_blocked_and_timeout_diagnostics_are_never_negative` | Confirm enabled and intentionally unavailable platforms display their actual reason. | `Linked` + `Configured` |
| A06 | Multiple emails and phone-only public evidence remain separate tasks in one subject/case. | `pipeline_query.py`, `pipeline_execution.py`, `pipeline_store.py` | `test_multiple_email_tasks_share_case_and_subject_but_keep_per_address_input_ids`; `test_nonusername_requests_have_compatible_tasks`; app journey coverage | Run development-safe synthetic addresses/numbers only. | `Linked` + `Configured` |
| A07 | Equivalent account/claim results consolidate without losing any observation lineage. | `pipeline_consolidation.py`, `pipeline_store.py`, `pipeline_evidence.py` | `test_three_engines_one_account_and_claim_all_lineages`; `test_consolidation_three_engines_preserves_all_evidence` | Inspect group drill-down and observation counts. | `Linked` + `Human` |
| A08 | Mirrors, snippets and model summaries cannot manufacture independent support. | `pipeline_consolidation.py`, `pipeline_assessment.py` | `test_mirrors_snippets_models_do_not_manufacture_independent_support`; `test_known_content_copies_union_roots_and_unknown_derivation_abstains` | None beyond exact-head CI unless provider fixtures change. | `Linked` |
| A09 | Same handles across platforms/people, handle reuse and stable-ID conflicts never auto-merge identities. | `pipeline_consolidation.py`, `pipeline_store.py` | `test_handle_across_platforms_is_never_same_account`; `test_stable_id_rename_retains_url_and_handle_history`; `test_handle_reuse_and_missing_stable_id_remain_reviewable` | Demonstrate split/reassignment in the review UI. | `Linked` + `Human` |
| A10 | Contradictions and historical/current affiliations remain distinct and can block finalization. | `pipeline_consolidation.py`, `pipeline_evidence_integrity.py`, QC store rules | `test_contradictory_single_value_claims_preserved`; `test_temporal_affiliations_are_not_assumed_single_valued`; `test_qc_contradiction_requires_explicit_exclusion_and_limitation` | Demonstrate the material-conflict blocker. | `Linked` + `Human` |
| A11 | Replay/backfill is idempotent; genuine later observations are retained. | connector ingestion, consolidation/store idempotency keys, migration backfill | `test_replay_idempotent_later_attempt_retained_and_mutation_rejected`; `test_idempotent_observations_preserve_source_and_attempt_lineage`; backfill tests | PostgreSQL repeated-backfill gate must pass. | `Linked` |
| A12 | Operator assessment works without AI or numerical probability. | pipeline workspace/review routes, `pipeline_assessment_runtime.py` | `test_optional_decision_note_and_review_progress_are_persisted`; browser journey review path | Disable AI in development and complete manual include/exclude review. | `Linked` + `Human` |
| A13 | Heuristics are not labeled as probability; unsupported/stale scopes abstain. | `pipeline_probability.py`, `pipeline_assessment.py`, assessment projections | probability and assessment suites, including `test_unknown_scope_or_changed_source_requires_revalidation` and `test_timezone_unknown_is_undated_and_no_numeric_probability_is_allowed` | Confirm the UI says unavailable/abstained, not a fabricated percentage. | `Linked` + `Human` |
| A14 | Numerical probability requires a locked, independently labeled empirical validation set and published metrics. | probability artifact registry and validation guards | `test_synthetic_evaluation_cannot_be_exported_as_reviewed_production_model` plus probability suite | No synthetic test can satisfy empirical readiness. If no approved artifact exists, the runtime must abstain. | `Conditional` |
| A15 | Corrections, reversals and successive versions append immutable actor/time/reason history. | `pipeline_store.py`, review/version routes | `test_qc_is_explicit_and_final_is_immutable`; correction/reversal route tests | Inspect history in development. | `Linked` + `Human` |
| A16 | Export, confidence, job completion or claim approval cannot silently create a Final Persona. | QC/version state machine and export projections | `test_report_snapshot_exports_operator_approved_findings_without_qc`; `test_qc_is_explicit_and_final_is_immutable` | Verify Draft labelling before QC. | `Linked` + `Human` |
| A17 | Missing provenance, unresolved conflict, unmet requirement or stale submitted version blocks QC transactionally. | `pipeline_store.py`, QC routes | `test_unretained_or_missing_provenance_blocks_qc`; `test_stale_qc_is_rejected_and_final_manifest_survives_working_decision`; `test_qc_material_findings_and_structured_scope_remain_blockers` | Demonstrate failed QC with a precise actionable reason. | `Linked` + `Human` |
| A18 | Only an authorized operator can finalize; background work cannot. | authentication/RBAC/CSRF decorators and QC store transition | `test_authentication_csrf_role_and_foreign_scope_block_mutations`; browser acceptance | Test analyst/admin roles using development accounts. | `Linked` + `Human` |
| A19 | Failed QC creates bounded targeted research in the same case, followed by revision and reapproval. | QC/research records, `pipeline_routes.py`, `pipeline_job_context.py`, `pipeline_enqueue.py` | `test_failed_qc_research_same_case_revision_then_final`; `test_real_app_manual_review_qc_research_worker_and_final_projection`; P1-01 regressions | Demonstrate both identifier-backed and affiliation-only approved-evidence paths. | `Repair` + `Human` |
| A20 | Duplicate/unsatisfiable requirements, budget exhaustion and retries terminate visibly without unrelated cases or loops. | query/store state machine, execution budgets and retries | `test_followup_retains_rejection_lineage_and_scope_and_budget`; `test_qc_cannot_approve_and_open_new_research_in_one_action`; execution-budget suite | Inspect budget-limited/unsatisfiable states. | `Linked` + `Human` |
| A21 | Fresh contradiction after final approval cannot mutate the published final; it creates review-needed successor state. | immutable versions and dirty projections | `test_stale_qc_is_rejected_and_final_manifest_survives_working_decision`; final/version store tests | Compare final manifest before and after new evidence. | `Linked` + `Human` |
| A22 | Final Persona, graph, API and PDF agree and expose all included claims/evidence beyond 120 items. | `pipeline_routes.py`, `pipeline_pdf.py`, `persona_pdf.py`, graph projections | `test_projection_graph_retains_more_than_120_claims_and_all_sources`; `test_pdf_text_register_contains_every_curated_fact_and_observation`; `test_frozen_draft_and_final_pdf_have_same_manifest_identifiers` | Compare the four representations for one development final version. | `Linked` + `Human` |
| A23 | Cross-case evidence reuse preserves independent decisions and rejects foreign scope; shared evidence is not a personal relationship. | pipeline store scope/foreign keys and combined-case projection | `test_cross_case_evidence_and_versions_rejected`; `test_case_scope_cannot_cross_and_reuse_keeps_source_lineage`; relationship tests | Inspect a two-case synthetic example. | `Linked` + `Human` |
| A24 | Stop, timeout, throttle, worker death, lease loss and restart preserve committed evidence with bounded retries and one truthful terminal state. | persistent jobs, execution budgets, provider circuit breaker, leases | persistent-job and execution fault suites, including `test_failed_engine_retries_keep_partial_evidence`, `test_crash_recovery_is_atomic_fenced_and_resumes_same_request`, and cancellation tests | Stop a live synthetic development job and verify quiescence/reconnect. | `Linked` + `Human` |
| A25 | Legacy P2 migration/backfill preserves evidence/reviews, never auto-finalizes and is repeatable. | Alembic revisions, backfill/recovery scripts, release guard | `test_legacy_backfill_is_checkpointed_idempotent_and_never_finalizes`; PostgreSQL migration/recovery acceptance | Mandatory PostgreSQL legacy migration and restore artifacts. | `Linked` |
| A26 | Deletion cannot silently erase final provenance; archive/withdraw/purge behaviors remain explicit. | case store deletion/archive, version withdrawal and retention rules | `test_case_deletion_purges_pipeline_history_after_terminal_completion`; `test_withdraw_preserves_final_version_and_audit`; active-case refusal tests | Test stop, archive, exact-name delete and withdrawal separately. | `Linked` + `Human` |
| A27 | Update refuses missing SHA, wrong channel/tree/schema, dirty checkout and unknown migration. | `deploy/check-p2-release.py`, reviewed-build/update scripts and manifest | deployment/release/runtime-guard suites | Run read-only preflight against the exact candidate; do not deploy under this authorization. | `Linked` |
| A28 | Exact-head CI includes full regression, PostgreSQL migration/restore, 50k reconciliation, Chromium workflow, image build and app/worker/schema identity. | `.github/workflows`, `deploy/ci-test-selection.py`, `deploy/ci-rehearse-recovery.py` | required GitHub checks and their retained artifacts | Every mandatory job must pass with no missing or skipped mandatory test. | `Human` until CI evidence exists |

## Product and feature-preservation traceability

These rows cover the requested interface, source, Persona and existing-product
behavior that is broader than A01–A28. They prevent a pipeline change from
silently removing features already used on the previous machine or production
line.

| ID | Requested behavior | Implementation and automated evidence | Development acceptance | Status |
| --- | --- | --- | --- | --- |
| UX-01 | Approved O/ identity, black theme, restrained purple and Alliance No. 2 typography. | `templates/base.html`, `static/openledger.css`, `static/openledger-v2.css`, packaged fonts; template/static tests | Visually compare login, new investigation, Settings, workspace, Persona, graph and report at desktop and mobile widths. | `Human` |
| UX-02 | Collapsible navigation, clear non-gimmicky copy, compact settings and no overlapping controls. | base/navigation templates, `settings.html`, responsive CSS; `test_page_and_persona_titles_share_the_global_sticky_rule` and profile discovery UX tests | Exercise collapsed/expanded sidebar, keyboard focus and Settings at 360, 768 and desktop widths. | `Human` |
| UX-03 | Login uses the approved background/image treatment and transparent glass form without weakening authentication. | `templates/login.html`, static assets/CSS, login redirect/security tests | Inspect image loading, contrast, focus, errors and redirect behavior on the development hostname. | `Human` |
| UX-04 | Only Quick and Full collection modes are operator-facing and differ honestly in time/scope. | `execution_budget.py`, investigation builder, discovery policy | `test_investigation_builder_uses_canonical_mode_names_and_fixed_budgets`; execution-budget tests | Compare plan, estimated scope and live timing labels for both modes. | `Linked` + `Human` |
| UX-05 | Case-scoped source category and country filters persist without asserting subject location. | input plan and source selection | `test_case_source_filter_cannot_include_and_exclude_the_same_tag`; source/site selection suites | Create, reload and rerun a filtered case. | `Linked` + `Human` |
| SRC-01 | Maigret, User Scanner email/username, self-hosted SearXNG/native major-platform search, GitHub enrichment, Unfurl/Wayback and governed name/organization sources remain represented. Optional Google Places/OpenAI are explicit. | connector/source registries, query planner, adapters, Settings | A04 registry/adapter tests; `test_user_scanner_runner.py`; source-specific adapter tests; GitHub/URL persistence tests | For each configured provider, record Active/Conditional/Unavailable/Excluded and its reason; never require a paid provider for an unconfigured environment. | `Linked` + `Configured` |
| SRC-02 | Engine progress includes status, outcome and reason; disabled/blocked/error is never misleadingly “not found.” | task/attempt ledger and workspace/status projections | pipeline execution, persistent jobs, graph viewer and `test_profile_discovery_ux.py` | Observe one success, exclusion and controlled failure. | `Linked` + `Human` |
| SRC-03 | Collection is bounded by server budgets, cancellation, retries, circuit breakers and the exact detector-health registry; HTTP 509 remains deterministic. | execution budget, persistent worker, provider circuit breaker, detector registry/error detection | budget/circuit/reliability suites and PR #67 registry reconciliation | Confirm the exact merged registry tree remains unchanged and exercise stop/retry on development. | `Linked` + `Human` |
| DATA-01 | Same-subject mode creates one Persona; explicit independent mode creates separate Personas. | subject grouping and case store bindings | `test_name_handle_and_selected_aliases_default_to_one_persona`; `test_independent_identifier_mode_keeps_separate_personas` | Verify both modes in the UI. | `Linked` + `Human` |
| DATA-02 | All observations, unsuccessful attempts, conflicts, qualifiers and history remain traceable; false account/person merges can be split or reassigned. | observation/attempt ledgers, consolidation, correction/split routes | A07–A11, A15 and A23 suites; `test_split_preserves_observations_and_requires_new_operator_review` | Inspect observation lineage and perform a split. | `Linked` + `Human` |
| DATA-03 | AI may rank or propose cited pending facts, but cannot approve; the operator remains the approval authority and the UI does not present a misleading per-item “Record decision” step. | AI schema/citation gates, pending review store, pipeline workspace templates | AI enrichment/security tests; `test_cited_ai_proposals_are_pending_idempotent_and_preserve_rejection`; route/browser journey | Run with AI off, then optionally with a development-only key; verify all suggestions remain pending. | `Linked` + `Configured` + `Human` |
| PERS-01 | Persona hero shows approved name/photo/location; the map distinguishes person from organization location and uses approved coordinates/land centroid fallback. | `pipeline_persona.html`, `pipeline-persona.css`, geocoding/location serialization, `persona_pdf.py` | `test_persona_renders_approved_photo_and_persisted_location_map`; coordinate/geocode tests; P1-02 regression | Inspect marker text, map fallback and photo failure behavior. | `Repair` + `Human` |
| PERS-02 | Persona includes reviewed affiliation, position/contact/social/asset/risk/certainty sections without turning unsupported or sensitive inference into fact. | Persona intelligence, review projections, evidence policies | positive-output/section coverage, AI proposal restrictions and claim-review tests | Compare every displayed fact to its review state and evidence. | `Linked` + `Human` |
| PERS-03 | “Cross-check all approved evidence” uses all bounded approved anchors, including affiliation-only Personas, and returns candidates to review rather than auto-approval. | P1-01 enforcement points and cited research pipeline | P1-01 regressions and cited-research execution test | Complete identifier-backed and affiliation-only synthetic runs with cited sources configured. | `Repair` + `Configured` + `Human` |
| PERS-04 | Relationships/graph is complete rather than first-finding-only; every factual edge is traceable and shared attributes are labeled, not asserted as identity. | graph projection, relationship overlay, complete evidence register | `test_projection_graph_retains_more_than_120_claims_and_all_sources`; `test_final_graph_distinguishes_provenance_absence_failures_and_contradictions`; relationship tests | Inspect dense graph pagination/drill-down. | `Linked` + `Human` |
| PERS-05 | PDF export is self-contained, paginated, safe and matches the selected draft/final manifest. | `pipeline_pdf.py`, `persona_pdf.py` | persona/P2 PDF suites and A22 tests | Download and compare one long multilingual final PDF. | `Linked` + `Human` |
| FLOW-01 | An approved affiliation can start a distinct organization investigation without silently converting it into a fact or merging cases. | affiliation branch routes/store/job context | `test_approved_affiliation_can_open_a_separate_investigation_branch`; affiliation selection tests | Run with and without optional organization providers. | `Linked` + `Configured` + `Human` |
| FLOW-02 | Case chat, timeline, relationships, name/account history and combined cases remain durable, scoped and evidence-aware. | case chat/timeline/combined-case stores and views | chat presentation/enrichment, timeline, combined-case, relationship and case-store suites | Open each view after refresh/restart and verify cross-case boundaries. | `Linked` + `Human` |
| FLOW-03 | Stop, archive and delete are separate, safe paths; active work cannot be silently deleted. | persistent job stop, case archive/delete routes/store | `test_active_case_has_a_safe_stop_then_archive_path`; case deletion/archive and CSRF tests | Exercise the three paths on disposable synthetic cases. | `Linked` + `Human` |
| SEC-01 | Admin/analyst authorization, CSRF, XSS-safe rendering/media and foreign-case scope are enforced. | auth decorators, CSRF tokens, templates/DOM construction, safe media/report handling | route security, login, P2 security gate, report/chat escaping tests and P1-02 | Attempt unauthorized/foreign mutations and the exact popup payload in a browser. | `Repair` + `Human` |
| REL-01 | Cases, decisions, chat, evidence and jobs survive refresh/restart; app and worker share exact source/build/schema identity. | PostgreSQL stores, persistent worker, runtime attestation | PostgreSQL persistence, persistent jobs, release identity and container acceptance | Restart both containers and compare attestations plus retained state. | `Linked` + `Human` |
| REL-02 | Migration, backfill, backup/restore and recovery preserve existing P2 data; no legacy/P3 fallback or destructive downgrade is allowed. | Alembic chain, release guards, recovery rehearsal | A25/A27/A28 suites and CI recovery artifact | Restore the disposable CI backup and reconcile counts/hashes. | `Linked` |
| GOV-01 | Work follows repair → `dev` PR → exact development acceptance → `dev`-to-`main` PR. No direct feature PR/push to `main`; merge and deploy are separate authorizations. | `docs/dev/development-promotion.md`, branch-promotion workflow and release scripts | release/deployment workflow tests | Confirm branch rules/check requirements in GitHub before any merge. | `Human` |
| GOV-02 | Public-only, lawful collection and provider retention/privacy/cost restrictions remain explicit; residential/private-network access is blocked. | source registry policies, external evidence bounds, SSRF/provider gates, Settings | connector registry, retention, external evidence and security suites | Review enabled development providers, credentials and retention modes; use synthetic subjects only. | `Linked` + `Configured` + `Human` |

## Exact automated gate

Before this repair can be considered merge-ready, the PR head must pass all
required GitHub checks. In particular, the mandatory persistence job described
in [`deploy/README.md`](../../deploy/README.md) must prove:

1. the complete required Python regression selection, including every
   `test_pipeline*` module and both new P1 regressions;
2. PostgreSQL fresh migration, legacy migration, repeated backfill and
   disposable dump/restore reconciliation;
3. the 50,000-observation consolidation/load invariant;
4. real Chromium interaction through intake, review, QC rejection, targeted
   research, revision, finalization, graph and PDF;
5. real app and worker containers reporting one source/build/schema identity;
6. no missing or skipped mandatory acceptance test.

A green subset is not a green release. A skipped mandatory test, mismatched
identity, unresolved P0/P1 review, or missing artifact keeps this gate closed.

## Exact human development checklist

After a separately authorized merge and development deployment, record the
commit, tree, image and schema once, then perform these checks at
`https://dev-openledger.nexorus.io` using only synthetic subjects:

- authenticate as analyst and admin; verify forbidden, CSRF and foreign-scope
  actions fail without side effects;
- inspect the approved visual system, sidebar, Settings and all key screens at
  360 px, 768 px and desktop widths with no overlaps;
- compare Quick and Full plans and run username, full-name, email-only,
  phone-only, exact-URL and mixed-input cases;
- reconcile every selected engine with its displayed task/outcome/reason,
  including unavailable and excluded sources;
- consolidate evidence, inspect every observation, split a false match, make a
  correction and verify decision history;
- curate a Persona, fail explicit QC with a structured requirement, run the
  resulting research in the same case, revise, approve and compare Persona,
  graph, API and PDF;
- specifically run “Cross-check all approved evidence” on a Persona whose only
  approved anchor is an affiliation/location, and test hostile popup label and
  precision text as literal non-executing text;
- start an investigation from an approved affiliation and verify it remains a
  distinct, pending organization case;
- refresh/restart the app and worker; verify cases, decisions, chat, timeline,
  evidence and jobs persist and the two runtime identities still match;
- test stop, archive, exact-confirmation delete and final-version withdrawal on
  disposable cases.

## Promotion blockers and sign-off record

Promotion to `main` is blocked until all of the following are true:

- zero unresolved P0/P1 defects or review threads;
- every required PR and post-merge check passes on the exact candidate with no
  mandatory skip;
- every matrix row has its automated evidence, or an explicitly documented
  conditional abstention such as A14;
- every mandatory human/configuration check above is recorded against the same
  development commit, tree, image and schema;
- existing-feature preservation is accepted on development;
- the operator explicitly approves a `dev` to `main` promotion PR.

| Sign-off field | Value |
| --- | --- |
| Repair PR/head/tree | Pending publication |
| PR checks/artifact links | Pending |
| Merged `dev` commit/tree | Not authorized / not created |
| Development image/schema | Not authorized / not created |
| Human acceptance result | Not run for this repair |
| Unresolved P0/P1 | Must be zero before merge/promotion |
| `main` promotion | Blocked |
