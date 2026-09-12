# P2 evidence assessment and probability contract

This implements the assessment stage of the approved complete pipeline. It does
not accept evidence, attribute accounts to people, curate a Persona or pass QC.
Those remain separately recorded operator and QC actions. Assessment failure or
missing calibration never prevents the direct operator path.

No production probability model, reference labels or measured calibration results
are included. Engineering fixtures verify code; they do not establish accuracy.

## Runtime contract

`maigret.web.pipeline_assessment.assess_group(group, observations, ...)` consumes
the normalized consolidation group and its normalized source observations. Pass
the group's `normalized` document, not the storage wrapper. The caller persists
the returned snapshot by canonical group key. `assess_groups` is an optional
convenience wrapper when observations are indexed by group ID.

Required group scope is `kind` (`account` or `claim`), `case_id`, `subject_id` and
its canonical `id`/`key`. Each observation retains its original identifier,
outcome, source URL, time, retention policy and origin-family provenance.
`origin_by_observation` from consolidation is authoritative for canonical
copy/origin dependence; original observation origins are not overwritten.

The result includes:

- `assessment_id`, `schema_version`, `assessed_at` and `evidence_digest` binding
  the exact scoped group and every observation revision. The digest is distinct
  from consolidation's membership hash. The expected previous digest can be
  supplied to withhold a stale assessment.
- `evidence_counts`, `origin_families`, per-source `outcomes`, `contradictions`,
  `group_conflicts`, `freshness`, `missing_evidence` and `warnings`.
- `features` and versioned `feature_schema`, with every contributing source family
  and signal available for inspection. Heuristic discovery scores stay separate.
- `probability.value` (0–1 or null), a precise `reason` when withheld, event and
  model identity, feature contributions, reviewed artifact digest and validated
  scope. Numerical results carry `serving_gate_passed: true`; this is an internal
  result marker, never an authorization accepted from an API request.
- `operator_review_available: true` in every returned snapshot.

Do not render the number when `value` is null. Do not convert legacy confidence
or a discovery-ranking score into probability. Do not call the count of source
families an identity-confidence score.

## Defined events

| Event | Estimated unit and meaning |
| --- | --- |
| `account_attribution` | This account belonged to or was operated by this subject at the stated time. |
| `claim_correctness` | This qualified claim about this subject is correct at the stated time. This is the joint subject/claim event, including qualifiers. |

There is no single Persona-wide percentage. A found account is an existence
finding, not a probability that its owner is the subject. Matching handles,
matching names, registration results, a high attribution probability and QC
approval do not automatically establish another event. The two event models are
separate; their outputs are never multiplied.

## Source-backed feature protocol

An observation's `evidence_signals` may contain only explicit source-backed
signals. Each signal must contain `matched: true`, the exact `subject_id`, its
canonical `hypothesis_key`, an `evidence_ref` matching that observation's ID,
source/canonical URL or native record ID, and the following recognized method.
An opaque confidence value, arbitrary method string, model output or unbound
boolean is ignored. Extraction code must actually verify the cited relationship;
the field's presence is not a substitute for that verification.

| Signal | Required method | Meaning |
| --- | --- | --- |
| `source_link_match` | `explicit_profile_link` | Cited source explicitly links this subject to this account. |
| `stable_id_match` | `stable_identifier_cross_reference` | Stable platform identity cross-referenced to this subject's source evidence. |
| `public_identifier_match` | `exact_public_identifier_cross_reference` | Public identifier connects the cited subject and hypothesis; shared contacts alone do not establish ownership. |
| `temporal_match` | `dated_source_assertion` | Cited evidence supports the hypothesis at its stated time. |
| `qualified_claim_support` | `qualified_fact_with_subject_binding` | Source backs this subject and the complete qualified/time-scoped claim. |
| `contradiction` | `conflicting_source_assertion` | Cited source contradicts this particular hypothesis. |

Signals count at most once per canonical original-source family, capped at 10
families by feature schema `p2-evidence-v1`. Repeating one origin 100 times cannot
raise feature weight. Independent observations remain in the ledger regardless.
Unknown dependence never becomes independent corroboration. Blocked, not-found,
failed or cancelled queries remain visible but cannot supply positive or negative
identity features. Metadata-only provider results cannot support numeric scores.

Freshness uses effective time, then publication time, then observed time. A new
fetch does not turn an old assertion into present ownership evidence. A dated
assertion older than 90 days is stale under this schema. Each supporting origin
uses its oldest applicable assertion time so copied fetches cannot refresh it.
Timezone-naive, absent or future dates do not establish freshness. Historical
assessment can pass an explicit `as_of` time appropriate to its target event.
Changing this rule requires a new feature schema and empirical revalidation.

If no source-backed signal is available, evidence and source status are still
displayed with `insufficient_source_backed_evidence`. Adapters are not allowed to
invent signals to make this message disappear.

## Models and reproducibility

`pipeline_probability_eval.py` provides deterministic L2-regularized logistic
regression for each event, followed by a separately fitted sigmoid calibrator.
Training scales bounded features using training-only maxima, then exports
coefficients in original feature units. No preprocessing is fitted on calibration
or test data. Runtime serves only JSON coefficients and never deserializes pickle
or starts a model service. Training and runtime use the Python standard library;
the engineering reference environment is Python 3.12.14. Artifact metadata records
algorithm, iterations, regularization, feature schema and frozen lineage.

Dataset JSON schema is `openledger-probability-reference-v1`. Top-level fields are
`dataset_id`, `feature_schema`, `reference_kind`, `population` and `examples`.
The population record must declare `role: representative_candidate_stream`, a
concrete `definition` and `sampling_method` to pass the human-label gate.
Enriched challenge sets must not impersonate representative population samples.
Each example has:

```json
{
  "id": "reference-hypothesis-id",
  "event": "account_attribution",
  "subject_id": "reference-subject-id",
  "account_ids": ["platform:stable-account-id"],
  "source_origin_ids": ["original-source-family-id"],
  "reference_time": "2026-09-01T00:00:00Z",
  "scope": {
    "platform": "platform-name",
    "input_type": "username",
    "language": "id",
    "claim_family": "account",
    "source_revision": "reviewed-source-parser-revision"
  },
  "features": {"all_feature_schema_fields": "numeric values from frozen assessment"},
  "eligible": true,
  "label": null,
  "reviews": [
    {"reviewer_id": "human-a", "blinded_to_model": true, "label": null, "evidence_refs": ["reference-source"]},
    {"reviewer_id": "human-b", "blinded_to_model": true, "label": null, "evidence_refs": ["reference-source"]}
  ],
  "reference_evidence_refs": ["reference-source"]
}
```

The feature placeholder above must be replaced with **all** fields in
`pipeline_probability.FEATURE_NAMES`; it is documentation, not training data.
Labels are 0, 1 or null. Unresolved labels stay in disclosed denominators and are
not fitted as negatives. Disagreements require an `adjudication` object with a
reviewer, label, reason and source references. Production references use
`reference_kind: independent_human_labels`; synthetic fixtures use `synthetic`
and always fail empirical release eligibility. Label provenance must be reviewed
by humans; software cannot authenticate the truth of a supplied reference label.

## Frozen protocol and gates

1. Freeze dataset and approximately 60/20/20 train/calibration/locked-test splits.
   Connected subjects, accounts and original-source families stay in one split.
   Duplicates or altered frozen data fail before fitting. Components are ordered
   by time; the locked test must start after all train/calibration references.
   Long-lived connected components can make this impossible; the gate fails
   visibly rather than pretending independence.
2. Require two distinct blinded human labels with evidence and disagreement
   adjudication. Require at least 3,000 adjudicated examples per event, 500 distinct
   subjects and 500 adjudicated locked-test examples.
3. Fit the logistic model on training only and calibrator on calibration only.
   Select the lowest qualifying high-band threshold from 0.95, 0.96, 0.97, 0.98
   and 0.99 on calibration data, then freeze it. The band needs at least 300
   adjudicated examples, 98% precision and a 95% lower bound of at least 95% in
   calibration and again in the locked test. No test-driven threshold tuning.
4. Compare Brier score and log loss with the train-prior constant and uncalibrated
   model. Both calibrated proper scores must beat the prior and may not worsen
   either raw-model score by more than an absolute 0.005, the explicit
   noninferiority tolerance used for “not materially worsen.” Ten-bin ECE must
   be at most 0.05 overall and 0.10 in adequately sampled declared slices.
5. Report counts, confidence intervals, reliability-bin data, precision, recall,
   false-association rate, coverage and abstention by platform, input type,
   language, claim family and source revision. Exact displayed scope must have
   at least 100 test records and both classes; unsupported intersections cannot
   borrow counts from unrelated families. Feature extrapolation also abstains.
6. Use Wilson intervals only for disjoint subject/account/origin identities.
   Otherwise use deterministic subject-cluster bootstrap, expanded to include
   shared account/origin dependence (400 resamples). At an all-equal-label
   boundary, use the independent-cluster Wilson bound so a repeated single
   subject/source cannot yield a fabricated [1, 1] confidence interval.
7. Report prospective validated-scope coverage separately from effective numeric
   coverage. If any empirical gate fails, effective numeric coverage is zero.

The report includes reliability data with bin counts and confidence intervals for
rendering calibration diagrams. It records dataset, split, locked-test and model
digests. Challenge sets should be separately reported; they do not replace the
representative population sample for calibration.

Run from the repository root, using a controlled evaluation environment:

```bash
python -m utils.evaluate_pipeline_probability freeze \
  --dataset /controlled/reference.json --split /controlled/frozen-split.json \
  --event account_attribution

python -m utils.evaluate_pipeline_probability evaluate \
  --dataset /controlled/reference.json --split /controlled/frozen-split.json \
  --event account_attribution --model-id attribution-v1 \
  --output /controlled/evaluation.json

python -m utils.evaluate_pipeline_probability export-reviewed \
  --evaluation /controlled/evaluation.json --approval-reference reviewed-change-id \
  --output /controlled/reviewed-artifact.json
```

The CLI refuses to overwrite frozen artifacts. Evaluation writes an exclusive
`.evaluation-used` marker before fitting; repeating the same locked test is
refused. After a failed run, a reviewer must inspect the consumed evaluation
instead of deleting the marker to tune against the same test. A new model
iteration needs a newly frozen appropriate holdout, with the prior evaluation
retained. These controls are local workflow guards, not a tamper-proof external
human-label authority. Export cannot approve a failed or synthetic evaluation.

## Activation and suspension

Root runtime integration must default to **no artifact**. A minimal optional
configuration is `OPENLEDGER_PROBABILITY_ARTIFACT` plus
`OPENLEDGER_PROBABILITY_ARTIFACT_SHA256`, where the latter is the externally
reviewed `artifact_sha256` printed by export (canonical payload digest, excluding
the embedded digest field; it is not the raw-file SHA). Load via `load_artifact`
and supply that configured hash to `assess_group`. A separate artifact is required
per event; the runtime may select a reviewed artifact by event. Passing a model
for the other event safely abstains.

Strict JSON rejects duplicate keys, nonfinite constants and files over 2 MB.
Serving validates the externally pinned digest, explicit review reference,
empirical gates, event, feature schema, coefficient types, exact scope, feature
ranges and expiry. Scope must explicitly name `platform`, `input_type`,
`language`, `claim_family`, and `source_revision`; unknown values abstain.
The caller must derive these from current case/source configuration, not a user
claim that a scope was validated.

Artifacts expire after 30 days and cannot declare an audit window over 31 days.
Monthly audited samples and any material source/parser/model change require
revalidation. Removing the configured hash immediately suspends numeric serving.
A revised, newly pinned artifact can set a scope's `suspended: true`. Evidence
access, source outcomes, operator review, curation and QC remain available.

Current status: engineering implementation available; independent-reference
collection and empirical model validation remain open acceptance dependencies.
No model should be activated merely because these regression tests pass.

## Durable runtime and QC integration

`pipeline_assessment_runtime.assess_consolidated_groups(store, case_id, persona_id)`
loads normalized persisted source documents and their scoped collection tasks,
consolidates them, assesses every account/claim membership and persists a mapping
keyed by canonical source group ID. Missing members fail explicitly; the helper
never silently drops source observations. It accepts either CaseStore or
PipelineStore. Pure callers can use `assess_consolidated` with their own normalized
observations and task lookup.

Operating scope comes from actual account/platform identifiers, source website
hosts, task input types, explicit source/claim language metadata, and the sorted
set of observed engine/parser/configuration versions. Composite values preserve
all observed types, rather than guessing one primary type. No Indonesia language
default or browser locale is used. Missing source/version metadata remains null,
which cannot satisfy a calibrated model's exact validated scope. Source revision
is a reproducible digest of that complete revision set.

The helper reloads the externally pinned artifact before assessment and reports
configuration failures without exposing host filesystem paths. Replayed evidence
whose digest, features, scope and newly checked serving outcome are unchanged
retains its original immutable assessment snapshot and original assessment time.
This avoids dirtying the case or invalidating a submitted QC version solely
because a worker replayed completed delivery. Changes in evidence, validity,
model, source scope or the freshness boundary create a new snapshot.

To configure both event models simultaneously, use separate optional
`OPENLEDGER_PROBABILITY_ACCOUNT_ATTRIBUTION_ARTIFACT` /
`OPENLEDGER_PROBABILITY_ACCOUNT_ATTRIBUTION_ARTIFACT_SHA256` and
`OPENLEDGER_PROBABILITY_CLAIM_CORRECTNESS_ARTIFACT` /
`OPENLEDGER_PROBABILITY_CLAIM_CORRECTNESS_ARTIFACT_SHA256` pairs. An event-specific
pair takes precedence over the generic pair; an incomplete pair abstains instead
of falling back silently. Each event still requires its own independently reviewed
artifact. All configuration remains absent by default.

QC calls `validate_frozen_probability` for numerical results. A version freezes
the full `assessed_group` and actual `evidence[].payload` documents; the guard
recomputes the evidence digest and feature vector, checks scope/event identity,
explicit artifact review and its audit validity, and rejects stale or foreign
assessments. Comparing two self-declared digest strings alone is insufficient.
Operator corrections move the earlier assessment to historical provenance and
withhold current probability pending reassessment. A null probability remains
compatible with explicit operator curation and QC approval.
