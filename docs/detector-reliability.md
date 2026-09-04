# Detector reliability and profile triage

OpenLedger treats Maigret's `CLAIMED` status as raw detector output, not as a
verified profile and never as proof that an account belongs to the subject.

## Result classes

- **Supported profile lead**: returned content contains profile-specific account
  evidence. It appears in the discovery graph and can create a pending Persona
  proposal. Subject identity remains unverified until analyst review.
- **Candidate**: the detector returned `CLAIMED`, but the response did not contain
  enough account-specific evidence or the detector is degraded. It remains
  visible for corroboration but is not counted as a finding and does not enter a
  Persona automatically.
- **Suppressed detector hit**: a quarantined detector, generic page shell,
  login/challenge/rate-limit response, or unsafe URL produced the result. It is
  excluded from findings, graph, AI evidence and Personas.
- **Unknown/blocked**: collection could not determine availability. It must never
  be converted to `AVAILABLE` or treated as a negative finding.

The downloadable scanner reports retain raw results for lawful diagnostic and
audit use. The OpenLedger graph, headline count and Persona pipeline consume only
the supported class.

## Pre-triage investigations

Completed investigations saved without a reliability version remain available
as raw, explicitly untriaged leads. They contribute zero supported profiles;
their historical graph and profile-derived collector observations are withheld
from graph, AI and Persona synchronization until the usernames are rerun.
Independent evidence such as silent email-registration checks remains separate.

On upgrade, Persona claims previously synchronized from a pre-triage profile are
marked `legacy_untriaged` rather than deleted. They leave the default Persona,
maps, relationship graphs, exports and AI context, while their evidence and
review history remain auditable in the review queue. A current-version rerun
that supports the same claim reactivates it and restores the latest human review
decision; an untriaged claim cannot be newly approved before that rerun. An
orphaned profile claim whose source job was deleted is handled the same way,
because its reliability version can no longer be verified. New source-job
deletions first repoint a claim to its newest surviving current-version profile
or independent governed lineage; claims without either are retired in the
deletion transaction. The upgrade sweep also catches claims orphaned by an
older deployment.

## Canary and quarantine policy

The `Maigret detector health` GitHub Actions workflow runs weekly and on manual
dispatch. For each selected username detector it checks:

1. the site's declared existing account;
2. the site's declared missing account; and
3. three syntax-compatible, high-entropy likely-missing usernames.

One semantic contradiction changes a detector to `degraded`. A second
consecutive contradiction changes it to `quarantined`, removing it from normal
investigations. Network failures, blocks and rate limits degrade the detector but
do not count as false-positive proof. A degraded or quarantined detector needs
two consecutive clean runs to recover.

The workflow writes health changes to
`automation/maigret-detector-health` and opens or updates a data-only pull
request. It never modifies Maigret's source database or production state
directly. Per-probe diagnostics are retained as a workflow artifact for 30 days.

## Manual operation

Run a bounded subset before changing detector rules:

```bash
python utils/detector_health_canary.py \
  --site GitHub \
  --site SoundCloud \
  --samples 3 \
  --output /tmp/detector-health.json \
  --report /tmp/detector-health-report.json
```

Use the generated registry only after reviewing the detailed report. Production
can read an alternate reviewed registry from `OPENLEDGER_DETECTOR_HEALTH_FILE`.
