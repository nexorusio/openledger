# OpenLedger licensing transition

This repository records two separate boundaries:

- Maigret fork base: `af3de564c706e677221ab9f82f90166bb8b346ea`.
- First OpenLedger modification:
  `8bc569097af94d992ac2f32a7293eeb6b140bfd4`.

PT Daya Prana Inovasi asserts copyright in original company-authored or
assigned OpenLedger additions beginning with that first modification. The
repository changed from a blanket MIT presentation to an explicit mixed-license
distribution after commit
`60b187135c94621d729a42b0d09294c59a3d8cb7`.

The transition is prospective. It does not revoke the MIT permissions that
accompanied earlier public revisions. It also does not claim ownership of
Maigret or any other third-party component.

## Boundary

| Material | Treatment |
|---|---|
| Maigret material at the fork base and later upstream integrations | Maigret MIT notice remains controlling and must be distributed. |
| Original OpenLedger additions beginning with `8bc569097af94d992ac2f32a7293eeb6b140bfd4` | PT Daya Prana Inovasi asserts copyright ownership to the extent supported by authorship or assignment. Earlier MIT grants covering public copies remain effective. |
| Repository revisions through `60b187135c94621d729a42b0d09294c59a3d8cb7` | Remain available under the MIT terms shipped with those revisions. |
| Retained or modified Maigret material | Maigret MIT notice remains controlling and must be distributed. |
| Current and future PT Daya Prana Inovasi material after the transition | Distributed as proprietary unless expressly licensed otherwise, without cancelling previously granted MIT rights. |
| Other third-party dependencies and assets | Their respective licenses remain controlling. |

The root [`LICENSE`](../LICENSE) defines the prospective OpenLedger terms.
[`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md) and the files under
`LICENSES/` preserve third-party permissions and conditions.

Ownership is not the same as exclusivity. The company may own original
OpenLedger code beginning with the first modification while recipients of an
earlier public version continue to exercise the MIT permissions granted with
that version. The transition therefore cannot prevent use of an already
MIT-licensed snapshot; it controls how PT Daya Prana Inovasi offers its work in
current and future distributions.

## Release controls

Before distributing a source archive, wheel, container image, virtual-machine
image, or on-premises release:

1. verify that the root licensing notice, `THIRD_PARTY_NOTICES.md`, and the
   complete `LICENSES/` directory are present in the artifact;
2. generate or refresh the software bill of materials and review direct,
   transitive, font, and browser-asset licenses;
3. confirm that the customer agreement expressly excludes third-party
   components from proprietary ownership claims;
4. retain Maigret's full MIT text for every artifact containing copies or
   substantial portions of Maigret; and
5. obtain legal approval for the commercial agreement and final notice set.

There is no source-disclosure or user-interface credit requirement in
Maigret's MIT terms. The complete notice still belongs in shipped artifacts,
including Docker and on-premises packages.

## Repository visibility and deployment

Changing the GitHub repository to private cannot withdraw permissions already
granted for public revisions. It can protect access to future proprietary work.

Do not change visibility until every deployed server has a tested read-only
GitHub deploy key or other approved machine credential. The supported update
script performs an authenticated `git pull`; making the repository private
first would interrupt deployments.

After private access has been tested:

1. retain the historical cutoff commit in the permanent audit record;
2. change the repository visibility to private;
3. remove or restrict stale collaborators and automation credentials;
4. verify CI, Dependabot, upstream checks, and deployment pulls; and
5. keep future proprietary features out of public pull requests and forks.

## Development controls

New proprietary files must carry both:

```text
SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
```

Do not add this marker to Maigret-derived files. New work that modifies a mixed
or historical file requires an explicit licensing review; moving or renaming
third-party code does not change its license.

External contributions must not be merged without written terms that give
PT Daya Prana Inovasi the rights required for the intended product license.
