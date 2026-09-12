# OpenLedger development promotion

OpenLedger uses one controlled promotion route:

1. Create a feature or repair branch from `dev`.
2. Open a pull request into `dev` and require all CI checks.
3. Merge only after review, then deploy the exact merged `dev` commit to the
   disposable development Droplet.
4. Record human acceptance against that exact commit, tree, schema and image.
5. Promote only through a pull request from `dev` into `main`.
6. Build a new immutable release from the resulting `main` commit before any
   separately authorized production deployment.

Direct feature pull requests and direct pushes to `main` are not part of the
supported process. The branch-promotion workflow rejects a pull request into
`main` unless its head branch is `dev`. GitHub branch rules must also require
pull requests and the named status checks because a workflow cannot prevent an
administrator from pushing directly.

## Required GitHub branch rules

Create rulesets for both branches and include repository administrators:

- `main`: require a pull request, require the branch-promotion, test, CodeQL,
  source-audit and persistence checks, block force pushes and deletion, and
  allow promotion only from `dev`.
- `dev`: require a pull request and the same test/security/persistence checks,
  block force pushes and deletion, and do not allow a bypass for routine work.

Maintenance workflows propose their updates to `dev`. They cannot change
production until the resulting `dev` state passes staging acceptance and is
promoted separately.

## Development Droplet isolation

The development Droplet is disposable, but its cloned production data and
secrets are not. Before testing:

- use a development hostname, never the production hostname;
- stop the cloned web ingress and worker until isolation is complete;
- replace application login credentials and every external-provider secret
  with development-only credentials, or disable that provider;
- never submit real investigation subjects from the development UI;
- keep PostgreSQL, Docker, and internal service ports closed to the internet;
- allow SSH only from the operator's current public IP;
- expose TCP 80/443 only when browser testing requires it; UDP 443 is optional;
- remove all public `All TCP` and `All UDP` inbound rules.

The recommended hostname is `dev.openledger.nexorus.io`, with its DNS record
pointing only to the development Droplet. The development environment must use
its own `.env`, authentication file, API keys and backup records.

## SSH access from Windows

An SSH response ending in `Permission denied (publickey)` proves the network
path and SSH service are reachable. Specify the remote account and private key:

```powershell
ssh -i "$env:USERPROFILE\.ssh\openledger-dev" root@167.172.78.28
```

If the private key does not exist, create a dedicated key locally:

```powershell
ssh-keygen -t ed25519 -f "$env:USERPROFILE\.ssh\openledger-dev" -C "openledger-dev"
Get-Content "$env:USERPROFILE\.ssh\openledger-dev.pub"
```

Use the DigitalOcean browser console to add only that public key to the chosen
sudo-capable account's `~/.ssh/authorized_keys`. Never upload or paste the
private-key file. After key authentication works, create/use a non-root operator
and keep root password login disabled.

## Deploy the exact `dev` commit

Run these only on the isolated development Droplet after the `dev` pull request
has been merged and its push checks are green:

```bash
cd /opt/openledger
git fetch --no-tags origin dev
git switch --detach origin/dev
DEV_RELEASE_SHA="$(git rev-parse HEAD)"
test "$DEV_RELEASE_SHA" = "$(git rev-parse origin/dev)"
test -z "$(git --no-optional-locks status --porcelain --untracked-files=all)"
python3 deploy/check-p2-release.py
bash deploy/build-reviewed-release.sh \
  --commit "$DEV_RELEASE_SHA" \
  --manifest "/tmp/openledger-dev-${DEV_RELEASE_SHA}.json"
sudo bash deploy/update.sh \
  --commit "$DEV_RELEASE_SHA" \
  --manifest "/tmp/openledger-dev-${DEV_RELEASE_SHA}.json" --check
sudo bash deploy/update.sh \
  --commit "$DEV_RELEASE_SHA" \
  --manifest "/tmp/openledger-dev-${DEV_RELEASE_SHA}.json"
```

The update remains pinned to one commit, source tree and immutable local image.
It backs up the development database and evidence before applying the approved
migration chain. It does not select a branch tip or floating Docker tag during
activation.

## Accept, reject, or reset

Accept only after the health endpoint, login, collection-engine accounting,
single-Persona consolidation, evidence review, QC rejection/research/reapproval,
final Persona, report, favicon and deletion/archive paths work against the exact
deployed development identity.

If acceptance fails, do not promote `dev`. Fix forward through another pull
request into `dev`, or revert the failed merge through a pull request. Do not
force-reset the shared branch. When a clean database reset is preferable,
destroy and rebuild only the development Droplet from a sanitized development
baseline snapshot. Never downgrade the evidence schema or restore the
production host as part of development rollback.
