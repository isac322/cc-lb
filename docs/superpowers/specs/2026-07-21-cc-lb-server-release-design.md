# cc-lb Server Release Design

Date: 2026-07-21
Status: Approved design

## Problem

`cc-lb-server` inherits its version from the root workspace:

```toml
[workspace.package]
version = "0.1.0"
```

The repository currently configures `cc-lb-server` as a `publish = false` release-plz package and expects release-plz to produce a `vX.Y.Z` GitHub Release. The release-plz workflow then calls the Docker and Helm publishing workflows with the server version.

This path does not currently release the server. The merged release PR #477 did not change the workspace version or include `cc-lb-server`. A subsequent successful release-plz run left `cc_lb_server_version` empty and skipped both server publishing jobs. General pull requests also cannot change the workspace version because `scripts/check-crate-versions.py` rejects effective version changes under `crates/`.

The result is an incomplete release path: Docker and Helm publishers exist, but no supported workflow advances the server version and orchestrates a complete server release.

## Goals

- Use the root Cargo workspace version as the only server release version.
- Release the server independently from crates.io packages.
- Create one immutable `cc-lb-vX.Y.Z` source tag for each server release.
- Publish the Docker image and Helm chart with the same version.
- Publish the GitHub Release only after the image and chart pass registry verification.
- Recover safely from missed workflow runs and interrupted publication.
- Keep normal feature pull requests from changing managed package versions.
- Preserve prerelease behavior and the existing no-`latest` image policy.

## Non-goals

- Changing how public Rust packages are versioned or published to crates.io.
- Automatically selecting major, minor, or patch versions from Conventional Commits.
- Publishing standalone server binaries as GitHub Release assets.
- Adding a `latest` Docker tag.
- Making GHCR package versions mutable after a release is public.

## Ownership Boundaries

### release-plz

release-plz continues to own only crates.io package releases. Remove the `cc-lb-server` package entry from `release-plz.toml` and remove the server-version extraction and Docker/Helm orchestration from `.github/workflows/release-plz.yml`.

### Server release workflow

A new `.github/workflows/release-server.yml` owns the server deployment unit:

- root Cargo workspace version
- `cc-lb-vX.Y.Z` source tag
- GitHub Release
- `ghcr.io/isac322/cc-lb` image tags
- `oci://ghcr.io/isac322/charts/cc-lb` chart version

The workflow calls the existing Docker and chart workflows through `workflow_call`; it does not wait for a separate `release.published` event.

Remove the `release: types: [published]` trigger from both publisher workflows. GitHub currently suppresses new workflow runs for events created with the repository's `GITHUB_TOKEN`, but retaining the trigger still leaves a second publication entry point for manual releases or future alternate-token use. Keep `release-server.yml` as the only normal orchestrator, with `workflow_call` for publisher invocation and `workflow_dispatch` for explicit recovery.

## Release Pull Request

A server release starts with a dedicated pull request titled:

```text
chore: release cc-lb vX.Y.Z
```

The pull request may change only:

- root `Cargo.toml`
- `Cargo.lock`
- root `CHANGELOG.md`

The root manifest changes `[workspace.package].version`. `cc-lb-server` inherits the new value through `version.workspace = true`. `Cargo.lock` records the resulting workspace package versions. `CHANGELOG.md` moves the release contents out of `Unreleased` into the new version section.

`deploy/helm/cc-lb/Chart.yaml` is not manually versioned for each release. The chart publisher already supplies the Cargo-derived version with:

```text
helm package --version X.Y.Z --app-version X.Y.Z
```

This keeps Cargo as the single version source.

## Pull Request Validation

The version guard must continue rejecting version changes in ordinary pull requests. It may allow a server release pull request only when all of these conditions hold:

1. The title exactly matches `chore: release cc-lb vX.Y.Z`.
2. The title version is valid SemVer without build metadata. A `+build` suffix is rejected because Docker tags cannot represent the same version string.
3. The title version equals `[workspace.package].version`.
4. `cargo metadata` reports the same version for `cc-lb-server`.
5. The new version is greater than the base branch's workspace version.
6. No literal `package.version` owned by release-plz changes.
7. The changed-file set is exactly `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md`.
8. The remote `cc-lb-vX.Y.Z` tag does not already exist.

The validation belongs in a repository script with deterministic tests. The GitHub workflow supplies PR metadata and invokes the script; version rules do not live only in shell expressions.

The release pull request also runs the existing project CI plus proportional release checks:

- Docker production-image build and `cc-lb --version` smoke test
- Helm lint and package using the proposed version
- verification that the packaged chart's `version` and `appVersion` equal the Cargo version
- actionlint for changed workflows

No registry push occurs from a pull request.

## Idempotent Release Gate

The release workflow must not use push-range state such as `github.event.before` as its release-state gate. A skipped run, force-push, or multi-commit push can otherwise select the wrong source commit or leave an untagged version with no recovery path.

The workflow runs on `master` pushes that change the root `Cargo.toml` and on explicit `workflow_dispatch`. Each run enters one serialized concurrency group. An automatic run uses `github.sha` as the candidate release target, reads `[workspace.package].version` from that commit and its first parent, and starts only when those values differ. This commit-local check prevents an ordinary workspace-dependency edit from releasing an old untagged version. Tag and Release state determine only idempotent no-op and resume behavior. A manual run requires an explicit target commit and version, which heals a missed automatic run without tagging later unrelated changes.

1. Read the `cc-lb-server` version `X` from Cargo metadata at the selected release target.
2. Verify the target is reachable from `master`.
3. Inspect the remote tag and GitHub Release state for `cc-lb-vX`.
4. If a published `cc-lb-vX` Release exists, finish successfully without changing anything.
5. If neither tag nor Release exists, start a new release for the selected target.
6. If an immutable `cc-lb-vX` tag and draft Release exist, do not retry automatically. Report that an explicit resume is required.
7. If the tag and Release state disagree, fail with the exact inconsistent state and require operator repair.

`concurrency.cancel-in-progress` is `false`. Rapid release pushes queue instead of cancelling an active release. Once the first run creates `cc-lb-vX`, queued or manually repeated runs observe it and become safe no-ops.

The workflow supports `workflow_dispatch` for missed-run recovery and draft resume. A dispatch requires the version and release target SHA. For a new release, it verifies that the target contains the requested Cargo version and is reachable from `master`. For a resume, it verifies that the target equals the immutable tag commit and that the Release is still a draft.

## Publication Flow

For a new version `X`:

1. Create immutable tag `cc-lb-vX` at the current master commit.
2. Create a draft GitHub Release for `cc-lb-vX`.
3. Call the Docker publishing workflow with version `X` and source ref `cc-lb-vX`.
4. Smoke-test the pushed image by running `cc-lb --version`.
5. Call the Helm publishing workflow with version `X` and source ref `cc-lb-vX`.
6. Pull the published chart from GHCR.
7. Verify the pulled chart reports `version: X` and `appVersion: X`.
8. Publish the draft GitHub Release.

The Docker and chart reusable workflows need an explicit source-ref input so a resume always rebuilds the immutable tagged source, even when workflow definitions have since changed on `master`.

Every published artifact records the immutable source revision. The Docker image must carry `org.opencontainers.image.revision=<tag commit>`. The packaged chart must carry a `cc-lb.io/source-revision: <tag commit>` annotation in addition to matching `version` and `appVersion`. Resume accepts an existing artifact only when this revision metadata matches the release tag.

The final GitHub Release body contains generated release notes and the canonical artifact coordinates:

```text
ghcr.io/isac322/cc-lb:X
oci://ghcr.io/isac322/charts/cc-lb --version X
```

## Version and Tag Semantics

Server source tags use the `cc-lb-v` namespace. The repository already contains a historical generic `v0.3.0` tag for `cc-lb-plugin-api`; a generic server `vX.Y.Z` sequence would eventually collide with it. Cargo, Docker, and Helm versions remain plain SemVer without the tag prefix.

Stable version `1.2.3` publishes Docker tags:

- `1.2.3`
- `1.2`
- `1`

The exact `1.2.3` image tag is immutable. The moving `1.2` alias updates only when the candidate is newer than every published stable `1.2.*` server Release. The moving `1` alias updates only when the candidate is newer than every published stable `1.*` server Release. Resuming an older draft therefore cannot move either alias backward.

Prerelease `1.2.3-rc.1` publishes only:

- `1.2.3-rc.1`

The workflow never publishes `latest`. Helm charts use the exact full version only. A prerelease Cargo version produces a GitHub prerelease.

## Failure and Recovery

### Before tag creation

A failed gate or validation changes no external state. A missed automatic run is recovered with `workflow_dispatch` using the release PR's merge commit and Cargo version. Landing the workflow without changing `Cargo.toml` does not create an initial release accidentally.

### After tag or draft creation

Automatic runs do not retry. The tag pins the release source, and the draft records the incomplete release.

- For a publishing workflow, credentials, runner, or registry defect, fix the defect and explicitly resume the draft release. The resume checks out `cc-lb-vX` and preserves the original source commit.
- For a server source defect, do not move or delete `cc-lb-vX`. Close the draft as failed according to the operator runbook, fix the source, and create a new release pull request with the next patch version.
- Never overwrite a public GitHub Release, released chart version, or immutable source tag.

A resume verifies existing registry state before acting. Matching artifacts may be accepted; conflicting metadata or source revision fails closed and requires a new version.

## Permissions and Security

Use job-level least privilege:

- validation: `contents: read`
- tag and GitHub Release management: `contents: write`
- Docker and Helm publication: `contents: read`, `packages: write`

The workflows use `GITHUB_TOKEN`; they do not add long-lived GHCR credentials. Existing self-hosted runner and BuildKit secret handling remain unchanged. Release jobs must not expose registry tokens or runner-provided AWS cache credentials.

## Required File Changes

- Add `.github/workflows/release-server.yml`.
- Update `.github/workflows/cd.yml` by removing its `release` trigger, adding an explicit source-ref input and checkout, and verifying the published image's source-revision label.
- Update `.github/workflows/publish-chart.yml` by removing its `release` trigger, adding an explicit source-ref input and checkout, recording a source-revision chart annotation, and performing post-push pull verification.
- Remove server release orchestration from `.github/workflows/release-plz.yml`.
- Remove the `cc-lb-server` package block from `release-plz.toml`.
- Extend or replace `scripts/check-crate-versions.py` with server release PR validation while retaining public-package protections.
- Add deterministic tests for the version validation script.
- Update the root release runbook or README section describing how to cut and resume a server release.

## Verification

Before merging the implementation:

1. Test valid patch, minor, major, and prerelease version transitions.
2. Test malformed, equal, lower, and already-tagged versions.
3. Test that an ordinary feature PR cannot change the workspace version.
4. Test that a release PR cannot change a literal public crate version or an unrelated file.
5. Run actionlint on all changed workflows.
6. Build the production Docker image and run `cc-lb --version`.
7. Lint and package the chart with a test version; inspect `version` and `appVersion`.
8. Exercise the release gate against mocked tag/Release states: absent, published, draft, and inconsistent.
9. Verify a queued second run becomes a no-op after the first creates the version tag.
10. Verify manual resume selects the tagged source ref rather than current master.
11. Verify resumed Docker and Helm artifacts must match the tagged source revision.
12. Verify publishing the draft Release cannot trigger a second Docker or Helm publication run.
13. Verify a workflow-only master change does not create a release for the existing Cargo version.
14. Verify automatic publication tags the Cargo-version bump commit, not a later master commit.
15. Verify a root `Cargo.toml` dependency-only change does not start a release when `[workspace.package].version` is unchanged from the candidate commit's first parent.

A live GHCR push or public GitHub Release is not required during pull-request validation. The first production server release is the final end-to-end proof.

## Acceptance Criteria

- A dedicated release pull request is the only supported way to change the server version.
- Merging that pull request eventually produces one source tag, one GitHub Release, matching Docker tags, and one matching Helm chart version.
- Public Rust package releases continue independently through release-plz.
- A missed run can be healed with `workflow_dispatch` because the gate compares the requested target's Cargo version with actual tag and Release state rather than relying on `github.event.before`.
- Failed draft releases never retry automatically.
- Explicit resume always builds the immutable tagged source.
- The public GitHub Release is not published until Docker and Helm registry verification succeeds.
- Publishing the GitHub Release does not trigger duplicate Docker or Helm jobs.
