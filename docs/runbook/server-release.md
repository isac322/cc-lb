# cc-lb server release

The root Cargo workspace version is the `cc-lb-server` release version. The five public crates use literal package versions with `release-plz.toml`; the server uses the git-only `release-plz-server.toml`.

Server source tags use `cc-lb-vX.Y.Z`. Docker and Helm use the plain `X.Y.Z` version. The workflows never publish a `latest` image tag.

## Cut a release

1. Create a branch containing only `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` changes.
2. Set `[workspace.package].version` in the root `Cargo.toml` to the chosen SemVer. Prerelease identifiers are allowed; build metadata (`+...`) is not.
3. Refresh `Cargo.lock`:

   ```bash
   cargo metadata --no-deps --format-version 1 > /dev/null
   ```

4. Move the release notes from `Unreleased` to the chosen version and date.
5. Open the pull request with the exact title `chore: release cc-lb vX.Y.Z`.
6. Merge only after every required check passes.

The release PR's `verify server release artifacts` check reuses the Garage-backed Buildx cache, smoke-tests the production image, packages and verifies the source-stamped chart, and actionlints the release workflows before merge.

After merge, `release-server` verifies that the workspace version changed and invokes release-plz. Release-plz creates immutable tag `cc-lb-vX.Y.Z` and a draft GitHub Release. Separate GHA jobs then publish and verify Docker first, publish and verify Helm second, and finally publish the Release.

Stable versions publish immutable Docker tag `X.Y.Z`. Moving tags `X.Y` and `X` update only if this is the newest published stable server release for that prefix. Prereleases publish only their exact tag.

## Inspect a release

Set the operator-entered version and inspect all four release surfaces:

```bash
VERSION=X.Y.Z
TAG="cc-lb-v$VERSION"
REVISION="$(git rev-list -n1 "$TAG")"

gh run list --workflow release-server.yml --branch master
gh release view "$TAG" --json tagName,isDraft,isPrerelease,url
```

The expected artifacts are:

- source tag: `cc-lb-vX.Y.Z`
- image: `ghcr.io/isac322/cc-lb:X.Y.Z`
- chart: `oci://ghcr.io/isac322/charts/cc-lb --version X.Y.Z`
- GitHub Release: published, not draft

Verify the packaged chart metadata from a temporary directory:

```bash
tmp_chart="$(mktemp -d)"
helm pull oci://ghcr.io/isac322/charts/cc-lb \
  --version "$VERSION" \
  --destination "$tmp_chart"
tar -xzf "$tmp_chart/cc-lb-$VERSION.tgz" -C "$tmp_chart"
python3 scripts/chart_metadata.py verify \
  --chart "$tmp_chart/cc-lb/Chart.yaml" \
  --version "$VERSION" \
  --revision "$REVISION"
```

The Docker workflow separately verifies that the exact image and any moving aliases carry `org.opencontainers.image.revision=$REVISION`, and that `cc-lb --version` contains `$VERSION`.

## Resume an infrastructure failure

Never rerun a failed job blindly. First identify and fix the workflow, runner, credential, network, or registry defect that caused the failure.

A failed artifact job leaves the GitHub Release as a draft. Confirm that the immutable tag still points to the reviewed release commit:

```bash
VERSION=X.Y.Z
TAG="cc-lb-v$VERSION"
TARGET_SHA="$(git rev-list -n1 "$TAG")"
gh release view "$TAG" --json isDraft,tagName
```

Then dispatch the orchestrator:

```bash
gh workflow run release-server.yml \
  -f operation=resume \
  -f version="$VERSION" \
  -f target_sha="$TARGET_SHA"
```

Resume fails unless the Release is still draft, the tag points to `target_sha`, and any existing exact image or chart carries the same source revision. A matching artifact is reused; an absent artifact is published.

## Recover a missed automatic run

Use this only when the release pull request merged but no source tag or GitHub Release was created.

```bash
VERSION=X.Y.Z
TARGET_SHA=<reviewed-release-pr-merge-commit>

gh workflow run release-server.yml \
  -f operation=start \
  -f version="$VERSION" \
  -f target_sha="$TARGET_SHA"
```

The target must be reachable from `master`. Its root workspace version must equal `VERSION` and must differ from its first parent's workspace version. The `start` operation invokes the same git-only release-plz command as the automatic path.

## Source defect after tagging

Never move or delete an immutable `cc-lb-vX.Y.Z` source tag, overwrite a public GitHub Release, or reuse a published image or chart version.

If the tagged source is defective:

1. Leave the failed draft Release and immutable tag as evidence.
2. Fix the source on a normal pull request.
3. Cut the next patch version through a new dedicated server release pull request.

Explicit resume always rebuilds or verifies artifacts from the existing immutable tag. It is not a way to publish later `master` content under an old version.

## Inconsistent tag and Release state

The orchestrator fails closed when the tag and GitHub Release disagree. Examples include a tag without a Release, a draft Release without its tag, or a tag that points to a different commit.

Stop and record:

- the failed Actions run URL;
- the tag target SHA;
- whether the GitHub Release is absent, draft, or published;
- whether the exact image or chart version exists;
- the source revision recorded by any existing artifact.

Do not delete or move public state as a shortcut. Repair a tag-without-Release failure only after confirming the tag points to the reviewed release commit and no existing artifact points to different source content. Create the matching draft Release for that immutable tag, then use explicit `resume`. Escalate any other inconsistent state for repository-owner review before changing public release data.
