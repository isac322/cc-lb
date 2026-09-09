#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "::error::usage: $0 EXPECTED_REVISION IMAGE [IMAGE...]" >&2
  exit 2
fi

expected_revision=$1
shift

declare -A verified_revisions=()
temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT
raw_file="$temp_dir/manifest.json"

verify_revision() {
  local ref=$1
  local labels revision

  if [[ -v 'verified_revisions[$ref]' ]]; then
    return
  fi

  if ! labels=$(docker buildx imagetools inspect "$ref" \
      --format '{{json .Image.Config.Labels}}'); then
    echo "::error::could not inspect image config for $ref" >&2
    exit 1
  fi
  revision=$(jq -r '."org.opencontainers.image.revision" // empty' <<<"$labels")
  if [ "$revision" != "$expected_revision" ]; then
    echo "::error::image $ref revision '$revision' does not match '$expected_revision'" >&2
    exit 1
  fi

  verified_revisions["$ref"]=$revision
}

for image in "$@"; do
  if ! docker buildx imagetools inspect "$image" --raw > "$raw_file"; then
    echo "::error::could not inspect image $image" >&2
    exit 1
  fi

  repository=${image%%@*}
  last_component=${repository##*/}
  if [[ "$last_component" == *:* ]]; then
    repository=${repository%:*}
  fi

  refs=()
  if jq -e 'has("manifests")' "$raw_file" > /dev/null; then
    digests=()
    while IFS= read -r digest; do
      if [ -n "$digest" ]; then
        digests+=("$digest")
      fi
    done < <(
      jq -r '
        .manifests[]
        | select(.platform.os != "unknown")
        | .digest
      ' "$raw_file"
    )
    if [ "${#digests[@]}" -eq 0 ]; then
      echo "::error::image $image contains no runnable platform manifests" >&2
      exit 1
    fi
    for digest in "${digests[@]}"; do
      refs+=("$repository@$digest")
    done
  else
    manifest_digest=$(sha256sum "$raw_file")
    manifest_digest=${manifest_digest%% *}
    refs+=("$repository@sha256:$manifest_digest")
  fi

  for ref in "${refs[@]}"; do
    verify_revision "$ref"
  done

  printf 'verified %s platform manifest(s) for %s at revision %s\n' \
    "${#refs[@]}" "$image" "$expected_revision"
done
