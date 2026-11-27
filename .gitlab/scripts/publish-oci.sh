#!/usr/bin/env bash
set -euo pipefail

# Shell runners share a home directory. Keep registry credentials local to this job.
DOCKER_CONFIG=$(mktemp -d)
export DOCKER_CONFIG
trap 'rm -rf -- "$DOCKER_CONFIG"' EXIT

# STAGE records the digest only after the published index passes validation.
publish_oci_layout() {
  local environment=$1 repository=$2 layout=${3:?expected the path to the release OCI layout}
  local target="$repository:${CI_COMMIT_TAG:?}" checksum digest
  checksum=$(sha256sum "$layout/index.json")
  digest="sha256:${checksum%% *}"
  publish_release_tag "$target" "$digest" crane push --index "$layout"
  if [[ $environment == stage ]]; then
    crane validate --remote "$repository@$digest"
    printf '%s\n' "$digest" > stage-index.digest
  fi
  echo "$target -> $digest"
}

# PROD accepts the verified STAGE index while the release tag still points to it.
promote_stage_to_prod() {
  local stage_repo=${STAGE_CLI_REPOSITORY:?} stage_tag stage_digest digest target
  login_to_registry "$stage_repo" "${STAGE_CLI_REGISTRY_LOGIN:?}" "${STAGE_CLI_REGISTRY_PASSWORD:?}"
  read -r digest < stage-index.digest
  if [[ ! $digest =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "Invalid STAGE index digest in stage-index.digest" >&2
    return 1
  fi
  stage_tag="$stage_repo:${CI_COMMIT_TAG:?}"
  stage_digest=$(crane digest "$stage_tag")
  if [[ $stage_digest != "$digest" ]]; then
    echo "STAGE tag $stage_tag points to $stage_digest, expected verified digest $digest" >&2
    return 1
  fi
  login_to_registry "${PROD_CLI_REPOSITORY:?}" "${PROD_CLI_REGISTRY_LOGIN:?}" "${PROD_CLI_REGISTRY_PASSWORD:?}"
  target="$PROD_CLI_REPOSITORY:$CI_COMMIT_TAG"
  publish_release_tag "$target" "$digest" crane copy --no-clobber "$stage_repo@$digest"
  echo "$target -> $digest"
}

# Registry credentials apply to the host of the release repository.
login_to_registry() {
  local repository=$1 username=$2 password=$3
  printf '%s' "$password" | crane auth login "${repository%%/*}" --username "$username" --password-stdin
}

# Create a missing release tag. A retry must resolve to the same index digest.
publish_release_tag() {
  local target=$1 expected=$2 current
  local -a crane_command=("${@:3}")
  if ! current=$(crane digest "$target" 2>"$DOCKER_CONFIG/lookup.err"); then
    if ! grep -Eq '^Error: GET .*: (MANIFEST_UNKNOWN|NAME_UNKNOWN):' "$DOCKER_CONFIG/lookup.err"; then
      cat "$DOCKER_CONFIG/lookup.err" >&2
      return 1
    fi
    "${crane_command[@]}" "$target"
    current=$(crane digest "$target")
  fi
  if [[ $current != "$expected" ]]; then
    echo "Release tag $target points to $current, expected $expected" >&2
    return 1
  fi
}

case "${1:-}" in
  dev)
    login_to_registry "${DEV_CLI_REPOSITORY:?}" "${DEV_CLI_REGISTRY_LOGIN:?}" "${DEV_CLI_REGISTRY_PASSWORD:?}"
    publish_oci_layout dev "$DEV_CLI_REPOSITORY" "${2:-}"
    ;;
  stage)
    login_to_registry "${STAGE_CLI_REPOSITORY:?}" "${STAGE_CLI_REGISTRY_LOGIN:?}" "${STAGE_CLI_REGISTRY_PASSWORD:?}"
    publish_oci_layout stage "$STAGE_CLI_REPOSITORY" "${2:-}"
    ;;
  prod) promote_stage_to_prod ;;
  *)
    echo "Usage: publish-oci.sh {dev|stage} LAYOUT | prod" >&2
    exit 1
    ;;
esac
