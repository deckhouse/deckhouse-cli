#!/usr/bin/env bash
#
# Reproduce the trdl server release build from trdl.yaml, without a tag.
#
# The trdl server builds a release by generating a Dockerfile
# (werf/trdl: server/pkg/docker/dockerfile.go):
#
#   FROM <dockerImage> AS builder
#   COPY . /git
#   WORKDIR /git
#   RUN mkdir -p /result
#   RUN --mount=type=secret,... <commands joined with &&>
#   FROM scratch
#   COPY --from=builder /result /result/
#
# This script generates the same Dockerfile and runs `docker build`
# with the same secret ids the server mounts. If this build passes,
# the release build for a future tag passes too.
#
# Usage:
#   trdl-build-check.sh [--dry-run] [tag]
#
#   --dry-run  print the generated Dockerfile and exit (no secrets needed)
#   tag        synthetic release tag, default v0.0.0-trdl-check
#
# Required env (unless --dry-run):
#   SOURCE_REPO_SSH_KEY     SSH private key for the private repo (plain text)
#   DECKHOUSE_PRIVATE_REPO  hostname of the private repo

set -euo pipefail

dry_run=0
if [ "${1:-}" = "--dry-run" ]; then
  dry_run=1
  shift
fi
tag="${1:-v0.0.0-trdl-check}"

cd "$(dirname "$0")/../.."

# trdl.yaml is flat: a scalar dockerImage and a plain list of commands.
# Keep it that way - this parser does not understand anything else.
image="$(awk '$1 == "dockerImage:" {print $2; exit}' trdl.yaml)"
commands="$(grep '^  - ' trdl.yaml \
  | sed -e 's/^  - //' -e "s/{{ \.Tag }}/${tag}/g" \
  | awk 'NR > 1 {printf " && "} {printf "%s", $0} END {print ""}')"

if [ -z "$image" ] || [ -z "$commands" ]; then
  echo "failed to parse dockerImage/commands from trdl.yaml" >&2
  exit 1
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

cat > "$workdir/Dockerfile" <<EOF
FROM ${image} AS builder
COPY . /git
WORKDIR /git
RUN mkdir -p /result
RUN --mount=type=secret,id=deckhouse-private-repo --mount=type=secret,id=stronghold-ssh ${commands}
FROM scratch
COPY --from=builder /result /result/
EOF

if [ "$dry_run" = 1 ]; then
  cat "$workdir/Dockerfile"
  exit 0
fi

: "${SOURCE_REPO_SSH_KEY:?SOURCE_REPO_SSH_KEY is required}"
: "${DECKHOUSE_PRIVATE_REPO:?DECKHOUSE_PRIVATE_REPO is required}"

# The build reads the key as base64 (see trdl.yaml) and ssh-add needs
# a trailing newline in the decoded key.
umask 077
printf '%s\n' "$SOURCE_REPO_SSH_KEY" | base64 > "$workdir/stronghold-ssh"
printf '%s' "$DECKHOUSE_PRIVATE_REPO" > "$workdir/deckhouse-private-repo"

DOCKER_BUILDKIT=1 docker build \
  --platform linux/amd64 \
  --file "$workdir/Dockerfile" \
  --secret "id=stronghold-ssh,src=$workdir/stronghold-ssh" \
  --secret "id=deckhouse-private-repo,src=$workdir/deckhouse-private-repo" \
  --output "type=local,dest=$workdir/out" \
  --progress plain \
  .

echo
echo "Artifacts built for ${tag}:"
ls -lR "$workdir/out/result"
test -n "$(ls -A "$workdir/out/result")"
