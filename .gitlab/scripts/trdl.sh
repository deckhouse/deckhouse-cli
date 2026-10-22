#!/usr/bin/env bash
#
# Run a trdl release or publish task on the trdl Vault server and follow it.
#
# Port of werf/trdl-vault-actions (release, publish): approle login, start
# the task, stream its log, and retry the whole operation every minute until
# it succeeds or TRDL_MAX_DELAY runs out. A release is expected to fail
# for a while: the tag gets signed (task release:sign) after it is pushed.
#
# Usage:
#   trdl.sh release <git-tag>
#   trdl.sh publish
#
# Required env:
#   TRDL_VAULT_ADDR, TRDL_VAULT_ROLE_ID, TRDL_VAULT_SECRET_ID
# Optional env:
#   TRDL_PROJECT    trdl project (Vault mount path), default deckhouse-cli
#   TRDL_MAX_DELAY  seconds to keep retrying, default 21600 (6h)

set -euo pipefail

project="${TRDL_PROJECT:-deckhouse-cli}"
max_delay="${TRDL_MAX_DELAY:-21600}"

case "${1:-}" in
  release) op=release; body="$(jq -nc --arg tag "${2:?git tag is required}" '{git_tag: $tag}')" ;;
  publish) op=publish; body='{}' ;;
  *) echo "usage: $0 release <git-tag> | publish" >&2; exit 2 ;;
esac

: "${TRDL_VAULT_ADDR:?TRDL_VAULT_ADDR is required}"
: "${TRDL_VAULT_ROLE_ID:?TRDL_VAULT_ROLE_ID is required}"
: "${TRDL_VAULT_SECRET_ID:?TRDL_VAULT_SECRET_ID is required}"

# vault METHOD PATH [curl args...]: print the response body, fail on HTTP errors.
vault() {
  local method="$1" path="$2"
  shift 2
  curl -sS --fail-with-body -X "$method" -H "X-Vault-Token: ${token:-}" "$@" "${TRDL_VAULT_ADDR%/}/v1/${path}"
}

# Credentials go through env and stdin, not argv.
login() {
  token="$(jq -n 'env | {role_id: .TRDL_VAULT_ROLE_ID, secret_id: .TRDL_VAULT_SECRET_ID}' \
    | vault POST auth/approle/login --data @- | jq -r .auth.client_token)"
}

# Start one task and follow it to the end; non-zero unless it succeeded.
run_task() {
  local resp uuid task chunk offset=0
  login || return 1
  # The server does not start a task while another one of the project runs.
  until resp="$(vault POST "${project}/${op}" --data "$body")"; do
    grep -q busy <<<"$resp" || { echo "$resp" >&2; return 1; }
    echo "trdl server is busy, retrying in 5s"
    sleep 5
  done
  uuid="$(jq -r .data.task_uuid <<<"$resp")"
  echo "Started trdl ${op} task ${uuid}"
  while :; do
    # Log in on every poll, as the upstream action does: a build can outlive the token.
    login || return 1
    task="$(vault GET "${project}/task/${uuid}")" || return 1
    # The trailing "x" keeps the log's trailing newlines, which $(...) would strip.
    chunk="$(vault GET "${project}/task/${uuid}/log?limit=1000000000&offset=${offset}" | jq -j .data.result; echo x)"
    chunk="${chunk%x}"
    printf '%s' "$chunk"
    offset=$((offset + $(printf '%s' "$chunk" | wc -c)))
    case "$(jq -r .data.status <<<"$task")" in
      SUCCEEDED) return 0 ;;
      FAILED) echo "trdl task ${uuid} failed: $(jq -r .data.reason <<<"$task")" >&2; return 1 ;;
    esac
    sleep 5
  done
}

start="$SECONDS"
until run_task; do
  if (( SECONDS - start >= max_delay )); then
    echo "trdl ${op} did not succeed within ${max_delay}s" >&2
    exit 1
  fi
  echo "Retrying trdl ${op} in 1 minute..."
  sleep 60
done
