#!/usr/bin/env bash
set -euo pipefail

# Use current origin branches and their full history, including in reused runner checkouts.
fetch_options=(--no-tags --prune)
if [[ $(git rev-parse --is-shallow-repository) == true ]]; then
  fetch_options+=(--unshallow)
fi
git fetch "${fetch_options[@]}" origin \
  "+refs/heads/$CI_DEFAULT_BRANCH:refs/remotes/origin/$CI_DEFAULT_BRANCH" \
  '+refs/heads/release-*:refs/remotes/origin/release-*'

# A release tag may point to any commit already in main or a release-<major>.<minor> branch.
branches=$(git for-each-ref --contains="$CI_COMMIT_SHA" --format='%(refname:strip=3)' \
  "refs/remotes/origin/$CI_DEFAULT_BRANCH" 'refs/remotes/origin/release-*')
while IFS= read -r branch; do
  if [[ $branch == "$CI_DEFAULT_BRANCH" || $branch =~ ^release-[0-9]+\.[0-9]+$ ]]; then
    echo "Release commit $CI_COMMIT_SHA is in $branch."
    exit 0
  fi
done <<< "$branches"

echo "Release commit $CI_COMMIT_SHA is outside $CI_DEFAULT_BRANCH and all release-<major>.<minor> branches." >&2
exit 1
