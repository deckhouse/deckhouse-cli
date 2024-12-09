#!/bin/sh -e

apt-get update && apt-get install -y apt-utils libbtrfs-dev file git gcc
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/bin
git config --global url."https://gitlab-ci-token:$PRIVATE_REPO_TOKEN@fox.flant.com/".insteadOf https://fox.flant.com/
git config --global --add safe.directory '*'
