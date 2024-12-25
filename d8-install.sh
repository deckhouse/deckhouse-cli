#!/usr/bin/env bash

# Copyright 2024 Flant JSC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Inspired by https://github.com/helm/helm/blob/main/scripts/get-helm-3

set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"


# init discovers the operating system, architecture and available tools for this system
init() {
  OS=$(uname | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)

  case $ARCH in
    aarch64) ARCH="arm64";;
    x86_64) ARCH="amd64";;
  esac

  HAS_CURL="$(command -v curl &> /dev/null && echo true || echo false)"
  HAS_WGET="$(command -v wget &> /dev/null && echo true || echo false)"
}


# preflight checks that the os/arch combination is supported for
# binary builds, as well whether or not necessary tools are present
preflight() {
  local supported="darwin-amd64\ndarwin-arm64\nlinux-amd64"
  if ! echo "${supported}" | grep -q "${OS}-${ARCH}"; then
    echo "No prebuilt binary for ${OS}-${ARCH}."
    echo "To build from source, go to https://github.com/deckhouse/deckhouse-cli"
    exit 1
  fi

  if [ "${HAS_CURL}" != "true" ] && [ "${HAS_WGET}" != "true" ]; then
    echo "Either curl or wget is required"
    exit 1
  fi
}


getLatestTag() {
  local LATEST_RELEASE_URL="https://api.github.com/repos/deckhouse/deckhouse-cli/releases/latest"
  if [ "${HAS_CURL}" == "true" ]; then
    TAG=$(curl -SsL "$LATEST_RELEASE_URL" | grep "tag_name" | cut -d: -f2 | tr -d '", ')
  elif [ "${HAS_WGET}" == "true" ]; then
    TAG=$(wget -qO- "$LATEST_RELEASE_URL" | grep "tag_name" | cut -d: -f2 | tr -d '", ')
  fi
}


downloadRelease() {
  D8_DIST="d8-$TAG-$OS-$ARCH.tar.gz"
  DOWNLOAD_URL="https://deckhouse.io/downloads/deckhouse-cli/$TAG/$D8_DIST"
  CHECKSUM_URL="$DOWNLOAD_URL.sha256sum"

  D8_TMP="$(mktemp -dt d8-installer-XXXXXX)"
  D8_TMP_FILE="$D8_TMP/$D8_DIST"
  D8_SUM_FILE="$D8_TMP/$D8_DIST.sha256sum"

  echo "Downloading $DOWNLOAD_URL"
  if [ "${HAS_CURL}" == "true" ]; then
    curl -SsL "$CHECKSUM_URL" -o "$D8_SUM_FILE"
    curl -SsL "$DOWNLOAD_URL" -o "$D8_TMP_FILE"
  elif [ "${HAS_WGET}" == "true" ]; then
    wget -qO "$D8_SUM_FILE" "$CHECKSUM_URL"
    wget -qO "$D8_TMP_FILE" "$DOWNLOAD_URL"
  fi
}


verifyChecksum() {
  printf "Verifying checksum... "
  pushd "$D8_TMP" > /dev/null
  sha256sum --check "$D8_SUM_FILE"
  popd > /dev/null
}


installBinary() {
  tar -xf "$D8_TMP_FILE" -C "$D8_TMP"
  echo "Installing d8 into ${INSTALL_DIR}"

  set +e
  local output
  local rc
  output=$(install "$D8_TMP/$OS-$ARCH/bin/d8" "$INSTALL_DIR" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    if grep -q "Permission denied" <<< "$output"; then
      echo "Got Permission denied error. Trying with sudo (you may need to enter sudo password)"
      set -e
      sudo install "$D8_TMP/$OS-$ARCH/bin/d8" "$INSTALL_DIR"
    else
      echo "Unexpected error during install: $output"
      set -e
      exit 1
    fi
  fi

  set -e
  echo "d8 installed into $INSTALL_DIR"
}


testInstallation() {
  if ! command -v d8 > /dev/null ; then
    echo "d8 not found. Is $INSTALL_DIR on your "'$PATH?'
    exit 1
  fi

  D8_VERSION=$(d8 --version)
  if [[ "$D8_VERSION" != "d8 version $TAG" ]]; then
    echo ""
    echo "Unexpected error: d8 --version output does not match the installed version."
    echo
    echo "Installed d8 version $TAG"
    echo "Used $D8_VERSION"
    echo ""
    echo "Maybe there is a different version of d8 in your "'$PATH'"."
    exit 1
  fi
}


cleanup() {
  if [[ -d "${D8_TMP:-}" ]]; then
    rm -rf "$D8_TMP"
  fi
}


trap "cleanup" EXIT

init
preflight
getLatestTag
downloadRelease
verifyChecksum
installBinary
testInstallation
