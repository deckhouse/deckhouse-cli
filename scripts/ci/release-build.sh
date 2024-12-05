#!/bin/sh -e

VERSION=$1
if [ -z "$VERSION" ] ; then
    echo "Required version argument!" 1>&2
    echo 1>&2
    echo "Usage: $0 VERSION" 1>&2
    exit 1
fi

apt-get update && apt-get install -y apt-utils libbtrfs-dev file git gcc software-properties-common && add-apt-repository ppa:longsleep/golang-backports && apt install golang-1.23
export PATH=$PATH:/usr/lib/go-1.23/bin
git config --global url."https://gitlab-ci-token:scwnA_eeAQy9qEmSL7z9@fox.flant.com/".insteadOf https://fox.flant.com/
git config --global --add safe.directory '*'
go run github.com/mitchellh/gox@latest -osarch="linux/amd64" -output="release-build/v0.7.0/{{.OS}}-{{.Arch}}/bin/d8" -ldflags="-linkmode external -extldflags=-static" -tags="dfrunsecurity dfrunnetwork dfrunmount dfssh containers_image_openpgp osusergo exclude_graphdriver_devicemapper netgo no_devmapper static_build cni" github.com/deckhouse/deckhouse-cli
