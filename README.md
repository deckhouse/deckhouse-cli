<p align="center">
  <img src="https://github.com/user-attachments/assets/67c237f7-c8a4-4df3-b3f0-c5994876757a"/>
</p>


# Deckhouse Client

Deckhouse Client (d8) is a command-line client for Deckhouse.

## How to install?

### Using trdl package manager (Recommended)

Deckhouse CLI is distributed and updated via [trdl](https://trdl.dev/). You
should [install trdl client](https://trdl.dev/quickstart.html#installing-the-client) first.

After that is dealt with, add the Deckhouse CLI repository into trdl. Proceed with the following shell command:

```bash
URL=https://trrr.flant.dev/trdl-deckhouse-cli
ROOT_VERSION=1
ROOT_SHA512=$(curl -Ls ${URL}/${ROOT_VERSION}.root.json | sha512sum | tr -d '\-[:space:]\n')
REPO=trdl-deckhouse-cli

trdl add $REPO $URL $ROOT_VERSION $ROOT_SHA512
```

And install stable release using:

```bash
trdl update $REPO $ROOT_VERSION stable
```

Validate that the `d8` binary is installed:

```bash
. $(trdl use $REPO $ROOT_VERSION stable) && d8 --version
```

If you dont want to call `. $(trdl use $REPO $ROOT_VERSION stable)` every time you need to use `d8`, consider adding `export PATH=$PATH:$(trdl bin-path trdl-deckhouse-cli 1 stable)` to your shell RC file.

### From binary releases

To install the `d8` binary from the provided GitHub release link, follow these steps:

1. Download your [desired version](https://github.com/deckhouse/deckhouse-cli/releases)
2. Unpack it (`tar xvf d8-vX.Y.Z-OS-ARCH.tar.gz`)
3. Find the `d8` binary in the unpacked directory, and move it to its desired destination under the $PATH.
4. On macOS you might need to remove the quarantine attribute from binary to prevent Gatekeeper from blocking it (
   `sudo xattr -d com.apple.quarantine /path/to/d8`)

## How to build?
###  On local machine

You need to have access to Stronghold's private repository for pulling libraries.

Install [Task](https://taskfile.dev/installation) with any suitable method. For example:
`go install github.com/go-task/task/v3/cmd/task@latest`

To correctly access private repository run:
```
export PRIVATE_REPO=private.repo.com # replace with correct domain
git config --global url."ssh://git@${PRIVATE_REPO}/".insteadOf "https://flant.internal/"
export GOPRIVATE="flant.internal/team/foxtrot/*"
go mod tidy
```

To build for all platforms run:  
`task build:dist:all`

For any specific platform:  
`task build:dist:linux:amd64`  
`task build:dist:darwin:amd64`  
`task build:dist:darwin:arm64`  
`task build:dist:windows:amd64`