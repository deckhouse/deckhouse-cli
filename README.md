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
URL=https://deckhouse.ru/downloads/deckhouse-cli-trdl
ROOT_VERSION=1
ROOT_SHA512=343bd5f0d8811254e5f0b6fe292372a7b7eda08d276ff255229200f84e58a8151ab2729df3515cb11372dc3899c70df172a4e54c8a596a73d67ae790466a0491
REPO=d8

trdl add $REPO $URL $ROOT_VERSION $ROOT_SHA512
```

Validate that the `d8` binary is installed:

```bash
. $(trdl use d8 0 stable) && d8 --version
```

If you dont want to call `. $(trdl use d8 0 stable)` every time you need to use `d8`, consider adding `alias d8='trdl exec d8 0 stable -- "$@"'` to your shell RC file.

### From binary releases

To install the `d8` binary from the provided GitHub release link, follow these steps:

1. Download your [desired version](https://github.com/deckhouse/deckhouse-cli/releases)
2. Unpack it (`tar xvf d8-vX.Y.Z-OS-ARCH.tar.gz`)
3. Find the `d8` binary in the unpacked directory, and move it to its desired destination under the $PATH.
4. On macOS you might need to remove the quarantine attribute from binary to prevent Gatekeeper from blocking it (
   `sudo xattr -d com.apple.quarantine /path/to/d8`)

## How to build?

### On local machine

You need to have access to Stronghold's private repository for pulling libraries.

Install [Task](https://taskfile.dev/installation) with any suitable method. For example:
`go install github.com/go-task/task/v3/cmd/task@latest`

To correctly access private repository run:

```sh
export PRIVATE_REPO=private.repo.com # replace with correct domain
git config --global url."ssh://git@${PRIVATE_REPO}/".insteadOf "https://flant.internal/"
export GOPRIVATE="flant.internal"
go mod tidy
```

To build for all platforms run:  
`task build:dist:all`

For any specific platform:  
`task build:dist:linux:amd64`  
`task build:dist:darwin:amd64`  
`task build:dist:darwin:arm64`  
`task build:dist:windows:amd64`
