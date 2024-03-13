# Deckhouse Client

Deckhouse Client (d8) is a command-line client for Deckhouse.

## How to install?

To install the `d8` binary from the provided GitHub release link, follow these steps:

1. **Define variables** (`darwin/amd64` in this case, release v0.0.3):
   ```bash
   RELEASE_VERSION=0.0.3
   OS=darwin
   ARCH=amd64

1. **Download the Binary:**
   Download the `d8` binary for your operating system and architecture . You can do this by clicking on the link or by using the `curl` command:
   ```bash
   curl -LO "https://github.com/deckhouse/deckhouse-cli/releases/download/v${RELEASE_VERSION}/d8-v${RELEASE_VERSION}-${OS}-${ARCH}.tar.gz"
   ```

1. **Extract the Binary:**
   Once the download is complete, extract the contents of the downloaded `.tar.gz` file. You can do this using the `tar` command:
   ```bash
   tar -xvf "d8-v${RELEASE_VERSION}-${OS}-${ARCH}.tar.gz" "${OS}-${ARCH}/d8"
   ```

1. **Move the Binary to a Directory in Your PATH:**
   Move the extracted binary (`d8`) to a directory that is included in your system's PATH environment variable. This ensures that you can run the binary from any location in your terminal.
   ```bash
   sudo mv "${OS}-${ARCH}/d8" /usr/local/bin/
   ```

1. **Verify Installation:**
   You can verify that the `d8` binary is installed correctly by running the following command:
   ```bash
   d8 help
   ```

   This command should output the version of the installed `d8` binary, confirming that it was installed successfully.

Now you have successfully installed the `d8` binary on your system! You can start using it to interact with Deckhouse Kubernetes Platform according to its documentation and usage instructions.
