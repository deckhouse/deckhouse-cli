# Deckhouse Client

Deckhouse Client (d8) is a command-line client for Deckhouse.

## How to install?

### From install script (Linux or macOS)

The install script will automatically grab the latest version of `d8` and install it locally.

You can fetch and run this script via `curl`:

```bash
curl -fsSL -o d8-install.sh https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/d8-install.sh
bash d8-install.sh
```

of via `wget`:

```bash
wget -q https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/d8-install.sh
bash d8-install.sh
```

### From binary releases

To install the `d8` binary from the provided GitHub release link, follow these steps:

1. Download your [desired version](https://github.com/deckhouse/deckhouse-cli/releases)
2. Unpack it (`tar -xvf d8-v0.1.0-linux-amd64.tar.gz`)
3. Find the `d8` binary in the unpacked directory, and move it to its desired destination (`mv linux-amd64/d8 /usr/local/bin/d8`)
