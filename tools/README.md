# D8 (Deckhouse CLI) Installation Script

This directory contains the installation script for D8 (Deckhouse CLI).

## Quick Install

### Using curl

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
```

### Using wget

```bash
sh -c "$(wget -qO- https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
```

## Installation Options

### Install Specific Version

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --version v1.0.0
```

### Install to Custom Directory

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --install-dir ~/bin
```

### Force Reinstall

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --force
```

### Non-Interactive Installation

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --unattended
```

## Environment Variables

The installer respects the following environment variables:

- `VERSION` - Version to install (default: latest)
- `INSTALL_DIR` - Installation directory (default: /usr/local/bin)
- `REPO` - GitHub repository (default: deckhouse/deckhouse-cli)
- `FORCE` - Force reinstall if already exists (yes/no)

### Example with Environment Variables

```bash
VERSION=v1.0.0 INSTALL_DIR=~/bin sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
```

## Manual Installation

1. Download the script:
   ```bash
   wget https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh
   ```

2. Make it executable:
   ```bash
   chmod +x install.sh
   ```

3. Run the script:
   ```bash
   ./install.sh --version v1.0.0 --install-dir ~/bin
   ```

## Supported Platforms

The installer supports the following platforms:

- **Linux**
  - amd64 (x86_64)
  - arm64 (aarch64)

- **macOS**
  - amd64 (x86_64)
  - arm64 (Apple Silicon)

## Requirements

The following tools must be available:

- `curl` or `wget` - for downloading the binary
- `tar` - for extracting the archive
- `sudo` - for installing to system directories (if needed)

## Troubleshooting

### Permission Denied

If you get a permission denied error, try:

1. Install to a user-writable directory:
   ```bash
   sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --install-dir ~/bin
   ```

2. Or use sudo:
   ```bash
   sudo sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
   ```

### Binary Not in PATH

If `d8` is not found after installation, add the installation directory to your PATH:

```bash
# For bash/zsh
echo 'export PATH="/usr/local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# For custom installation directory
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### Unsupported Platform

If you see "Unsupported platform" error, you may need to build from source:

```bash
git clone https://github.com/deckhouse/deckhouse-cli.git
cd deckhouse-cli
go build -o d8 ./main.go
```

## Uninstallation

To uninstall D8, simply remove the binary:

```bash
sudo rm /usr/local/bin/d8
```

Or if installed to a custom directory:

```bash
rm ~/bin/d8
```
