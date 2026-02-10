<div align="center">

# 🚀 D8 - Deckhouse CLI

**Command-line client for Deckhouse Kubernetes Platform**

[![GitHub Release](https://img.shields.io/github/v/release/deckhouse/deckhouse-cli)](https://github.com/deckhouse/deckhouse-cli/releases)
[Features](#-features) •
[Installation](#-installation) •
[Quick Start](#-quick-start) •
[Documentation](#-documentation) •
[Contributing](#-contributing)

</div>

---

## 📖 Overview

**D8** (Deckhouse CLI) is a powerful command-line client for managing and interacting with the [Deckhouse Kubernetes Platform](https://deckhouse.io/). It provides essential tools for cluster operations, module management, backup/restore, and system administration.

### Why D8?

- ✅ **Cluster Management**: Comprehensive cluster status and control
- 🚀 **Module Operations**: Mirror, backup, and manage Deckhouse modules
- 🔧 **System Tools**: Debug info collection and system diagnostics
- 🎯 **CI/CD Ready**: Perfect for automated deployment pipelines
- 📦 **Multi-platform**: Native binaries for Linux, macOS, and Windows

---

## 🎯 Features

### 🔍 Cluster Operations

D8 provides comprehensive cluster management capabilities:

| Command | Purpose | Key Features |
|---------|---------|--------------|
| [**backup**](internal/backup/) | Backup operations | ETCD snapshots, configuration backups, data export |
| [**mirror**](internal/mirror/) | Module mirroring | Registry operations, image synchronization, air-gapped deployments |
| [**system**](internal/system/) | System diagnostics | Debug info collection, logs analysis, troubleshooting |
| **user-operation** | Local user operations | Request `UserOperation` in `user-authn` (ResetPassword/Reset2FA/Lock/Unlock) |

### 🚀 Module Management

Advanced tools for Deckhouse module lifecycle management:

- **Mirror Operations**: Copy modules to local registries or air-gapped environments
- **Backup/Restore**: Full cluster and module backup capabilities
- **Data Export**: Extract and export cluster data for migration or analysis
- **Virtualization**: Manage virtual machines in Kubernetes clusters

### ⚙️ System Administration

Essential tools for system administrators:

- **Debug Collection**: Automated system information gathering
- **Log Analysis**: Centralized logging and troubleshooting
- **Platform Updates**: Safe update management for Deckhouse installations
- **Security Tools**: Stronghold integration for secure operations

---

## 📦 Installation

### Method 1: Install Script (Recommended)

Quick one-line installation for Linux and macOS:

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
```

<details>
<summary>Alternative installation commands</summary>

**Using wget:**
```bash
sh -c "$(wget -qO- https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
```

**Install specific version:**
```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --version v1.0.0
```

**Install to custom directory:**
```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)" "" --install-dir ~/bin
```

See [installation guide](tools/README.md) for more options.
</details>

### Method 2: Download Binary

Download the latest release for your platform from the [releases page](https://github.com/deckhouse/deckhouse-cli/releases).

**Supported Platforms:**
- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64)

### Method 3: Using trdl (Recommended for Updates)

[trdl](https://trdl.dev/) is a tool release delivery system that provides automatic updates and channel management:

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

If you don't want to call `. $(trdl use d8 0 stable)` every time you need to use `d8`, consider adding `alias d8='trdl exec d8 0 stable -- "$@"'` to your shell RC file.

### Method 4: Go Install

If you have Go installed:

```bash
go install github.com/deckhouse/deckhouse-cli@main
```

> **Note**: Ensure `~/go/bin` is in your PATH after installation.

### Verify Installation

```bash
d8 --version
```

---

## 🧰 User operations (user-authn)

Request local user operations for Dex static users via `UserOperation` custom resources.

```bash
# Reset user's 2FA (TOTP)
d8 user-operation reset2fa test-user --timeout 5m

# Lock user for 10 minutes
d8 user-operation lock test-user 10m --timeout 5m

# Unlock user
d8 user-operation unlock test-user --timeout 5m

# Reset password (bcrypt hash is required)
HASH="$(echo -n 'Test12345!' | htpasswd -BinC 10 \"\" | cut -d: -f2 | tr -d '\n')"
d8 user-operation reset-password test-user "$HASH" --timeout 5m
```

---

## 🤝 Contributing

We welcome contributions! Here's how you can help:

1. **Report Bugs**: Open an issue describing the problem
2. **Suggest Features**: Share your ideas for improvements
3. **Submit PRs**: Fix bugs or add features
4. **Improve Docs**: Help make documentation better

### Building from Source

You need to have access to private repositories for pulling dependencies.

Install [Task](https://taskfile.dev/installation) with any suitable method:

```bash
go install github.com/go-task/task/v3/cmd/task@latest
```

To correctly access private repository run:

```bash
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

---

## 🔗 Links

- **Website**: [deckhouse.io](https://deckhouse.io/)
- **Issues**: [Report a bug or request a feature](https://github.com/deckhouse/deckhouse-cli/issues)
- **Releases**: [Download binaries](https://github.com/deckhouse/deckhouse-cli/releases)
- **Documentation**: [Deckhouse Documentation](https://deckhouse.io/documentation/)

---

## 🌟 Support

If you find D8 helpful, please consider:
- ⭐ Starring the repository
- 🐛 Reporting bugs
- 💡 Suggesting features
- 📖 Contributing to documentation
- 🔀 Submitting pull requests
