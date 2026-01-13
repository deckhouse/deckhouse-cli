# d8 mirror Command

## Overview

The `d8 mirror` command facilitates the distribution of Deckhouse Kubernetes Platform to air-gapped or restricted network environments. It enables users to download (pull) the complete Deckhouse distribution bundle from an external registry and upload (push) it to a private container registry.

This functionality is exclusively available to users holding a valid license for any commercial version of the Deckhouse Kubernetes Platform.

**Official Documentation:** https://deckhouse.io/products/kubernetes-platform/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

## License Note

© Flant JSC 2025

The `d8 mirror` functionality requires a valid commercial Deckhouse Kubernetes Platform license.

## Commands

- [`d8 mirror pull`](#d8-mirror-pull) - Download Deckhouse distribution to local filesystem
- [`d8 mirror push`](#d8-mirror-push) - Upload Deckhouse distribution to a third-party registry

---

## d8 mirror pull

### Description

Downloads the Deckhouse Kubernetes Platform distribution bundle to the local filesystem. The bundle contains specific platform releases, modules, and security databases that can later be pushed to an air-gapped container registry.

### Synopsis

```bash
d8 mirror pull <images-bundle-path> [flags]
```

### Arguments

- `<images-bundle-path>` - Path to the directory where the bundle will be stored

### Flags

#### Authentication

| Flag | Short | Environment Variable | Description |
|------|-------|---------------------|-------------|
| `--source` | | | Source registry to pull from (default: `registry.deckhouse.ru/deckhouse/ee`) |
| `--source-login` | | `D8_MIRROR_SOURCE_LOGIN` | Source registry login |
| `--source-password` | | `D8_MIRROR_SOURCE_PASSWORD` | Source registry password |
| `--license` | `-l` | `D8_MIRROR_LICENSE_TOKEN` | Deckhouse license key (shortcut for `--source-login=license-token --source-password=<token>`) |

#### Version Selection

| Flag | Description |
|------|-------------|
| `--since-version` | Minimal Deckhouse release to pull. Ignored if above current Rock Solid release. Conflicts with `--deckhouse-tag` |
| `--deckhouse-tag` | Specific Deckhouse build tag to pull. Conflicts with `--since-version`. If the registry contains a release channel image for the specified tag, all release channels in the bundle will point to it |

#### Module Filtering

| Flag | Short | Description |
|------|-------|-------------|
| `--include-module` | `-i` | Whitelist specific modules. Use one flag per module. Disables `--exclude-module`. See [Module Filtering](#module-filtering) for format details |
| `--exclude-module` | `-e` | Blacklist specific modules. Format: `module-name[@version]`. Use one flag per module. Overridden by `--include-module` |
| `--modules-path-suffix` | | Suffix to append to source repo path to locate modules (default: `/modules`) |

#### Component Selection

| Flag | Description |
|------|-------------|
| `--no-platform` | Do not pull Deckhouse Kubernetes Platform into bundle |
| `--no-security-db` | Do not pull security databases into bundle |
| `--no-modules` | Do not pull Deckhouse modules into bundle |
| `--only-extra-images` | Pull only extra images for modules (security databases, scanners, etc.) without main module images |

#### Bundle Options

| Flag | Short | Description |
|------|-------|-------------|
| `--images-bundle-chunk-size` | `-c` | Split resulting bundle into chunks of at most N gigabytes (default: 0 = no splitting) |
| `--gost-digest` | | Calculate GOST R 34.11-2012 STREEBOG digest for downloaded bundle |
| `--force` | | Overwrite existing bundle packages if they conflict with current pull operation |
| `--no-pull-resume` | | Do not continue last unfinished pull operation; start from scratch |

#### Connection Options

| Flag | Description |
|------|-------------|
| `--tls-skip-verify` | Disable TLS certificate validation |
| `--insecure` | Interact with registries over HTTP |
| `--tmp-dir` | Path to temporary directory for processing. Ensure sufficient disk space for the entire bundle |

### Module Filtering

The `--include-module` and `--exclude-module` flags support version constraints for fine-grained control over which module versions to include.

#### Filter Format

```
module-name[@version-constraint]
```

#### Version Constraint Types

1. **No version specified** - Includes all versions from all release channels
   ```bash
   --include-module mymodule
   ```

2. **Semver caret constraint (^)** - Compatible versions (default when version is specified without operator)
   ```bash
   --include-module mymodule@1.3.0
   --include-module mymodule@^1.3.0  # equivalent
   ```
   - Example: For available versions `v1.0.0, v1.1.0, v1.2.0, v1.3.0, v1.3.3, v1.4.1`
   - Constraint `^1.3.0` includes: `v1.3.0, v1.3.3, v1.4.1`
   - Also pulls current versions from all release channels

3. **Semver tilde constraint (~)** - Patch-level changes only
   ```bash
   --include-module mymodule@~1.3.0
   ```
   - Example: For available versions `v1.0.0, v1.1.0, v1.2.0, v1.3.0, v1.3.3, v1.4.1`
   - Constraint `~1.3.0` (equivalent to `>=1.3.0 <1.4.0`) includes: `v1.3.0, v1.3.3`
   - Also pulls current versions from all release channels

4. **Semver range constraint** - Explicit version range
   ```bash
   --include-module mymodule@">=1.1.0 <1.3.0"
   ```

5. **Exact tag match (=)** - Single specific version
   ```bash
   --include-module mymodule@=v1.3.0
   ```
   - Includes only `v1.3.0`
   - Publishes to all release channels (alpha, beta, early-access, stable, rock-solid)

6. **Exact tag with custom build identifier**
   ```bash
   --include-module mymodule@=bobV1
   ```
   - Includes only the `bobV1` tag
   - Publishes to all release channels

7. **Exact tag with specific channel**
   ```bash
   --include-module mymodule@=v1.3.0+stable
   ```
   - Includes only `v1.3.0`
   - Publishes only to the `stable` channel

#### Examples

```bash
# Download all modules
d8 mirror pull /tmp/d8-bundle --license $LICENSE_TOKEN

# Download only specific modules
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-module neuvector \
  --include-module prometheus

# Download module with version constraint
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-module neuvector@^1.2.0

# Exclude specific modules
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --exclude-module module1 \
  --exclude-module module2

# Download only platform and security databases (no modules)
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --no-modules

# Download specific Deckhouse version
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --deckhouse-tag v1.59.0
```

### Environment Variables

Additional configuration options are available through environment variables:

| Variable | Description |
|----------|-------------|
| `SSL_CERT_FILE` | Path to SSL certificate. If set, system certificates are not used |
| `SSL_CERT_DIR` | Colon-separated list of directories to search for SSL certificate files. If set, system certificates are not used. More info: https://docs.openssl.org/1.0.2/man1/c_rehash/ |
| `HTTP_PROXY` | URL of proxy server for HTTP requests to hosts not listed in `NO_PROXY` |
| `HTTPS_PROXY` | URL of proxy server for HTTPS requests to hosts not listed in `NO_PROXY` |
| `NO_PROXY` | Comma-separated list of hosts to exclude from proxying. Supports IP addresses, CIDR notations, domains, and asterisk. Domains match name and subdomains. A single `*` disables all proxying |

### Bundle Structure

The pull command creates a bundle with the following structure:

```
<images-bundle-path>/
├── platform.tar              # Deckhouse platform images (if not --no-platform)
├── security.tar              # Security databases (if not --no-security-db)
├── module-<name>.tar         # Individual module bundles (if not --no-modules)
└── module-<name>-<n>.chunk   # Chunked bundles (if --images-bundle-chunk-size is set)
```

If `--gost-digest` is specified, `.gostsum` files are created alongside each `.tar` file.

---

## d8 mirror push

### Description

Uploads a previously downloaded Deckhouse Kubernetes Platform distribution bundle to a third-party container registry. This is typically used to populate an air-gapped registry with Deckhouse images.

### Synopsis

```bash
d8 mirror push <images-bundle-path> <registry> [flags]
```

### Arguments

- `<images-bundle-path>` - Path to the directory containing the bundle created by `d8 mirror pull`
- `<registry>` - Target registry address (format: `registry-host[:port]/path`)

### Flags

#### Authentication

| Flag | Short | Environment Variable | Description |
|------|-------|---------------------|-------------|
| `--registry-login` | `-u` | `D8_MIRROR_REGISTRY_LOGIN` | Username to log into the target registry |
| `--registry-password` | `-p` | `D8_MIRROR_REGISTRY_PASSWORD` | Password to log into the target registry |

#### Connection Options

| Flag | Description |
|------|-------------|
| `--tls-skip-verify` | Disable TLS certificate validation |
| `--insecure` | Interact with registries over HTTP |
| `--tmp-dir` | Path to temporary directory for processing. Ensure sufficient disk space for the entire bundle |

#### Other Options

| Flag | Description |
|------|-------------|
| `--modules-path-suffix` | Suffix to append to source repo path to locate modules (default: `/modules`) |

### Examples

```bash
# Push bundle to private registry with authentication
d8 mirror push /tmp/d8-bundle registry.company.com/deckhouse \
  --registry-login admin \
  --registry-password secretpassword

# Push to registry over HTTP (insecure)
d8 mirror push /tmp/d8-bundle registry.local:5000/deckhouse \
  --insecure

# Push with custom temporary directory
d8 mirror push /tmp/d8-bundle registry.company.com/deckhouse \
  --registry-login admin \
  --registry-password secretpassword \
  --tmp-dir /mnt/large-disk/tmp
```

### Environment Variables

The same environment variables used by `d8 mirror pull` are also supported:

| Variable | Description |
|----------|-------------|
| `SSL_CERT_FILE` | Path to SSL certificate |
| `SSL_CERT_DIR` | Colon-separated list of directories for SSL certificates |
| `HTTP_PROXY` | HTTP proxy server URL |
| `HTTPS_PROXY` | HTTPS proxy server URL |
| `NO_PROXY` | Comma-separated list of hosts to exclude from proxying |

---

## Complete Workflow Example

### Scenario: Air-Gapped Installation

1. **On a machine with internet access**, download the bundle:
   ```bash
   d8 mirror pull /tmp/deckhouse-bundle \
     --license $DECKHOUSE_LICENSE_TOKEN \
     --since-version 1.59.0 \
     --include-module prometheus \
     --include-module ingress-nginx \
     --images-bundle-chunk-size 10
   ```

2. **Transfer the bundle** to the air-gapped environment (USB drive, secure file transfer, etc.)

3. **On a machine in the air-gapped environment**, push to the internal registry:
   ```bash
   d8 mirror push /tmp/deckhouse-bundle registry.internal.company.com/deckhouse \
     --registry-login admin \
     --registry-password admin_password
   ```

4. **Configure Deckhouse** to use the internal registry during installation

---

## Troubleshooting

### Common Issues

#### Insufficient Disk Space

**Problem:** Pull operation fails due to lack of disk space.

**Solution:** 
- Use `--tmp-dir` to specify a location with more space
- Use `--images-bundle-chunk-size` to split the bundle into smaller chunks
- Use component flags (`--no-modules`, `--no-security-db`) to reduce bundle size

#### TLS Certificate Errors

**Problem:** TLS certificate validation failures when connecting to registries.

**Solution:**
- Use `--tls-skip-verify` to disable certificate validation (not recommended for production)
- Set `SSL_CERT_FILE` or `SSL_CERT_DIR` to provide custom certificates
- Ensure system certificates are up to date

#### Authentication Failures

**Problem:** Unable to authenticate to source or target registry.

**Solution:**
- Verify credentials are correct
- For Deckhouse registry, use `--license` flag with your license token
- Check that the license is valid and not expired
- Ensure network access to the registry

#### Pull Resume Issues

**Problem:** Pull operation keeps retrying from scratch.

**Solution:**
- The tool automatically resumes incomplete pulls within 24 hours
- Use `--no-pull-resume` to force starting from scratch
- Use `--force` to overwrite conflicting bundle packages

### Debug Logging

Enable debug logging to troubleshoot issues:

```bash
# Set debug level (1-4, higher = more verbose)
export D8_LOG_LEVEL=3

d8 mirror pull /tmp/bundle --license $LICENSE_TOKEN
```

---

## Best Practices

### Performance Optimization

1. **Use Local Storage:** Ensure `--tmp-dir` points to fast local storage (SSD preferred)
2. **Network Bandwidth:** Large bundles can be several gigabytes; ensure adequate bandwidth
3. **Parallel Operations:** The tool automatically parallelizes image pulling
4. **Chunking:** For very large bundles or size-constrained transfers, use `--images-bundle-chunk-size`

### Security Considerations

1. **Protect Credentials:** Use environment variables for sensitive values
2. **Secure Transfer:** Use encrypted channels when transferring bundles
3. **Verify Integrity:** Use `--gost-digest` to generate checksums for bundle verification
4. **TLS Validation:** Avoid `--tls-skip-verify` in production environments

### Version Management

1. **Use `--since-version`:** To pull only newer releases, reducing bundle size
2. **Specific Versions:** Use `--deckhouse-tag` for controlled, predictable deployments
3. **Module Versioning:** Leverage semver constraints for flexible module version management
4. **Release Channels:** Understand that pulling includes all configured release channels (alpha, beta, early-access, stable, rock-solid)

### Bundle Management

1. **Regular Updates:** Periodically pull new bundles to stay current with security patches
2. **Version Control:** Track which bundle versions are deployed in which environments
3. **Storage Planning:** Account for bundle size growth over time (multiple versions, modules)
4. **Cleanup:** Remove old bundles after successful deployment to free space

---

## Architecture Notes

### Bundle Components

The mirror system handles three primary component types:

1. **Platform Images:** Core Deckhouse Kubernetes Platform containers
2. **Modules:** Optional Deckhouse modules with versioned releases
3. **Security Databases:** Vulnerability scanning databases and related data

### Module Filtering Logic

- **Whitelist Mode:** Only specified modules are included
- **Blacklist Mode:** All modules except specified ones are included (default)
- **Version Constraints:** Semver-based filtering for granular control
- **Release Channels:** Each module can have multiple release channel versions

### Storage Layout

Internally, the tool uses OCI-compliant image layouts for organizing container images and manifests. This ensures compatibility with standard container tools and registries.

---

## Additional Resources

- [Deckhouse Official Documentation](https://deckhouse.io/products/kubernetes-platform/documentation/)
- [Air-Gapped Installation Guide](https://deckhouse.io/products/kubernetes-platform/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry)
- [Deckhouse Licensing](https://deckhouse.io/products/kubernetes-platform/pricing.html)

---

## Support

For issues, questions, or feature requests:
- GitHub Issues: https://github.com/deckhouse/deckhouse-cli/issues
- Community Support: https://deckhouse.io/community/
- Commercial Support: Available with Deckhouse commercial licenses
