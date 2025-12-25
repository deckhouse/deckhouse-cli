# E2E Tests for d8 mirror

End-to-end tests for the `d8 mirror pull` and `d8 mirror push` commands.

## Overview

These tests perform a complete mirror cycle:
1. Collect reference digests from source registry
2. Pull images to a local bundle
3. Push bundle to a target registry
4. Validate the target registry structure
5. Compare all digests between source and target

## Requirements

- Built `d8` binary (run `task build` from project root)
- Valid license token for the source registry
- Network access to the source registry

## Running Tests

### Basic Usage

```bash
# Run with license token
go test -v ./testing/e2e/mirror/... \
  -license-token=YOUR_LICENSE_TOKEN

# Or use environment variables (recommended)
E2E_LICENSE_TOKEN=YOUR_LICENSE_TOKEN \
go test -v ./testing/e2e/mirror/...
```

### Full Configuration

```bash
# Using license token
go test -v ./testing/e2e/mirror/... \
  -source-registry=registry.deckhouse.ru/deckhouse/fe \
  -license-token=YOUR_LICENSE_TOKEN \
  -target-registry=my-registry.local:5000/deckhouse \
  -target-user=admin \
  -target-password=secret \
  -tls-skip-verify \
  -keep-bundle

# Using explicit source credentials (with self-signed cert)
go test -v ./testing/e2e/mirror/... \
  -source-registry=my-source-registry.local/deckhouse \
  -source-user=admin \
  -source-password=secret \
  -target-registry=my-target-registry.local:5000/deckhouse \
  -tls-skip-verify

# Using environment variables (recommended)
E2E_SOURCE_REGISTRY=localhost:443/deckhouse-etalon \
E2E_SOURCE_USER=admin \
E2E_SOURCE_PASSWORD=secret \
E2E_TLS_SKIP_VERIFY=true \
go test -v ./testing/e2e/mirror/...
```

### Environment Variables

All flags can be set via environment variables:

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `-source-registry` | `E2E_SOURCE_REGISTRY` | `registry.deckhouse.ru/deckhouse/fe` | Source registry to pull from |
| `-source-user` | `E2E_SOURCE_USER` | | Source registry username (alternative to license-token) |
| `-source-password` | `E2E_SOURCE_PASSWORD` | | Source registry password |
| `-license-token` | `E2E_LICENSE_TOKEN` | | License token for Deckhouse registry (shortcut for source-user=license-token) |
| `-target-registry` | `E2E_TARGET_REGISTRY` | (empty = in-memory) | Target registry to push to |
| `-target-user` | `E2E_TARGET_USER` | | Target registry username |
| `-target-password` | `E2E_TARGET_PASSWORD` | | Target registry password |
| `-tls-skip-verify` | `E2E_TLS_SKIP_VERIFY` | `false` | Skip TLS certificate verification (for self-signed certs) |
| `-keep-bundle` | `E2E_KEEP_BUNDLE` | `false` | Keep bundle directory after test |
| `-d8-binary` | `E2E_D8_BINARY` | `bin/d8` | Path to d8 binary |

**Note:** Either `-license-token` OR `-source-user`/`-source-password` must be provided for authentication.

## Test Scenarios

### TestMirrorE2E_FullCycle

Complete end-to-end test that:
1. Collects all image digests from source registry
2. Runs `d8 mirror pull` to create a bundle
3. Runs `d8 mirror push` to push to target
4. Validates that all segments exist in target
5. Compares every digest between source and target

### TestMirrorE2E_PullOnly

Tests only the pull operation:
1. Runs `d8 mirror pull` to create a bundle
2. Verifies the bundle directory contains expected files

## Target Registry Options

### In-Memory Registry (Default)

When `-target-registry` is not specified, tests use an in-memory registry.
This is useful for CI/CD and quick local testing.

### External Registry

Specify `-target-registry` to push to a real registry:

```bash
# Docker registry with self-signed cert
go test -v ./testing/e2e/mirror/... \
  -license-token=TOKEN \
  -target-registry=localhost:5000/deckhouse \
  -tls-skip-verify

# Registry with auth  
go test -v ./testing/e2e/mirror/... \
  -license-token=TOKEN \
  -target-registry=registry.example.com/deckhouse \
  -target-user=admin \
  -target-password=secret
```

## What Gets Validated

### Structure Validation

- Root Deckhouse images exist
- `/install` segment exists with release channel tags
- `/install-standalone` segment exists
- `/release-channel` segment exists
- `/security/*` databases exist (if present in source)
- `/modules/*` exist (if present in source)

### Digest Comparison

Every image tag in the source is compared with the target:
- All images must be present in target
- All digests must match exactly (SHA256)

## Timeouts

The full cycle test has a 60-minute timeout. This can be adjusted in the test code if needed.

## Troubleshooting

### "License token not provided"

Set the license token via flag or environment variable:
```bash
E2E_LICENSE_TOKEN=your_token go test -v ./testing/e2e/mirror/...
```

### "Pull failed"

Check that:
1. The `d8` binary exists at `bin/d8` (run `task build` first) or specify path with `-d8-binary`
2. You have network access to the source registry
3. The license token is valid

### "Push failed"

Check that:
1. The target registry is accessible
2. Credentials are correct (if using authenticated registry)
3. Use `-tls-skip-verify` for self-signed certificates

### Viewing Bundle Contents

Use `-keep-bundle` to preserve the bundle directory:
```bash
go test -v ./testing/e2e/mirror/... \
  -license-token=TOKEN \
  -keep-bundle

# Bundle will be at /tmp/d8-mirror-e2e-TIMESTAMP/
```
