# E2E Tests for d8 mirror

End-to-end tests for the `d8 mirror pull` and `d8 mirror push` commands.

## Overview

These tests perform a **complete mirror cycle with deep comparison** to ensure source and target registries are **100% identical**.

### Test Steps

1. **Analyze source registry** - Discover all repositories and count all images
2. **Pull images** - Execute `d8 mirror pull` to create a bundle
3. **Push images** - Execute `d8 mirror push` to target registry
4. **Deep comparison** - Compare every repository, tag, and digest between source and target

### What Gets Compared (Deep Comparison)

- **Repository level**: All repositories in source must exist in target
- **Tag level**: All tags in each repository must exist in target  
- **Manifest digest**: Every image manifest digest must match (SHA256)
- **Config digest**: Image config blob digest is verified
- **Layer digests**: ALL layer digests of every image are compared
- **Layer count**: Number of layers must match

This ensures **byte-for-byte identical** registries.

## Requirements

- Built `d8` binary (run `task build` from project root)
- Valid credentials for the source registry
- Network access to the source registry
- Sufficient disk space for the bundle (can be several GB, aroud 20)

## Running Tests

### Basic Usage

```bash
# Run with license token (official Deckhouse registry)
go test -v ./testing/e2e/mirror/... \
  -license-token=YOUR_LICENSE_TOKEN

# Run with local registry (self-signed cert)
go test -v ./testing/e2e/mirror/... \
  -source-registry=localhost:443/deckhouse \
  -source-user=admin \
  -source-password=secret \
  -tls-skip-verify
```

### Full Configuration

```bash
go test -v ./testing/e2e/mirror/... \
  -source-registry=my-registry.local/deckhouse \
  -source-user=admin \
  -source-password=secret \
  -target-registry=my-target.local:5000/deckhouse \
  -target-user=admin \
  -target-password=secret \
  -tls-skip-verify \
  -keep-bundle
```

### Environment Variables

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `-source-registry` | `E2E_SOURCE_REGISTRY` | `registry.deckhouse.ru/deckhouse/fe` | Source registry |
| `-source-user` | `E2E_SOURCE_USER` | | Source registry username |
| `-source-password` | `E2E_SOURCE_PASSWORD` | | Source registry password |
| `-license-token` | `E2E_LICENSE_TOKEN` | | License token |
| `-target-registry` | `E2E_TARGET_REGISTRY` | (local disk-based registry) | Target registry |
| `-target-user` | `E2E_TARGET_USER` | | Target registry username |
| `-target-password` | `E2E_TARGET_PASSWORD` | | Target registry password |
| `-tls-skip-verify` | `E2E_TLS_SKIP_VERIFY` | `false` | Skip TLS verification |
| `-keep-bundle` | `E2E_KEEP_BUNDLE` | `false` | Keep bundle after test |
| `-d8-binary` | `E2E_D8_BINARY` | `bin/d8` | Path to d8 binary |

## Test Output

### Log Directory Structure

Test logs are stored in `testing/e2e/.logs/<test>-<timestamp>/`:

```
testing/e2e/.logs/fullcycle-20251225-123456/
├── test.log       # Full command output (pull/push)
├── report.txt     # Test summary report
└── comparison.txt # Detailed registry comparison
```

### Sample Report Output

```
================================================================================
E2E TEST REPORT: TestMirrorE2E_FullCycle
================================================================================

Duration: 25m30s

REGISTRIES:
  Source: localhost:443/deckhouse (15 repos, 1847 images)
  Target: 127.0.0.1:54321/deckhouse/ee (15 repos, 1847 images)

COMPARISON:
  ✓ Matched:      1847 images (manifest digest)
  ✓ Deep checked: 1847 images
  ✓ Layers:       15234 verified

STEPS:
  ✓ Analyze source (15 repos, 1847 images) (45s)
  ✓ Pull images (5.23 GB bundle) (12m15s)
  ✓ Push to registry (8m30s)
  ✓ Deep comparison (1847 images verified) (4m00s)

--------------------------------------------------------------------------------
RESULT: PASSED - REGISTRIES ARE IDENTICAL
        1847 images, 15234 layers - all hashes verified
================================================================================
```

### Comparison Report (comparison.txt)

Contains detailed breakdown per repository with layer-level verification:

```
REGISTRY COMPARISON SUMMARY
===========================

Source: localhost:443/deckhouse
Target: 127.0.0.1:54321/deckhouse/ee
Duration: 4m0s

REPOSITORIES:
  Source: 15
  Target: 15
  Missing in target: 0
  Extra in target: 0

IMAGES (manifest digest comparison):
  Source: 1847
  Target: 1847
  Matched: 1847
  Missing: 0
  Mismatched: 0
  Extra: 0

DEEP COMPARISON (layers + config):
  Images deep-checked: 1847
  Source layers: 15234
  Target layers: 15234
  Matched layers: 15234
  Missing layers: 0
  Config mismatches: 0

✓ REGISTRIES ARE IDENTICAL (all hashes match)

REPOSITORY BREAKDOWN:
---------------------
✓ (root): 523/523 tags, 4521 layers checked
✓ install: 6/6 tags, 48 layers checked
✓ install-standalone: 78/78 tags, 624 layers checked
✓ release-channel: 6/6 tags, 12 layers checked
✓ security/trivy-db: 1/1 tags, 2 layers checked
✓ modules/deckhouse-admin: 15/15 tags, 120 layers checked
...
```

## Timeouts

The test has a **120-minute timeout** to handle large registries.

## Troubleshooting

### "Source authentication not provided"

Set credentials:
```bash
E2E_LICENSE_TOKEN=your_token go test -v ./testing/e2e/mirror/...
# or
E2E_SOURCE_USER=admin E2E_SOURCE_PASSWORD=secret go test -v ./testing/e2e/mirror/...
```

### "Pull failed" or "Push failed"

1. Check `d8` binary exists (`task build`)
2. Check network access
3. Use `-tls-skip-verify` for self-signed certs
4. Check credentials

### "Registries differ"

Check `comparison.txt` for details:
- **Missing images**: Images in source but not in target
- **Mismatched digests**: Images exist but have different content

### Viewing Bundle Contents

```bash
go test -v ./testing/e2e/mirror/... \
  -license-token=TOKEN \
  -keep-bundle

# Bundle location shown in output
```
