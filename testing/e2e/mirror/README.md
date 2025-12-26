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
- Sufficient disk space for the bundle (can be ~20 GB)

## Running Tests

### Using Taskfile (Recommended)

```bash
# With environment variables
E2E_SOURCE_REGISTRY=localhost:443/deckhouse \
E2E_SOURCE_USER=admin \
E2E_SOURCE_PASSWORD=secret \
E2E_TLS_SKIP_VERIFY=true \
task test:e2e:mirror

# With command-line flags
task test:e2e:mirror -- \
  -source-registry=localhost:443/deckhouse \
  -source-user=admin \
  -source-password=admin \
  -tls-skip-verify

# With license token (official Deckhouse registry)
E2E_LICENSE_TOKEN=xxx task test:e2e:mirror
```

### Using go test directly

```bash
# Note: requires -tags=e2e flag
go test -v -tags=e2e -timeout=120m ./testing/e2e/mirror/... \
  -source-registry=localhost:443/deckhouse \
  -source-user=admin \
  -source-password=secret \
  -tls-skip-verify
```

### Configuration Options

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
| `-new-pull` | `E2E_NEW_PULL` | `false` | Use new pull implementation |

## Test Artifacts

Tests produce detailed artifacts in `testing/e2e/.logs/<test>-<timestamp>/`:

```
testing/e2e/.logs/fullcycle-20251226-114128/
├── test.log       # Full command output (pull/push)
├── report.txt     # Test summary report
└── comparison.txt # Detailed registry comparison
```

### Cleaning Up Logs

```bash
task test:e2e:mirror:logs:clean
```

### Sample Report (report.txt)

```
================================================================================
E2E TEST REPORT: TestMirrorE2E_FullCycle
================================================================================

EXECUTION:
  Started:  2025-12-26T11:41:28+03:00
  Finished: 2025-12-26T11:46:28+03:00
  Duration: 5m1s

REGISTRIES:
  Source: localhost:443/deckhouse-etalon
  Target: 127.0.0.1:61594/deckhouse/ee

IMAGES TO VERIFY:
  Source: 324 images (82 repos)
  Target: 324 images (82 repos)
  (excluded 1071 internal tags from comparison)

BUNDLE:
  Size: 22.52 GB

VERIFICATION RESULTS:
  Images matched:    324 (manifest + config + layers)
  Layers verified:   1172
  Missing images:    0
  Digest mismatch:   0
  Missing layers:    0

STEPS:
  [PASS] Analyze source (82 repos) (330ms)
  [PASS] Pull images (22.52 GB bundle) (2m59.826s)
  [PASS] Push to registry (1m57.742s)
  [PASS] Deep comparison (324 images verified) (2.266s)

================================================================================
RESULT: PASSED - REGISTRIES ARE IDENTICAL
  82 repositories verified
  324 images verified
================================================================================
```

### Comparison Report (comparison.txt)

Contains detailed breakdown per repository with layer-level verification:

```
REGISTRY COMPARISON SUMMARY
===========================

Source: localhost:443/deckhouse
Target: 127.0.0.1:54321/deckhouse/ee
Duration: 2s

REPOSITORIES:
  Source: 82
  Target: 82
  Missing in target: 0
  Extra in target: 0

IMAGES TO VERIFY:
  Source: 324 images
  Target: 324 images
  (excluded 1071 internal tags: digest-based, .att, .sig)

VERIFICATION RESULTS:
  Matched: 324
DEEP COMPARISON (layers + config):
  Images deep-checked: 324
  Source layers: 1172
  Target layers: 1172
  Matched layers: 1172
  Missing layers: 0
  Config mismatches: 0

✓ REGISTRIES ARE IDENTICAL (all hashes match)

REPOSITORY BREAKDOWN:
---------------------
✓ (root): 6/6 tags, 66 layers checked
✓ install: 6/6 tags, 48 layers checked
✓ install-standalone: 78/78 tags, 624 layers checked
✓ release-channel: 6/6 tags, 12 layers checked
✓ security/trivy-db: 1/1 tags, 2 layers checked
✓ modules/deckhouse-admin: 5/5 tags, 10 layers checked
...
```

## Timeouts

The test has a **120-minute timeout** to handle large registries.

## Troubleshooting

### "Source authentication not provided"

Set credentials:
```bash
E2E_LICENSE_TOKEN=your_token task test:e2e:mirror
# or
E2E_SOURCE_USER=admin E2E_SOURCE_PASSWORD=secret task test:e2e:mirror
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
E2E_KEEP_BUNDLE=true task test:e2e:mirror -- ...

# Bundle location shown in output
```
