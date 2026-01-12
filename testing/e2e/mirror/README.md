# E2E Tests for d8 mirror

End-to-end tests for the `d8 mirror pull` and `d8 mirror push` commands.

## Overview

These tests perform **complete mirror cycles with verification** to ensure:
1. All expected images are downloaded from source
2. All images are correctly pushed to target registry
3. All images match between source and target (deep comparison)

## Test Types

| Test | Description | Timeout | Command |
|------|-------------|---------|---------|
| **Full Cycle** | Platform + Modules + Security | 3h | `task test:e2e:mirror` |
| **Platform** | Deckhouse core only | 2h | `task test:e2e:mirror:platform` |
| **Platform Stable** | Only stable channel | 2h | `task test:e2e:mirror:platform` + `E2E_DECKHOUSE_TAG=stable` |
| **Modules** | All modules | 2h | `task test:e2e:mirror:modules` |
| **Single Module** | One module (fast) | 2h | `task test:e2e:mirror:modules` + `E2E_INCLUDE_MODULES=module-name` |
| **Security** | Security DBs only | 30m | `task test:e2e:mirror:security` |

## Verification Approach

### Step 1: Read Expected Images from Source
Before pulling, we independently read what SHOULD be downloaded:
- Release channel versions from source registry
- `images_digests.json` from each installer image (platform)
- Module list, versions, and `images_digests.json` from each module image

### Step 2: Pull & Push
Execute `d8 mirror pull` and `d8 mirror push`

### Step 3: Verify
Compare expected images with what's actually in target:
- All expected digests must exist in target
- All images in target must match source (manifest, config, layers)

This catches:
- **Pull bugs** - if pull forgets to download an image
- **Push bugs** - if push fails to upload an image
- **Data corruption** - if any digest doesn't match

## Running Tests

### Quick Start

```bash
# Full cycle with license token
E2E_LICENSE_TOKEN=xxx task test:e2e:mirror

# Platform only (faster)
E2E_LICENSE_TOKEN=xxx E2E_DECKHOUSE_TAG=stable task test:e2e:mirror:platform

# Single module (fastest)
E2E_LICENSE_TOKEN=xxx E2E_INCLUDE_MODULES=module-name task test:e2e:mirror:modules
```

### Using Environment Variables

```bash
# Official registry with license
E2E_LICENSE_TOKEN=your_license_token \
task test:e2e:mirror

# Local registry
E2E_SOURCE_REGISTRY=localhost:443/deckhouse \
E2E_SOURCE_USER=admin \
E2E_SOURCE_PASSWORD=secret \
E2E_TLS_SKIP_VERIFY=true \
task test:e2e:mirror:platform

# Specific release channel
E2E_LICENSE_TOKEN=xxx \
E2E_DECKHOUSE_TAG=stable \
task test:e2e:mirror:platform

# Specific modules only
E2E_LICENSE_TOKEN=xxx \
E2E_INCLUDE_MODULES="pod-reloader,neuvector" \
task test:e2e:mirror:modules
```

### Configuration Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `-source-registry` | `E2E_SOURCE_REGISTRY` | `registry.deckhouse.ru/deckhouse/fe` | Source registry |
| `-source-user` | `E2E_SOURCE_USER` | | Source registry username |
| `-source-password` | `E2E_SOURCE_PASSWORD` | | Source registry password |
| `-license-token` | `E2E_LICENSE_TOKEN` | | License token |
| `-target-registry` | `E2E_TARGET_REGISTRY` | `""` (in-memory) | Target registry |
| `-target-user` | `E2E_TARGET_USER` | | Target registry username |
| `-target-password` | `E2E_TARGET_PASSWORD` | | Target registry password |
| `-tls-skip-verify` | `E2E_TLS_SKIP_VERIFY` | `false` | Skip TLS verification |
| `-deckhouse-tag` | `E2E_DECKHOUSE_TAG` | | Specific tag/channel (e.g., `stable`, `v1.65.8`) |
| `-no-modules` | `E2E_NO_MODULES` | `false` | Skip modules |
| `-no-platform` | `E2E_NO_PLATFORM` | `false` | Skip platform |
| `-no-security` | `E2E_NO_SECURITY` | `false` | Skip security DBs |
| `-include-modules` | `E2E_INCLUDE_MODULES` | | Comma-separated module list |
| `-keep-bundle` | `E2E_KEEP_BUNDLE` | `false` | Keep bundle after test |
| `-existing-bundle` | `E2E_EXISTING_BUNDLE` | | Path to existing bundle (skip pull) |
| `-d8-binary` | `E2E_D8_BINARY` | `bin/d8` | Path to d8 binary |
| `-new-pull` | `E2E_NEW_PULL` | `false` | Use experimental pull |

## Test Artifacts

Tests produce artifacts in `testing/e2e/.logs/<test>-<timestamp>/`:

```
testing/e2e/.logs/TestFullCycleE2E-20251226-114128/
├── test.log       # Full command output (pull/push)
├── report.txt     # Test summary report
└── comparison.txt # Detailed registry comparison
```

### Cleaning Up Logs

```bash
task test:e2e:mirror:logs:clean
```

## Requirements

- Built `d8` binary (run `task build`)
- Valid credentials for source registry
- Network access
- Disk space for bundle (20-50 GB depending on scope)

## What Gets Verified

### Platform Test
1. Release channels exist (alpha, beta, stable, rock-solid)
2. Install images for each version
3. All digests from `images_digests.json` exist in target

### Modules Test
1. Module list matches expected
2. Release channel tags exist and digests match source
3. Module version images exist (`:v1.2.3` tags from release channels)
4. All digests from `images_digests.json` exist in target (if module has them)

### Security Test
1. All security databases exist (trivy-db, trivy-bdu, etc.)
2. Tags match source

## Troubleshooting

### "Source authentication not provided"
```bash
E2E_LICENSE_TOKEN=your_token task test:e2e:mirror
```

### "Pull failed"
1. Check `d8` binary: `task build`
2. Check network access
3. For self-signed certs: `E2E_TLS_SKIP_VERIFY=true`

### "Verification failed"
Check `comparison.txt`:
- **Missing digests**: Pull or push didn't transfer the image
- **Missing module versions**: Module version image not in target
- **Missing module digests**: Module's internal images not transferred
- **Digest mismatch**: Data corruption or version skew

### Running Against Local Registry
```bash
E2E_SOURCE_REGISTRY=localhost:5000/deckhouse \
E2E_SOURCE_USER=admin \
E2E_SOURCE_PASSWORD=admin \
E2E_TLS_SKIP_VERIFY=true \
task test:e2e:mirror:platform
```

### Keep Bundle for Debugging
```bash
E2E_KEEP_BUNDLE=true E2E_LICENSE_TOKEN=xxx task test:e2e:mirror:platform
# Bundle location shown in test output
```

### Use Existing Bundle (Skip Pull)
```bash
E2E_EXISTING_BUNDLE=/path/to/bundle E2E_LICENSE_TOKEN=xxx task test:e2e:mirror:platform
# Test will skip pull step and use existing bundle
```
