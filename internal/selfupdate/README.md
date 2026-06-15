# d8 self-update (`d8 cli`)

The `internal/selfupdate` package updates the `d8` binary itself through the
cluster.

## Why

- `d8` is a single binary; before this, updating meant "download and replace by hand".
- The cluster itself knows which CLI versions are published for it - its registry
  is the source of updates.
- Access to updates is controlled by the user's ordinary RBAC permissions
  (kubeconfig), with no registry credentials handed out.

## Commands

| Command | What it does |
|---|---|
| `d8 cli check` | reports whether a version newer than the current one is available |
| `d8 cli update [--version X]` | installs the version into the store and repoints `current` at it |
| `d8 cli use <version>` | switches to a version: an installed one is a pure symlink repoint (instant, offline), a missing one is downloaded first |
| `d8 cli versions` *(alias `list`)* | lists published versions newest-first; the current one is starred, locally installed ones are marked `installed` |

## The version store and the `current` symlink (`store.go`)

Versions live in a per-user store with the same versions-directory-plus-symlink
layout the plugin installer uses:

```
/opt/deckhouse/bin/d8 -> ~/.deckhouse-cli/cli/current -> versions/v0.13.1/d8
     (PATH entry,           (stable symlink,               (the store)
      migrated once)         atomic repoint)
```

- **Switching = repointing `current`** (atomic: staged symlink + rename). The
  PATH binary is never rewritten after migration, so switching needs no
  elevated privileges and copies no files.
- **The store is addressed by its own well-known paths, never through
  `os.Executable()`**: on Linux `/proc/self/exe` resolves to the symlink
  *target*, so "replace whatever the executable resolves to" would overwrite a
  stored version in place. The two-level layout (PATH -> `current` -> version)
  is what makes the symlink scheme safe - the updater always knows where the
  switchable link lives.
- **Migration**: when the running binary is a plain file outside the store (the
  pre-store layout, or a binary dropped in by external tooling), the first
  update/use seeds the store with it (under its own version, when semver),
  backs it up as `<exe>.old` and replaces the PATH entry with a symlink to
  `current`. If external tooling later overwrites the symlink with a real file,
  the next update/use simply migrates again - self-healing.
- `d8 cli use <version>` resolves the request against the store by **semver
  value** (`0.13.1` finds `v0.13.1`); a hit needs no network and no kubeconfig,
  a miss falls back to the regular download path and stays installed afterwards.
- Store entries are immutable (re-installing an existing tag is a no-op) and are
  smoke-tested **while staged** (`.staged` suffix), so a corrupt artifact never
  becomes a visible entry. Foreign content in the store directory is ignored.
  Dev builds (non-semver versions) are never archived - `use` cannot address them.
- The store is per-user (`~/.deckhouse-cli/cli`): after migration the PATH entry
  points into the home of the user who ran it. On shared machines each user who
  manages d8 gets their own store; on cluster masters that user is root.
- `d8 cli use <TAB>` shell-completes from the store (newest-first, prefix
  filtered). Completion never touches the network - the offline-switchable
  versions are exactly the ones worth suggesting.

## What RPP is and how this package talks to it

RPP (**registry-packages-proxy**) is the in-cluster HTTP proxy in front of the
platform's container registry (module `registry-packages-proxy`, ns
`d8-cloud-instance-manager`):

- **Why it exists**: users do not have (and should not have) registry
  credentials. The proxy serves artifacts based on the **kubeconfig identity** -
  the same token the user already presents to kube-apiserver.
- **Transport**: kube-rbac-proxy on `:4219` of the master nodes; the public
  Ingress `registry-packages-proxy.<publicDomain>` (valid TLS, the default path).
- **Per-request authorization**: the Bearer token from kubeconfig ->
  TokenReview ("who") + SubjectAccessReview ("is it allowed"). Access is granted
  by the ClusterRole `d8:registry-packages-proxy:cli-download`; it is NOT bound
  to anyone by default - the cluster administrator decides who may download the CLI.
- **API used by this package** (the HTTP client lives in `internal/rpp`):
  - `GET /v1/images/deckhouse-cli/tags` -> `{"name": ..., "tags": [...]}` - the version list;
  - `GET /v1/images/deckhouse-cli/tags/<tag>` -> gzip-tar of the image contents
    (containing the `d8` file).
- **Endpoint** is discovered automatically (Ingress -> pod-IP fallback) or set
  explicitly (`--rpp-endpoint` / `D8_RPP_ENDPOINT`).

## How an update works (`Updater.Apply` -> `SwitchTo`)

1. A lock in the store (`install.lock`, `internal/lockfile`) - two switches
   cannot run in parallel; a lock orphaned by a kill is reclaimed.
2. A plain-file install is seeded into the store first (best-effort, semver
   versions only), so the displaced version stays switchable.
3. The requested version is installed into the store unless already present:
   download to `versions/<tag>/d8.staged`, smoke test (`--version` must exit
   cleanly - a corrupt artifact or one built for another platform is rejected
   while staged), atomic rename to its final name.
4. Pre-existing store entries are smoke-tested again before they become active.
5. `current` is atomically repointed at the version.
6. A plain-file install is migrated: the original binary becomes `<exe>.old`
   and the PATH entry becomes a symlink to `current` (rolled back if the link
   cannot be created). Store-managed installs skip this step entirely.

Rollback is `d8 cli use <previous>` - the previous version remains installed
(the command prints it). The `.old` file exists only as the migration backup.

Version selection:

- by default - the highest **stable** semver tag;
- pre-releases (`rc`/`alpha`/`beta`) are installed only explicitly via
  `--version` (which also allows a downgrade).

Platform tags (`rpp_source.go`):

- releases may be published per platform, one single-platform image per tag
  (`v1.2.3-linux-amd64` - the same convention the plugin CI uses);
- `ListTags` reports this platform's tags as their bare version (so the Updater
  selects them) and passes other platforms' tags through raw - their suffix
  parses as a semver pre-release and is never auto-selected;
- `ExtractBinary` downloads `<tag>-<os>-<arch>` first and falls back to the bare
  `<tag>` (legacy / platform-neutral publishing) on 404.

## Switches

| Need | How |
|---|---|
| explicit RPP endpoint | `--rpp-endpoint` / `D8_RPP_ENDPOINT` |
| custom CA / skip TLS verification | `--rpp-ca-file` / `--rpp-insecure-skip-tls-verify` |
| identity | `-k/--kubeconfig`, `--context` |

## Boundaries and deliberate decisions

- Windows is not supported: a running `.exe` cannot be replaced.
- The client does not follow redirects: the Bearer token must never travel to a
  foreign host.

## Package map

| File | Responsibility |
|---|---|
| `cmd/command.go` | the `d8 cli ...` cobra commands; building the `Updater` |
| `cmd/list.go` | `d8 cli versions`: rendering the version list |
| `cmd/use.go` | `d8 cli use`: switching versions, store-first; shell completion |
| `update.go` | `Updater` and `SwitchTo`: version selection, store install, repoint, migration |
| `store.go` | the version store + `current` symlink (`~/.deckhouse-cli/cli`) |
| `source.go` / `rpp_source.go` | the `Source` interface and its RPP implementation |

Related:

- `internal/rpp` - the HTTP client for the proxy (transport, discovery, tar extraction);
- `internal/lockfile` - the file lock (shared with plugin installs).
