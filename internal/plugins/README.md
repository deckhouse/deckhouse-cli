# d8 plugins

The `internal/plugins` package manages d8 plugins: standalone binaries
published to an OCI registry that d8 installs, updates, and runs as if
they were native subcommands. The machinery lives in this package (the
`Manager`); the `d8 plugins` cobra commands are a thin layer on top of it in
`internal/plugins/cmd` (package `pluginscmd`), one file per command - the same
split `internal/selfupdate` / `internal/selfupdate/cmd` uses.

## Why

- Isolate dependencies and let teams develop plugins independently of d8.
- Keep `d8` itself compact - heavy functionality ships as plugins.
- Guarantee compatibility: a plugin declares requirements (Kubernetes,
  Deckhouse, modules, other plugins) and d8 enforces them both at install time
  and before every run.

## Commands

| Command | What it does |
|---|---|
| `d8 plugins install <name> [--version X] [--use-major N] [--force]` | install or switch a plugin version |
| `d8 plugins update <name>` | update to the newest cluster-compatible version within the current major |
| `d8 plugins update all` | the same for every installed plugin |
| `d8 plugins list [--installed\|--available]` | list plugins |
| `d8 plugins versions <name>` | list all published versions of one plugin (installed one marked; same verb as `d8 cli versions`) |
| `d8 plugins contract <name>` | show a plugin's contract |
| `d8 plugins remove <name>` | remove an installed plugin |
| `d8 <plugin> ...` *(wrapper, with `DECKHOUSE_PLUGINS_ENABLED=true`)* | run an installed plugin; auto-installs it on first use |

## Plugin source

`rppPluginSource` (`rpp_source.go`) implements the `PluginSource` interface
(`source.go`) and is the only source: plugins are pulled through the in-cluster
registry-packages-proxy using the **kubeconfig identity**, with no registry
credentials on the user side (ADR: deckhouse-cli reaches the registry
exclusively through the proxy, so every command needs a reachable cluster).
See `internal/selfupdate/README.md` for what RPP is and how authorization works;
the plugin routes are `/v1/images/deckhouse-cli/plugins/<name>/...`.

## What a plugin image contains

- `plugin` - the executable;
- `contract.yaml` - the contract: name, version, description, requested env
  vars, flags, and `requirements` (Kubernetes / Deckhouse / modules / plugins).

The registry source can also read the contract from the OCI annotation
(base64 JSON); the RPP source reads the `contract.yaml` file from the image tar.

## On-disk layout

```
<plugins-dir>/                       # /opt/deckhouse/lib/deckhouse-cli by default
├── plugins/<name>/
│   ├── v<major>/<name>             # one binary per major version
│   ├── current -> v<major>/<name>  # the active version (atomic symlink swap)
│   └── install.lock                # one install lock per plugin
└── cache/contracts/<name>.json     # contract of the installed version (atomic writes)
```

- `--plugins-dir` / `DECKHOUSE_CLI_PATH` override the root; if it is not
  writable, installs fall back to `~/.deckhouse-cli`.
- "Installed" means "has a `current` symlink" - a leftover directory from a
  failed install is never treated as an installed plugin.

## How install works (`InstallPlugin`)

1. Validate the plugin name (a single OCI path component - nothing else may
   reach filesystem paths or registry routes).
2. Pick the version (see policy below) and take the per-plugin lock.
3. If the selected version is already current - nothing to do (`--force` re-pulls).
4. Fetch the contract and validate ALL requirements BEFORE any switch -
   including the fast path that merely repoints `current` to an already
   installed version.
5. Download into a staged file (`<binary>.new`) - the live binary keeps working
   for the whole download.
6. Smoke-test the staged binary (`--version`, fallback `version`; only a clean
   exit is required) - a corrupt or wrong-platform artifact is rejected before
   it replaces anything.
7. Back up the old binary (`.old`), atomically swap the new one in, write the
   contract cache, then atomically repoint `current`.

A failure at any step leaves the previous version installed and working.

## Version selection policy

- Default pick: the **newest stable** semver tag whose **cluster-side
  requirements are satisfied** - versions are probed newest to oldest and the
  first compatible one wins (a too-new release does not block updates).
- Updates stay **within the installed major**; crossing majors requires an
  explicit `--use-major N`. The major is read from disk (the `current`
  symlink), so a broken binary cannot drop the pin.
- **Downgrade guard**: the implicit path never installs a version older than the
  installed one - e.g. when the newest tag's contract is temporarily unreadable.
  Downgrades are explicit only (`--version`, `--use-major`).
- Pre-releases (`rc`/`alpha`/`beta`) are never picked by default; install them
  via `--version`.
- An unreachable cluster or a malformed contract is a hard error, not a silent
  fallback to an older version.

## Requirements enforcement

- **Cluster-side** (`kubernetes`, `deckhouse`, `modules` incl.
  mandatory/conditional/anyOf): verified against a one-shot cluster snapshot
  (the `requirements/` package); the cluster is queried only when the plugin
  actually declares such requirements, so contract-less plugins install offline.
- **Plugin-to-plugin**: conflicts with already installed plugins and their
  constraints; `--resolve-plugins-conflicts` tries to install missing
  dependencies.
- **At runtime**: the wrapper re-validates requirements before EVERY plugin run
  (the gate is skipped for purely local queries: `--help`, `--version`,
  `completion`).
- Escape hatch for air-gapped setups: `--skip-cluster-checks` /
  `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1` (downgrades the check to a warning).

## Running a plugin (the wrapper)

- All arguments are forwarded verbatim (the wrapper parses no flags itself).
- Env requested by the contract is injected: `KUBECONFIG` (the path d8 uses)
  and `PLUGINS_CALLER` (the d8 executable); everything else passes through.
- stdin/stdout/stderr are inherited; the plugin's exact exit code is propagated.
- On d8's own termination the plugin gets SIGTERM and a grace period, not an
  instant SIGKILL.

## Switches

| Need | How |
|---|---|
| install root | `--plugins-dir` / `DECKHOUSE_CLI_PATH` |
| identity (rpp + cluster checks) | `-k/--kubeconfig`, `--context` |
| RPP endpoint / TLS | `--rpp-endpoint`, `--rpp-ca-file`, `--rpp-insecure-skip-tls-verify` |
| skip cluster-side requirement checks | `--skip-cluster-checks` / `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1` |

## Boundaries and deliberate decisions

- Listing the full plugin catalog over RPP is not supported (the proxy has no
  catalog endpoint); install/update by name works.
- Idempotency compares the version reported by the binary itself; a plugin that
  prints a non-semver banner is re-pulled on every explicit `update`.
- The image stream is not digest-verified - trust rests on the TLS channel and
  the kubeconfig identity; artifact health is checked by the smoke test.
- Plugin-to-plugin dependency backtracking during version selection is out of
  scope (conflicts are enforced at install time).

## Package map

| File | Responsibility |
|---|---|
| `plugins.go` | the `Manager`: shared state of the plugin machinery |
| `install.go` | the install pipeline: lock, staged download, smoke, atomic swap, idempotency |
| `select.go` | newest-compatible version selection, contract memoization |
| `update.go` | `UpdateAll`, installed-plugin discovery, home-fallback switch |
| `remove.go` | `Remove` / `RemoveAll` |
| `validators.go` | plugin-to-plugin requirement checks + the Manager glue over `requirements/` (snapshot cache, kubeconfig clients, `--skip-cluster-checks`) |
| `requirements/` | cluster-side requirements: the one-shot cluster snapshot (k8s / Deckhouse / modules) and the named checks against it |
| `run.go` | running an installed plugin: requirement gate, env injection, exec |
| `list.go` / `versions.go` | data for the `list` / `versions` commands |
| `source.go` / `rpp_source.go` / `init.go` | the `PluginSource` interface, its RPP implementation, source wiring |
| `layout/` | on-disk path layout |
| `flags/` | the `d8 plugins` flag set |
| `cmd/` | the `d8 plugins ...` command tree and the per-plugin wrapper command, one file per command |

Related: `internal/rpp` (proxy HTTP client), `internal/lockfile` (install lock),
`internal/selfupdate` (the same store-and-symlink update pattern for the d8
binary itself).
