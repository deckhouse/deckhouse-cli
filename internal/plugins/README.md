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
| `d8 plugins update <name> [--use-major N]` | update to the newest cluster-compatible version within the current major |
| `d8 plugins update all` | the same for every installed plugin |
| `d8 plugins list` | list installed plugins (the proxy serves no catalog, so available plugins cannot be listed) |
| `d8 plugins versions <name>` | list all published versions of one plugin (installed one marked; same verb as `d8 cli versions`) |
| `d8 plugins contract <name>` | show a plugin's contract |
| `d8 plugins remove <name>` | remove an installed plugin |
| `d8 <plugin> ...` *(wrapper, with `DECKHOUSE_PLUGINS_ENABLED=true`)* | run an installed plugin; auto-installs it on first use |

## Plugin source

The `pluginSource` interface (`source.go`) has two implementations, chosen in
`InitPluginServices` (`init.go`) by whether the hidden `--source` flag is set:

- **`rppPluginSource` (`rpp_source.go`) - the default and only supported
  source.** Plugins are pulled through the in-cluster registry-packages-proxy
  using the **kubeconfig identity**, with no registry credentials on the user
  side (ADR #386: deckhouse-cli reaches the registry exclusively through the
  proxy, so every command needs a reachable cluster). See
  `internal/selfupdate/README.md` for what RPP is and how authorization works -
  plugin download is gated by the `d8:registry-packages-proxy:packages-download`
  ClusterRole, distinct from self-update's `cli-download`. The plugin routes are
  `/v1/images/deckhouse-cli/plugins/<name>/{tags,manifests/<ref>,images/<version>}`.
- **`registryPluginSource` (`source_legacy.go`) - a temporary, hidden `--source`
  bypass.** It pulls straight from a registry repo with go-containerregistry,
  skipping the proxy and the cluster, and force-sets `--skip-cluster-checks`. It
  exists for pre-#386 workflows and is documented for removal (grep marker
  `legacy --source`).

## What a plugin image contains

- `plugin` - the executable, in the image layers;
- the contract - name, version, description, requested env vars, flags, and
  `requirements` (Kubernetes / Deckhouse / modules / plugins) - published as a
  base64-JSON `contract` annotation on the image manifest.

The RPP source fetches the raw image manifest over the proxy `manifests/<ref>`
route (a single manifest fetch, no layer pull) and reads the contract from its
base64-JSON `contract` annotation itself. The binary is pulled separately (full
image, over the `images/<version>` route) only when a plugin is installed.

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
7. Atomically swap the new binary in (rename over the live one - the original is
   untouched on failure), write the contract cache, then atomically repoint `current`.

A failure at any step leaves the previous version installed and working.

## Version selection policy

- Default pick: the **newest stable** semver tag whose **cluster-side
  requirements are satisfied** AND whose **plugin->plugin dependency chain is
  resolvable** - versions are probed newest to oldest and the first that passes
  both wins (a too-new release, or one whose dependencies cannot be satisfied,
  does not block updates).
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

- **Cluster-side** (`kubernetes`, `deckhouse`, and `modules` -
  mandatory/conditional/anyOf/**noneOf**, the last *forbidding* a module):
  verified against a one-shot cluster snapshot (the `requirements/` package)
  built from three reads - the API-server version, the `deckhouse` deployment's
  `core.deckhouse.io/version` annotation, and a `modules.deckhouse.io` list. The
  snapshot is lazy (built only when the plugin declares such requirements, so
  contract-less plugins install offline), cached once per run, and bounded by a
  30s probe timeout. A non-release Deckhouse version (e.g. `dev`) skips the
  Deckhouse check with a warning rather than failing.
- **Plugin-to-plugin**: a plugin's mandatory dependencies are installed and
  upgraded automatically (the resolution planner: constraint-aware, newest
  satisfying version, within each dependency's own major - or across it when
  `--use-major` cascades). Conflicts with already-installed plugins skip a
  candidate during selection. Conditional dependencies are enforced only when
  that plugin is already installed and are never auto-installed.
- **At runtime**: the wrapper re-validates requirements before EVERY plugin run.
  The gate is skipped for local-only invocations - `--help`/`-h` (anywhere
  before a `--`), `--version`/`-v` or `help`/`completion` as the first arg, and
  cobra's `__complete*` requests - and when the plugin ships no contract.
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

## Air-gapped delivery

`d8 mirror pull` mirrors plugins into the images bundle automatically (plugins
whose contracts name the mirrored modules, plus their mandatory plugin
dependencies; `--include-plugin` adds more). After `d8 mirror push` the target
registry holds them at `deckhouse-cli/plugins/<name>`, where the proxy serves
them - install/update work as usual. See `internal/mirror/README.MD`
(Plugin Mirroring).

## Boundaries and deliberate decisions

- Listing the full plugin catalog over RPP is not supported (the proxy has no
  catalog endpoint); install/update by name works.
- Idempotency compares the version reported by the binary itself; a plugin that
  prints a non-semver banner is re-pulled on every explicit `update`.
- Dependency resolution is dry-run during selection (a candidate whose chain
  cannot be resolved is skipped); the chain is actually installed only for the
  finally chosen version. Recursion has a cycle guard and a depth cap.
- Dependencies are only upgraded, never downgraded, to satisfy a constraint.
- A plugin contract may depend on **external** modules only (those served from
  `deckhouse/<edition>/modules/<name>`), never on modules embedded in the
  platform image: `d8 mirror` auto-selects plugins from the mirrored external
  modules alone, so an embedded-module requirement never triggers selection
  and, as a secondary requirement, gets the plugin skipped as "not in the
  bundle".

## Package map

| File | Responsibility |
|---|---|
| `plugins.go` | the `Manager`: shared state of the plugin machinery |
| `install.go` | the install pipeline: lock, staged download, smoke, atomic swap, idempotency |
| `select.go` | newest-compatible version selection, contract memoization |
| `planner.go` | plugin-to-plugin dependency resolution: constraint-aware planning, conflict/cycle/depth guards, upgrade-only |
| `update.go` | `UpdateAll`, installed-plugin discovery, home-fallback switch |
| `remove.go` | `Remove` / `RemoveAll` |
| `validators.go` | plugin-to-plugin requirement checks + the Manager glue over `requirements/` (snapshot cache, kubeconfig clients, `--skip-cluster-checks`) |
| `requirements/` | cluster-side requirements: the one-shot cluster snapshot (k8s / Deckhouse / modules) and the named checks against it |
| `run.go` | running an installed plugin: requirement gate, env injection, exec |
| `list.go` / `versions.go` | data for the `list` / `versions` commands |
| `source.go` / `rpp_source.go` / `init.go` | the `pluginSource` interface, its default RPP implementation, and source selection |
| `source_legacy.go` | the hidden `--source` direct-registry bypass (temporary, pre-#386; force-enables `--skip-cluster-checks`) |
| `builtins.go` | built-in command names (`delivery-kit`, `package`) that satisfy a same-named plugin dependency by presence - no version check, no registry lookup |
| `layout/` | on-disk path layout |
| `flags/` | the `d8 plugins` flag set |
| `cmd/` | the `d8 plugins ...` command tree and the per-plugin wrapper command, one file per command |
| `cmd/errdetect/` | maps registry-packages-proxy errors (401/403/404/5xx/endpoint-discovery) to actionable hints |

Related: `internal/rpp` (proxy HTTP client), `internal/lockfile` (install lock),
`internal/selfupdate` (the same store-and-symlink update pattern for the d8
binary itself).
