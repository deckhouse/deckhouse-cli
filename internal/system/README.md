# `d8 system` - Platform Operations

`d8 system` is the cluster-side operations subtree of the Deckhouse CLI. Where `d8 mirror` and `d8 cr` work against registries, `d8 system` talks to a **running** Deckhouse Kubernetes Platform (DKP) cluster: it reads and edits bootstrap configuration, drives the module lifecycle (enable/disable, maintenance, release approvals), triggers package-repository scans, dumps the controller's reconciliation queues, streams controller logs, and packages a full debug archive.

It is aimed at cluster administrators and SREs operating a live DKP installation. Every subcommand authenticates with the standard kubeconfig, so it works anywhere `kubectl` does.

© Flant JSC 2025

---

## Table of contents

- [Command map](#command-map)
- [Global flags](#global-flags)
- [How `d8 system` reaches the cluster](#how-d8-system-reaches-the-cluster)
- [Configuration: `get` and `edit`](#configuration-get-and-edit)
- [Modules: `module`](#modules-module)
- [Packages: `package`](#packages-package)
- [Queues: `queue`](#queues-queue)
- [Logs: `logs`](#logs-logs)
- [Debug archive: `collect-debug-info`](#debug-archive-collect-debug-info)
- [Examples](#examples)
- [Behavior and safety notes](#behavior-and-safety-notes)

---

## Command map

```
d8 system  (aliases: s, p, platform)
├── get                                 Read bootstrap configuration Secrets (kube-system)
│   ├── cluster-configuration
│   ├── provider-cluster-configuration
│   └── static-cluster-configuration
├── edit                                Edit those Secrets in $EDITOR and patch them back
│   ├── cluster-configuration
│   ├── provider-cluster-configuration
│   └── static-cluster-configuration
├── module                              Operate DKP modules
│   ├── list                            List enabled modules
│   ├── enable  <module>                Set ModuleConfig spec.enabled=true
│   ├── disable <module>                Set ModuleConfig spec.enabled=false
│   ├── maintenance enable|disable <module>   Toggle spec.maintenance
│   ├── approve   <module> <version>    Approve a Manual-policy ModuleRelease
│   ├── apply-now <module> <version>    Deploy a ModuleRelease now, ignoring update windows
│   ├── values    <module>             Dump the module's computed hook values
│   └── snapshots <module>             Dump the module's hook snapshots
├── package                             Operate DKP packages
│   └── scan <repository-name>          Create a PackageRepositoryOperation scan task
├── queue                               Dump the controller reconciliation queues
│   ├── list                            Dump all queues (optionally watch)
│   └── main                            Dump the main queue
├── logs                                Stream deckhouse-controller logs
└── collect-debug-info                  Stream a gzipped debug tarball to stdout
```

The `s` alias is the recommended short form (`d8 s module list`). `p` and `platform` are legacy aliases kept for backward compatibility with older documentation.

> **Availability:** the built-in `system` command is registered only when the environment variable `DECKHOUSE_PLUGINS_ENABLED` is **not** `true`. When plugins are enabled, `d8 system` is served by a plugin shim instead, and the exact surface may differ from what is documented here.

---

## Global flags

These persistent flags are declared on `d8 system` and inherited by **every** subcommand below. There are no other cluster-wide flags for this subtree.

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--kubeconfig` | `-k` | string | `$KUBECONFIG`, else the OS-default kubeconfig (e.g. `~/.kube/config`) | Path to the kubeconfig file. Supports the OS path-list form (e.g. colon-separated on Linux/macOS). |
| `--context` | | string | current-context of the kubeconfig | Name of the kubeconfig context to use. |

Before any subcommand runs, `d8 system` validates that `--kubeconfig` points at an **existing regular file** and fails fast otherwise (`Invalid --kubeconfig: ...`). It does not validate connectivity at this stage - that surfaces when the subcommand actually calls the cluster.

---

## How `d8 system` reaches the cluster

Commands in this subtree use one of three access paths. Knowing which a command uses explains its prerequisites and its failure modes.

1. **Direct Kubernetes API.** The command reads or writes a specific resource through the API server using your kubeconfig credentials. Used by `get`/`edit` (Secrets in `kube-system`), `module enable`/`disable`/`maintenance` (`ModuleConfig`), `module approve`/`apply-now` (`ModuleRelease`), and `package scan` (`PackageRepositoryOperation`). Requires the corresponding RBAC (get/patch/create on those resources).

2. **Exec into the Deckhouse leader pod.** The command shells into the running controller and either curls the controller's internal self-API at `http://127.0.0.1:9652/...` (`module list`/`values`/`snapshots`, `queue list`/`main`) or runs a battery of diagnostic commands (`collect-debug-info`). The leader pod is located in namespace `d8-system` by the label selector `leader=true`, container `deckhouse`. This path needs RBAC to `create pods/exec` in `d8-system`, and the relevant tools (`curl`, `kubectl`, `deckhouse-controller`, ...) must exist inside that container. If no leader pod is present the command fails with `no pods deckhouse available in namespace d8-system`.

3. **Pod log stream.** `logs` reads the leader pod's `deckhouse` container log through the Kubernetes log API (not an exec).

---

## Configuration: `get` and `edit`

`get` and `edit` operate on the three DKP bootstrap-configuration Secrets stored in the `kube-system` namespace. Both the namespace and the Secret/data-key names are fixed - there is no flag to point them elsewhere.

| Subcommand | Secret (`kube-system`) | Data key |
|---|---|---|
| `cluster-configuration` | `d8-cluster-configuration` | `cluster-configuration.yaml` |
| `provider-cluster-configuration` | `d8-provider-cluster-configuration` | `cloud-provider-cluster-configuration.yaml` |
| `static-cluster-configuration` | `d8-static-cluster-configuration` | `static-cluster-configuration.yaml` |

Note that the provider key is `cloud-provider-cluster-configuration.yaml`, which does not match the Secret-name stem.

### `d8 system get <config>`

Reads the Secret and prints the decoded YAML to stdout verbatim - no re-formatting, no highlighting, no filtering. Pipe it into `yq`/`grep` to slice it. This is read-only; it makes no cluster changes and writes no files. Neither positional args nor local flags apply.

### `d8 system edit <config>`

Dumps the decoded YAML into a temporary file, opens it in your editor, and - **only if the content changed** - base64-encodes the result and patches it straight back onto the live Secret with a JSON merge patch.

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--editor` | `-e` | string | `$EDITOR`, else `vi` | Editor to launch. |

Behavior worth knowing before you use it:

- Change detection is a SHA-256 comparison of the file bytes. Identical content prints `Configurations are equal. Nothing to update.` and makes no API call; a real change prints `Secret updated successfully`.
- There is **no YAML or schema validation and no diff/confirmation prompt.** Whatever you save is written to the live Secret as-is, taking effect immediately. Malformed YAML will be stored unchanged.
- If the editor exits non-zero the command aborts before patching, so quitting your editor with an error is a safe way to cancel.
- The temporary file holds plaintext cluster configuration while you edit; it is removed on exit. Requires RBAC to get and patch the named Secret in `kube-system`.

---

## Modules: `module`

Manage the DKP module lifecycle. The state-changing commands (`enable`, `disable`, `maintenance`, `approve`, `apply-now`) edit `ModuleConfig`/`ModuleRelease` resources through the Kubernetes API; the read commands (`list`, `values`, `snapshots`) dump data from the controller's in-pod self-API and therefore need a running leader pod.

Output convention across the group: lines reporting an **applied change** go to **stdout**; "already in that state" notices, warnings, and errors go to **stderr**. Message prefixes are colorized - `[INFO]` green, `[WARN]` yellow, `[ERROR]` red.

### `d8 system module list`

Lists the enabled modules by querying the controller. The payload is rendered by DKP and printed raw (there are no client-side columns).

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--output` | `-o` | string | `yaml` | Output format: `yaml` or `json`. |

### `d8 system module enable <module>` / `disable <module>`

Sets `spec.enabled` on the module's `ModuleConfig` (`deckhouse.io/v1alpha1`, cluster-scoped) to `true` / `false`. Takes exactly one argument, the module name.

- If the `ModuleConfig` does not exist, **both** commands create it with the corresponding `spec.enabled` value - `disable` on an unknown module does not error, it creates a disabled config.
- If the module is already in the requested state, the command reports it on stderr and makes no change.
- `enable` has a dedicated hint path: when the admission webhook rejects a module as experimental, it prints a ready-to-run `kubectl patch` that sets `allowExperimentalModules: true` on the `deckhouse` ModuleConfig, then exits with an error.

### `d8 system module maintenance enable|disable <module>`

Toggles maintenance mode by setting or clearing `spec.maintenance` on the module's `ModuleConfig` (takes exactly one argument). While maintenance is on, Deckhouse stops reconciling that module's resources, which lets you hand-edit them.

- `enable` sets `spec.maintenance: "NoResourceReconciliation"`; `disable` removes the field (restoring normal reconciliation).
- Unlike `module enable`/`disable`, this **does not create** the `ModuleConfig`. If it is missing the command prints `[ERROR] ModuleConfig '<name>' does not exist.` and points you at `d8 system module enable <name>` first.

### `d8 system module approve <module> <version>`

Approves a pending `ModuleRelease` for a module whose update policy is **Manual**, by adding the annotation `modules.deckhouse.io/approved="true"`. Requires exactly two arguments; a missing `v` prefix on the version is added automatically (`0.3.10` -> `v0.3.10`).

- Only releases in the `Pending` phase can be approved. If the release is already approved, or is not in `Pending`, the command prints a notice and **exits 0** without changing anything - it does not treat these as errors.
- If the release is not found, it suggests the nearest versions and lists the pending releases available for that module.

### `d8 system module apply-now <module> <version>`

Forces immediate deployment of a `ModuleRelease` (for modules on the **Auto** policy that have update windows or a future `applyAfter`), by adding the annotation `modules.deckhouse.io/apply-now="true"`. Same argument rules, phase checks, exit-0-on-noop behavior, and not-found suggestions as `approve`.

### `d8 system module values <module>` / `snapshots <module>`

Dump, respectively, the module's computed hook **values** and its hook **snapshots** (cached hook objects) from the controller's in-pod API. Each takes exactly one argument, the module name.

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--output` | `-o` | string | `yaml` | Output format: `yaml` or `json`. |

---

## Packages: `package`

### `d8 system package scan <repository-name>`

Triggers a full scan of a `PackageRepository` by creating a `PackageRepositoryOperation` resource (`deckhouse.io/v1alpha1`) with `spec.type: Update` and `spec.update.fullScan: true`. This is **fire-and-forget**: the command returns once the operation resource is created and does not wait for, or report, scan results. The repository name argument is required (shell completion offers existing `PackageRepository` names).

| Flag | Type | Default | Description |
|---|---|---|---|
| `--timeout` | duration | `5m` | Scan timeout embedded into the created resource (`spec.update.timeout`). This is the **scan-side** timeout, not the CLI's API timeout. |
| `--name` | string | auto-generated | Name for the `PackageRepositoryOperation`. If omitted, a name is generated (`<repo>-scan-manual-...`). An explicit name that already exists makes creation fail (no upsert). |
| `--dry-run` | bool | `false` | Print the resource that would be created (as YAML) without creating it. |

Note that `--dry-run` still contacts the cluster: the target `PackageRepository` is fetched (and validated to exist) *before* the dry-run branch, so dry-run needs connectivity and an existing repository.

---

## Queues: `queue`

Dump the controller's reconciliation queues. Both leaves exec into the leader pod and curl the controller self-API, then print its response.

### `d8 system queue list`

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--output` | `-o` | string | `text` | Output format: `text`, `yaml`, or `json`. |
| `--show-empty` | `-e` | bool | `false` | Include empty queues. |
| `--watch` | `-w` | bool | `false` | Continuously re-render the queue in place. |

`--watch` is a full-screen view that refreshes about once a second until you press `Ctrl+C`; it is only valid with `--output text` (combining it with `json`/`yaml` is rejected up front).

### `d8 system queue main`

Dumps only the main queue. Supports `--output` (`text`/`yaml`/`json`, default `text`) - it has no `--show-empty` or `--watch`.

---

## Logs: `logs`

Streams the `deckhouse` container log from the leader pod (`d8-system`) through the Kubernetes log API and copies it to stdout verbatim (no JSON parsing or reformatting). There is no per-module filter - it streams the whole controller log.

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--tail` | | int | `-1` | Limit output to the last N lines. `-1` means no limit (must be `>= -1`; `0` is also treated as no limit). |
| `--follow` | `-f` | bool | `false` | Stream new log lines as they arrive. |
| `--since` | | string | | Show logs newer than a relative duration, e.g. `5s`, `2m`, `1h`. |
| `--since-time` | | string | | Show logs after a timestamp, e.g. `2025-05-19 12:00:00` (interpreted as UTC). |

`--since` and `--since-time` are mutually exclusive. Logs come from the current leader pod's live container instance only (there is no `--previous` and no multi-replica aggregation).

---

## Debug archive: `collect-debug-info`

Collects a wide cluster snapshot into a **gzipped tar streamed to stdout**, so you always redirect it to a file:

```bash
d8 system collect-debug-info > deckhouse-debug-$(date +"%Y_%m_%d").tar.gz
```

It refuses to run when stdout is a terminal (to avoid dumping binary to your screen) unless you pass `--list-exclude`. The collection runs **inside** the leader pod: it executes on the order of ~60 diagnostic commands there (`deckhouse-controller queue list`, redacted global values, module/source/release inventories, cluster-wide `kubectl get` snapshots, and controller/etcd/apiserver/VPA/Prometheus logs, plus cloud-provider/cert-manager/istio/cni-cilium extras when those modules are Ready), writing each result as a file in the archive.

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--exclude` | | string list | (none) | Comma-separated list of elements to leave out of the archive. Matches by base name, so e.g. `ccm-logs` also drops the per-cloud `ccm-logs-<module>.txt`. |
| `--list-exclude` | `-l` | bool | `false` | Print the names of everything that can be excluded, then exit. This path makes no cluster calls. |
| `--command-timeout` | | duration | `2m` | Timeout applied to each individual in-pod command. |
| `--request-interval` | | duration | `0` | Minimum gap between commands to avoid overloading the cluster (e.g. `200ms`, `1s`). `0` disables rate limiting. |

**Handle the archive as sensitive.** Only `global-values.json` is redacted (its `kubeRBACProxyCA` and registry `dockercfg`); container logs and the raw `audit-policy` Secret are included unredacted. Also note that a file is written even when its source command fails or times out, so an entry may be empty rather than absent.

---

## Examples

```bash
# --- Configuration ---

# View the cluster configuration
d8 system get cluster-configuration

# Extract one field with yq
d8 system get provider-cluster-configuration | yq '.masterNodeGroup.replicas'

# Edit the static cluster configuration in a specific editor
d8 system edit static-cluster-configuration --editor nano


# --- Modules ---

# List enabled modules as JSON
d8 system module list -o json

# Enable / disable a module
d8 system module enable  cert-manager
d8 system module disable cni-cilium

# Put a module into maintenance mode to hand-edit its resources, then release it
d8 system module maintenance enable  my-module
d8 system module maintenance disable my-module

# Approve a Manual-policy release, or force an Auto-policy release out the door now
d8 system module approve   csi-hpe v0.3.10
d8 system module apply-now  csi-hpe 0.3.10          # 'v' prefix added automatically

# Inspect a module's computed values and hook snapshots
d8 system module values    prometheus -o json
d8 system module snapshots node-manager


# --- Packages ---

# Trigger a repository scan; preview first with --dry-run
d8 system package scan my-repo --dry-run
d8 system package scan my-repo --timeout 10m


# --- Queues ---

# Dump all queues, including empty ones, as YAML
d8 system queue list -o yaml --show-empty

# Live-watch the queues (text only, Ctrl+C to stop)
d8 system queue list --watch

# Just the main queue
d8 system queue main


# --- Logs ---

# Follow the controller log, last 100 lines to start
d8 system logs --tail 100 --follow

# Everything from the last 15 minutes
d8 system logs --since 15m


# --- Debug archive ---

# List what can be excluded, then collect a trimmed archive
d8 system collect-debug-info --list-exclude
d8 system collect-debug-info --exclude ccm-logs,csi-controller-logs \
  > deckhouse-debug-$(date +"%Y_%m_%d").tar.gz


# --- Global flags ---

# Target a specific cluster/context
d8 system --kubeconfig ~/.kube/prod.config --context prod module list
```

---

## Behavior and safety notes

- **`edit` writes live, unvalidated.** No schema check, no diff, no confirmation - the patch lands on the `kube-system` Secret the moment you save a changed file. Quit the editor with a non-zero status to cancel.
- **`disable` creates a `ModuleConfig`** when none exists (as a disabled config), whereas **`maintenance` requires** the `ModuleConfig` to pre-exist. `enable`/`disable` auto-create; `maintenance` does not.
- **`approve` / `apply-now` are annotation-only and idempotent.** They never error on an already-annotated or non-`Pending` release; they print a notice and exit 0.
- **`package scan` does not scan locally and does not wait.** It creates a `PackageRepositoryOperation` and returns; results are reported by the platform, not the CLI.
- **In-pod commands need a leader pod.** `module list`/`values`/`snapshots`, `queue`, and `collect-debug-info` exec into the pod labeled `leader=true` in `d8-system`; without it they fail with `no pods deckhouse available in namespace d8-system`.
- **The debug archive is sensitive** (unredacted logs and the raw audit-policy Secret) and must be redirected to a file.
- **stdout vs stderr:** `module` state changes print to stdout while notices/warnings/errors print to stderr, which makes it easy to script against applied changes only.
