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
- [Runtime API: `api` (hidden)](#runtime-api-api-hidden)
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
├── collect-debug-info                  Stream a gzipped debug tarball to stdout
│   └── virtualization                  Stream a d8-virtualization-only debug tarball
└── api  (hidden)                       Query the controller runtime API (/api/v1, Module v2)
    ├── healthz | readyz | endpoints | metrics
    ├── pprof <name>                    Fetch a /debug/pprof profile
    ├── get <path>                      GET any route as is
    ├── queues dump                     Task queues
    ├── scheduler dump                  Scheduler nodes
    ├── requirements dump               Values for the release requirement checks
    └── packages dump | global dump | render <name> | snapshots <name>
```

The `s` alias is the recommended short form (`d8 s module list`). `p` and `platform` are legacy aliases kept for backward compatibility with older documentation.

> **Availability:** the built-in `system` command documented here is registered only when no plugin named `system` is installed. When one is, `d8 system` is served by that plugin instead and the exact surface may differ. Check with `d8 dist plugins list`; `d8 dist plugins remove system` restores the built-in.

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

Commands in this subtree use one of four access paths. Knowing which a command uses explains its prerequisites and its failure modes.

1. **Direct Kubernetes API.** The command reads or writes a specific resource through the API server using your kubeconfig credentials. Used by `get`/`edit` (Secrets in `kube-system`), `module enable`/`disable`/`maintenance` (`ModuleConfig`), `module approve`/`apply-now` (`ModuleRelease`), and `package scan` (`PackageRepositoryOperation`). Requires the corresponding RBAC (get/patch/create on those resources).

2. **Exec into the Deckhouse leader pod.** The command shells into the running controller and either curls the controller's internal self-API at `http://127.0.0.1:9652/...` (`module list`/`values`/`snapshots`, `queue list`/`main`) or runs a battery of diagnostic commands (`collect-debug-info`). The leader pod is located in namespace `d8-system` by the label selector `leader=true`, container `deckhouse`. This path needs RBAC to `create pods/exec` in `d8-system`, and the relevant tools (`curl`, `kubectl`, `deckhouse-controller`, ...) must exist inside that container. If no leader pod is present the command fails with `no pods deckhouse available in namespace d8-system`.

3. **Port-forward to the Deckhouse leader pod.** `queue list`/`main` with `--http` send the HTTP request to the same self-API at `127.0.0.1:9652` themselves, over the API server's `pods/portforward` subresource (WebSockets, falling back to SPDY, like kubectl), so nothing has to run inside the container. This path needs RBAC to `create pods/portforward` in `d8-system` instead of `pods/exec`; with user-authz that is the separate `portForwarding` switch, while exec comes with the `PrivilegedUser` level. The leader pod is found the same way as for exec. `pods/proxy` cannot replace it: the self-API listens on loopback only, and the kube-rbac-proxy that publishes it on port 4204 never receives your token, because the API server drops the `Authorization` header before proxying.

4. **Pod log stream.** `logs` reads the leader pod's `deckhouse` container log through the Kubernetes log API (not an exec).

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

Dump the controller's reconciliation queues. Both leaves exec into the leader pod and curl the controller self-API, then print its response. With `--http` they reach the same self-API through a port-forward instead of the exec (see [How `d8 system` reaches the cluster](#how-d8-system-reaches-the-cluster)); the output is the same.

### `d8 system queue list`

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--output` | `-o` | string | `text` | Output format: `text`, `yaml`, or `json`. |
| `--show-empty` | `-e` | bool | `false` | Include empty queues. |
| `--watch` | `-w` | bool | `false` | Continuously re-render the queue in place. |
| `--http` | | bool | `false` | Fetch through a `pods/portforward` tunnel instead of exec into the pod. |

`--watch` is a full-screen view that refreshes about once a second until you press `Ctrl+C`; it is only valid with `--output text` (combining it with `json`/`yaml` is rejected up front). With `--http` the watch keeps one port-forward connection open and reconnects, finding the leader pod again, after a failed refresh.

### `d8 system queue main`

Dumps only the main queue. Supports `--output` (`text`/`yaml`/`json`, default `text`) and `--http` - it has no `--show-empty` or `--watch`.

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

It refuses to run when stdout is a terminal (to avoid dumping binary to your screen) unless you pass `--list-exclude`. The collection runs **inside** the leader pod: it executes the 63 declared commands there (`deckhouse-controller queue list`, redacted global values, module/source/release inventories, cluster-wide `kubectl get` snapshots, and controller/etcd/apiserver/VPA/Prometheus logs, plus cloud-provider/cert-manager/istio/cni-cilium/virtualization extras when those modules are Ready), writing each result as a file in the archive. The two per-cloud log collections are repeated once per matching provider module, so a cloud cluster ends up with slightly more archive entries than commands.

Before collecting, the command reads the list of `Ready` modules to decide which module-gated commands apply. If that read fails, it prints an error and keeps going: module-gated commands run anyway (and may produce empty files), except the per-module log collections whose file name contains the module name - those are skipped, since their archive entry name cannot be resolved.

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--exclude` | | string list | (none) | Comma-separated list of entries to leave out of the archive. Accepts exactly the names printed by `--list-exclude`, with or without the file extension and ignoring surrounding spaces; a name matches that entry only, never a group of files sharing a prefix. A name that matches nothing is an error listing close matches, so a typo cannot pass as "collect everything". |
| `--list-exclude` | `-l` | bool | `false` | Print the names accepted by `--exclude`, then exit. This path makes no cluster calls. The names are the archive file names as declared in the command table, except the per-module cloud logs, which are printed as the module-independent keys `ccm-logs` and `csi-controller-logs` (their real entry is `d8-<module>-ccm-logs.txt`, and both spellings are accepted). |
| `--command-timeout` | | duration | `2m` | Timeout applied to each individual in-pod command. |
| `--request-interval` | | duration | `0` | Minimum gap between commands to avoid overloading the cluster (e.g. `200ms`, `1s`). `0` disables rate limiting. |

While collecting, the command prints only its start and completion banners to stderr; individual entries are not announced. Failures are the exception - they are reported as they happen.

A file is written even when its source command fails or times out, so an entry may be empty or truncated rather than absent. Every such command is listed in a **`collection-errors.txt`** entry added to the archive, naming the entry, the command, the error (or the timeout) and how many bytes were kept. The archive has no `collection-errors.txt` when everything succeeded. Check for it before treating an empty entry as "the resource holds nothing" - the warnings printed during collection go to stderr, which is not part of the archive.

**Handle the archive as sensitive.** Only `cluster-global-values.json` is redacted (its `kubeRBACProxyCA` and registry `dockercfg`); container logs and the raw audit policy Secret (`kube-system-audit-policy.json`) are included unredacted.

### `collect-debug-info virtualization`

Collects a separate, virtualization-focused archive: the pod list of the `d8-virtualization` namespace plus the **full** log of every pod in it (`--tail=-1`, no line cap). Same stdout rules as the parent command.

The pod list is read through the Kubernetes API with **your own** kubeconfig, so the account you run `d8` with needs `list pods` in `d8-virtualization`; the logs themselves are still collected by `kubectl` running inside the leader pod, like every other archive entry.

```bash
d8 system collect-debug-info virtualization > deckhouse-debug-virtualization-$(date +"%Y_%m_%d").tar.gz
```

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--skip-ds-logs` | | bool | `false` | Skip logs of pods owned by a DaemonSet (`virt-handler`, `virtualization-dra`, `vm-route-forge`, ...), whose volume scales with the number of nodes. |
| `--command-timeout` | | duration | `2m` | Timeout applied to each individual in-pod command, and to the pod list request. |
| `--request-interval` | | duration | `0` | Minimum gap between commands to avoid overloading the cluster. |

The pod list is the entire payload of this archive, so the command fails (and writes nothing) when the namespace cannot be listed or holds no pods - instead of producing a valid-looking archive with a single empty file. `--exclude`/`--list-exclude` do not apply here.

---

## Runtime API: `api` (hidden)

`d8 system api` covers the runtime API of the Deckhouse controller, one leaf per route. The command is hidden from help because the API exists only when the controller runs Module v2 (`DECKHOUSE_ENABLE_MODULE_V2=true`, the `enableModuleV2` setting). Without it the same port is served by addon-operator: `healthz`, `readyz` and `metrics` still answer, `/api/v1/...` and `/endpoints` answer 404.

Every command is a plain HTTP request through the API server: the `pods/proxy` subresource of the leader pod, to the controller's TCP listener on the pod IP, port `self` (`ADDON_OPERATOR_LISTEN_PORT`, 4222). It needs `get pods/proxy` in `d8-system`. A port-forward cannot reach this listener: port-forwarding dials localhost inside the pod's network namespace, and the listener binds the pod IP. With user-authz, `get pods/proxy` comes only with wildcard roles such as SuperAdmin; the RBACv2 `proxy_resources` capability grants `create` only.

The controller of deckhouse main registers `/api/v1/packages`, whose answers carry registry credentials, rendered Secrets and hook snapshots, only on a Unix socket inside its container (`/tmp/deckhouse-debug.socket`), not on the TCP listener. The `packages` commands therefore answer 404 until the controller publishes them over TCP; d8 does not reach into the socket.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--pod` | string | the leader (`app=deckhouse,leader=true`) | Controller pod to query, e.g. a standby replica for `readyz`. |
| `--output`, `-o` | string | `yaml` | On dumps: `yaml` or `json`, printed exactly as the controller encodes them (`?output=`); `text` gives a summary table on `queues`, `scheduler` and `packages dump`. |

| Command | Route | Answer |
|---|---|---|
| `healthz` | `GET /healthz` | `ok` |
| `readyz` | `GET /readyz` | 200 when ready (the leader finished its startup converge, a standby sees a ready leader), 500 with the reason otherwise; the command prints the message when ready and fails with `not ready: <reason>` otherwise |
| `endpoints` | `GET /endpoints` | `METHOD /route` lines |
| `metrics` | `GET /metrics` | Prometheus exposition |
| `pprof <name>` | `GET /debug/pprof/<name>` | Profile of `heap`, `goroutine`, `allocs`, `block`, `mutex`, `threadcreate`, `profile`/`trace` (`--seconds`), `cmdline`, `symbol`; binary output is refused on a terminal unless `--debug 1` or `2` asks for text |
| `get <path>` | any, query included | The body as is |
| `queues dump [--name P]` | `GET /api/v1/queues/dump` | `QueuesDump`: queues by name with length and tasks; `--name` keeps the package queue, its hook queues and, for a module, its `crd` and `webhooks` queues; the name of an unknown package selects nothing, which the controller treats as every queue |
| `scheduler dump [--name P]` | `GET /api/v1/scheduler/dump` | `SchedulerDump`, or one `SchedulerNode` (`null` for an unknown package) |
| `requirements dump` | `GET /api/v1/requirements/dump` | `RequirementsDump`: requirement key to value |
| `packages dump [--name P]` | `GET /api/v1/packages/dump` | `PackagesDump` (`apps` keyed by `<namespace>.<name>`, `modules`), or one `Application`/`Module` (`null` for an unknown package) |
| `packages global dump` | `GET /api/v1/packages/global/dump` | `GlobalModule`, `null` until the runtime initializes it |
| `packages render <name>` | `GET /api/v1/packages/render/<name>` | Rendered Helm manifests (YAML); 400 for a package without a chart, 500 when rendering fails, an unknown package included (`render failed: no package found`) |
| `packages snapshots <name>` | `GET /api/v1/packages/snapshots/<name>` | `Snapshots`: hook to Kubernetes binding to objects and informer counters; 404 for an unknown package |

The answer types live in `internal/system/cmd/api/apiclient/types.go` and mirror deckhouse main at 8010976436. The client reports failures with sentinel errors for `errors.Is` (`apiclient/errors.go`): a non-2xx answer is a `StatusError` that unwraps to `ErrNotFound` (404), `ErrBadRequest` (400) or `ErrServerError` (5xx); a dump that answers `null` for `--name` is `ErrUnknownPackage`, the global module before initialization `ErrGlobalNotLoaded`; `ErrNotReady`, `ErrNoLeader` and `ErrNoSelfPort` cover readiness and the pod lookup. Their tests decode JSON that the controller's own dump types produced (`apiclient/testdata`), refusing unknown fields, so a field the controller adds shows up as a test failure once the fixtures are regenerated.

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

# Same, without exec: port-forward through the API server (needs pods/portforward)
d8 system queue list --watch --http

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

# Collect the virtualization-only archive, without DaemonSet pod logs
d8 system collect-debug-info virtualization --skip-ds-logs \
  > deckhouse-debug-virtualization-$(date +"%Y_%m_%d").tar.gz


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
- **The debug archive is sensitive** (unredacted logs and the raw audit policy Secret, `kube-system-audit-policy.json`) and must be redirected to a file.
- **`collect-debug-info` takes no positional arguments.** A misspelled subcommand (`virtualisation`) is rejected with `unknown command` instead of silently running the full cluster-wide collection.
- **stdout vs stderr:** `module` state changes print to stdout while notices/warnings/errors print to stderr, which makes it easy to script against applied changes only.
