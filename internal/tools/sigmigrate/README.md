# sig-migrate

Subcommand for migrating Kubernetes resources by adding and removing migration annotations.

## Description

The `sig-migrate` command collects all Kubernetes resources (both namespaced and cluster-wide), adds a `d8-migration=<timestamp>` annotation, and then removes annotations with the `d8-migration-` prefix. This is useful for triggering reconciliation in Deckhouse controllers.

## Usage

```shell
d8 tools sig-migrate [flags]
```

## Flags

| Flag           | Description                                                                                            | Default                                     |
| -------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------- |
| `--retry`      | Retry annotation for objects that failed to be processed in the previous run                           | `false`                                     |
| `--as`         | Specify a Kubernetes service account for kubectl operations (impersonation)                            | `system:serviceaccount:d8-system:deckhouse` |
| `--log-level`  | Set the log level (INFO, DEBUG, TRACE)                                                                 | `DEBUG`                                     |
| `--kubeconfig` | Path to the kubeconfig file to use for CLI requests                                                    | `$HOME/.kube/config` or `$KUBECONFIG`       |
| `--context`    | The name of the kubeconfig context to use                                                              | `kubernetes-admin@kubernetes`               |
| `--object`     | Process only one specific object in `namespace/name/kind` format (`clusterwide/name/kind` for cluster) | `""`                                        |

## Usage Examples

### Basic Usage

Run migration for all resources in the cluster:

```shell
d8 tools sig-migrate
```

### With kubeconfig and context

```shell
d8 tools sig-migrate --kubeconfig ~/.kube/config --context my-cluster
```

### With a different service account

Use a different service account for operations:

```shell
d8 tools sig-migrate --as system:serviceaccount:my-namespace:my-serviceaccount
```

### Retry for failed objects

Retry annotation for objects that failed to be processed in the previous run:

```shell
d8 tools sig-migrate --retry
```

### With verbose logging

Enable verbose logging (TRACE level):

```shell
d8 tools sig-migrate --log-level TRACE
```

### Run only one object

Process only one specific object:

```shell
d8 tools sig-migrate --object default/my-configmap/configmaps
```

For cluster-scoped resources, use `clusterwide` as namespace:

```shell
d8 tools sig-migrate --object clusterwide/my-clusterrole/clusterroles
```

### Combined example

```shell
d8 tools sig-migrate \
  --kubeconfig ~/.kube/config \
  --context production \
  --as system:serviceaccount:d8-system:deckhouse \
  --log-level DEBUG
```

## How It Works

1. **Resource Collection**: The command uses Kubernetes API discovery to automatically discover all available resources (both namespaced and cluster-wide).

2. **Optional Object Filter**: If `--object` is specified, the command skips full resource fetching/listing and processes only the matching object (`namespace/name/kind`).

3. **Adding Annotation**: For each selected resource, an annotation `d8-migration=<timestamp>` is added, where `timestamp` is the current time in Unix timestamp format.

4. **Removing Annotations**: After adding the new annotation, all annotations starting with the `d8-migration-` prefix are removed.

5. **Error Handling**:
   - If a resource does not support annotations (MethodNotAllowed), it is added to the list of unsupported types and skipped in the future.
   - If the operation is forbidden for the current service account, the command automatically attempts to use an alternative service account (`system:serviceaccount:d8-multitenancy-manager:multitenancy-manager`).
   - Each run writes failed/skipped artifacts to run-scoped files in `/tmp` with a timestamp suffix (for example: `/tmp/failed_annotations_20260414T151625Z.txt`, `/tmp/failed_errors_20260414T151625Z.txt`, `/tmp/skipped_objects_20260414T151625Z.txt`).
   - For backward-compatible retry UX, the latest failed annotations are also synced to legacy `/tmp/failed_annotations.txt`, so `--retry` continues to work without extra arguments.
   - A dedicated trace debug log is written to `/tmp/sigmigrate_trace_<timestamp>.log` with detailed execution/error diagnostics.

## Retry Files

The command creates run-scoped files to track failed operations:

- `/tmp/failed_annotations_<timestamp>.txt` - list of objects in `namespace|name|kind` format that failed to be processed
- `/tmp/failed_errors_<timestamp>.txt` - detailed error information in `namespace|name|kind|error_message` format
- `/tmp/skipped_objects_<timestamp>.txt` - skipped objects with reason/details

For retry compatibility, failed annotations are also mirrored into legacy `/tmp/failed_annotations.txt` and `--retry` reads from that legacy file.

### Automatic Failure Detection

At the end of execution, if any objects failed to be annotated, the command will automatically display a warning message with:

- The number of failed objects
- Paths to error artifacts and trace log
- Instructions on how to investigate and retry

Example output when failures occur:

```
⚠️  Migration completed with 5 failed object(s).

Some objects could not be annotated. Please check the error details:
  Error log file: /tmp/failed_errors_<timestamp>.txt
  Failed objects list: /tmp/failed_annotations_<timestamp>.txt
  Trace log file: /tmp/sigmigrate_trace_<timestamp>.log

To investigate the issues:
  1. Review the trace and error log files to understand why objects failed
  2. Check permissions and resource availability
  3. Retry migration for failed objects only using:
     d8 tools sig-migrate --retry
```

To retry failed objects, use the `--retry` flag:

```shell
d8 tools sig-migrate --retry
```

## Log Levels

- **INFO**: Minimal output, only important messages
- **DEBUG**: Detailed output with progress information (default)
- **TRACE**: Maximum verbose output, including all commands and API responses (also persisted into `/tmp/sigmigrate_trace_<timestamp>.log`)

## Limitations

- Some resource types may not support annotations (e.g., some CRDs). Such types are automatically detected and skipped.
- The command requires appropriate access rights to annotate resources in the cluster.
- For large clusters, execution may take a significant amount of time.

## Notes

- The command uses impersonation to perform operations on behalf of the specified service account.
- All operations are performed sequentially to ensure stability.
- Execution progress is displayed in real-time with completion percentage.

## See Also

- [d8 tools](https://github.com/deckhouse/deckhouse-cli) - other tools in the tools category
- [Deckhouse Documentation](https://deckhouse.io/) - Deckhouse Kubernetes Platform documentation
