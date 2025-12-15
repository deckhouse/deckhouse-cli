# sig-migrate

Subcommand for migrating Kubernetes resources by adding and removing migration annotations.

## Description

The `sig-migrate` command collects all Kubernetes resources (both namespaced and cluster-wide), adds a `d8-migration=<timestamp>` annotation, and then removes annotations with the `d8-migration-` prefix. This is useful for triggering reconciliation in Deckhouse controllers.

## Usage

```shell
d8 tools sig-migrate [flags]
```

## Flags

| Flag           | Description                                                                  | Default                                     |
| -------------- | ---------------------------------------------------------------------------- | ------------------------------------------- |
| `--retry`      | Retry annotation for objects that failed to be processed in the previous run | `false`                                     |
| `--as`         | Specify a Kubernetes service account for kubectl operations (impersonation)  | `system:serviceaccount:d8-system:deckhouse` |
| `--log-level`  | Set the log level (INFO, DEBUG, TRACE)                                       | `DEBUG`                                     |
| `--kubeconfig` | Path to the kubeconfig file to use for CLI requests                          | `$HOME/.kube/config` or `$KUBECONFIG`        |
| `--context`    | The name of the kubeconfig context to use                                    | `kubernetes-admin@kubernetes`               |

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

2. **Adding Annotation**: For each resource, an annotation `d8-migration=<timestamp>` is added, where `timestamp` is the current time in Unix timestamp format.

3. **Removing Annotations**: After adding the new annotation, all annotations starting with the `d8-migration-` prefix are removed.

4. **Error Handling**: 
   - If a resource does not support annotations (MethodNotAllowed), it is added to the list of unsupported types and skipped in the future.
   - If the operation is forbidden for the current service account, the command automatically attempts to use an alternative service account (`system:serviceaccount:d8-multitenancy-manager:multitenancy-manager`).
   - All failed attempts are recorded in `/tmp/failed_annotations.txt` and `/tmp/failed_errors.txt` files for subsequent retry.

## Retry Files

The command creates two files to track failed operations:

- `/tmp/failed_annotations.txt` - list of objects in `namespace|name|kind` format that failed to be processed
- `/tmp/failed_errors.txt` - detailed error information in `namespace|name|kind|error_message` format

### Automatic Failure Detection

At the end of execution, if any objects failed to be annotated, the command will automatically display a warning message with:

- The number of failed objects
- Paths to both error log files
- Instructions on how to investigate and retry

Example output when failures occur:

```
⚠️  Migration completed with 5 failed object(s).

Some objects could not be annotated. Please check the error details:
  Error log file: /tmp/failed_errors.txt
  Failed objects list: /tmp/failed_annotations.txt

To investigate the issues:
  1. Review the error log file to understand why objects failed
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
- **TRACE**: Maximum verbose output, including all commands and API responses

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
