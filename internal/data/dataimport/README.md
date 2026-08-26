# DataImport
Subcommand for the Deckhouse CLI to create/import/delete data via DataImport resources.

This command drives the **standalone PVC import**: the target PVC is fully defined by the
PVC template you pass to `create`, data is uploaded straight into it, and no
snapshot/`VolumeSnapshotContent` artifact is produced. (The snapshot-leaf import
mode is driven separately by `d8 snapshot upload`.)

The two modules that serve `DataImport` spell this destination differently, and `d8 data import`
writes whichever spelling belongs to the module it resolved (see below): `spec.mode: CreatePVC`
plus a root `spec.pvcTemplate` for `storage-foundation`, and `spec.targetRef` carrying the same
template for `storage-volume-data-manager`.

The PVC template **must** carry `metadata.name` — the DataImport targets the PVC by
that name; `create` rejects a template without it before contacting the API server.

### Which module serves DataImport

Two Deckhouse modules serve the same CRD under different API groups:

| Module | API group |
| --- | --- |
| `storage-foundation` | `storage-foundation.deckhouse.io/v1alpha1` |
| `storage-volume-data-manager` | `storage.deckhouse.io/v1alpha1` |

`d8 data` picks one per invocation instead of being built against a fixed group, because editions
differ in which module they ship: `storage-foundation` supersedes the other, but an edition without
it carries `storage-volume-data-manager` alone.

The choice is made from two questions the API server is asked before the command does any work:
which of the two groups it serves (discovery), and which of them the calling user may read in the
target namespace (`SelfSubjectAccessReview`). `storage-foundation` wins when both answers are yes
for it; otherwise the other module is used. Both questions are answerable by any authenticated
user, so this works for the ordinary users who run `d8 data` and not only for cluster admins.

The two answers are kept apart on purpose, so the error you get names the one thing that has to
change: a group that nothing serves means the module is not enabled, while a served group you are
not authorized for means an RBAC grant is missing. Check the latter with:

```shell
d8 k auth can-i get dataimports.storage-foundation.deckhouse.io -n NAMESPACE
d8 k auth can-i get dataimports.storage.deckhouse.io -n NAMESPACE
```

### Available Commands
- create    – ensure PVC (from template) and create DataImport
- upload    – upload file contents to the DataImport endpoint
- delete    – delete DataImport

### Flags
- Common
  - `-n, --namespace` – target namespace
  - `-P, --publish` – expose public URL for access

- create
  - `-f, --file` – PATH to PVC template file (path string; stdin is not supported)
  - `--ttl` – resource time-to-live (e.g. `60m`)
  - `--wffc` – wait for first consumer (true/false)

- upload
  - `-f, --file` – local source file path
  - `-d, --dstPath` – destination path on server
  - `-c, --chunks` – number of chunks to split a file before upload (>=1, defaults to 1)
  - `-P, --publish` – use public URL of DataImport
  - `--resume` – resume upload from server-reported offset (use this flag if upload process was interrupted)

### Examples
#### create
Create DataImport, providing PVC template via file path:
```bash
d8 data import create my-import \
  -n d8-storage-volume-data-manager \
  -f ./pvctemplate-block.yaml \
  --ttl 60m --publish --wffc=false
```

#### upload
Upload a local file (auto-detects uid/gid/permissions from the file):
```bash
d8 data import upload my-import -n d8-storage-volume-data-manager -P -d /myfile -f ./test-file
```

Resume an interrupted upload:
```bash
d8 data import upload my-import -n d8-storage-volume-data-manager -P -d /myfile -f ./test-file --resume
```

Split upload into chunks:
```bash
d8 data import upload my-import -n d8-storage-volume-data-manager -P -d /myfile -f ./test-file -c 4
```

#### delete
```bash
d8 data import delete my-import -n d8-storage-volume-data-manager
```


