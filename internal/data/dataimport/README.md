# DataImport
Subcommand for the Deckhouse CLI to create/import/delete data via DataImport resources.

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
  --ttl 60m --publish --wffc false
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


