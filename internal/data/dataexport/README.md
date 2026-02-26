# DataExport
Subcommand for the command line client for Deckhouse.

### Available Commands:
* create      - Create k8s DataExport object.

### Examples
#### create
```shell
d8 data export create export-name pvc/test-pvc-name
```

#### create from a snapshot

1. Enable snapshot-controller module

```shell
kubectl apply -f -<<EOF
apiVersion: deckhouse.io/v1alpha1
kind: ModuleConfig
metadata:
  name: snapshot-controller
spec:
  enabled: true
  version: 1
EOF
```

2. Get a volume snapshot class name - this entity is created automatically for each type of CSI and is needed for creating a snapshot.
For example, "sds-local-volume-snapshot-class" as shown in the example below

```shell
kubectl get volumesnapshotclass

sds-local-volume-snapshot-class   local.csi.storage.deckhouse.io   Delete           22h
```

3. Create a snapshot of a needed volume 

```shell
kubectl apply -f -<<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: my-snapshot
  namespace: <name of the namespace where the PVC is located>
spec:
  volumeSnapshotClassName: <volume snapshot class name>
  source:
    persistentVolumeClaimName: <name of the PVC to snapshot>
EOF
```

4. Check that snapshot is created normally and ready to use (usually takes a few minutes to be ready):
```shell
kubectl -n <name of the namespace> get volumesnapshot my-snapshot

NAMESPACE       NAME                  READYTOUSE   SOURCEPVC                        SOURCESNAPSHOTCONTENT   RESTORESIZE   SNAPSHOTCLASS                     SNAPSHOTCONTENT                                  
<namespace>    my-snapshot          true         test-pvc-for-snapshot                           2Gi           sds-local-volume-snapshot-class   snapcontent-faf2ab1f-891d-4e5e-972c-334a490c99d8
```

5. Create data export from that snapshot using d8 command as shown in the example below 
```shell
d8 data export create export-name snapshot/my-snapshot
```
