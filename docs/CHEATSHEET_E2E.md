# d8 snapshot download — cheatsheet

Скачивание с уже готовым неймспейсом с объектами:

1) Есть неймспейс
2) В нем есть объекты (например PVC, DemoVirtualDisk ..., ConfigMap итд)

## Модули

| Модуль | imageTag |
|--------|----------|
| `storage-volume-data-manager` | `mr132` |
| `state-snapshotter` | `pr37` |
| `storage-foundation` | `pr32` |

---

## Сборка (macOS → Linux amd64)

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build \
  -tags="dfrunsecurity dfrunnetwork dfrunmount dfssh containers_image_openpgp" \
  -o d8-dev \
  ./cmd/d8/
```

---

## Копировать на сервер

```bash
scp -J <user>@bastion <local-path>/d8-dev <user>@<host>:~/demo/d8/builds/
```

---

## Создать снапшот в существующем namespace

```bash
export DEMO_NS=<your-namespace>
export SNAP=demo-snap-1

# Убедиться что все PVC в Bound
kubectl -n "$DEMO_NS" get pvc

# Создать снапшот
kubectl -n "$DEMO_NS" apply -f - <<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: Snapshot
metadata:
  name: ${SNAP}
spec: {}
EOF

# Дождаться Ready=True
kubectl -n "$DEMO_NS" get snapshots.storage.deckhouse.io "$SNAP" -w
```

---

## Скачать

```bash
rm -rf ./snap-out && mkdir ./snap-out

~/demo/d8/builds/d8-dev snapshot download "$SNAP" \
  -n "$DEMO_NS" \
  -o ./snap-out \

# Посмотреть что скачалось
tree snap-out
```
