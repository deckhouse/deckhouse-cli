# d8 snapshot — cheatsheet (download / restore / import)

Скачивание с уже готовым неймспейсом с объектами:

1) Есть неймспейс
2) В нем есть объекты (например PVC, DemoVirtualDisk ..., ConfigMap итд)

## Модули

| Модуль | imageTag |
| --- | --- |
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

---

## Восстановление (restore) в том же namespace

`restore` восстанавливает дерево снапшота **в тот же namespace**, где лежит сам `Snapshot`
(`-n/--namespace` — это и источник, и цель). Том-снимки (`VolumeSnapshot` /
`VirtualDiskSnapshot`), на которые ссылаются восстанавливаемые PVC через `spec.dataSourceRef`,
**уже должны существовать** в этом namespace — тогда CSI сам провиженит данные.

```bash
# Восстановить снапшот на месте
~/demo/d8/builds/d8-dev snapshot restore "$SNAP" -n "$DEMO_NS"

# Восстановить и дождаться, пока восстановленные PVC станут Bound
~/demo/d8/builds/d8-dev snapshot restore "$SNAP" -n "$DEMO_NS" --wait --timeout 15m
```

`--wait` отслеживает только те PVC, что попали в набор манифестов. Disk-backed PVC доменных
объектов пересоздаются доменным контроллером асинхронно и не ожидаются.

---

## Импорт (import) в другой namespace

Кросс-namespace перенос — это три шага: `download` → `import` → `restore`. `import` только
воссоздаёт дерево снапшота (CR'ы `Snapshot`/`VolumeSnapshot`) через агрегированный API
state-snapshotter и загружает данные томов через `DataImport` (storage-volume-data-manager).
Сами рабочие объекты (PVC, ConfigMap, доменные ресурсы) в namespace при этом **не**
применяются — это делает последующий `restore`.

```bash
export TARGET_NS=restored
kubectl create namespace "$TARGET_NS" 2>/dev/null || true

# Шаг 1: импортировать ранее скачанный архив ./snap-out в namespace $TARGET_NS.
# Воссоздаёт дерево снапшота и загружает данные томов.
~/demo/d8/builds/d8-dev snapshot import \
  -n "$TARGET_NS" \
  -i ./snap-out \
  --ttl 1h \
  --timeout 30m

# Шаг 2: восстановить рабочие объекты из импортированного снапшота в том же namespace.
~/demo/d8/builds/d8-dev snapshot restore "$SNAP" -n "$TARGET_NS" --wait --timeout 15m
```

Под капотом `import` помечает каждый воссоздаваемый узел унифицированным маркером
`spec.source.import: {}` (имя `DataImport` на листе не хранится) и на каждый data-leaf создаёт
`DataImport` с `targetRef.{group,kind,name}` на этот лист (kind = вид листа-снимка, например
`VolumeSnapshot`) плюс параметры тома `storageClassName`/`size`/`volumeMode` прямо в `spec`
(берутся из архива). state-snapshotter находит `DataImport` обратным поиском по `targetRef` и
дожидается durable `VolumeSnapshotContent`.

Ограничения текущей реализации:

- импортируются только деревья core `Snapshot` и data-leaf'ы CSI `VolumeSnapshot` (блочные
  тома); доменные/demo-узлы (например, промежуточный `DemoVirtualMachineSnapshot`) не имеют
  клиентского import-маркера и должны воссоздаваться доменным контроллером;
- поддерживаются только блочные тома (`data.bin`); импорт данных файловых томов (`data.tar`)
  пока не реализован.

---

## RBAC: какой kubeconfig нужен

Read-only роль snapshot-admin достаточна только для `download` **без** данных томов (выгрузка
одних манифестов). Любой `download` с данными томов, а также `restore` и `import` выполняют
запись в namespace — для них нужен admin-kubeconfig.

| Команда | Read-only snapshot-admin достаточно | Нужен admin-kubeconfig |
| --- | --- | --- |
| `download` (только манифесты) | да | — |
| `download` (с данными томов) | нет | да |
| `restore` | нет | да |
| `import` | нет | да |

`download` при выгрузке данных томов создаёт временные объекты для чтения байтов тома:

- shadow `VolumeSnapshot` / `VolumeSnapshotContent`;
- `DataExport` (`storage.deckhouse.io`, storage-volume-data-manager).

`restore` применяет (apply) манифесты всего поддерева в namespace.

`import` дополнительно требует прав на:

- создание/изменение `DataImport` (`storage.deckhouse.io`, storage-volume-data-manager);
- вызов subresource `manifests-and-children-refs-upload` агрегированного API state-snapshotter;
- создание import-mode CR (`Snapshot`, `VolumeSnapshot`) в целевом namespace.
