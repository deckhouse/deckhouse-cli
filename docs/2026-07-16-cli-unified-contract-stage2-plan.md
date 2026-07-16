# План правок `deckhouse-cli` — Этап 2 (import-маркер + отказ от cluster-scoped SnapshotContent)

Статус: **черновик на ревью (ред. 3)**. Код не менялся — это только анализ и план.

- Область: **только `deckhouse-cli`**. Другие репозитории (`state-snapshotter`,
  `storage-foundation`, `sds-unified-snapshots-poc`) не трогаем.
- **SSOT контракта — `docs/2026-06-29-unified-snapshots-overview.md` (далее «контракт» /
  «2.md») — уже согласован и в рамках этой работы НЕ изменяется.** Если текущий код
  `deckhouse-cli` расходится с документом — исправляется **код CLI**, а не контракт.
  Исполнителю запрещено «уточнять» контракт правками 2.md.
- Этап 1 (миграция группы `state-snapshotter.deckhouse.io`, `DataImport`/`DataExport`
  контракты) уже закоммичен на ветке `d8-snapshot-unified-contract`.
- **Совместимость со старыми архивами и незавершёнными download/resume-сессиями,
  созданными предыдущей версией `d8`, НЕ поддерживается** (см. §3.4). Dual-reader,
  миграция и legacy-идентичность не проектируются.
- **Контракт ожидания готовности (§5.B, §Приложение A) установлен read-only анализом
  кода `state-snapshotter` и `storage-foundation`** — не эвристикой по именам reason.

---

## 0. TL;DR

Этап 2 = обязательный **Шаг 0** (дизайн) + **2a** (низкий риск) + **2b** (рефактор).

- **Шаг 0 (§3).** Модель `Node` (structured `SourceRef`, singular `Data`, **UID самого
  snapshot-CR**), **три** раздельные сущности идентичности: `CanonicalSnapshotIdentity`
  (непрозрачный внутренний ключ resume/checksum — **не путь**), `ArchiveNodeDirName`
  (path-safe читаемое имя каталога по существующему layout) и `CanonicalSourceIdentity`
  (provenance), различие `status.sourceRef` vs `status.data.source`, политика «старое не
  поддерживаем», typed парсинг namespaced status с fail-closed, требование data — в
  data-пути (`RequireNodeData`), а не в общем парсере.
- **2a (§4).** `spec.source.import: {}` → `spec.mode: Import`; убрать
  `--snapshot-class`/`spec.snapshotClassName`, ставить `spec.mode: Capture`; тип
  `SnapshotMode` + fail-closed детектор.
- **2b (§5).** Отказ от cluster-scoped `SnapshotContent` в `download` (дерево) и `import`
  (ожидание). Чтение — из namespaced `status.sourceRef`/`status.data`; готовность — из
  namespaced `Ready` узла по код-подтверждённому алгоритму (§5.B).

---

## 1. Что требует контракт (нормативные факты; 2.md не меняем)

1. **`d8` не читает cluster-scoped `SnapshotContent`.** 2.md, 1901–1902.
2. **`status.data` — singular object (Variant A, ≤1 на узел)**, пишет ядро (зеркалит из
   `SnapshotContent.status.data`); несёт `source`/`artifact`/`volumeMode`/`fsType`/
   `accessModes`/`storageClassName`/`size`. 2.md, 1909–1928. Форма 1:1 с
   `SnapshotDataBinding` (`api/v1alpha1/types.go:234-259`).
3. **orphan/extended `VolumeSnapshot`** несёт собственный namespaced `status.data`
   (самодостаточный для `d8`) и `status.sourceRef`. 2.md, 1320–1339, 1298–1306.
4. **Идентичность источника — `status.sourceRef`** — `{apiVersion,kind,name,namespace,uid}`,
   пишет домен. 2.md, 1900–1908. Совпадает с `SourceRefIdentity` (`source/source_ref.go:27-33`).
5. **`status.sourceRef` ≠ `status.data.source`.** `sourceRef` — идентичность доменного
   объекта-источника узла; `data.source` — источник data-артефакта (PVC). Для доменного
   диск-узла это разные объекты; для orphan/VS совпадают. Одно **не** fallback другого.
6. **Единственное пользовательское условие — `Ready` на самом узле** (namespaced; ядро
   зеркалит из привязанного контента, рекурсивно включает готовность детей).
   Подтверждено кодом, см. §Приложение A.
7. **Import-маркер — `spec.mode: Import`**; штатный захват — `spec.mode: Capture`.

---

## 2. Инвентаризация текущего кода (что не соответствует)

### 2a — спека объектов

| Место | Сейчас | Проблема |
|---|---|---|
| `snapimport/markers.go:91` | `spec.source.import: {}` | нужно `spec.mode: Import` |
| `snapimport/import.go:548-551` (`isImportModeMarker`) | детект по `spec.source.import` | детект по `spec.mode`, fail-closed (§4) |
| `snapimport/import.go:537-540` | текст «spec.source.import is not set» | обновить формулировку |
| `cmd/create/create.go:116-117,173-176,245-247` | `--snapshot-class` → `spec.snapshotClassName` | поля нет в контракте; убрать |
| `cmd/create/create.go:242-261` (`buildSnapshot`) | не ставит `spec.mode` | ставить `spec.mode: Capture` |
| `api/v1alpha1/types.go:92-94` (`SnapshotSpec`) | поле `SnapshotClassName` | заменить на `Mode SnapshotMode` |

### 2b — cluster-scoped `SnapshotContent` (все точки runtime-доступа)

| Место | Что делает | Замена по контракту |
|---|---|---|
| `source/tree.go:112-115` | Get bound `SnapshotContent` каждого узла | namespaced `status.data` узла |
| `source/tree.go:177-183` | `content.DataRefList()` → `OwnDataRefs` | singular `Data` из `status.data` |
| `source/tree.go:117-123` | `sourceRef` из аннотации + `spec.sourceRef` | namespaced `status.sourceRef` |
| `source/tree.go:199-217` (`visitVisibilityLeaf`) | Get VS → Get дочерний `SnapshotContent` | namespaced `status.data` самого VS-узла |
| `snapimport/import.go:785` (`waitSnapshotContentReady`) | Get cluster-scoped `SnapshotContent`, 4 условия | ждать namespaced `Ready` (§5.B) |
| `snapimport/import.go:51` | `snapshotContentGVR` | удаляется вместе с `waitSnapshotContentReady` |
| `snapimport/import.go:45-47,790-793` | `condManifestsReady/condVolumesReady/condChildrenReady` | не нужны — единственное `Ready` |
| `cmd/download/download.go:255` | `snapshotapi.AddToScheme` | см. §5.C (тип в scheme — не нарушение; нарушение — Get/List/watch) |

> Замечание по факту: в `state-snapshotter` data-нога `SnapshotContent` называется
> **`DataReady`**, а не `VolumesReady`. Текущий `condVolumesReady="VolumesReady"`
> (`import.go:46`) — устаревшая строка, которая и так не совпала бы. Оба вопроса снимаются:
> клиент перестаёт читать условия `SnapshotContent` вовсе.

---

## 3. Шаг 0 — внутренняя модель и политика совместимости (ОБЯЗАТЕЛЬНО до кода 2b)

### 3.1 Целевая модель `Node`

```go
type Node struct {
    // Идентичность самого snapshot-CR узла (обход дерева + resume key + имя каталога).
    APIVersion string
    Kind       string
    Name       string
    Namespace  string
    UID        types.UID // UID самого snapshot-CR — часть resume-идентичности/хэша, но НЕ единственная основа читаемого имени каталога

    // Идентичность захваченного доменного объекта-источника узла (status.sourceRef).
    // nil, когда контракт не даёт source-идентичности узлу — например у некоторых
    // import-узлов или manifest-only узлов (для capture-root это может быть исходный Namespace).
    SourceRef *SourceRefIdentity

    // Захваченные данные узла (status.data). Singular (Variant A): максимум ОДИН биндинг.
    // nil у агрегатора/manifest-only узла.
    Data *snapshotapi.SnapshotDataBinding

    Parent   *Node
    Children []*Node
}
```

Правила заполнения (единые для ВСЕХ kind, включая orphan/VS):

- `SourceRef` — **только** из `status.sourceRef`; никогда не выводится из `status.data.source`.
- `Data` — из `status.data` (или `nil`); `status.data.source` — источник артефакта, не
  идентичность узла, в `SourceRef` не попадает.
- `UID` — из `metadata.uid` самого snapshot-CR узла.

**Устаревшие производные поля** (`SourceRef string`, `SourceName`, `SpecSourceRef`,
`OwnDataRefs []`, `Binding *`) заменяются (совместимость со старыми архивами не нужна, §3.4).
Downstream-контракт сохраняем через явные производные:

- перечисление data узла: `Data` (0/1) вместо `OwnDataRefs`/`Binding`; при большом дифе —
  локальный compatibility-adapter (только внутри текущего кода CLI, не для архивов):

```go
// dataRefsFromStatusData — compatibility adapter под старый downstream, ожидающий срез.
// НЕ новая модель: Variant A закрепляет singular Data (максимум один биндинг).
func dataRefsFromStatusData(data *snapshotapi.SnapshotDataBinding) []snapshotapi.SnapshotDataBinding {
    if data == nil {
        return nil
    }
    return []snapshotapi.SnapshotDataBinding{*data}
}
```

**Имя отображаемого источника** (`SourceRef.Name`) — только для мест, где downstream
реально показывает имя исходного объекта. **Ключ resume строится из идентичности самого
snapshot-узла (§3.2), а не из `status.sourceRef`** (у root `SourceRef` может быть `nil`;
имена source-объектов не уникальны между kind/namespace). **Имя каталога — отдельная
path-safe функция (§3.2), не raw canonical key.**

### 3.2 Три раздельные сущности идентичности (ключ ≠ путь ≠ provenance)

`SourceRef` может быть `nil` (root, manifest-only), поэтому он **не** годится как
универсальный ключ узла. Разводим **три** сущности:

```go
// (1) Структурная идентичность самого snapshot-CR узла — универсальна (есть у любого узла).
type SnapshotIdentity struct {
    APIVersion string
    Kind       string
    Namespace  string
    Name       string
    UID        types.UID
}

// (2) Непрозрачный ВНУТРЕННИЙ ключ: сравнение, checksum, resume-index.
// Детерминированный, порядок и разделитель \x00 фиксированы. НЕ пригоден как путь
// (\x00 запрещён в path-компоненте, apiVersion содержит '/').
func CanonicalSnapshotIdentity(id SnapshotIdentity) string

// (3) Path-safe ЧИТАЕМОЕ имя каталога узла по СУЩЕСТВУЮЩЕМУ layout архива.
// Читаемая база — существующие kind/name (или source-kind/source-name), плюс короткий
// discriminator/hash (в т.ч. от UID) ТОЛЬКО для устранения коллизий. UID участвует в
// hash/идентичности, но не подменяет читаемое имя.
func ArchiveNodeDirName(id SnapshotIdentity) string

// Provenance захваченного объекта — по SourceRef (может отсутствовать).
func CanonicalSourceIdentity(id SourceRefIdentity) string
```

`CanonicalSnapshotIdentity`/`CanonicalSourceIdentity` — детерминированные (фиксированный
порядок, разделитель `\x00`), с unit-тестами на стабильность. `ArchiveNodeDirName` —
path-safe (без `\x00` и `/`), детерминированная, сохраняет существующий читаемый формат
имён каталогов. Применение:

- **resume-ключ / checksum / индекс узла** → `CanonicalSnapshotIdentity` (внутренний ключ,
  **не** попадает в filesystem path);
- **имя каталога узла в архиве** → `ArchiveNodeDirName` (существующий читаемый layout;
  raw canonical key в путь не кладём);
- **provenance захваченного объекта** → `CanonicalSourceIdentity` (по `SourceRef`);
- **data provenance** → по `Data.Source` (при необходимости).

> Новый layout сейчас не изобретаем: формат имён каталогов сохраняется существующий, с
> существующим discriminator либо коротким hash от `CanonicalSnapshotIdentity` для
> устранения коллизий. Дерево каталогов не должно превращаться в UID/hash — читаемая часть
> имени обязана оставаться понятной человеку.

### 3.3 Различие sourceRef vs data.source (инвариант)

Код **не** использует одно как fallback другого (§1.5). Для доменного диск-узла
`SourceRef=DemoVirtualDisk` и `Data.Source=PVC` сохраняются раздельно.

### 3.4 Политика совместимости (явно)

- **Старые архивы и незавершённые download/resume-сессии предыдущей версии `d8` НЕ
  поддерживаются.** Внутренний формат идентичности меняется без compatibility-слоя;
  reader/миграция не реализуются.
- **Требуется стабильность нового формата между запусками новой версии**:
  `CanonicalSnapshotIdentity`/`CanonicalSourceIdentity`/`ArchiveNodeDirName` детерминированы
  (unit-тест). Риск — не «сломаем старое», а «сделаем новый resume/layout недетерминированным».

### 3.5 Парсинг namespaced status — typed через converter, fail-closed

Узлы тянутся `fetchUnstructured` (discovery динамический ради доменных kind). Typed-модель
уже содержит `SourceRefIdentity` и `SnapshotDataBinding`, поэтому — единая typed-функция
через `runtime.DefaultUnstructuredConverter.FromUnstructured` для фрагментов status:

```go
// ParseNodeStatus извлекает контрактные фрагменты namespaced status узла.
// discovery остаётся динамическим (obj — unstructured), фрагменты — typed.
// Parser НЕ решает, «должен ли» узел иметь data: при отсутствии data возвращает Data=nil.
func ParseNodeStatus(obj *unstructured.Unstructured) (
    ident SnapshotIdentity,       // из metadata (apiVersion/kind/namespace/name/uid)
    src *SourceRefIdentity,        // status.sourceRef или nil
    data *snapshotapi.SnapshotDataBinding, // status.data или nil
    err error,
)
```

Fail-closed правила внутри `ParseNodeStatus`:

- `status.sourceRef`: отсутствует **или** валидный object; при наличии — обязательны
  `apiVersion`, `kind`, `name`, `uid`; иначе ошибка с GVK/name узла.
- **namespace в `status.sourceRef`:** на Этапе 2 поддерживаются **только namespaced source
  kinds** — `namespace` обязателен; cluster-scoped source kinds **вне области Этапа 2**
  (не гадать scope по apiVersion/kind без RESTMapper). Если позже понадобятся
  cluster-scoped — вводится RESTMapper-lookup; сейчас явный запрет.
- `status.data`: отсутствует (данных нет — допустимо на уровне парсера) **или** валидный
  object; **повреждённый `status.data` — ошибка, а не «данных нет»**.
- при наличии `status.data`: `source` и `artifact` обязательны и валидируются
  (`apiVersion/kind/name/uid`); `size` парсится как `resource.Quantity`. **Оговорка по
  `uid`:** сверить фактические typed-типы — если `artifact.uid` (или `source.uid`) объявлен
  optional на промежуточной стадии, дерево **не должно** читать узел как готовый до
  `Ready=True`; полный artifact-ref обязателен только после `Ready=True`. То есть строгую
  проверку полноты применяем к уже готовому узлу, а не к промежуточному.
- неизвестные поля во фрагментах — допускаются.

**Требование наличия data — НЕ в парсере, а в data-пути.** Отдельная функция там, где узел
реально обрабатывается как data-несущий:

```go
// RequireNodeData вызывается downstream-кодом, который ожидает volume payload
// (например обработчик data-листа). Manifest-only/агрегатор его не вызывают.
func RequireNodeData(node *Node) (*snapshotapi.SnapshotDataBinding, error)
```

Источник инварианта «узел — data-лист» — тип узла из архива/дерева и конкретный
downstream-обработчик, а не глобальная проверка «нет data → ошибка».

---

## 4. Этап 2a — import-маркер + чистка спеки (низкий риск)

1. `api/v1alpha1`: отдельный тип и константы (не «сырой» `string`):

```go
type SnapshotMode string
const (
    SnapshotModeCapture SnapshotMode = "Capture"
    SnapshotModeImport  SnapshotMode = "Import"
)
type SnapshotSpec struct {
    Mode SnapshotMode `json:"mode,omitempty"`
}
```

2. `snapimport/markers.go` (`importMarkerCR`): писать `spec.mode: Import`.
3. `snapimport/import.go` (`isImportModeMarker`) — **fail-closed**, сигнатура `(bool, error)`:

```
spec.mode отсутствует   → Capture (не import-маркер)
spec.mode == "Capture"  → не import-маркер
spec.mode == "Import"   → import-маркер
любое другое значение   → ошибка «некорректный объект»
значение не строка      → ошибка «некорректный объект»
```

   Обновить текст ошибки в `reconcileExistingMarker`.
4. `cmd/create/create.go`: удалить флаг `--snapshot-class` и обвязку (`flagSnapshotClass`,
   поле `snapshotClass`); `buildSnapshot` ставит `spec.mode: Capture` **явно** (даже если
   `Capture` — default CRD: это делает экспортируемый объект однозначным).

**Почему заработает:** маркер импорта в контракте — `spec.mode: Import`; сервер ключует
режим по нему. Поле `snapshotClassName` отсутствует в целевой схеме и не является частью
контракта; его передача либо будет отброшена (prune), либо отклонена — в зависимости от
structural-схемы/валидации CRD, поэтому CLI не должен его генерировать.

**Признак завершённости 2a:**
- `buildSnapshot` → `spec.mode: Capture`, без `spec.snapshotClassName`; проверить **все**
  вызовы `buildSnapshot`, не только builder (unit);
- флаг `--snapshot-class` отсутствует в help/usage; передача даёт `unknown flag` (unit);
  убрать из docs и (если не автоген) shell-completion;
- `importMarkerCR` → `spec.mode: Import`; `isImportModeMarker` истинно на нём, ложно на
  `Capture`, ошибка на мусорном/нестроковом `spec.mode` (unit);
- `go build ./...`, `go test ./internal/snapshot/...` зелёные.

---

## 5. Этап 2b — отказ от cluster-scoped `SnapshotContent`

### 5.A `download` / построение дерева (`source/tree.go`, `source/node.go`)

1. Ввести `ParseNodeStatus` (§3.5).
2. `visit()`: `SourceRef` — из `status.sourceRef`; `Data` — из `status.data` (singular);
   `UID` — из metadata. Убрать Get `SnapshotContent` (`tree.go:112-115`) и `DataRefList()`.
3. `visitVisibilityLeaf()`: читать `status.data` самого VS-объекта (namespaced форк-поле)
   вместо прохода VS → дочерний `SnapshotContent`. `SourceRef` — из `status.sourceRef` VS
   (не из `data.source`).
4. Downstream (`pipeline`, `describe`, `volume/manifest_worker.go`, `archive/resume.go`)
   перевести на новую модель `Node`. **resume-ключ / checksum — из
   `CanonicalSnapshotIdentity`; имя каталога — из `ArchiveNodeDirName`** (§3.2), не из
   `SourceRef.Name` и не raw canonical key.

### 5.B `import` / ожидание готовности — код-подтверждённый алгоритм

Терминальность в `state-snapshotter` установлена read-only анализом (§Приложение A): есть
частичный нормативный enum терминальных reason (`TerminalReadyReasons`/`IsReasonTerminal`,
`api/storage/v1alpha1/conditions.go:85-102`) **и** монотонный терминальный sink
`captureState.domainSpecificController.phase == Failed` (`pkg/snapshotsdk/capture.go:522-541`,
зеркалится в `Ready=False` в `ready_mirror.go:107-117`). Поэтому немедленная ошибка **не**
эвристика по суффиксу `*Failed`, а проверка конкретных сигналов.

**Ожидание одного snapshot-узла (root или выбранный `--node` лист):**

```
Читаем namespaced status узла (Snapshot / XxxxSnapshot / extended VolumeSnapshot). Порядок:
1) conditions[Ready].status == True                      → УСПЕХ
2) НЕМЕДЛЕННАЯ ОШИБКА, если:
   a) status.captureState.domainSpecificController.phase == "Failed"
      (капчур-путь; монотонный sink; reason домена может быть free-form — показать как есть)
   b) Ready.status == False И Ready.reason ∈ TERMINAL_REASONS (§Приложение A.1)
   c) Ready.status == False И Ready.reason ∈ {"DataImportAmbiguous","DataArtifactInvalid"}
      (import-лист терминалы, не входящие в enum; genericbinder/import.go:188-211)
3) ИНАЧЕ (Ready отсутствует / False с нетерминальным reason / Unknown) → ЖДАТЬ до timeout
   При timeout — показать последний Ready.status/reason/message.
```

- `TERMINAL_REASONS` в CLI — **копия** enum из кода (§Приложение A.1) с явным version
  coupling; не изобретаем новые reason и не считаем `*Failed` терминальным по имени.
  Неизвестный reason остаётся нетерминальным (обрабатывается timeout — безопаснее ложной
  терминальной ошибки):

```go
// Keep synchronized with state-snapshotter
// api/storage/v1alpha1/conditions.go TerminalReadyReasons.
// Unknown reasons remain non-terminal and are handled by timeout.
```
- Import-путь: `captureState` отсутствует (`mode=Import`), значит проверка (2a) сама собой
  не срабатывает — это ожидаемо.

**Топология ожидания (важно — устраняет избыточность):**

```
Pass 1: создать namespaced import-узлы (spec.mode: Import), top-down (ownerRef c UID) — как сейчас.
Pass 2a: залить манифесты + childRefs всех узлов — как сейчас.
Pass 2b (на каждый data-лист, конкурентно):
    создать DataImport (mode: PopulateData); залить байты; дождаться data-ноги листа (см. ниже).
Финал:
    ПОЛНЫЙ импорт: ждать Ready ТОЛЬКО у корневого Snapshot — root Ready=True рекурсивно
      включает готовность всего дерева контента (§Приложение A.3). Per-leaf Ready НЕ ждём
      (избыточно + барьер-2 отстаёт: домен-лист может быть Ready=False/ChildrenPending, пока
      корень уже готов — ready_mirror.go:118-131).
    --node импорт одного листа: ждать Ready именно у выбранного листа (алгоритм выше).
```

**Изменения по call sites:**
- `waitRootReady` (core `Snapshot`): оставить `waitSnapshotReady` (namespaced `Ready`),
  но применить алгоритм терминальности выше; **удалить** `waitSnapshotContentReady`.
- `waitLeafReady`/`waitDomainLeafReady` (`--node`): ждать namespaced `Ready` листа (+phase),
  не проход VS → bound `SnapshotContent`.
- Удалить `waitSnapshotContentReady`, `snapshotContentGVR`,
  `condManifestsReady/condVolumesReady/condChildrenReady`.

**Ожидание data-ноги листа — `DataImport mode=PopulateData` (§Приложение A.4):**

```
УСПЕХ  = conditions[Completed].status == True  И  status.data.artifact присутствует и валиден
         (контроллер ставит оба вместе: data_import_resource.go:591-598; НЕ ждать Ready=True —
          Ready остаётся False после завершения, это норма)
ОШИБКА  = (опционально, harden) Ready.status == False с reason ∈ {TargetFailed,CleanupFailed,Deleted}
          ИЛИ Expired.status == True  → немедленная ошибка (иначе висит до timeout)
ЖДАТЬ   = Completed != True / artifact отсутствует / Ready в pending-состоянии
```

Текущий CLI уже ждёт именно `Completed=True` + наличие `status.data.artifact`
(`volume.go:450-452`) и **не** читает cluster-scoped ресурсы — эта часть контракту уже
соответствует. **Опциональный harden** (не требование контракта, но закрывает реальный
hang-до-timeout, `volume.go:472-473`): добавить немедленную ошибку на терминальных
`Ready.reason`/`Expired=True` в post-upload ожидании. Terminal-reasons `DataImport`
частично free-form (VCR reason прокидывается в `TargetFailed`), поэтому проверять
**конкретные** значения из кода, а не суффикс.

### 5.C Удаление runtime-зависимости от `SnapshotContent` (не удаление типа, не рефактор scheme ради scheme)

Цель — запретить `SnapshotContent` в **runtime-пути** CLI:

- убрать все `Get`/`List`/`watch` cluster-scoped `snapshotcontents` и требование
  соответствующего RBAC;
- **само присутствие Go-типа `SnapshotContent` в scheme нарушением НЕ считается.**
  `snapshotapi.AddToScheme` трогать только если после рефактора он реально больше не нужен;
  не заводить узкий scheme-builder ради удаления одного типа;
- тип `SnapshotContent` в `api/v1alpha1` **не удалять** (dead-code cleanup — отдельно).

**Нормативный критерий — поведенческий:** fake client настроен так, что **любой** запрос
к cluster-scoped `snapshotcontents` (Get/List/watch) проваливает тест. Вспомогательный
guard (не доказательство): `rg 'snapshotContentGVR|Resource\([^)]*snapshotcontents|Get\(.*SnapshotContent' internal/snapshot cmd`
пусто. (Название закономерно останется в `boundSnapshotContentName`, docs, тестах
отсутствия доступа и в `VolumeSnapshotContent` — это другой, всё ещё нужный data-артефакт.)
Дополнительно: если в `deckhouse-cli` есть RBAC/манифесты — убедиться, что cluster
get/list/watch на `snapshotcontents` больше не требуется.

---

## 6. Тестовые критерии 2b (happy-path + отрицательные)

Fake client проваливает тест на **любом** запросе к cluster-scoped `snapshotcontents` (№7).

1. `status.data` отсутствует у агрегатора — допустимо (дерево строится).
2. data-путь получает узел без `status.data` (`RequireNodeData`) — явная ошибка;
   manifest-only узел без data — допустим.
3. malformed `status.sourceRef` — явная ошибка с GVK/name узла.
4. malformed `status.data.artifact` — явная ошибка.
5. Ожидание узла:
   - `Ready=True` → успех;
   - `phase=Failed` (капчур) → немедленная ошибка (reason домена показан);
   - `Ready=False` + terminal reason из enum → немедленная ошибка;
   - `Ready=False` + нетерминальный reason (`ImportPending`/`ChildrenPending`/…) → ждём;
   - import-лист `Ready=False` + `DataImportAmbiguous`/`DataArtifactInvalid` → ошибка.
6. root `Ready=True` — полный импорт завершается без чтения `SnapshotContent` и без
   отдельного per-leaf ожидания.
7. любой Get/List/watch cluster-scoped `snapshotcontents` → тест падает.
8. доменный источник ≠ data-источник: `status.sourceRef=DemoVirtualDisk`,
   `status.data.source=PVC` — узел сохраняет оба раздельно.
9. extended `VolumeSnapshot`: `status.sourceRef=PVC`, `status.data.source=PVC`,
   `status.data.artifact=VolumeSnapshotContent` — собирается из namespaced status.
10. `DataImport PopulateData`: успех = `Completed=True` + `status.data.artifact`;
    `Ready=False` при `Completed=True` — НЕ ошибка; (harden) терминальный `Ready.reason`/
    `Expired=True` → немедленная ошибка.
11. `CanonicalSnapshotIdentity`/`CanonicalSourceIdentity`/`ArchiveNodeDirName` детерминированы
    между запусками (стабильность нового формата; совместимость со старым не проверяется — §3.4).
12. `ArchiveNodeDirName` — path-safe: результат не содержит `\x00` и `/` (валидный
    path-компонент); при коллизии базовых имён каталоги различимы (discriminator/hash);
    читаемая база сохраняется. Raw `CanonicalSnapshotIdentity` в путь не попадает.

Плюс: `go build ./...`, `go test ./internal/snapshot/...` зелёные.

---

## Приложение A. Установленный кодом контракт готовности (read-only, ссылки)

Источник: анализ `state-snapshotter` и `storage-foundation`. Код не менялся.

**A.0 Общее.** `Ready` — единственное пользовательское условие на snapshot-CR
(`api/storage/v1alpha1/conditions.go:24-33`). SDK **никогда** не пишет `Ready`
(`pkg/snapshotsdk/adapter.go:32-35`); ядро (`SnapshotContentController`) вычисляет `Ready`
на `SnapshotContent` и зеркалит на namespaced CR (`.../snapshotcontent/ready_mirror.go:61-232`),
с барьером-2 (держит `Ready=False/ChildrenPending`, пока домен `phase != Finished`) и
bubble `phase=Failed`. `Ready=False` используется и для «в процессе», и для терминала —
различие в `reason`/`phase`. `Ready=Unknown` на агрегатном `Ready` не ставится.

**A.1 Терминальные reason (частичный нормативный enum).**
`TerminalReadyReasons`/`IsReasonTerminal` — `api/storage/v1alpha1/conditions.go:85-102`:
`ListFailed`, `ManifestCheckpointFailed`, `NamespaceNotFound`, `VolumeCaptureFailed`,
`DuplicateCoveredPVCUID`, `ChildrenFailed`, `GraphPlanningFailed`, `CreateChildFailed`,
`ChildSnapshotLost`.
Терминальны, но **вне** enum: любой `captureState.domainSpecificController.phase == Failed`
(reason домена free-form, `capture_state_types.go:122-124`, `conditions.go:83-84`);
import-лист `DataImportAmbiguous`, `DataArtifactInvalid` (`genericbinder/import.go:188-211`).
Полного закрытого enum терминальных причин **нет** → CLI полагается на (enum ∪ phase=Failed
∪ import-лист терминалы), а не на имена.

**A.2 Нетерминальные (ждать).** `ImportPending` (`import_pending.go:62-66`),
`ContentBindingPending`, `ContentMissing`, `ManifestCapturePending`, `DataCapturePending`,
`ChildrenPending`, `ChildrenLinkPending`, `SubtreeManifestCapturePending`,
`ChildSnapshotDeleted` (явно non-terminal) — из `deriveReadyStatus`
(`.../snapshotcontent/controller.go:774-814`) и mirror.

**A.3 Рекурсивность root Ready.** `ChildrenReady` требует `Ready=True` у каждого
привязанного дочернего `SnapshotContent`; терминальный ребёнок → `ChildrenFailed` у
родителя (`.../snapshotcontent/controller.go:1101-1207`, деривация `774-814`). Значит
root `Snapshot.Ready=True` рекурсивно гарантирует готовность всего дерева контента →
per-leaf ожидание при полном импорте избыточно. (Домен-лист `Ready` отстаёт из-за
барьера-2, `ready_mirror.go:118-131`, поэтому его как гейт полного импорта использовать
нельзя.)

**A.4 `DataImport mode=PopulateData` (`storage-foundation`).** Успех =
`conditions[Completed]=True` **и** `status.data.artifact` (оба ставятся вместе:
`images/data-manager-controller/.../data-import/data_import_resource.go:591-598`,
идемпотентно `243-252`). `Ready` после завершения остаётся `False` — норма
(`updateReadiness` прекращает вести `Ready` после `UploadFinished=True`, ~`842-843`).
Терминалы: `Ready=False` reason `TargetFailed`/`CleanupFailed`/`Deleted`
(`200-211`,`198-199`,`119-124`), `Expired=True` (ставит importer-под, `data-exporter/.../k8s.go:225-230`).
VCR-reason, попадающий в `TargetFailed`, — free-form → проверять конкретные значения.
Текущий CLI: успех по `Completed`+artifact (`volume.go:450-452`), pre-upload по
`Ready=True`+`url`+`volumeMode` (`volume.go:423`); post-upload не проверяет терминалы
(`volume.go:472-473`) — отсюда опциональный harden в §5.B.

---

## 7. Разрешённые вопросы и остаточные риски

**Разрешено (контракт/код):**
- VS/orphan-лист без `SnapshotContent` — namespaced `status.data` (2.md 1320-1339).
- Готовность — namespaced `Ready` + phase (§Приложение A); клиентские
  `ManifestsReady/DataReady/ChildrenReady` не читаем.
- Терминальность — код-подтверждена (§Приложение A.1), не эвристика.

**Остаточные риски:**
- **resume-детерминизм** (не совместимость со старым, §3.4): единые
  `CanonicalSnapshotIdentity`/`CanonicalSourceIdentity` + unit-тест стабильности.
- **Наличие `status.data`/`status.sourceRef`/терминальных сигналов на живых объектах**
  зависит от версии ядра. Пишем по 2.md; проверка на живых CRD — этап 3.
- **Частичность enum терминальных причин**: домен-reason free-form. Митигируется тем, что
  `phase=Failed` — самостоятельный нормативный терминальный сигнал; enum используем как
  дополнение, неизвестный нетерминальный reason → ждём до timeout (безопасно).

---

## 8. Что НЕ трогаем (отложено)

- `snapshot.yaml` → `kind: ExportedSnapshot` (Фаза 4).
- Standalone `d8 data import` (Mode B) на группу `storage-foundation.deckhouse.io`.
- Восстановление из «корзины».
- Удаление типа `SnapshotContent` из `api/v1alpha1` (dead-code cleanup — отдельно).
- Этап 3 (кросс-сборка `linux/amd64`, доставка `d8`, e2e `download → upload(import) → restore`).

---

## 9. Порядок работ и ревью

0. **Ревью этого документа (ты).**
1. Шаг 0 (§3): модель `Node` + `UID`; три сущности идентичности
   `CanonicalSnapshotIdentity` (ключ) / `ArchiveNodeDirName` (path-safe имя) /
   `CanonicalSourceIdentity` (provenance); политика совместимости; различие
   `sourceRef`/`data.source`; `ParseNodeStatus` fail-closed; `RequireNodeData`.
2. Реализовать **2a** → тесты 2a → коммит (без push).
3. `ParseNodeStatus` + unit-тесты.
4. Перевести `source/tree.go` на namespaced чтение.
5. Downstream на новую модель `Node` (напрямую/adapter); resume-ключ — по
   `CanonicalSnapshotIdentity`, имя каталога — по `ArchiveNodeDirName`.
6. Import state machine (§5.B) — алгоритм готовности + топология; удалить content-wait.
7. Убрать runtime Get/List/watch + RBAC-зависимость от `SnapshotContent` (§5.C).
8. Только затем — dead-code cleanup (вне этого этапа).

## 10. Реализация: факт-корректировки (по коду, ред. 3.1)

Уточнения, выявленные при реализации Шага 0 + 2a (сам контракт `docs/2026-06-29-unified-snapshots-overview.md` неизменен):

- **`status.data` — `source`, а не `target`.** Реальный тип
  `state-snapshotter api/storage/v1alpha1.SnapshotDataBinding` использует `source`
  (`SnapshotSubjectRef`, uid внутри source) и живёт на `SnapshotContent.status.data`
  (json `data`), а не `dataRef`. Локальный CLI-тип
  `internal/snapshot/api/v1alpha1.SnapshotDataBinding` (`target`/`targetUID`/`dataRef`)
  **устарел** — §1.2 «1:1» было неточным.
  Поэтому `ParseNodeStatus` в Шаге 0 введён с корректным self-contained типом
  `source.NodeData` (`source`/`artifact`/…, поле `Source` переиспользует
  `SourceRefIdentity`), аддитивно, не трогая устаревший тип и downstream. Миграция
  `tree.go`/downstream на `NodeData` и удаление устаревшего типа — в **2b**.
- **`status.sourceRef` = `SnapshotSourceObjectRef`** `{apiVersion,kind,name,namespace,uid}`
  — совпадает с CLI `SourceRefIdentity`. `uid` **нормативен и обязателен** в `ParseNodeStatus`:
  все YAML-примеры `status.sourceRef` в `2.md` (стр. 61-66, 95-99, 169-174, 236-241, 301-306,
  769-774) содержат `uid` (и `namespace`), в отличие от облегчённого `spec.sourceRef`
  `{apiVersion,kind,name}`. Полнота data-ноги (`status.data.source.uid`, artifact) проверяется
  отдельно в `RequireNodeData` на Ready-узле.
- **Layout каталогов архива не меняется в 0/2a.** Текущая схема (инвентаризация):
  `archive.NodeDirName(kind,name)` = `<kindlower>_<name>` (`names.go:166`), где `name` =
  `node.SourceName` c fallback `node.Name` (`pipeline.go:673-679`, `1266-1268`); коллизии —
  суффикс `__<short>` (`writer.go:37`); корень — пользовательский путь (`ScanAbsolute`).
  Новый `source.ArchiveNodeDirName` (`<kindlower>-<name>-<hash8>`) **расходится** с этой схемой
  и пока **никуда не подключён**. Подключать/выравнивать его — это решение по сохранению layout,
  которое принимается на ревью **перед 2b** (варианты: сохранить `<kindlower>_<name>` +
  `__<short>` как есть, либо перейти на identity-based имя).
- **`Node.UID` и `Node.Data`** добавлены аддитивно (legacy `OwnDataRefs`/`Binding` остаются
  до 2b, где заменяются на `Data`).
- **`VolumesReady → DataReady` перенесено в 2b.** Константа `condVolumesReady` и
  `waitSnapshotContentReady` завязаны на чтение `SnapshotContent`, которое удаляется в 2b;
  переименование условия делается там же вместе с удалением content-wait, чтобы не плодить
  churn. 2a ограничен `spec.mode` + удалением `--snapshot-class`.
