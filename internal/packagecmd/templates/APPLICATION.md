# Шаблоны чарта пакета-приложения (`templates/`)

Справочник по каталогу `templates/` пакета типа `Application`: что в нём лежит, как заполнять
манифесты, какие конструкции шаблонизатора использовать и какие хелперы доступны.

Каталог `templates/` — это **Helm-совместимый чарт**. При сборке и развёртывании его рендерит движок
`nelm`/`delivery-kit`: платформа подставляет реальные значения (`.Application`, `.Platform`,
`.Capabilities`), доступны функции [Sprig](http://masterminds.github.io/sprig/) и `include`.
Посмотреть результат локально: `package render` (см. [в конце](#предпросмотр)).

> Для модулей набор контекста и хелперов другой — см. [MODULE.md](MODULE.md).

---

## Оглавление

- [Что лежит в `templates/`](#что-лежит-в-templates)
- [Контекст: какие данные доступны](#контекст-какие-данные-доступны)
- [Конструкции шаблонизатора](#конструкции-шаблонизатора)
  - [Базовый синтаксис Go template](#базовый-синтаксис-go-template)
  - [Функции Sprig](#функции-sprig)
- [Манифесты](#манифесты)
- [Хелперы `_helpers/*.tpl`](#хелперы-_helperstpl)
- [Предпросмотр](#предпросмотр)

---

## Что лежит в `templates/`

```
templates/
├── deployment.yaml        # Deployment приложения
├── service.yaml           # ClusterIP-сервис
├── pdb.yaml               # PodDisruptionBudget
├── vpa.yaml               # VerticalPodAutoscaler
├── registry-secret.yaml   # секрет доступа к реестру образов
└── _helpers/              # хелперы — вызываются через include, сами ресурсов не создают
    ├── quantity_bytes.tpl
    ├── bytes_quantity.tpl
    └── public_domain.tpl
```

Каждый `*.yaml` рендерится в Kubernetes-манифест. Файлы в `_helpers/` (по соглашению с префиксом `_`)
не дают собственных ресурсов — это библиотека именованных шаблонов для переиспользования.

Ключевая идея приложения — **мультиэкземплярность**: все имена и неймспейсы строятся от
`.Application.Instance`, чтобы несколько установок пакета в кластере не конфликтовали.

---

## Контекст: какие данные доступны

Внутри манифестов `.` — это корневой контекст со значениями, которые подставляет платформа:

| Выражение | Что это |
|-----------|---------|
| `.Application.Instance.Name` | имя конкретного экземпляра приложения |
| `.Application.Instance.Namespace` | неймспейс экземпляра |
| `.Application.Settings.<x>` | пользовательские настройки из `openapi/settings.yaml` (`.replicas`, `.msg`, …) |
| `.Application.Package.Images` | map «имя образа → ссылка»; доступ через `index … "<name>"` |
| `.Application.Package.Registry.dockercfg` | dockerconfig для секрета доступа к реестру |
| `.Platform.applications.publicDomainTemplate` | шаблон публичного FQDN (для хелпера `public_domain`) |
| `.Capabilities.APIVersions.Has "<gvk>"` | есть ли данный API/CRD в кластере |

> Поля `.Application.Settings.*` берутся из схемы `openapi/settings.yaml`: что объявлено там —
> то и доступно здесь. Внутренние значения (не задаваемые пользователем) лежат в `.Application`
> согласно `openapi/values.yaml`.

---

## Конструкции шаблонизатора

### Базовый синтаксис Go template

| Конструкция | Назначение | Пример |
|-------------|-----------|--------|
| `{{ expr }}` | подставить значение | `name: {{ .Application.Instance.Name }}` |
| `\| pipe` | передать значение в функцию | `{{ .Application.Settings.msg \| quote }}` |
| `{{- ... -}}` | обрезать пробелы/перевод строки слева/справа | `{{- if … }}` |
| `{{ $x := expr }}` | объявить переменную | `{{ $mem := include "quantity_bytes" "70Mi" \| int64 }}` |
| `{{ if … }}…{{ else }}…{{ end }}` | условие | см. `vpa.yaml` |
| `{{ range $i, $v := list }}…{{ end }}` | цикл | перебор коллекций |
| `{{ with expr }}…{{ end }}` | сменить `.` на expr в блоке | работа с вложенным объектом |
| `{{ define "name" }}…{{ end }}` | объявить именованный шаблон | в `_helpers/*.tpl` |
| `{{ include "name" arg }}` | вызвать именованный шаблон/хелпер | `{{ include "quantity_bytes" "50Mi" }}` |
| `{{ fail "msg" }}` | прервать рендер с ошибкой | валидация входных данных |

Практические правила:

- **Отступы в YAML** делайте через `nindent` / `indent`, а не пробелами в шаблоне:
  `{{- include "server_resources" . | nindent 14 }}` — вставит блок с отступом 14 и переносом строки.
- **`| quote`** для строковых значений, попадающих в YAML (`args`, `env.value`), чтобы не сломать
  синтаксис спецсимволами.
- **`| default`** страхует от пустых значений: `{{ .Application.Settings.replicas | default 1 }}`.
- **`{{- -}}`** используйте, чтобы условные блоки не оставляли пустых строк в выводе.

### Функции Sprig

Часто используемые в этих чартах (полный список — в [документации Sprig](http://masterminds.github.io/sprig/)):

| Функция | Назначение |
|---------|-----------|
| `default d x` | значение по умолчанию, если `x` пуст |
| `quote` / `squote` | обернуть в двойные/одинарные кавычки |
| `index m "k"` | доступ к элементу map/списка по ключу/индексу |
| `nindent n` / `indent n` | отступ блока (с переносом строки / без) |
| `toString` / `atoi` | преобразование в строку / в int |
| `int64` / `float64` | приведение к числовым типам |
| `mul` / `div` / `mod` | целочисленная арифметика |
| `mulf` / `divf` / `addf` | арифметика с плавающей точкой |
| `floor` / `ceil` / `round` | округление |
| `hasSuffix` / `trimSuffix` / `contains` / `trim` | работа со строками |
| `dict "k" v …` / `list a b …` | собрать map / список (для передачи в хелперы) |

---

## Манифесты

Эталон — [deployment.yaml](application/templates/deployment.yaml):

```yaml
{{- define "server_resources" }}          # локальный именованный шаблон
cpu: 30m
memory: 70Mi
{{- end }}

apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Application.Instance.Name }}-server      # имя привязано к экземпляру
spec:
  replicas: {{ .Application.Settings.replicas | default 1 }}   # значение из openapi/settings
  ...
      containers:
        - name: server
          image: {{ index .Application.Package.Images "echo" }}   # образ пакета по имени
          args:
            - -text={{ .Application.Settings.msg | default "hello world" | quote }}
          env:
            - name: EPHEMERAL_STORAGE_BYTES
              value: {{ include "quantity_bytes" "50Mi" | quote }}     # хелпер, см. ниже
            {{- $memBytes := include "quantity_bytes" "70Mi" | int64 }}
            {{- $cacheBytes := mulf (float64 $memBytes) 0.5 | floor | int64 }}
            - name: CACHE_MAX_BYTES
              value: {{ $cacheBytes | quote }}
            - name: CACHE_MAX_SIZE
              value: {{ include "bytes_quantity" $cacheBytes | quote }}  # обратный хелпер
          resources:
            requests:
              ephemeral-storage: 50Mi
              {{- include "server_resources" . | nindent 14 }}
```

Что важно и почему:

- **`{{ .Application.Instance.Name }}-server`** — единый префикс имён на весь пакет (его же
  используют `service.yaml`, `pdb.yaml`, `vpa.yaml`), чтобы селекторы совпадали, а два экземпляра не
  пересекались. Префикс экземпляра проверяет линтер `templates`.
- **`index .Application.Package.Images "echo"`** — ссылка на образ по имени; ключ `echo`
  соответствует каталогу `images/echo/`.
- **`securityContext`** прописан явно (`runAsNonRoot`, `readOnlyRootFilesystem`, `drop: ALL`).
  В приложении нет `helm_lib_*`, поэтому политики безопасности задаются руками — не удаляйте их.
- **`resources.requests`** обязателен; Deployment должен покрываться PDB и VPA (линтер `templates`:
  правила `pdb`, `vpa`).

Остальные манифесты:

| Файл | Назначение | На что обратить внимание |
|------|-----------|--------------------------|
| [service.yaml](application/templates/service.yaml) | ClusterIP-сервис | `targetPort: http` — **именованный** порт (правило `service-port` запрещает числовые) |
| [pdb.yaml](application/templates/pdb.yaml) | PodDisruptionBudget | селектор совпадает с Deployment; обязателен |
| [vpa.yaml](application/templates/vpa.yaml) | VerticalPodAutoscaler | обёрнут в `{{ if .Capabilities.APIVersions.Has … }}` — рендерится только при наличии CRD; задайте `minAllowed`/`maxAllowed` |
| [registry-secret.yaml](application/templates/registry-secret.yaml) | dockerconfig для pull образов | `.Application.Package.Registry.dockercfg` подставляется платформой |

---

## Хелперы `_helpers/*.tpl`

Хелперы — именованные шаблоны (`{{ define "name" }}…{{ end }}`), вызываемые через
`{{ include "name" аргумент }}`. Аргумент передаётся один; чтобы отдать несколько значений,
собирают `list` или `dict`. В приложении доступны три хелпера.

### `quantity_bytes` — quantity → байты

[quantity_bytes.tpl](application/templates/_helpers/quantity_bytes.tpl) превращает
Kubernetes-quantity (строку) в целое число байт.

```yaml
{{ include "quantity_bytes" "2Gi" }}    # -> 2147483648
{{ include "quantity_bytes" "512Mi" }}  # -> 536870912
{{ include "quantity_bytes" "1G" }}     # -> 1000000000  (десятичный суффикс)
{{ include "quantity_bytes" "1024" }}   # -> 1024        (без суффикса)
```

- Поддерживает бинарные суффиксы (`Ki`,`Mi`,`Gi`,`Ti`,`Pi`,`Ei`), десятичные SI (`k`,`M`,`G`,`T`,`P`,`E`)
  и голые числа. Целые мантиссы умножаются точно, дробные (`1.5Gi`) — через `float` с округлением вниз.
- **Для чего:** приложению внутри контейнера нужно число байт (лимит буфера, размер кэша), а в
  манифесте удобнее писать `50Mi`. Хелпер убирает ручной пересчёт и рассинхрон.

### `bytes_quantity` — байты → quantity (обратный)

[bytes_quantity.tpl](application/templates/_helpers/bytes_quantity.tpl) — инверсия предыдущего.

```yaml
{{ include "bytes_quantity" 536870912 }}                                # -> 512Mi
{{ include "bytes_quantity" 2147483648 }}                               # -> 2Gi
{{ include "bytes_quantity" (dict "bytes" 1000000 "base" "decimal") }}  # -> 1M
{{ include "bytes_quantity" 500 }}                                      # -> 500
```

- Выбирает наибольший суффикс, на который байты делятся нацело; если ровно не делится — наибольший
  подходящий с двумя знаками (`1.5Gi`). По умолчанию бинарные суффиксы; для SI передайте
  `dict "bytes" N "base" "decimal"`.
- **Для чего:** посчитали что-то в байтах (например, «половина запрошенной памяти под кэш») и хотите
  вернуть значение в опрятную quantity. См. round-trip в `deployment.yaml` (`CACHE_MAX_BYTES` /
  `CACHE_MAX_SIZE`).

### `public_domain` — публичный FQDN приложения

[public_domain.tpl](application/templates/_helpers/public_domain.tpl) собирает публичный домен
экземпляра из шаблона платформы.

```yaml
{{ include "public_domain" (list . "server") }}
```

- Аргумент — `list`: первый элемент контекст (`.`), второй — имя компонента.
- Берёт `.Platform.applications.publicDomainTemplate` и подставляет три `%s` **строго в порядке**:
  компонент, `.Application.Instance.Name`, `.Application.Instance.Namespace`.
- Если в шаблоне платформы не ровно три `%s` — прерывает рендер понятной ошибкой (`fail`).
- **Для чего:** единообразно строить внешний адрес приложения (Ingress/ссылки), не хардкодя
  доменную схему в каждом манифесте.


---

## Предпросмотр

Отрендерить чарт с подставными значениями и увидеть итоговые манифесты:

```bash
cd my-app
package render                                  # все манифесты в stdout
package render --file deployment.yaml           # только один шаблон
package render | kubectl apply --dry-run=client -f -   # синтаксическая проверка
```

Контракты шаблонов (мультиэкземплярность, PDB/VPA, именованные порты) проверяет `package verify`.
Подробности по командам — в корневом [README.md](../README.md).
