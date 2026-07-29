# Шаблоны чарта пакета-модуля (`templates/`)

Справочник по каталогу `templates/` пакета типа `Module`: что в нём лежит, как заполнять манифесты,
какие конструкции шаблонизатора использовать и какие хелперы доступны.

Каталог `templates/` — это **Helm-совместимый чарт**. При сборке и развёртывании его рендерит движок
`nelm`/`delivery-kit`: платформа подставляет глобальные `.Values`, подключается библиотека хелперов
`helm_lib_*` (чарт `deckhouse_lib_helm` в `charts/`), доступны функции
[Sprig](http://masterminds.github.io/sprig/) и `include`. Посмотреть результат локально:
`package render` (см. [в конце](#предпросмотр)).

> Модуль — это полноценный Deckhouse-модуль: глобальные `.Values`, библиотека `helm_lib_*`, обычно
> один экземпляр в кластере. Приложение устроено иначе (контекст `.Application`, мультиэкземплярность)
> — см. [APPLICATION.md](APPLICATION.md).

---

## Оглавление

- [Что лежит в `templates/`](#что-лежит-в-templates)
- [Контекст: какие данные доступны](#контекст-какие-данные-доступны)
- [Конструкции шаблонизатора](#конструкции-шаблонизатора)
  - [Базовый синтаксис Go template](#базовый-синтаксис-go-template)
  - [Функции Sprig](#функции-sprig)
- [Библиотека `helm_lib_*`](#библиотека-helm_lib_)
- [Манифесты](#манифесты)
- [Локальные хелперы `_helpers/*.tpl`](#локальные-хелперы-_helperstpl)
- [Предпросмотр](#предпросмотр)

---

## Что лежит в `templates/`

```
templates/
├── namespace.yaml         # неймспейс модуля
├── deployment.yaml        # Deployment
├── service.yaml           # ClusterIP-сервис
├── ingress.yaml           # внешний доступ (Ingress + TLS)
├── pdb.yaml               # PodDisruptionBudget
├── vpa.yaml               # VerticalPodAutoscaler
├── registry-secret.yaml   # секрет доступа к реестру образов
└── _helpers/              # локальные хелперы (в дополнение к helm_lib_*)
    ├── quantity_bytes.tpl
    └── bytes_quantity.tpl
```

Каждый `*.yaml` рендерится в Kubernetes-манифест. Файлы в `_helpers/` (по соглашению с префиксом `_`)
не дают собственных ресурсов — это библиотека именованных шаблонов. В отличие от приложения, модуль
активно использует **библиотеку `helm_lib_*`** для образов, лейблов, security-контекстов, доменов и TLS.

Ресурсы модуля привязаны к фиксированному неймспейсу (в шаблоне — `test`; замените на неймспейс
своего модуля).

---

## Контекст: какие данные доступны

Внутри манифестов `.` — корневой контекст со значениями, которые подставляет платформа:

| Выражение | Что это |
|-----------|---------|
| `.Values.<x>` | значения модуля из `openapi/settings.yaml` + `values.yaml` (`.replicas`, `.msg`, `.internal.*`) |
| `.Module.Package.Registry.dockercfg` | dockerconfig для секрета доступа к реестру |
| `.Capabilities.APIVersions.Has "<gvk>"` | есть ли данный API/CRD в кластере |

> Различие с приложением: в модуле пользовательские настройки читаются как `.Values.replicas`, а в
> приложении — как `.Application.Settings.replicas`. Что объявлено в `openapi/settings.yaml` — то и
> доступно в `.Values`; внутренние значения описываются в `openapi/values.yaml` (`.Values.internal.*`).

---

## Конструкции шаблонизатора

### Базовый синтаксис Go template

| Конструкция | Назначение | Пример |
|-------------|-----------|--------|
| `{{ expr }}` | подставить значение | `replicas: {{ .Values.replicas }}` |
| `\| pipe` | передать значение в функцию | `{{ .Values.msg \| quote }}` |
| `{{- ... -}}` | обрезать пробелы/перевод строки слева/справа | `{{- if … }}` |
| `{{ $x := expr }}` | объявить переменную | `{{ $mem := include "quantity_bytes" "70Mi" \| int64 }}` |
| `{{ if … }}…{{ else }}…{{ end }}` | условие | TLS-блок в `ingress.yaml` |
| `{{ range $i, $v := list }}…{{ end }}` | цикл | перебор коллекций |
| `{{ with expr }}…{{ end }}` | сменить `.` на expr в блоке | работа с вложенным объектом |
| `{{ define "name" }}…{{ end }}` | объявить именованный шаблон | в `_helpers/*.tpl` |
| `{{ include "name" arg }}` | вызвать шаблон/хелпер/функцию библиотеки | `{{ include "helm_lib_module_image" (list . "echo") }}` |
| `{{ fail "msg" }}` | прервать рендер с ошибкой | валидация входных данных |

Практические правила:

- **Отступы в YAML** — через `nindent` / `indent`, а не пробелами в шаблоне. Библиотечные хелперы
  почти всегда вставляются так: `{{- include "helm_lib_module_labels" (list . (dict "app" "server")) | nindent 2 }}`.
- **Аргументы библиотечных хелперов** — это `list`, где **первый элемент всегда контекст `.`**, далее
  доп. параметры (часто через `dict`).
- **`| quote`** для строковых значений в YAML; **`| default`** — от пустых значений.
- **`{{- -}}`** — чтобы условные блоки не оставляли пустых строк.

### Функции Sprig

Часто используемые (полный список — в [документации Sprig](http://masterminds.github.io/sprig/)):

| Функция | Назначение |
|---------|-----------|
| `default d x` | значение по умолчанию, если `x` пуст |
| `quote` / `squote` | обернуть в кавычки |
| `index m "k"` | доступ к элементу map/списка |
| `nindent n` / `indent n` | отступ блока (с переносом / без) |
| `toString` / `atoi` | преобразование в строку / int |
| `int64` / `float64` | приведение к числовым типам |
| `mul` / `div` / `mod` | целочисленная арифметика |
| `mulf` / `divf` / `addf` | арифметика с плавающей точкой |
| `floor` / `ceil` / `round` | округление |
| `hasSuffix` / `trimSuffix` / `contains` / `trim` | работа со строками |
| `dict "k" v …` / `list a b …` | собрать map / список (для передачи в хелперы) |

---

## Библиотека `helm_lib_*`

Файл `charts/deckhouse_lib_helm-*.tgz` — упакованная чарт-библиотека Deckhouse. Она подключается как
зависимость и даёт функции `helm_lib_*`, вызываемые через `include`. **Не распаковывайте и не
редактируйте** архив; для обновления замените `.tgz` на новую версию.

Функции, используемые в шаблонах модуля, и для чего они:

| Хелпер | Для чего |
|--------|----------|
| `helm_lib_module_labels` | стандартные лейблы ресурса модуля; аргумент `(list . (dict "app" "server"))` |
| `helm_lib_module_image` | ссылка на образ по имени; `(list . "echo")` |
| `helm_lib_module_public_domain` | публичный FQDN компонента; `(list . "server")` |
| `helm_lib_module_ingress_class` | класс Ingress модуля |
| `helm_lib_module_https_ingress_tls_enabled` | включён ли TLS для Ingress (для условия) |
| `helm_lib_module_https_secret_name` | имя TLS-секрета; `(list . "ingress-tls")` |
| `helm_lib_module_ephemeral_storage_only_logs` | requests ephemeral-storage под логи |
| `helm_lib_module_container_security_context_run_as_user_deckhouse_pss_restricted` | security-контекст контейнера по PSS |
| `helm_lib_module_pod_security_context_run_as_user_deckhouse` | security-контекст пода |

> Именно из-за библиотеки в модуле **нет** локального хелпера `public_domain` (как в приложении):
> его роль выполняет `helm_lib_module_public_domain`. Прежде чем писать свой хелпер, проверьте, нет
> ли готового `helm_lib_*`.

---

## Манифесты

Эталон — [deployment.yaml](module/templates/deployment.yaml):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: server
  namespace: test
  {{- include "helm_lib_module_labels" (list . (dict "app" "server")) | nindent 2 }}   # лейблы модуля
spec:
  replicas: {{ .Values.replicas | default 1 }}          # значение из openapi/settings
  ...
      containers:
        - name: server
          image: {{ include "helm_lib_module_image" (list . "echo") }}   # образ через библиотеку
          args:
            - -text={{ .Values.msg | default "hello world" | quote }}
          env:
            - name: EPHEMERAL_STORAGE_BYTES
              value: {{ include "quantity_bytes" "50Mi" | quote }}       # локальный хелпер, см. ниже
            {{- $memBytes := include "quantity_bytes" "70Mi" | int64 }}
            {{- $cacheBytes := mulf (float64 $memBytes) 0.5 | floor | int64 }}
            - name: CACHE_MAX_BYTES
              value: {{ $cacheBytes | quote }}
            - name: CACHE_MAX_SIZE
              value: {{ include "bytes_quantity" $cacheBytes | quote }}  # обратный хелпер
          resources:
            requests:
              {{- include "helm_lib_module_ephemeral_storage_only_logs" . | nindent 14 }}
          {{- include "helm_lib_module_container_security_context_run_as_user_deckhouse_pss_restricted" . | nindent 10 }}
          {{- "readOnlyRootFilesystem: true" | nindent 12 }}
      {{- include "helm_lib_module_pod_security_context_run_as_user_deckhouse" . | nindent 6 }}
```

Что важно и почему:

- **`helm_lib_module_labels`** и прочие библиотечные хелперы вызываются с контекстом `.` первым
  элементом `list`; вставляются через `nindent`, чтобы не сломать отступы YAML.
- **`helm_lib_module_image (list . "echo")`** — ссылка на образ по имени; ключ `echo` соответствует
  каталогу `images/echo/`. Библиотека сама подставит корректный путь/digest.
- **security-контексты** задаются хелперами (`…_container_security_context_…`,
  `…_pod_security_context_…`) — это соответствие стандартам PSS Deckhouse; не заменяйте их ручными
  блоками без причины.
- Deployment должен покрываться PDB и VPA (линтер `templates`: правила `pdb`, `vpa`).

Остальные манифесты:

| Файл | Назначение | На что обратить внимание |
|------|-----------|--------------------------|
| [namespace.yaml](module/templates/namespace.yaml) | неймспейс модуля | замените `test` на реальное имя; лейблы через `helm_lib_module_labels` |
| [service.yaml](module/templates/service.yaml) | ClusterIP-сервис | `targetPort: http` — **именованный** порт (правило `service-port`) |
| [ingress.yaml](module/templates/ingress.yaml) | внешний доступ | хост — `helm_lib_module_public_domain`, класс — `helm_lib_module_ingress_class`, TLS — блок `{{ if (include "helm_lib_module_https_ingress_tls_enabled" .) }}` |
| [pdb.yaml](module/templates/pdb.yaml) | PodDisruptionBudget | селектор совпадает с Deployment; обязателен |
| [vpa.yaml](module/templates/vpa.yaml) | VerticalPodAutoscaler | задайте `minAllowed`/`maxAllowed` |
| [registry-secret.yaml](module/templates/registry-secret.yaml) | dockerconfig для pull образов | `.Module.Package.Registry.dockercfg` подставляется платформой |

---

## Локальные хелперы `_helpers/*.tpl`

Локальные хелперы — именованные шаблоны пакета в дополнение к библиотеке `helm_lib_*`. Вызываются
через `{{ include "name" аргумент }}`. В модуле их два (домены/TLS/образы закрывает библиотека,
поэтому `public_domain` здесь не нужен).

### `quantity_bytes` — quantity → байты

[quantity_bytes.tpl](module/templates/_helpers/quantity_bytes.tpl) превращает Kubernetes-quantity
(строку) в целое число байт.

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

[bytes_quantity.tpl](module/templates/_helpers/bytes_quantity.tpl) — инверсия предыдущего.

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


---

## Предпросмотр

Отрендерить чарт с подставными значениями и увидеть итоговые манифесты:

```bash
cd my-module
package render                                  # все манифесты в stdout
package render --file deployment.yaml           # только один шаблон
package render | kubectl apply --dry-run=client -f -   # синтаксическая проверка
```

Контракты шаблонов (PDB/VPA, именованные порты) проверяет `package verify`. Подробности по командам —
в корневом [README.md](../README.md).
