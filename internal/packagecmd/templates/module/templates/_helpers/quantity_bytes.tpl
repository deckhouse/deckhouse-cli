{{- /*
quantity_bytes converts a Kubernetes quantity string into an integer number of bytes.

It accepts binary suffixes (Ki, Mi, Gi, Ti, Pi, Ei), decimal SI suffixes
(k, M, G, T, P, E) and plain numbers without a suffix. Integer mantissas are
multiplied exactly, fractional ones (e.g. "1.5Gi") fall back to float math.

Usage:
  {{ include "quantity_bytes" "2Gi" }}    -> 2147483648
  {{ include "quantity_bytes" "512Mi" }}  -> 536870912
  {{ include "quantity_bytes" "1G" }}     -> 1000000000
  {{ include "quantity_bytes" "1024" }}   -> 1024
*/ -}}
{{- define "quantity_bytes" -}}
{{- $q := . | toString | trim -}}
{{- $units := list
  (dict "suffix" "Ki" "factor" 1024)
  (dict "suffix" "Mi" "factor" 1048576)
  (dict "suffix" "Gi" "factor" 1073741824)
  (dict "suffix" "Ti" "factor" 1099511627776)
  (dict "suffix" "Pi" "factor" 1125899906842624)
  (dict "suffix" "Ei" "factor" 1152921504606846976)
  (dict "suffix" "k"  "factor" 1000)
  (dict "suffix" "M"  "factor" 1000000)
  (dict "suffix" "G"  "factor" 1000000000)
  (dict "suffix" "T"  "factor" 1000000000000)
  (dict "suffix" "P"  "factor" 1000000000000000)
  (dict "suffix" "E"  "factor" 1000000000000000000)
-}}
{{- $bytes := "" -}}
{{- range $unit := $units -}}
  {{- if and (eq $bytes "") (hasSuffix $unit.suffix $q) -}}
    {{- $num := trimSuffix $unit.suffix $q -}}
    {{- if contains "." $num -}}
      {{- $bytes = mulf (float64 $num) (float64 $unit.factor) | floor | int64 | toString -}}
    {{- else -}}
      {{- $bytes = mul (atoi $num) $unit.factor | toString -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- if eq $bytes "" -}}
  {{- if contains "." $q -}}
    {{- $bytes = $q | float64 | floor | int64 | toString -}}
  {{- else -}}
    {{- $bytes = $q | atoi | toString -}}
  {{- end -}}
{{- end -}}
{{- $bytes -}}
{{- end -}}
