{{- /*
bytes_quantity converts an integer number of bytes back into a Kubernetes
quantity string. It is the inverse of quantity_bytes.

It picks the largest unit that divides the byte count exactly so the result
stays an integer (e.g. 536870912 -> "512Mi"). When no unit divides evenly it
falls back to the largest fitting unit with two decimals (e.g. "1.5Gi"). Values
smaller than the smallest unit are emitted as a plain number.

By default binary suffixes (Ki, Mi, Gi, Ti, Pi, Ei) are used. Pass a dict with
"base" set to "decimal" to use SI suffixes (k, M, G, T, P, E) instead.

Usage:
  {{ include "bytes_quantity" 536870912 }}                              -> 512Mi
  {{ include "bytes_quantity" 2147483648 }}                             -> 2Gi
  {{ include "bytes_quantity" (dict "bytes" 1000000 "base" "decimal") }} -> 1M
  {{ include "bytes_quantity" 1024 }}                                   -> 1Ki
  {{ include "bytes_quantity" 500 }}                                    -> 500
*/ -}}
{{- define "bytes_quantity" -}}
{{- $bytes := 0 -}}
{{- $base := "binary" -}}
{{- if kindIs "map" . -}}
  {{- $bytes = .bytes | int64 -}}
  {{- $base = .base | default "binary" -}}
{{- else -}}
  {{- $bytes = . | int64 -}}
{{- end -}}
{{- $units := list
  (dict "suffix" "Ei" "factor" 1152921504606846976)
  (dict "suffix" "Pi" "factor" 1125899906842624)
  (dict "suffix" "Ti" "factor" 1099511627776)
  (dict "suffix" "Gi" "factor" 1073741824)
  (dict "suffix" "Mi" "factor" 1048576)
  (dict "suffix" "Ki" "factor" 1024)
-}}
{{- if eq $base "decimal" -}}
  {{- $units = list
    (dict "suffix" "E" "factor" 1000000000000000000)
    (dict "suffix" "P" "factor" 1000000000000000)
    (dict "suffix" "T" "factor" 1000000000000)
    (dict "suffix" "G" "factor" 1000000000)
    (dict "suffix" "M" "factor" 1000000)
    (dict "suffix" "k" "factor" 1000)
  -}}
{{- end -}}
{{- $out := "" -}}
{{- range $unit := $units -}}
  {{- if and (eq $out "") (ge $bytes $unit.factor) (eq (mul (div $bytes $unit.factor) $unit.factor) $bytes) -}}
    {{- $out = printf "%d%s" (div $bytes $unit.factor) $unit.suffix -}}
  {{- end -}}
{{- end -}}
{{- if eq $out "" -}}
  {{- range $unit := $units -}}
    {{- if and (eq $out "") (ge $bytes $unit.factor) -}}
      {{- $out = printf "%v%s" (round (divf (float64 $bytes) (float64 $unit.factor)) 2) $unit.suffix -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- if eq $out "" -}}
  {{- $out = printf "%d" $bytes -}}
{{- end -}}
{{- $out -}}
{{- end -}}
