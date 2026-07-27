{{- /* Usage: {{ include "ingress_class" . }} */ -}}
{{- /* returns the IngressClass name configured for Deckhouse ecosystem applications */ -}}
{{- define "ingress_class" -}}
  {{- $context := . -}} {{- /* Template context with .Application, .Platform, etc */ -}}
  {{- $context.Platform.applications.ingressClass -}}
{{- end -}}
