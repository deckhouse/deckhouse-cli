{{- /* Usage: {{ include "public_domain" (list . "<component>(optional)") }} */ -}}
{{- /* returns rendered publicDomainTemplate as the application public fqdn */ -}}
{{- /* verbs order: component, "%s" instance name, "%s" instance namespace */ -}}
{{- define "public_domain" }}
  {{- $context   := index . 0 -}} {{- /* Template context with .Application, .Platform, etc */ -}}
  {{- $component := "" -}}
  {{- if ge (len .) 2 -}}
    {{- $component = index . 1 -}} {{- /* Component name portion */ -}}
  {{- end -}}

  {{- $template := $context.Platform.applications.publicDomainTemplate -}}
  {{- if ne (int (sub (len (splitList "%s" $template)) 1)) 2 }}
    {{ fail "Error!!! Platform.applications.publicDomainTemplate must contain exactly two \"%s\" patterns (instance name, instance namespace) to render application fqdn!" }}
  {{- end }}

  {{- $fqdn := printf $template $context.Application.Instance.Name $context.Application.Instance.Namespace -}}

{{- /* if $component is present, return the component.fqdn */ -}}
  {{- if $component }}
    {{ printf "%s.%s" $component $fqdn }}
  {{- else -}}
    {{ print $fqdn }}
  {{- end -}}
{{- end }}
