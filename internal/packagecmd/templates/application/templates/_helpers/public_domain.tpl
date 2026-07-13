{{- /* Usage: {{ include "public_domain" (list . "<component>") }} */ -}}
{{- /* returns rendered publicDomainTemplate as the application public fqdn */ -}}
{{- /* verbs order: "%s" component, "%s" instance name, "%s" instance namespace */ -}}
{{- define "public_domain" }}
  {{- $context   := index . 0 -}} {{- /* Template context with .Application, .Platform, etc */ -}}
  {{- $component := index . 1 -}} {{- /* Component name portion */ -}}

  {{- $template := $context.Platform.applications.publicDomainTemplate -}}
  {{- if ne (int (sub (len (splitList "%s" $template)) 1)) 3 }}
    {{ fail "Error!!! Platform.applications.publicDomainTemplate must contain exactly three \"%s\" patterns (component, instance name, instance namespace) to render application fqdn!" }}
  {{- end }}
  {{- printf $template $component $context.Application.Instance.Name $context.Application.Instance.Namespace -}}
{{- end }}
