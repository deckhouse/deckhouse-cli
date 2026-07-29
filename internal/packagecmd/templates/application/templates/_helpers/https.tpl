{{- /* Usage: {{ include "https_mode" . }} */ -}}
{{- /* returns the HTTPS mode configured for Deckhouse ecosystem applications: */ -}}
{{- /* Disabled | CertManager | CustomCertificate | OnlyInURI */ -}}
{{- define "https_mode" -}}
  {{- $context := . -}} {{- /* Template context with .Application, .Platform, etc */ -}}
  {{- $context.Platform.applications.https.mode -}}
{{- end -}}

{{- /* Usage: {{ if (include "https_ingress_tls_enabled" .) }} */ -}}
{{- /* returns a non-empty string when TLS should be enabled on the application Ingress */ -}}
{{- define "https_ingress_tls_enabled" -}}
  {{- $context := . -}} {{- /* Template context with .Application, .Platform, etc */ -}}
  {{- $mode := include "https_mode" $context -}}
  {{- if or (eq "CertManager" $mode) (eq "CustomCertificate" $mode) -}}
    not empty string
  {{- end -}}
{{- end -}}

{{- /* Usage: {{ include "https_cert_manager_cluster_issuer_name" . }} */ -}}
{{- /* returns the cert-manager ClusterIssuer name for Deckhouse ecosystem applications */ -}}
{{- define "https_cert_manager_cluster_issuer_name" -}}
  {{- $context := . -}} {{- /* Template context with .Application, .Platform, etc */ -}}
  {{- $context.Platform.applications.https.certManager.clusterIssuerName -}}
{{- end -}}
