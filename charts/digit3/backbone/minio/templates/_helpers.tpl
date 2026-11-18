{{- define "minio.fullname" -}}
{{- default "minio" .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "minio.headless" -}}
{{- printf "%s-svc" (include "minio.fullname" .) -}}
{{- end -}}

{{- define "minio.namespace" -}}
{{- default .Release.Namespace .Values.namespace -}}
{{- end -}}

{{- define "minio.secretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- include "minio.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Key names of the root login inside the Secret. Change them to share ONE Secret with an
app that uses other names, e.g. DIGIT filestore's `minio` Secret (accesskey/secretkey).
*/}}
{{- define "minio.rootUserKey" -}}
{{- ((.Values.auth.existingSecretKeys | default dict).rootUser) | default "rootUser" -}}
{{- end -}}
{{- define "minio.rootPasswordKey" -}}
{{- ((.Values.auth.existingSecretKeys | default dict).rootPassword) | default "rootPassword" -}}
{{- end -}}

{{- define "minio.labels" -}}
app: {{ include "minio.fullname" . }}
release: {{ .Release.Name }}
{{- end -}}

{{/*
Selector labels: intentionally exclude the release name, so Services keep their
endpoints whatever release name the chart is installed under.
*/}}
{{- define "minio.selectorLabels" -}}
app: {{ include "minio.fullname" . }}
{{- end -}}
