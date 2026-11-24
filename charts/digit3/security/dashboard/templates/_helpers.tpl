{{- define "dashboard.name" -}}security-dashboard{{- end -}}
{{- define "dashboard.pageConfigMap" -}}security-dashboard-page{{- end -}}
{{- define "dashboard.labels" -}}
app.kubernetes.io/name: {{ include "dashboard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: security
{{- end -}}
