{{/*
clickhouse.init merges the environment block (the top-level key named after the
chart, `clickhouse:` in env.yaml) over .Values in place. Every template calls it
first, so env overrides reach all of them.
*/}}
{{- define "clickhouse.init" -}}
{{- $envOverrides := index .Values .Chart.Name -}}
{{- if $envOverrides -}}
{{- $_ := mustMergeOverwrite .Values $envOverrides -}}
{{- end -}}
{{- end -}}

{{- define "clickhouse.namespace" -}}
{{- .Values.namespace | default .Release.Namespace -}}
{{- end -}}

{{- define "clickhouse.keeperName" -}}
{{- .Values.keeperName | default (printf "%s-keeper" .Values.name) -}}
{{- end -}}

{{- define "clickhouse.labels" -}}
app.kubernetes.io/name: clickhouse
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- with .Values.labels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Env var carrying a user's password into the ClickHouse container. */}}
{{- define "clickhouse.userPasswordEnv" -}}
{{- printf "CLICKHOUSE_USER_%s_PASSWORD" (. | upper | replace "-" "_" | replace "." "_") -}}
{{- end -}}
