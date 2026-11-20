{{/*
superset.init merges the environment block (the top-level key named after the
chart, `superset:` in env.yaml) over .Values in place. Every template calls it
first, so env overrides reach all of them; the helpers below read .Values after
that merge.
*/}}
{{- define "superset.init" -}}
{{- $envOverrides := index .Values (default .Chart.Name .Values.name) -}}
{{- if $envOverrides -}}
{{- $_ := mustMergeOverwrite .Values $envOverrides -}}
{{- end -}}
{{- end -}}

{{/* Image reference for the Superset containers. */}}
{{- define "superset.image" -}}
{{ .Values.image.repository }}:{{ required "superset.image.tag is required" .Values.image.tag }}
{{- end -}}

{{/* envFrom shared by web, worker, beat and the init job. */}}
{{- define "superset.envFrom" -}}
- secretRef:
    name: {{ .Values.secrets.name }}
{{- with .Values.extraEnvFrom }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* extraEnv as name/value pairs, sorted by name. */}}
{{- define "superset.extraEnv" -}}
{{- range $k, $v := .Values.extraEnv }}
- name: {{ $k }}
  value: {{ $v | toString | quote }}
{{- end }}
{{- end -}}

{{/* Pod-level settings shared by every pod: pull secrets and scheduling. */}}
{{- define "superset.podSpec" -}}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.priorityClassName }}
priorityClassName: {{ . }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
