{{- define "common.name" -}}
{{- $envOverrides := index .Values (tpl (default .Chart.Name .Values.name) .) -}} 
{{- $baseCommonValues := .Values.common | deepCopy -}}
{{- $values := dict "Values" (mustMergeOverwrite $baseCommonValues .Values $envOverrides) -}}
{{- with mustMergeOverwrite . $values -}}
{{- default .Chart.Name .Values.name -}}    
{{- end }}
{{- end }}

{{- define "common.labels" -}}
app: {{ template "common.name" . }}
{{- if .Values.labels.group }}      
group: {{ .Values.labels.group }}  
{{- end }}  
{{- range $key, $val := .Values.additionalLabels }}
{{ $key }}: {{ $val | quote }}
{{- end }}    
{{- end }}

{{/*
  common.image - resolves <registry>/<repository>:<tag> with a mode-based tag.
  Input dict: Values (root .Values), appVersion (.Chart.AppVersion), repository, tag.
  global.mode (default "prod"; only "prod" | "dev"):
    prod -> tag is ALWAYS .Chart.AppVersion (image.tag cannot override)
    dev  -> tag is .Values.image.tag if set, else falls back to .Chart.AppVersion
*/}}
{{- define "common.image" -}}
{{- $mode := "prod" -}}
{{- $reg := "" -}}
{{- if .Values.global -}}
{{- $mode = (.Values.global.mode | default "prod") -}}
{{- $reg = (.Values.global.containerRegistry | default "") -}}
{{- end -}}
{{- if not (or (eq $mode "prod") (eq $mode "dev")) -}}
{{- fail (printf "common.image: global.mode must be \"prod\" or \"dev\" (got %q)" $mode) -}}
{{- end -}}
{{- $tag := .appVersion -}}
{{- if eq $mode "dev" -}}
{{- $tag = (default .appVersion .tag) -}}
{{- end -}}
{{- if or (not $tag) (eq ($tag | toString) "") -}}
{{- fail "common.image: no image tag resolved - set the chart appVersion (prod), or image.tag in dev mode" -}}
{{- end -}}
{{- if contains "/" .repository -}}
{{- printf "%s:%s" .repository $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" $reg .repository $tag -}}
{{- end -}}
{{- end -}}