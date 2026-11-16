{{- /*
  pgr-services.ingressExact - an Ingress that routes a list of exact paths straight to this
  chart's Service (not through the gateway). common 3.1.1 only ships a single-Prefix
  ingress, so this mirrors common.ingress (namespace, class/WAF annotations, host, TLS).
  Input dict: root (.), name, paths (list of plain paths), annotations (dict, optional).
*/ -}}
{{- define "pgr-services.ingressExact" -}}
{{- $ := .root -}}
{{- range .paths }}
{{- if not (regexMatch "^/[A-Za-z0-9/_-]+$" .) }}
{{- fail (printf "pgr-services: %q is not a plain path" .) }}
{{- end }}
{{- end }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ .name }}
{{- if $.Values.global.namespace }}
  namespace: {{ $.Values.global.namespace }}
{{- else if $.Values.ingress.namespace }}
  namespace: {{ $.Values.ingress.namespace }}
{{- else }}
  namespace: {{ $.Values.namespace }}
{{- end }}
  annotations:
{{- toYaml $.Values.ingress.annotations | nindent 4 }}
{{- if $.Values.ingress.waf.enabled }}
{{- toYaml $.Values.ingress.waf.annotations | nindent 4 }}
{{- end }}
{{- with .annotations }}
{{- toYaml . | nindent 4 }}
{{- end }}
  labels:
{{- include "common.labels" $ | nindent 4 }}
spec:
  rules:
{{- if $.Values.ingress.host }}
  {{- tpl $.Values.ingress.host $ | nindent 2 }}
{{- else if $.Values.ingress.hostName }}
  - host: {{ $.Values.ingress.hostName }}
{{- else }}
  - host: {{ $.Values.global.domain }}
{{- end }}
    http:
      paths:
{{- range .paths }}
      - path: {{ . }}$
        pathType: ImplementationSpecific
        backend:
          service:
            name: {{ include "common.name" $ }}
            port:
              number: {{ $.Values.httpPort }}
{{- end }}
{{- if ne $.Values.global.setup "quickstart" }}
  tls:
{{- if $.Values.ingress.tls }}
  {{- tpl $.Values.ingress.tls $ | nindent 2 }}
{{- else if $.Values.ingress.hostName }}
  - hosts:
    - {{ $.Values.ingress.hostName }}
    secretName: {{ $.Values.ingress.hostName }}-tls-certs
{{- else }}
  - hosts:
    - {{ $.Values.global.domain }}
    secretName: {{ $.Values.global.domain }}-tls-certs
{{- end }}
{{- end }}
{{- end -}}
