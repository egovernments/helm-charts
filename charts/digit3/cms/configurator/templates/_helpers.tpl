{{- /* config.js body, shared by the ConfigMap and the Deployment's checksum/config
       annotation so a runtimeConfig change rolls the pods. Precedence in the app:
       this file > build-time VITE_* > in-code default; a blank value falls through. */ -}}
{{- define "configurator.runtimeConfigJs" -}}
{{- $cfg := .Values.runtimeConfig | default dict -}}
// Rendered by the configurator Helm chart from `runtimeConfig`. Edit the environment's
// values, not this file.
window.__CONFIGURATOR_CONFIG__ = {
{{- range $key := list "STATE_TENANT_ID" "OVERPASS_URL" "TURBOPASS_URL" "BOUNDARY_SEARCH_LIMIT" "SENTRY_DSN" "POSTHOG_KEY" "POSTHOG_HOST" }}
  {{ $key }}: {{ index $cfg $key | default "" | toString | toJson }},
{{- end }}
};
{{- end -}}
