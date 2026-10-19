{{- /*
  cc.checkUnique - fails the render if two items resolve to the same name in the same namespace,
  or if an item mixes a data: map with top-level data keys (ambiguous).
  Call: {{ include "cc.checkUnique" (dict "kind" "secret" "items" <map>) }}
*/ -}}
{{- define "cc.checkUnique" -}}
{{- $kind := .kind -}}
{{- $seen := dict -}}
{{- range $key, $item := .items -}}
  {{- if and $item.data (gt (len (omit $item "name" "namespace" "type" "data")) 0) -}}
    {{- fail (printf "cluster-configs: %s %q mixes data: with top-level keys - put all data under data:, or none" $kind $key) -}}
  {{- end -}}
  {{- $name := $item.name | default $key -}}
  {{- range $ns := ($item.namespace | default list) -}}
    {{- $id := printf "%s|%s" $ns $name -}}
    {{- if hasKey $seen $id -}}
      {{- fail (printf "cluster-configs: duplicate %s name %q in namespace %q (items %q and %q)" $kind $name $ns (index $seen $id) $key) -}}
    {{- end -}}
    {{- $_ := set $seen $id $key -}}
  {{- end -}}
{{- end -}}
{{- end -}}
