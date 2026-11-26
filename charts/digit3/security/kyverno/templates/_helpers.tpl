{{/*
Shared plumbing for the DIGIT policies in templates/digit-*.yaml.

Everything these helpers read lives in one place -- the `digitPolicies` block
of values.yaml -- so that turning a policy on, changing it from Audit to
Enforce, or narrowing it to a single namespace is a one-line edit there and
never a change to the CEL below.
*/}}

{{/*
validationActions for a policy.

  Audit    -> the violation is recorded in a PolicyReport. Nothing is rejected.
  Warn     -> also returned to kubectl as a warning. Still nothing is rejected.
  Enforce  -> the request is REJECTED, and still recorded, so the dashboard
              keeps showing what would have been blocked.

`Deny` and `Warn` may not appear together, which is why these are three
separate words rather than a free-form list.

Usage: {{ include "digitPolicies.validationActions" (dict "policy" $p "root" $) }}
*/}}
{{- define "digitPolicies.validationActions" -}}
{{- $action := .policy.action | default .root.Values.digitPolicies.action -}}
{{- if eq $action "Enforce" -}}
[Deny, Audit]
{{- else if eq $action "Warn" -}}
[Warn, Audit]
{{- else if eq $action "Audit" -}}
[Audit]
{{- else -}}
{{- fail (printf "digitPolicies: action must be Audit, Warn or Enforce, got %q" $action) -}}
{{- end -}}
{{- end -}}

{{/*
Namespace scope, rendered as matchConditions.

The rollout plan this exists for is "audit everywhere, then enforce one
namespace at a time", so scope is expressed by name rather than by label: a
namespace does not have to be relabelled to come under a policy, and the list
of what is covered is readable on one screen.

A policy that sets `namespaces.include` or `namespaces.exclude` REPLACES the
global list of the same name; it does not merge with it. Setting
`include: []` on a policy therefore widens it back to every namespace even
when the global default names some.

Usage: {{ include "digitPolicies.scope" (dict "policy" $p "root" $) }}
*/}}
{{- define "digitPolicies.scope" -}}
{{- $g := .root.Values.digitPolicies.namespaces | default dict -}}
{{- $p := .policy.namespaces | default dict -}}
{{- $include := $g.include -}}
{{- if hasKey $p "include" }}{{- $include = $p.include }}{{- end -}}
{{- $exclude := $g.exclude -}}
{{- if hasKey $p "exclude" }}{{- $exclude = $p.exclude }}{{- end -}}
{{- if $include }}
- name: digit-namespace-included
  expression: >-
    object.metadata.namespace in {{ toJson $include }}
{{- end }}
{{- if $exclude }}
- name: digit-namespace-not-excluded
  expression: >-
    !(object.metadata.namespace in {{ toJson $exclude }})
{{- end }}
{{- end -}}

{{/*
Labels, so that `kubectl get validatingpolicies -l digit.org/policy-set=digit`
separates what this chart adds from the upstream Pod Security set.
*/}}
{{- define "digitPolicies.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: digit-security
digit.org/policy-set: digit
{{- end -}}
