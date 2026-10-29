# common

Base chart-building components and helpers for DIGIT 3.0. Provides the shared templates (`common.deployment`, `common.service`, `common.ingress`, `common.cronjob`, `common.servicemonitor`) and helpers (`common.name`, `common.labels`, `common.image`) that every digit3 service chart renders through, so a service chart only has to supply values.

| | |
|---|---|
| **Project / Module** | `digit3` / `common` |
| **Chart type** | library |
| **Used by** | every digit3 service chart (as a dependency) |

## Usage

`common` is a library chart. It is not installed on its own; it is declared as a dependency of a service chart, which then renders its templates. Add it to the service chart's `Chart.yaml`:

```yaml
dependencies:
  - name: common
    version: 3.1.0
    repository: https://helm.digit.org/digit3/common
```

A service chart's templates then include the shared definitions, for example:

```yaml
{{ include "common.deployment" . }}
{{ include "common.service" . }}
{{ include "common.ingress" . }}
```

## How overriding works

Every value a service runs with is resolved by the `common.name` helper (`templates/_helpers.tpl`), which runs first in each template. It performs a 3-tier `mustMergeOverwrite` from lowest to highest priority:

1. **common defaults** - `.Values.common`, the shared platform defaults carried in this library chart's `values.yaml` (lowest).
2. **service values** - the service chart's own `.Values` (medium).
3. **environment block** - the block in the env file named after the chart, looked up as `index .Values (default .Chart.Name .Values.name)` (highest).

```go
{{- $envOverrides := index .Values (tpl (default .Chart.Name .Values.name) .) -}}
{{- $baseCommonValues := .Values.common | deepCopy -}}
{{- $values := dict "Values" (mustMergeOverwrite $baseCommonValues .Values $envOverrides) -}}
{{- with mustMergeOverwrite . $values -}}
```

`mustMergeOverwrite` deep-merges in order, so a key set in the environment block wins over the service default, which wins over the common default. The merge mutates the root `.Values` in place (via `mustMergeOverwrite . $values`), so every template rendered afterwards in the same pass sees the already-merged values. This is why a service chart can rely on common's defaults being present without copying them.

```yaml
# env file  -- the block is named after the chart and wins over service + common defaults
filestore:
  replicas: 2
  httpPort: 8080
```

## Shared parameters

Defaults below come from this chart's `values.yaml`. A service chart inherits them and overrides any of them through its own `values.yaml` or the environment block (see above).

| Key | Description | Default |
|---|---|---|
| `global.mode` | Image-tag resolution mode, `prod` or `dev`. In `prod` the tag is always the chart `appVersion` (`image.tag` cannot override). In `dev` the tag is `image.tag` if set, else `appVersion`. Any other value fails the render. | `prod` |
| `global.containerRegistry` | Registry prefix for images whose `repository` has no `/` | `egovio` |
| `global.namespace` | When set, overrides the namespace for all rendered objects | (unset) |
| `namespace` | Service namespace; used when `global.namespace` is unset. Effective order: `global.namespace` > service `namespace` > `egov` | `egov` |
| `replicas` | Number of pods | `1` |
| `httpPort` | Container and service port | `8080` |
| `appType` | When `java`, injects the shared Java env (`extraEnv.java`) and applies `resources` | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag; honoured only in `dev` mode (see `global.mode`) | (unset) |
| `ingress.enabled` | Render an Ingress for the service | `false` |
| `ingress.gateway` | Route the Ingress backend through the API gateway instead of the service, and annotate the Service with `gateway/route-path: <ingress.context>` | `false` |
| `ingress.gatewayServiceName` | Gateway (kong) proxy Service name used as the Ingress backend when `ingress.gateway` is true | `kong-proxy` |
| `ingress.gatewayServicePort` | Gateway proxy Service port | `8000` |
| `ingress.waf.enabled` | Add the WAF (lua-resty-waf) annotations to the Ingress | `true` |
| `affinity.preferSpreadAcrossAZ` | Prefer spreading replicas across availability zones (pod anti-affinity) | `true` |
| `initContainers.dbMigration.enabled` | Add the Flyway DB-migration init container (requires `schemaTable`) | `false` |
| `initContainers.dbMigration.image.pullPolicy` | Pull policy for the migration init container | `IfNotPresent` |
| `initContainers.dbMigration.image.tag` | Tag for the migration init container | `latest` |
| `initContainers.dbMigration.env` | Env block for the migration init container (templated string); defaults wire `DB_URL`, `SCHEMA_TABLE`, `FLYWAY_USER`, `FLYWAY_PASSWORD`, `FLYWAY_LOCATIONS` from `egov-config` and the `db` secret | (see values.yaml) |
| `initContainers.gitSync.enabled` | Add a git-sync init container that clones a repo via SSH | `false` |
| `healthChecks.enabled` | Enable readiness and liveness probes | `false` |
| `healthChecks.livenessProbe` / `healthChecks.readinessProbe` | Probe definitions (templated string); default to `httpGet` on `/<ingress.context><probePath>` at `httpPort` | (see values.yaml) |
| `lifecycle.preStop` | Pre-stop hook; defaults to `sleep 10` for graceful shutdown | `sleep 10` |
| `cpu_requests` | CPU request (applied only when `appType` is `java`) | `100m` |
| `memory_requests` | Memory request (java only) | `256Mi` |
| `cpu_limits` | CPU limit (java only) | `300m` |
| `memory_limits` | Memory limit (java only) | `512Mi` |
| `resources` | Resource block (templated string); renders requests/limits from the four values above only when `appType` is `java` | (see values.yaml) |
| `extraEnv.java` | Shared Java env injected into every `appType: java` service (datasource + Hikari pool, Flyway off in-app, Kafka bootstrap, timezone, Tomcat tuning); sourced from `egov-config` and the `db` secret. Do not redefine the datasource at service level. | (see values.yaml) |
| `extraEnv.otel` | OpenTelemetry env (`OTEL_ENABLED`, `OTEL_EXPORTER_OTLP_ENDPOINT` from `egov-config`); injected only when tracing is enabled via `global.tracing-enabled` or `tracing-enabled` | (see values.yaml) |
| `extraEnv.app` | Optional per-service env block, appended after `otel` | (unset) |
| `extraEnv.promote` | Optional per-service env block, appended last so its entries override earlier duplicates | (unset) |
| `extraInitContainers` | Additional init containers (templated string) | `""` |
| `extraContainers` | Additional sidecar containers (templated string) | `""` |
| `extraVolumes` | Additional volumes (templated string) | `""` |
| `extraVolumeMounts` | Additional volume mounts (templated string) | `""` |
| `additionalLabels` | Extra labels merged into `common.labels` | `{}` |
| `podSecurityContext` | Pod-level security context | `{}` |
| `securityContext` | Container-level security context | `{}` |
| `service.additionalAnnotations` | Extra annotations on the Service (templated string) | `{}` |
| `serviceMonitor.honorLabels` | `honorLabels` on the ServiceMonitor | `true` |
| `cron.concurrencyPolicy` | Concurrency policy for `common.cronjob` jobs | `Forbid` |
| `cron.restartPolicy` | Restart policy for cron jobs | `OnFailure` |
| `cron.httpPort` | Port for cron workloads | `8080` |

### Env assembly and merge order

`common.deployment` builds the container `env` list by concatenating, in order, `env` < `extraEnv.java` (when `appType: java`) < `extraEnv.otel` (when tracing enabled) < `extraEnv.app` < `extraEnv.promote`, then de-duplicates by variable name keeping the last occurrence. `extraEnv.java` and `extraEnv.otel` are the platform-wide blocks: they are injected into every Java service and are sourced from the `egov-config` ConfigMap (and the `db` secret) managed by the `cluster-configs` chart, so a service does not set the datasource, Kafka brokers, or tracing endpoint itself.

## Source

- Chart: [`charts/digit3/common`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/common)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/common/values.yaml)
