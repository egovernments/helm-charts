# template-config

Template configuration and payload enrichment for DIGIT 3.0. Stores reusable templates in PostgreSQL and enriches request payloads from them, optionally emitting lifecycle events.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `template-config` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install template-config digit3-core/template-config -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
template-config:
  events-enabled: "true"
  tracing-export-enabled: "true"
```

Shared infrastructure endpoints (database, Kafka, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `template-config` |
| `appType` | Runtime type | `java` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `template-config` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/template-config` |

### Datasource / migrations

Migration runs in an init container; app-boot Flyway is off.

| Key | Env var | Description | Default |
|---|---|---|---|
| `flyway-enabled` | `SPRING_FLYWAY_ENABLED` | App-boot Flyway (kept off) | `false` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `template_config_schema` |

### Lifecycle events

| Key | Env var | Description | Default |
|---|---|---|---|
| `events-enabled` | `TEMPLATE_CONFIG_EVENTS_ENABLED` | Emit lifecycle events (fire-and-forget) | `false` |
| `topic-create` | `TEMPLATE_CONFIG_TOPICS_CREATE` | Create topic | `template-config-create-template-config` |
| `topic-update` | `TEMPLATE_CONFIG_TOPICS_UPDATE` | Update topic | `template-config-update-template-config` |
| `topic-delete` | `TEMPLATE_CONFIG_TOPICS_DELETE` | Delete topic | `template-config-delete-template-config` |
| `pubsub-type` | `TRACER_PUBSUB_TYPE` | Messaging backend (kafka required) | `kafka` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `template-config` |

### Observability

| Key | Env var | Description | Default |
|---|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | Export spans | `false` |
| `metrics-export-enabled` | `MANAGEMENT_DEFAULTS_METRICS_EXPORT_ENABLED` | Export metrics | `false` |
| `management-exposure-include` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | Exposed actuator endpoints | `health,info` |
| `tracing-sampling-probability` | `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | Trace sampling probability | `1.0` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/template-config`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/template-config)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/template-config/values.yaml)
