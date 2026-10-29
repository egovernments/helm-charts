# idgen

Formatted, unique human-readable ID generation for DIGIT 3.0

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `idgen` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install idgen digit3-core/idgen -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
idgen:
  idgen-timezone: "Asia/Kolkata"
  events-enabled: "true"
  tracing-export-enabled: "true"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (timezone, topics, feature flags) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `idgen` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Gateway route segment | `idgen` |

The servlet context path is fixed in the application image (`/idgen`); the gateway route is `ingress.context`.

### App config

| Key | Env var | Description | Default |
|---|---|---|---|
| `idgen-timezone` | `IDGEN_TIMEZONE` | Zone for `{DATE:*}` rendering and sequence scope keys | `UTC` |
| `flyway-enabled` | `SPRING_FLYWAY_ENABLED` | App-boot Flyway (off; migration runs in the init container) | `false` |

### Template lifecycle events

| Key | Env var | Default |
|---|---|---|
| `events-enabled` | `IDGEN_EVENTS_ENABLED` | `false` |
| `topic-create-template` | `IDGEN_EVENTS_TOPICS_CREATE` | `idgen-create-template` |
| `topic-update-template` | `IDGEN_EVENTS_TOPICS_UPDATE` | `idgen-update-template` |
| `topic-delete-template` | `IDGEN_EVENTS_TOPICS_DELETE` | `idgen-delete-template` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `pubsub-type` | `TRACER_PUBSUB_TYPE` | Tracer pub/sub backend | `kafka` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `idgen` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `idgen_schema` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |
| `metrics-export-enabled` | `MANAGEMENT_DEFAULTS_METRICS_EXPORT_ENABLED` | `false` |
| `management-endpoints` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | `health,info` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `SPRING_DATA_REDIS_HOST` | `egov-config` / `redis-host` |
| `SPRING_DATA_REDIS_PORT` | `egov-config` / `redis-port` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/idgen`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/idgen)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/idgen/values.yaml)
