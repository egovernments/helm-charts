# apportion

Apportions a collected payment across a bill's tax heads.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `apportion` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install apportion digit3-core/apportion -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
apportion:
  value-order: "false"
  enable-advance: "true"
  tracing-sampling-probability: "0.1"
```

Shared infrastructure endpoints (database, Kafka, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (apportion behaviour flags, consumer group) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Env var | Description | Default |
|---|---|---|---|
| `replicas` | - | Number of pods | `1` |
| `image.repository` | - | Backend image name | `apportion` |
| `httpPort` | `SERVER_PORT` | Container + service port (fixed to the app's port) | `8280` |
| `heap` | `JAVA_OPTS` | JVM memory flags | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Gateway route segment | `apportion` |

The servlet context path is fixed in the application image (`/apportion`); the gateway route is `ingress.context`.

### App config

| Key | Env var | Description | Default |
|---|---|---|---|
| `value-order` | `APPORTION_VALUE_ORDER` | Skip tax-head order validation; use order as equal-amount tie-break | `true` |
| `enable-advance` | `APPORTION_ENABLE_ADVANCE` | Emit an advance row for over-collection | `false` |
| `billing-tax-heads-path` | `APPORTION_BILLING_TAX_HEADS_PATH` | Billing-service tax-heads path | `billing/v3/tax-heads` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Tenant-migration consumer group | `apportion` |

### Datasource / migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `spring-flyway-enabled` | `SPRING_FLYWAY_ENABLED` | App-boot Flyway; off because the init container runs migrations | `false` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `apportion_schema` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `metrics-export-enabled` | `MANAGEMENT_DEFAULTS_METRICS_EXPORT_ENABLED` | `false` |
| `management-endpoints-exposure` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | `health,info` |
| `tracing-sampling-probability` | `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `1.0` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `APPORTION_BILLING_HOST` | `egov-service-host` / `billing` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/apportion`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/apportion)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/apportion/values.yaml)
