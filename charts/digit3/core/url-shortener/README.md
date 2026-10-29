# url-shortener

URL shortener (base62 keys) with HTTP redirects for DIGIT 3.0. Persists short-link mappings in PostgreSQL and serves redirects with an optional Redis cache.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `url-shortener` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install url-shortener digit3-core/url-shortener -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
url-shortener:
  cache-enabled: "true"
  cache-ttl: "24h"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (cache, topics, consumer groups) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `url-shortener` |
| `appType` | Runtime type | `java` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

The servlet context path is fixed in the application image (`/url-shortener`); the gateway route is `ingress.context`. The public host name (`URL_SHORTENER_HOST_NAME`) is derived as `https://<global.domain>`.

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `url-shortener` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `app-name` | `SPRING_APPLICATION_NAME` | Spring application name | `url-shortener` |

### Cache (Redis)

| Key | Env var | Description | Default |
|---|---|---|---|
| `cache-enabled` | `URL_SHORTENER_CACHE_ENABLED` | Enable redirect cache | `false` |
| `cache-type` | `URL_SHORTENER_CACHE_TYPE` | Cache backend | `redis` |
| `cache-ttl` | `URL_SHORTENER_CACHE_TTL` | Cache entry TTL | `24h` |
| `redis-db` | `SPRING_DATA_REDIS_DATABASE` | Redis database index | `0` |

### Datasource / migrations

Migration runs in an init container; app-boot Flyway is off.

| Key | Env var | Description | Default |
|---|---|---|---|
| `flyway-enabled` | `SPRING_FLYWAY_ENABLED` | App-boot Flyway (kept off) | `false` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `url_shortener_schema` |

### Lifecycle events

| Key | Env var | Description | Default |
|---|---|---|---|
| `events-enabled` | `URL_SHORTENER_EVENTS_ENABLED` | Emit lifecycle events | `false` |
| `topic-create-short-url` | `URL_SHORTENER_EVENTS_TOPICS_CREATE_SHORT_URL` | Create-short-url topic | `url-shortener-create-short-url` |
| `topic-create-config` | `URL_SHORTENER_EVENTS_TOPICS_CREATE_CONFIG` | Create-config topic | `url-shortener-create-url-config` |
| `topic-update-config` | `URL_SHORTENER_EVENTS_TOPICS_UPDATE_CONFIG` | Update-config topic | `url-shortener-update-url-config` |
| `topic-delete-config` | `URL_SHORTENER_EVENTS_TOPICS_DELETE_CONFIG` | Delete-config topic | `url-shortener-delete-url-config` |
| `pubsub-type` | `TRACER_PUBSUB_TYPE` | Messaging backend (kafka required) | `kafka` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `url-shortener` |

### Observability

| Key | Env var | Description | Default |
|---|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | Export spans | `false` |
| `metrics-export-enabled` | `MANAGEMENT_DEFAULTS_METRICS_EXPORT_ENABLED` | Export metrics | `false` |
| `management-endpoints-exposure` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | Exposed actuator endpoints | `health,info` |
| `tracing-sampling-probability` | `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | Trace sampling probability | `1.0` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_DATA_REDIS_HOST` | `egov-config` / `redis-host` |
| `SPRING_DATA_REDIS_PORT` | `egov-config` / `redis-port` |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/url-shortener`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/url-shortener)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/url-shortener/values.yaml)
