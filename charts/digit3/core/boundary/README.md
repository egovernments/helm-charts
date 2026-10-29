# boundary

Administrative boundaries and hierarchies (GeoJSON) for DIGIT 3.0.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `boundary` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install boundary digit3-core/boundary -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
boundary:
  cache-type: "redis"
  kafka-partitions: "3"
  redis-retention-days: "30"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups, cache mode) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Env var | Description | Default |
|---|---|---|---|
| `replicas` | - | Number of pods | `1` |
| `image.repository` | - | Backend image name | `boundary` |
| `httpPort` | `HTTP_PORT` | Container + service port (app reads `HTTP_PORT`, not `SERVER_PORT`) | `8080` (from common) |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `boundary` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/boundary` |

### App config

| Key | Env var | Description | Default |
|---|---|---|---|
| `boundary-basepath` | `BOUNDARY_BASEPATH` | Self base URL | `http://boundary:8080/` |
| `cache-type` | `CACHE_TYPE` | Cache backend (`inmemory` or `redis`) | `inmemory` |

### Filestore

| Key | Env var | Description | Default |
|---|---|---|---|
| `filestore-endpoint` | `FILESTORE_ENDPOINT` | Filestore files endpoint path | `/filestore/v1/files` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `schema-separation-topic` | `SCHEMA_SEPARATION_TOPIC` | Migration topic | `account-migration` |

### Migrations

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `MIGRATION_SCHEMA_TABLE` | Flyway history table | `boundary_schema` |
| `migration-locations` | `MIGRATION_FLYWAY_LOCATIONS` | App-level migration scripts | `classpath:db/migration` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-auto-create-topics` | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | Auto-create topics | `true` |
| `kafka-partitions` | `KAFKA_PARTITION_COUNT` | Default partitions | `1` |
| `kafka-replication-factor` | `KAFKA_REPLICATION_FACTOR` | Default replication | `1` |
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `boundary-service` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-create-boundary` | `PUBSUB_TOPIC_CREATE_BOUNDARY` | `boundary-create-boundary` |
| `topic-update-boundary` | `PUBSUB_TOPIC_UPDATE_BOUNDARY` | `boundary-update-boundary` |
| `topic-delete-boundary` | `PUBSUB_TOPIC_DELETE_BOUNDARY` | `boundary-delete-boundary` |
| `topic-create-boundary-hierarchy` | `PUBSUB_TOPIC_CREATE_BOUNDARY_HIERARCHY` | `boundary-create-boundary-hierarchy` |
| `topic-update-boundary-hierarchy` | `PUBSUB_TOPIC_UPDATE_BOUNDARY_HIERARCHY` | `boundary-update-boundary-hierarchy` |
| `topic-delete-boundary-hierarchy` | `PUBSUB_TOPIC_DELETE_BOUNDARY_HIERARCHY` | `boundary-delete-boundary-hierarchy` |
| `topic-create-boundary-relationship` | `PUBSUB_TOPIC_CREATE_BOUNDARY_RELATIONSHIP` | `boundary-create-boundary-relationship` |
| `topic-update-boundary-relationship` | `PUBSUB_TOPIC_UPDATE_BOUNDARY_RELATIONSHIP` | `boundary-update-boundary-relationship` |
| `topic-delete-boundary-relationship` | `PUBSUB_TOPIC_DELETE_BOUNDARY_RELATIONSHIP` | `boundary-delete-boundary-relationship` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |
| `redis-consumer-group` | `REDIS_CONSUMER_GROUP` | `boundary-service` |
| `redis-consumer-id` | `REDIS_CONSUMER_ID` | `boundary-service-1` |
| `redis-retention-days` | `REDIS_RETENTION_DAYS` | `7` |
| `redis-max-stream-length` | `REDIS_MAX_STREAM_LENGTH` | `1000000` |
| `redis-cleanup-interval` | `REDIS_CLEANUP_INTERVAL` | `86400` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `otel-enabled` | `OTEL_ENABLED` | `true` |
| `otel-service-name` | `OTEL_SERVICE_NAME` | `boundary` |
| `otel-service-version` | `OTEL_SERVICE_VERSION` | `1.0.0` |
| `otel-metrics-enabled` | `OTEL_METRICS_ENABLED` | `true` |
| `otel-sampler-arg` | `OTEL_TRACES_SAMPLER_ARG` | `1.0` |
| `prometheus-port` | `PROMETHEUS_PORT` | `9090` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `REDIS_ADDRESS` | `egov-config` / `redis-address` |
| `FILESTORE_BASEPATH` | `egov-service-host` / `filestore` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `egov-config` / `otel-exporter-otlp-endpoint` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/boundary`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/boundary)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/boundary/values.yaml)
