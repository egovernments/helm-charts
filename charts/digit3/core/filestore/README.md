# filestore

File upload, storage and retrieval for DIGIT 3.0. Stores objects in an S3-compatible backend (AWS S3 or in-cluster MinIO) and records metadata in PostgreSQL.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `filestore` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install filestore digit3-core/filestore -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
filestore:
  s3-bucket: "acme-prod-files"
  s3-region: "eu-west-1"
  s3-endpoint: "minio.backbone:9000"   # point at in-cluster MinIO for dev
  s3-use-ssl: "false"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (buckets, topics, consumer groups) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `filestore` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags | `-Xmx256m -Xms256m` |
| `persistence.enabled` | Mount a PVC (not needed with object storage) | `false` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `filestore` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/filestore` |

### Storage (S3 / MinIO)

Access credentials come from the `minio` secret (`accesskey` / `secretkey`) as `S3_ACCESS_KEY` / `S3_SECRET_KEY`.

| Key | Env var | Description | Default |
|---|---|---|---|
| `s3-bucket` | `S3_BUCKET` | Write bucket | `digit-filestore-bucket` |
| `s3-read-bucket` | `S3_READ_BUCKET` | Read bucket; falls back to `s3-bucket` when unset | `(s3-bucket)` |
| `s3-region` | `S3_REGION` | Bucket region | `ap-south-1` |
| `s3-endpoint` | `S3_ENDPOINT` | Object-store endpoint. AWS regional endpoint by default; set to `minio.backbone:9000` for in-cluster MinIO | `s3.ap-south-1.amazonaws.com` |
| `s3-source` | `S3_SOURCE` | Storage driver | `s3` |
| `s3-use-ssl` | `S3_USE_SSL` | TLS to the S3 endpoint (required for AWS) | `true` |
| `minio-use-ssl` | `MINIO_USE_SSL` | TLS to a MinIO endpoint | `false` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `schema-separation-topic` | `SCHEMA_SEPARATION_TOPIC` | Migration topic | `account-migration` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `MIGRATION_SCHEMA_TABLE` | Flyway history table | `filestore_schema` |
| `migration-locations` | `MIGRATION_FLYWAY_LOCATIONS` | App-level migration scripts | `classpath:db/migration` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-auto-create-topics` | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | Auto-create topics | `true` |
| `kafka-partitions` | `KAFKA_PARTITION_COUNT` | Default partitions | `1` |
| `kafka-replication-factor` | `KAFKA_REPLICATION_FACTOR` | Default replication | `1` |
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `filestore-service` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-upload-file` | `PUBSUB_TOPIC_UPLOAD_FILE` | `filestore-upload-file` |
| `topic-create-doc-category` | `PUBSUB_TOPIC_CREATE_DOC_CATEGORY` | `filestore-create-doc-category` |
| `topic-update-doc-category` | `PUBSUB_TOPIC_UPDATE_DOC_CATEGORY` | `filestore-update-doc-category` |
| `topic-delete-doc-category` | `PUBSUB_TOPIC_DELETE_DOC_CATEGORY` | `filestore-delete-doc-category` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |
| `redis-consumer-group` | `REDIS_CONSUMER_GROUP` | `filestore-service` |
| `redis-consumer-id` | `REDIS_CONSUMER_ID` | `filestore-service-1` |
| `redis-retention-days` | `REDIS_RETENTION_DAYS` | `7` |
| `redis-max-stream-length` | `REDIS_MAX_STREAM_LENGTH` | `1000000` |
| `redis-cleanup-interval` | `REDIS_CLEANUP_INTERVAL` | `86400` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `console-logs-enabled` | `CONSOLE_LOGS_ENABLED` | `true` |
| `otel-enabled` | `OTEL_ENABLED` | `true` |
| `otel-service-name` | `OTEL_SERVICE_NAME` | `filestore` |
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
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `egov-config` / `otel-exporter-otlp-endpoint` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | secret `minio` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/filestore`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/filestore)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/filestore/values.yaml)
