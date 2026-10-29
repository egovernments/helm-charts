# notification

Notification templates with email (SMTP) and SMS delivery.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `notification` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install notification digit3-core/notification -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
notification:
  kafka-consumer-group: "notification-service"
  sms-content-type: "application/x-www-form-urlencoded"
  otel-enabled: "true"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) and the email/SMS credentials are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups, template/filestore paths) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `notification` |
| `httpPort` | Container + service port (`HTTP_PORT`) | `8080` (from common) |
| `initContainers.dbMigration.schemaTable` | Flyway history table (`SCHEMA_TABLE` / `MIGRATION_SCHEMA_TABLE`) | `notification_schema` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `notification` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/notification` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `schema-separation-topic` | `SCHEMA_SEPARATION_TOPIC` | Migration topic | `account-migration` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `migration-locations` | `MIGRATION_FLYWAY_LOCATIONS` | App-level migration scripts | `classpath:db/migration` |

### External services

| Key | Env var | Description | Default |
|---|---|---|---|
| `template-config-path` | `TEMPLATE_CONFIG_PATH` | Template-config render path | `template-config/v3/render` |
| `filestore-path` | `FILESTORE_PATH` | Filestore files path | `filestore/v3/files` |

### Email / SMS

Email (SMTP) and SMS provider hosts and credentials are platform-sourced (see below).

| Key | Env var | Description | Default |
|---|---|---|---|
| `sms-content-type` | `SMS_PROVIDER_CONTENT_TYPE` | SMS request content type | `application/x-www-form-urlencoded` |

### Message broker

| Key | Env var | Description | Default |
|---|---|---|---|
| `message-broker-enabled` | `MESSAGE_BROKER_ENABLED` | Enable the message broker | `true` |
| `message-broker-type` | `MESSAGE_BROKER_TYPE` | Broker type | `KAFKA` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-auto-create-topics` | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | Auto-create topics | `true` |
| `kafka-partitions` | `KAFKA_PARTITION_COUNT` | Default partitions | `1` |
| `kafka-replication-factor` | `KAFKA_REPLICATION_FACTOR` | Default replication | `1` |
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `notification-service` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-create-template` | `PUBSUB_TOPIC_CREATE_TEMPLATE` | `notification-create-template` |
| `topic-update-template` | `PUBSUB_TOPIC_UPDATE_TEMPLATE` | `notification-update-template` |
| `topic-delete-template` | `PUBSUB_TOPIC_DELETE_TEMPLATE` | `notification-delete-template` |
| `topic-send-email` | `PUBSUB_TOPIC_SEND_EMAIL` | `notification-send-email` |
| `topic-send-sms` | `PUBSUB_TOPIC_SEND_SMS` | `notification-send-sms` |
| `topic-email` | `EMAIL_TOPIC` | `notification-email` |
| `topic-sms` | `SMS_TOPIC` | `notification-sms` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |
| `redis-consumer-group` | `REDIS_CONSUMER_GROUP` | `notification-service` |
| `redis-consumer-id` | `REDIS_CONSUMER_ID` | `notification-service-1` |
| `redis-retention-days` | `REDIS_RETENTION_DAYS` | `7` |
| `redis-max-stream-length` | `REDIS_MAX_STREAM_LENGTH` | `1000000` |
| `redis-cleanup-interval` | `REDIS_CLEANUP_INTERVAL` | `86400` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `otel-enabled` | `OTEL_ENABLED` | `false` |
| `otel-service-name` | `OTEL_SERVICE_NAME` | `notification` |
| `otel-service-version` | `OTEL_SERVICE_VERSION` | `1.0.0` |
| `otel-metrics-enabled` | `OTEL_METRICS_ENABLED` | `true` |
| `otel-sampler-arg` | `OTEL_TRACES_SAMPLER_ARG` | `1.0` |
| `prometheus-port` | `PROMETHEUS_PORT` | `9090` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `KAFKA_BROKERS` / `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `REDIS_ADDRESS` | `egov-config` / `redis-address` |
| `TEMPLATE_CONFIG_HOST` | `egov-service-host` / `template-config` |
| `FILESTORE_HOST` | `egov-service-host` / `filestore` |
| `SMTP_HOST` / `SMTP_PORT` | `egov-config` / `smtp-host`, `smtp-port` |
| `SMTP_FROM_NAME` | `egov-config` / `mail-sender-name` |
| `SMTP_USERNAME` / `SMTP_PASSWORD` / `SMTP_FROM_ADDRESS` | secret `egov-notification-mail` / `mailsenderusername`, `mailsenderpassword` |
| `SMS_PROVIDER_URL` | `egov-config` / `sms-provider-url` |
| `SMS_PROVIDER_USERNAME` / `SMS_PROVIDER_PASSWORD` | secret `egov-notification-sms` / `username`, `password` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `egov-config` / `otel-exporter-otlp-endpoint` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/notification`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/notification)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/notification/values.yaml)
