# otp

OTP generation, verification and invalidation for DIGIT 3.0. Issues one-time codes, tracks resend and verification limits in Redis, and sends them through the notification service.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `otp` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install otp digit3-core/otp -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
otp:
  otp-length: "6"
  otp-lifetime-seconds: "300"
  enable-rate-limit: "true"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry, Vault) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (OTP policy, topics, consumer groups) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `otp` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `initContainers.dbMigration.schemaTable` | Flyway history table (`SCHEMA_TABLE` / `MIGRATION_SCHEMA_TABLE`) | `otp_schema` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `otp` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/otp` |

### OTP policy

| Key | Env var | Description | Default |
|---|---|---|---|
| `otp-length` | `OTP_LENGTH` | Code length | `6` |
| `otp-charset` | `OTP_CHARSET` | Code character set | `digits` |
| `otp-lifetime-seconds` | `OTP_LIFETIME_SECONDS` | Code validity window | `300` |
| `resend-delay-seconds` | `RESEND_DELAY_SECONDS` | Minimum gap between resends | `30` |
| `max-resend-count` | `MAX_RESEND_COUNT` | Resends allowed per code | `300` |
| `max-verify-attempts` | `MAX_VERIFY_ATTEMPTS` | Verify attempts per code | `500` |
| `use-single-active-otp` | `USE_SINGLE_ACTIVE_OTP` | Keep one active code per destination | `true` |
| `resend-same-code` | `RESEND_SAME_CODE` | Resend the same code instead of a new one | `true` |
| `max-generate-per-dest-hour` | `MAX_GENERATE_PER_DEST_HOUR` | Generations per destination per hour | `200` |
| `max-generate-per-ip-hour` | `MAX_GENERATE_PER_IP_HOUR` | Generations per IP per hour | `200` |
| `enable-rate-limit` | `ENABLE_RATE_LIMIT` | Enforce the rate limits above | `false` |
| `notify-async` | `NOTIFY_ASYNC` | Send to notification asynchronously | `false` |

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

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-auto-create-topics` | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | Auto-create topics | `true` |
| `kafka-partitions` | `KAFKA_PARTITION_COUNT` | Default partitions | `1` |
| `kafka-replication-factor` | `KAFKA_REPLICATION_FACTOR` | Default replication | `1` |
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `otp-service` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-generate-otp` | `PUBSUB_TOPIC_GENERATE_OTP` | `otp-generate-otp` |
| `topic-resend-otp` | `PUBSUB_TOPIC_RESEND_OTP` | `otp-resend-otp` |
| `topic-verify-otp` | `PUBSUB_TOPIC_VERIFY_OTP` | `otp-verify-otp` |
| `topic-invalidate-otp` | `PUBSUB_TOPIC_INVALIDATE_OTP` | `otp-invalidate-otp` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |
| `redis-consumer-group` | `REDIS_CONSUMER_GROUP` | `otp-service` |
| `redis-consumer-id` | `REDIS_CONSUMER_ID` | `otp-service-1` |
| `redis-retention-days` | `REDIS_RETENTION_DAYS` | `7` |
| `redis-max-stream-length` | `REDIS_MAX_STREAM_LENGTH` | `1000000` |
| `redis-cleanup-interval` | `REDIS_CLEANUP_INTERVAL` | `86400` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `console-logs-enabled` | `CONSOLE_LOGS_ENABLED` | `true` |
| `otel-enabled` | `OTEL_ENABLED` | `false` |
| `otel-service-name` | `OTEL_SERVICE_NAME` | `otp` |
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
| `REDIS_HOST` / `REDIS_PORT` | `egov-config` / `redis-host`, `redis-port` |
| `REDIS_ADDRESS` | `egov-config` / `redis-address` |
| `NOTIFY_BASE_URL` | `egov-service-host` / `notification` |
| `VAULT_HOST` | `egov-service-host` / `vault` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `egov-config` / `otel-exporter-otlp-endpoint` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `HMAC_SECRET` | secret `hmac-secret` / `hmac-secret` (optional) |
| `VAULT_ROLE_ID` / `VAULT_SECRET_ID` | secret `vault-approle` / `role-id`, `secret-id` (optional) |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/otp`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/otp)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/otp/values.yaml)
