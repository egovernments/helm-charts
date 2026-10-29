# workflow

Configurable workflow / state-machine engine for DIGIT 3.0. Drives business processes and their state transitions from PostgreSQL-stored definitions, publishing lifecycle events to Kafka.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `workflow` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install workflow digit3-core/workflow -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
workflow:
  log-level: "debug"
  wf-inbox-assigned-only: "true"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups, limits) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `workflow` |
| `appType` | Runtime type | `java` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `workflow` |
| `context-path` | `CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/workflow` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `log-level` | `LOG_LEVEL` | App log level | `info` |
| `wf-inbox-assigned-only` | `WF_INBOX_ASSIGNEDONLY` | Restrict inbox to assigned items | `false` |

### Workflow limits

These override keys set defaults consumed by the application; they are not wired to a dedicated env var in this chart.

| Key | Description | Default |
|---|---|---|
| `workflow-default-limit` | Default search page size | `100` |
| `workflow-max-limit` | Maximum search page size | `1000` |

### Datasource / migrations

Migration runs in an init container; `MIGRATION_SCRIPT_PATH` and `MIGRATION_FLYWAY_BIN` are fixed in the image.

| Key | Env var | Description | Default |
|---|---|---|---|
| `migration-locations` | `MIGRATION_FLYWAY_LOCATIONS` | App-level migration scripts | `classpath:db/migration` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `MIGRATION_SCHEMA_TABLE` | Flyway history table | `workflow_schema` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `schema-separation-topic` | `SCHEMA_SEPARATION_TOPIC` | Migration topic | `account-migration` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-auto-create-topics` | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | Auto-create topics | `true` |
| `kafka-partitions` | `KAFKA_PARTITION_COUNT` | Default partitions | `1` |
| `kafka-replication-factor` | `KAFKA_REPLICATION_FACTOR` | Default replication | `1` |
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `workflow-service` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-create-process` | `PUBSUB_TOPIC_CREATE_PROCESS` | `workflow-create-process` |
| `topic-update-process` | `PUBSUB_TOPIC_UPDATE_PROCESS` | `workflow-update-process` |
| `topic-delete-process` | `PUBSUB_TOPIC_DELETE_PROCESS` | `workflow-delete-process` |
| `topic-create-state` | `PUBSUB_TOPIC_CREATE_STATE` | `workflow-create-state` |
| `topic-update-state` | `PUBSUB_TOPIC_UPDATE_STATE` | `workflow-update-state` |
| `topic-delete-state` | `PUBSUB_TOPIC_DELETE_STATE` | `workflow-delete-state` |
| `topic-create-action` | `PUBSUB_TOPIC_CREATE_ACTION` | `workflow-create-action` |
| `topic-update-action` | `PUBSUB_TOPIC_UPDATE_ACTION` | `workflow-update-action` |
| `topic-delete-action` | `PUBSUB_TOPIC_DELETE_ACTION` | `workflow-delete-action` |
| `topic-create-escalation-config` | `PUBSUB_TOPIC_CREATE_ESCALATION_CONFIG` | `workflow-create-escalation-config` |
| `topic-update-escalation-config` | `PUBSUB_TOPIC_UPDATE_ESCALATION_CONFIG` | `workflow-update-escalation-config` |
| `topic-delete-escalation-config` | `PUBSUB_TOPIC_DELETE_ESCALATION_CONFIG` | `workflow-delete-escalation-config` |
| `topic-transition` | `PUBSUB_TOPIC_TRANSITION` | `workflow-transition` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |
| `redis-consumer-group` | `REDIS_CONSUMER_GROUP` | `workflow-service` |
| `redis-consumer-id` | `REDIS_CONSUMER_ID` | `workflow-service-1` |
| `redis-retention-days` | `REDIS_RETENTION_DAYS` | `7` |
| `redis-max-stream-length` | `REDIS_MAX_STREAM_LENGTH` | `1000000` |
| `redis-cleanup-interval` | `REDIS_CLEANUP_INTERVAL` | `86400` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `otel-traces-exporter` | `OTEL_TRACES_EXPORTER` | `none` |
| `otel-enabled` | `OTEL_ENABLED` | `false` |
| `otel-service-name` | `OTEL_SERVICE_NAME` | `workflow` |
| `otel-service-version` | `OTEL_SERVICE_VERSION` | `1.0.0` |
| `otel-metrics-enabled` | `OTEL_METRICS_ENABLED` | `true` |
| `otel-sampler-arg` | `OTEL_TRACES_SAMPLER_ARG` | `1.0` |
| `prometheus-port` | `PROMETHEUS_PORT` | `9090` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `REDIS_ADDRESS` | `egov-config` / `redis-address` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `egov-config` / `otel-exporter-otlp-endpoint` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/workflow`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/workflow)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/workflow/values.yaml)
