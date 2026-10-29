# registry

Configurable JSON registry (dynamic schemas + JSONB data) for DIGIT 3.0. Stores schema definitions and their JSONB records in PostgreSQL, with ID generation and optional Vault-backed field encryption.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `registry` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install registry digit3-core/registry -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
registry:
  log-level: "debug"
  vault-required: "true"
  idgen-org-value: "REGISTRY"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (schemas, topics, ID templates, Vault) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `registry` |
| `appType` | Runtime type | `java` |
| `httpPort` | Container + service port (`PORT`) | `8080` (from common) |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `registry` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/registry` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `log-level` | `LOG_LEVEL` | App log level | `info` |

### Datasource / migrations

Migration runs in an init container; `MIGRATION_SCRIPT_PATH` and `MIGRATION_FLYWAY_BIN` are fixed in the image.

| Key | Env var | Description | Default |
|---|---|---|---|
| `migration-locations` | `MIGRATION_FLYWAY_LOCATIONS` | App-level migration scripts | `classpath:db/migration` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `MIGRATION_SCHEMA_TABLE` | Flyway history table | `registry_schema` |

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
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `registry-service` |

### PubSub

| Key | Env var | Description | Default |
|---|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | Enable pubsub | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | Messaging backend | `kafka` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |
| `redis-consumer-group` | `REDIS_CONSUMER_GROUP` | `registry-service` |
| `redis-consumer-id` | `REDIS_CONSUMER_ID` | `registry-service-1` |
| `redis-retention-days` | `REDIS_RETENTION_DAYS` | `7` |
| `redis-max-stream-length` | `REDIS_MAX_STREAM_LENGTH` | `1000000` |
| `redis-cleanup-interval` | `REDIS_CLEANUP_INTERVAL` | `86400` |

### Vault (field encryption)

| Key | Env var | Description | Default |
|---|---|---|---|
| `vault-required` | `VAULT_REQUIRED` | Require Vault for encryption | `false` |
| `vault-transit-mount` | `VAULT_TRANSIT_MOUNT` | Transit engine mount | `transit` |
| `vault-key-prefix` | `VAULT_KEY_PREFIX` | Key name prefix | `registry-` |
| `vault-timeout` | `VAULT_TIMEOUT` | Client timeout | `30s` |

### IDGen

| Key | Env var | Description | Default |
|---|---|---|---|
| `idgenBaseUrl` | `IDGEN_BASE_URL` | IDGen base URL | `http://idgen:8080/idgen` |
| `idgen-template-id` | `IDGEN_TEMPLATE_ID` | ID template | `registryId` |
| `idgen-org-value` | `IDGEN_ORG_VALUE` | Org value used in IDs | `REGISTRY` |
| `idgen-http-timeout` | `IDGEN_HTTP_TIMEOUT` | HTTP timeout | `5s` |
| `idgen-table-templates` | `IDGEN_TABLE_TEMPLATES` | Templates table | `idgen_templates` |
| `idgen-table-sequence-lookups` | `IDGEN_TABLE_SEQUENCE_LOOKUPS` | Sequence lookup table | `idgen_sequence_lookup` |
| `idgen-table-sequence-resets` | `IDGEN_TABLE_SEQUENCE_RESETS` | Sequence reset table | `idgen_sequence_resets` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `otel-enabled` | `OTEL_ENABLED` | `false` |
| `otel-metrics-enabled` | `OTEL_METRICS_ENABLED` | `false` |
| `otel-service-name` | `OTEL_SERVICE_NAME` | `registry` |
| `otel-service-version` | `OTEL_SERVICE_VERSION` | `1.0.0` |
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
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/registry`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/registry)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/registry/values.yaml)
