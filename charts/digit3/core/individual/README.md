# individual

Individual (person) registry for DIGIT 3.0

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `individual` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install individual digit3-core/individual -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
individual:
  log-level: "INFO"
  vault-enabled: "true"
  tracing-export-enabled: "true"
```

Shared infrastructure endpoints (database, Kafka, Redis, Vault, OpenTelemetry) and sibling-service hosts are **not** set here - they come from the `egov-config` and `egov-service-host` ConfigMaps and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups, feature flags) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `individual` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `individual` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/individual` |
| `canonical-api-prefix` | `CANONICAL_API_PREFIX` | Canonical API path prefix | `canonical` |
| `log-level` | `LOG_LEVEL` | Application log level | `DEBUG` |

### External services (IdGen)

| Key | Env var | Description | Default |
|---|---|---|---|
| `idgen-path` | `IDGEN_PATH` | IdGen generate endpoint path | `idgen/v3/generate` |
| `idgen-enabled` | `IDGEN_ENABLED` | Call IdGen for ID generation | `true` |
| `idgen-id-format` | `IDGEN_INDIVIDUAL_ID_FORMAT` | IdGen format name used for individual IDs | `individual` |

### Vault / PII encryption

| Key | Env var | Description | Default |
|---|---|---|---|
| `vault-enabled` | `VAULT_ENABLED` | Enable Vault-backed PII encryption | `false` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-create-individual` | `PUBSUB_TOPIC_CREATE_INDIVIDUAL` | `individual-create-individual` |
| `topic-update-individual` | `PUBSUB_TOPIC_UPDATE_INDIVIDUAL` | `individual-update-individual` |
| `topic-delete-individual` | `PUBSUB_TOPIC_DELETE_INDIVIDUAL` | `individual-delete-individual` |
| `topic-upsert-config` | `PUBSUB_TOPIC_UPSERT_CONFIG` | `individual-upsert-config` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `individual-service` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `individual_schema` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace. Vault and HMAC secrets are marked optional and tolerate absence when Vault is disabled.

| Env var | Source |
|---|---|
| `IDGEN_HOST` | `egov-service-host` / `idgen` |
| `VAULT_HOST` | `egov-service-host` / `vault` |
| `VAULT_ROLE_ID` | secret `vault-approle` / `role-id` (optional) |
| `VAULT_SECRET_ID` | secret `vault-approle` / `secret-id` (optional) |
| `HMAC_SECRET` | secret `hmac-secret` / `hmac-secret` (optional) |
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `REDIS_HOST` | `egov-config` / `redis-host` |
| `REDIS_PORT` | `egov-config` / `redis-port` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/individual`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/individual)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/individual/values.yaml)
