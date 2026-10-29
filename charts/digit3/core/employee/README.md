# employee

Employee records, onboarding and jurisdictions for DIGIT 3.0

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `employee` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install employee digit3-core/employee -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
employee:
  idgen-name: "EmployeeCode"
  keycloak-client-id: "employee-iam-client"
  tracing-export-enabled: "true"
```

Shared infrastructure endpoints (database, Kafka, Redis, Keycloak, OpenTelemetry) and sibling-service hosts are **not** set here - they come from the `egov-config` and `egov-service-host` ConfigMaps and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups, feature flags) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `employee` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `employee` |
| `context-path` | `SERVER_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/employee` |
| `canonical-api-prefix` | `CANONICAL_API_PREFIX` | Canonical API path prefix | `canonical` |

### External services

| Key | Env var | Description | Default |
|---|---|---|---|
| `idgen-path` | `IDGEN_PATH` | IdGen generate endpoint path | `idgen/v3/generate` |
| `idgen-name` | `IDGEN_NAME` | IdGen format name used for employee codes | `EmployeeCode` |
| `idgen-enabled` | `IDGEN_ENABLED` | Call IdGen for code generation | `true` |
| `boundary-path` | `BOUNDARY_PATH` | Boundary relationship endpoint path | `boundary/v3/relationship` |
| `boundary-enabled` | `BOUNDARY_ENABLED` | Call the boundary service | `true` |
| `individual-path` | `INDIVIDUAL_PATH` | Individual registry endpoint path | `individual/v3/individuals` |
| `individual-enabled` | `INDIVIDUAL_ENABLED` | Call the individual service | `true` |

### Keycloak

| Key | Env var | Description | Default |
|---|---|---|---|
| `keycloak-client-id` | `KEYCLOAK_EMPLOYEE_IAM_CLIENT_ID` | Employee IAM client id | `employee-iam-client` |

### PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-create-employee` | `PUBSUB_TOPIC_CREATE_EMPLOYEE` | `employee-create-employee` |
| `topic-update-employee` | `PUBSUB_TOPIC_UPDATE_EMPLOYEE` | `employee-update-employee` |
| `topic-delete-employee` | `PUBSUB_TOPIC_DELETE_EMPLOYEE` | `employee-delete-employee` |
| `topic-create-jurisdiction` | `PUBSUB_TOPIC_CREATE_JURISDICTION` | `employee-create-jurisdiction` |
| `topic-update-jurisdiction` | `PUBSUB_TOPIC_UPDATE_JURISDICTION` | `employee-update-jurisdiction` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `employee-service` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `employee_schema` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `IDGEN_HOST` | `egov-service-host` / `idgen` |
| `BOUNDARY_HOST` | `egov-service-host` / `boundary` |
| `INDIVIDUAL_HOST` | `egov-service-host` / `individual` |
| `KEYCLOAK_BASE_URL` | `egov-config` / `keycloak-base-url` |
| `KEYCLOAK_ENABLED` | `egov-config` / `keycloak-enabled` |
| `KEYCLOAK_EMPLOYEE_IAM_CLIENT_SECRET` | secret `employee-iam-secret` / `employee-iam-client-secret` |
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `REDIS_HOST` | `egov-config` / `redis-host` |
| `REDIS_PORT` | `egov-config` / `redis-port` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/employee`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/employee)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/employee/values.yaml)
