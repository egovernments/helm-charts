# studio-service-request

Service and service-definition registry for DIGIT Studio. Stores Studio service definitions and
requests in PostgreSQL (the `studio` schema) and publishes change events to Kafka.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `studio-service-request` |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-service-request digit3-studio/studio-service-request -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
studio-service-request:
  kafka-consumer-group: "studio-service-request"
  mdms-search-endpoint: "/mdms-v2/v1/_search"
```

Shared infrastructure (database, Kafka, timezone, OpenTelemetry) is injected by `common` for
`appType: java` from the `egov-config` ConfigMap and `db` secret - do not set it here. Because
Studio uses its own `studio` schema, the datasource URL is overridden to `egov-config/studio-db-url`
via `extraEnv.app`.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `studio-service-request` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx320m -Xms256m` |
| `cpu_requests` / `memory_requests` | Resource requests | `100m` / `256Mi` |
| `cpu_limits` / `memory_limits` | Resource limits | `200m` / `512Mi` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `studio-service-request` |
| `context-path` | `SERVER_CONTEXTPATH`, `SERVER_SERVLET_CONTEXTPATH` | App context path; blank derives `/<ingress.context>` | `""` -> `/studio-service-request` |

### Kafka topics

| Key | Env var | Default |
|---|---|---|
| `kafka-consumer-group` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | `studio-service-request` |
| `topic-save-service` | `EGOV_SERVICE_CREATE_TOPIC` | `save-service-studio` |
| `topic-update-service` | `EGOV_SERVICE_UPDATE_TOPIC` | `update-service-studio` |
| `topic-save-service-definition` | `EGOV_SERVICE_DEFINITION_CREATE_TOPIC` | `save-service-definition-studio` |
| `topic-update-service-definition` | `EGOV_SERVICE_DEFINITION_UPDATE_TOPIC` | `update-service-definition-studio` |

### External services

| Key | Env var | Description | Default |
|---|---|---|---|
| `mdms-search-endpoint` | `EGOV_MDMS_SEARCH_ENDPOINT` | MDMS v1 search path | `/mdms-v2/v1/_search` |

`EGOV_MDMS_HOST`, `EGOV_IDGEN_HOST` and `EGOV_LOCALIZATION_HOST` resolve from the
`egov-service-host` ConfigMap (keys `egov-mdms-service`, `egov-idgen`, `egov-localization`).

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table | `service_request_schema` |
| - | `FLYWAY_SCHEMAS` / `FLYWAY_DEFAULT_SCHEMA` | Target schema | `studio` |

The init image is `studio-service-request-db` at the chart `appVersion`. `DB_URL` comes from
`egov-config/studio-db-url`; `FLYWAY_USER` / `FLYWAY_PASSWORD` from the `db` secret.

## Source

- Chart: [`charts/digit3/studio/studio-service-request`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-service-request)
