# health-service-request

Service-definition and service-request registry for DIGIT Health - the health-domain twin of the generic `service-request`. Lets health programs define request types and capture requests against them, persisted to the health PostgreSQL database.

| | |
|---|---|
| **Project / Module** | `digit3` / `health` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `health-service-request` (+ `health-service-request-db` flyway init) |

## Install

```bash
helm repo add digit3-health https://helm.digit.org/digit3/health
helm repo update
helm install health-service-request digit3-health/health-service-request -n health -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest). For `appType: java`, `common` injects the datasource, Kafka bootstrap, Tomcat, timezone and actuator base-path env.
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
health-service-request:
  max-limit: "200"
  kafka-consumer-group: "service-request-health-v2"
```

> This service uses the **health** database. Its init container and datasource point at `egov-config/health-db-url` (the datasource is overridden via `extraEnv.app` so it wins over the default `db-url` that `common` injects).

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `health-service-request` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx512m -Xms512m` |
| `java-args` | Extra JVM args (`JAVA_ARGS`) | `-Dspring.profiles.active=monitoring` |
| `resources` | CPU/memory requests and limits | `100m/256Mi` .. `300m/1Gi` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `health-service-request` |
| `context-path` | `SERVER_CONTEXT_PATH`, `SERVER_SERVLET_CONTEXT_PATH` | App context path; blank derives `/<ingress.context>` | `""` -> `/health-service-request` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table (hyphenated, historical) | `service-request_schema` |
| - | `DB_URL` | Health DB URL | `egov-config` / `health-db-url` |

### Feature flags

| Key | Env var | Default |
|---|---|---|
| `security-basic-enabled` | `SECURITY_BASIC_ENABLED` | `false` |
| `management-security-enabled` | `MANAGEMENT_SECURITY_ENABLED` | `false` |

### Kafka

| Key | Env var | Default |
|---|---|---|
| `kafka-consumer-group` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | `service-request-health` |
| `kafka-producer-key-serializer` | `SPRING_KAFKA_PRODUCER_KEY_SERIALIZER` | `...StringSerializer` |
| `kafka-producer-value-serializer` | `SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER` | `...JsonSerializer` |
| `topic-service-definition-create` | `EGOV_SERVICE_DEFINITION_CREATE_TOPIC` | `save-service-definition-health` |
| `topic-service-create` | `EGOV_SERVICE_CREATE_TOPIC` | `save-service-health` |
| `topic-service-update` | `EGOV_SERVICE_UPDATE_TOPIC` | `update-service-health` |
| `topic-service-definition-update` | `EGOV_SERVICE_DEFINITION_UPDATE_TOPIC` | `update-service-definition-health` |

### Request limits

| Key | Env var | Default |
|---|---|---|
| `default-offset` | `EGOV_SERVICE_REQUEST_DEFAULT_OFFSET` | `0` |
| `default-limit` | `EGOV_SERVICE_REQUEST_DEFAULT_LIMIT` | `10` |
| `max-limit` | `EGOV_SERVICE_REQUEST_MAX_LIMIT` | `100` |
| `max-string-input-size` | `EGOV_MAX_STRING_INPUT_SIZE` | `8192` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_URL` / `DB_URL` | `egov-config` / `health-db-url` |
| `SPRING_DATASOURCE_USERNAME` / `PASSWORD` | secret `db` (via common) |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` (via common) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |
| `FLYWAY_LOCATIONS` | `egov-config` (init container) |

## Source

- Chart: [`charts/digit3/health/health-service-request`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/health/health-service-request)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/health/health-service-request/values.yaml)
