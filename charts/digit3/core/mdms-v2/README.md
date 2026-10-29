# mdms-v2

Master Data Management service (v2) for DIGIT 3.0. Stores and serves tenant master data and schema definitions, with changes published to Kafka.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `mdms-v2` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install mdms-v2 digit3-core/mdms-v2 -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
mdms-v2:
  kafka-consumer-group: "egov-mdms-service-consumer-group"
  heap: "-Xmx512m -Xms512m"
  otel-traces-exporter: "none"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (topics, consumer groups) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `mdms-v2` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx512m -Xms512m` |
| `java-args` | Extra JVM args (`JAVA_ARGS`) | `-Dspring.profiles.active=monitoring` |
| `initContainers.dbMigration.schemaTable` | Flyway history table | `mdms_v2_schema` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Gateway route segment | `mdms-v2` |

The servlet context path is fixed in the application image (`/mdms-v2`); the gateway route is `ingress.context`.

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `security-basic-enabled` | `SECURITY_BASIC_ENABLED` | HTTP basic auth | `false` |
| `management-security-enabled` | `MANAGEMENT_SECURITY_ENABLED` | Secure actuator endpoints | `false` |
| `java-enable-debug` | `JAVA_ENABLE_DEBUG` | Enable remote debug | `true` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-consumer-group` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | Consumer group | `egov-mdms-service-consumer-group` |

### MDMS topics

| Key | Env var | Default |
|---|---|---|
| `egov-mdms-schema-definition-save-topic` | `EGOV_MDMS_SCHEMA_DEFINITION_SAVE_TOPIC` | `save-mdms-schema-definition` |
| `egov-mdms-data-save-topic` | `EGOV_MDMS_DATA_SAVE_TOPIC` | `save-mdms-data` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `otel-traces-exporter` | `OTEL_TRACES_EXPORTER` | `none` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/mdms-v2`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/mdms-v2)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/mdms-v2/values.yaml)
