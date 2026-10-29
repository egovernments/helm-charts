# notify

Multi-channel notifications (SMS, email, WhatsApp, push).

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `notify` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install notify digit3-core/notify -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
notify:
  log-level: "INFO"
  gmail-from-name: "eGov Foundation"
  smscountry-sender-id: ""
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) and the email/SMS provider credentials are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (consumer groups, provider names/URLs) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `notify` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM memory flags (`JAVA_TOOL_OPTIONS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |
| `initContainers.dbMigration.schemaTable` | Flyway history table (`SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE`) | `notify_schema` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `notify` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/notify` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `log-level` | `LOGGING_LEVEL_ORG_DIGIT_NOTIFY` | App log level | `INFO` |
| `flyway-enabled` | `SPRING_FLYWAY_ENABLED` | Run Flyway from the app (migrations run in the init container) | `false` |

### Providers (email / SMS)

Provider credentials are platform-sourced (see below). Jars are baked into the image and read at `/providers` (`NOTIFY_PLUGINS_DIR`).

| Key | Env var | Description | Default |
|---|---|---|---|
| `gmail-from-name` | `GMAIL_FROM_NAME` | Email sender display name | `eGov Foundation` |
| `smscountry-api-url` | `SMSCOUNTRY_API_URL` | SMS provider API URL | `http://api.smscountry.com/SMSCwebservice_bulk.aspx` |
| `smscountry-sender-id` | `SMSCOUNTRY_SENDER_ID` | SMS sender id; blank lets the gateway apply the registered sender header | `""` |

### Management / actuator

| Key | Env var | Description | Default |
|---|---|---|---|
| `management-base-path` | `MANAGEMENT_ENDPOINTS_WEB_BASE_PATH` | Actuator base path (under the servlet context path, where probes look) | `/` |
| `management-exposure-include` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | Exposed actuator endpoints | `health,info` |
| `metrics-export-enabled` | `MANAGEMENT_DEFAULTS_METRICS_EXPORT_ENABLED` | Export metrics | `false` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `notify-service` |

### Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `pubsub-type` | `PUBSUB_TYPE` | PubSub backend | `kafka` |
| `kafka-consumer-group` | `KAFKA_CONSUMER_GROUP` | Consumer group | `notify-service` |

### Notification topics

| Key | Env var | Default |
|---|---|---|
| `topic-send-enabled` | `NOTIFY_TOPIC_SEND_ENABLED` | `true` |

### Redis

| Key | Env var | Default |
|---|---|---|
| `redis-db` | `REDIS_DB` | `0` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `REDIS_HOST` / `REDIS_PORT` | `egov-config` / `redis-host`, `redis-port` |
| `APP_TIMEZONE` | `egov-config` / `timezone` |
| `GMAIL_FROM_ADDRESS` / `GMAIL_APP_PASSWORD` | secret `egov-notification-mail` / `mailsenderusername`, `mailsenderpassword` |
| `SMSCOUNTRY_USERNAME` / `SMSCOUNTRY_PASSWORD` | secret `egov-notification-sms` / `username`, `password` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/notify`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/notify)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/notify/values.yaml)
