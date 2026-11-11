# studio-individual

Individual (person) registry for DIGIT Studio. Creates, searches and encrypts individuals, syncs them
to the user service, and persists into the `studio` database schema with a Redis cache.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `studio-individual` (migrations: `studio-individual-db`) |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-individual digit3-studio/studio-individual -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (datasource, Kafka, Tomcat, timezone, OTel).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named `studio-individual` in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml
studio-individual:
  redis-host: "redis.backbone"
  notification-sms-enabled: "true"
```

Shared infrastructure (database credentials, Kafka brokers, timezone, Tomcat tuning, OpenTelemetry) is
injected by `common` for `appType: java` from `egov-config` and the `db` secret. This chart runs against
the **`studio`** schema, so `SPRING_DATASOURCE_URL` is overridden (via `extraEnv.app`) to
`egov-config/studio-db-url`, and the Flyway init container migrates `individual_schema` there.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `studio-individual` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx256m -Xms256m` |
| `cpu_requests` / `memory_requests` | Resource requests | `100m` / `256Mi` |
| `cpu_limits` / `memory_limits` | Resource limits | `300m` / `512Mi` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `studio-individual` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | Servlet context; blank derives `/<ingress.context>` | `""` -> `/studio-individual` |

### Redis cache

| Key | Env var | Default |
|---|---|---|
| `redis-host` | `SPRING_REDIS_HOST` | `redis.backbone` |
| `redis-port` | `SPRING_REDIS_PORT` | `6379` |
| `cache-ttl` | `SPRING_CACHE_REDIS_TIME-TO-LIVE` | `60` |

### idgen

| Key | Env var | Default |
|---|---|---|
| `idgen-path` | `EGOV_IDGEN_PATH` | `egov-idgen/id/_generate` |
| `idgen-enabled` | `EGOV_IDGEN_INTEGRATION_ENABLED` | `true` |
| `idgen-id-format` | `IDGEN.INDIVIDUAL.ID.FORMAT` | `individual.id` |

### Service endpoints

| Key | Env var | Default |
|---|---|---|
| `egov-mdms-search-endpoint` | `EGOV_MDMS_SEARCH_ENDPOINT` | `/mdms-v2/v1/_search` |
| `egov-enc-encrypt-endpoint` | `EGOV_ENC_ENCRYPT_ENDPOINT` | `/egov-enc-service/crypto/v1/_encrypt` |
| `egov-enc-decrypt-endpoint` | `EGOV_ENC_DECRYPT_ENDPOINT` | `/egov-enc-service/crypto/v1/_decrypt` |
| `egov-create-user-url` | `EGOV_CREATE_USER_URL` | `/user/users/_createnovalidate` |
| `egov-search-user-url` | `EGOV_SEARCH_USER_URL` | `/user/_search` |
| `egov-update-user-url` | `EGOV_UPDATE_USER_URL` | `/user/users/_updatenovalidate` |
| `egov-boundary-search-url` | `EGOV_BOUNDARY_SEARCH_URL` | `/boundary-service/boundary/_search` |
| `egov-boundary-hierarchy` | `EGOV_BOUNDARY_HIERARCHY` | `ADMIN` |

Service **hosts** (`EGOV_IDGEN_HOST`, `EGOV_MDMS_HOST`, `EGOV_ENC_HOST`, `EGOV_LOCALIZATION_HOST`,
`EGOV_USER_HOST`, `EGOV_BOUNDARY_HOST`) come from the `egov-service-host` ConfigMap managed by
`cluster-configs`.

### Kafka

| Key | Env var | Default |
|---|---|---|
| `kafka-consumer-group` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | `studio-individual` |
| `topic-bulk-create` | `INDIVIDUAL.CONSUMER.BULK.CREATE.TOPIC` | `individual-consumer-bulk-create-studio-topic` |
| `topic-bulk-update` | `INDIVIDUAL.CONSUMER.BULK.UPDATE.TOPIC` | `individual-consumer-bulk-update-studio-topic` |
| `topic-bulk-delete` | `INDIVIDUAL.CONSUMER.BULK.DELETE.TOPIC` | `individual-consumer-bulk-delete-studio-topic` |
| `topic-save` | `INDIVIDUAL.PRODUCER.SAVE.TOPIC` | `save-individual-studio-topic` |
| `topic-update` | `INDIVIDUAL.PRODUCER.UPDATE.TOPIC` | `update-individual-studio-topic` |
| `topic-delete` | `INDIVIDUAL.PRODUCER.DELETE.TOPIC` | `delete-individual-studio-topic` |
| `topic-update-user-id` | `INDIVIDUAL_PRODUCER_UPDATE_USER_ID_TOPIC` | `update-user-id-studio-topic` |
| `topic-notification-sms` | `KAFKA_TOPICS_NOTIFICATION_SMS` | `egov.core.notification.sms` |

### Feature flags

| Key | Env var | Default |
|---|---|---|
| `user-integration-enabled` | `EGOV_USER_INTEGRATION_ENABLED` | `true` |
| `user-sync-enabled` | `USER_SYNC_ENABLED` | `true` |
| `user-service-user-type` | `USER_SERVICE_USER_TYPE` | `CITIZEN` |
| `user-service-account-locked` | `USER_SERVICE_ACCOUNT_LOCKED` | `false` |
| `notification-sms-enabled` | `NOTIFICATION_SMS_ENABLED` | `false` |
| `notification-sms-disabled-roles` | `NOTIFICATION_SMS_DISABLED_ROLES` | `ORG_ADMIN` |
| `aadhaar-pattern` | `AADHAAR_PATTERN` | `\d{12}` |
| `mobile-pattern` | `MOBILE_PATTERN` | `\d+` |
| `tracer-kafka-mdc-enabled` | `TRACER_KAFKA_MDC_ENABLED` | `true` |
| `security-basic-enabled` | `SECURITY_BASIC_ENABLED` | `false` |
| `management-security-enabled` | `MANAGEMENT_SECURITY_ENABLED` | `false` |

### Migrations (init container)

| Key | Description | Default |
|---|---|---|
| `initContainers.dbMigration.schemaTable` | Flyway history table | `individual_schema` |

## Source

- Chart: [`charts/digit3/studio/studio-individual`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-individual)
