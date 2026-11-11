# beneficiary-idgen

Beneficiary ID generation and dispatch for DIGIT Health. Maintains a pre-generated **ID pool**, supports bulk create/update over Kafka, and enforces per-user/device **rate limits** using Redis + Redisson. Persists to the health PostgreSQL database.

| | |
|---|---|
| **Project / Module** | `digit3` / `health` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `beneficiary-idgen` (+ `beneficiary-idgen-db` flyway init) |

## Install

```bash
helm repo add digit3-health https://helm.digit.org/digit3/health
helm repo update
helm install beneficiary-idgen digit3-health/beneficiary-idgen -n health -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest). For `appType: java`, `common` injects the datasource, Kafka bootstrap, Tomcat, timezone and actuator base-path env.
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
beneficiary-idgen:
  id-pool-generation-max-size: "50000"
  limit-id-user-device-per-day: "200"
  redisson-redis-connection-pool-size: "40"
```

> This service uses the **health** database. Its init container and datasource point at `egov-config/health-db-url` (the datasource is overridden via `extraEnv.app` so it wins over the default `db-url` that `common` injects). Shared infra endpoints (Kafka, timezone) still come from `egov-config`.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `beneficiary-idgen` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx768m -Xms256m` |
| `java-args` | Extra JVM args (`JAVA_ARGS`) | `-Dspring.profiles.active=monitoring` |
| `resources` | CPU/memory requests and limits | `100m/256Mi` .. `300m/1Gi` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `beneficiary-idgen` |
| `context-path` | `SERVER_CONTEXT_PATH`, `SERVER_SERVLET_CONTEXT_PATH` | App context path; blank derives `/<ingress.context>` | `""` -> `/beneficiary-idgen` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table | `beneficiary_idgen_schema` |
| - | `DB_URL` | Health DB URL | `egov-config` / `health-db-url` |

### Feature flags

| Key | Env var | Default |
|---|---|---|
| `idformat-from-mdms` | `IDFORMAT_FROM_MDMS` | `true` |
| `autocreate-new-seq` | `AUTOCREATE_NEW_SEQ` | `true` |
| `security-basic-enabled` | `SECURITY_BASIC_ENABLED` | `false` |
| `management-security-enabled` | `MANAGEMENT_SECURITY_ENABLED` | `false` |
| `id-validation-enabled` | `ID_VALIDATION_ENABLED` | `true` |
| `id-dispatch-retrieval-limit-to-today-enabled` | `ID_DISPATCH_RETRIEVAL_LIMIT_TO_TODAY_ENABLED` | `true` |

### MDMS

| Key | Env var | Default |
|---|---|---|
| `mdms-service-search-uri` | `MDMS_SERVICE_SEARCH_URI` | `mdms-v2/v1/_search` |
| `egov-mdms-search-uri` | `EGOV_MDMS_SEARCH_URI` | `mdms-v2/v1/_search` |

### Redis (cache) and Redisson

| Key | Env var | Default |
|---|---|---|
| `redis-host` | `SPRING_REDIS_HOST` | `redis.backbone` |
| `redis-port` | `SPRING_REDIS_PORT` | `6379` |
| `cache-type` | `SPRING_CACHE_TYPE` | `redis` |
| `cache-redis-ttl` | `SPRING_CACHE_REDIS_TIME_TO_LIVE` | `86400` |
| `cache-autoexpiry` | `SPRING_CACHE_AUTOEXPIRY` | `true` |
| `redis-ttl-in-days` | `REDIS_TTL_IN_DAYS` | `1` |
| `redisson-redis-host` | `REDISSON_REDIS_HOST` | `redis.backbone` |
| `redisson-redis-port` | `REDISSON_REDIS_PORT` | `6379` |
| `redisson-redis-timeout` | `REDISSON_REDIS_TIMEOUT` | `10000` |
| `redisson-redis-connection-pool-size` | `REDISSON_REDIS_CONNECTION_POOL_SIZE` | `20` |
| `redisson-redis-connection-min-idle-size` | `REDISSON_REDIS_CONNECTION_MIN_IDLE_SIZE` | `10` |
| `redisson-redis-threads` | `REDISSON_REDIS_THREADS` | `4` |
| `redisson-redis-netty-threads` | `REDISSON_REDIS_NETTY_THREADS` | `4` |

### Kafka (serializers + topics)

| Key | Env var | Default |
|---|---|---|
| `kafka-producer-key-serializer` | `SPRING_KAFKA_PRODUCER_KEY_SERIALIZER` | `...StringSerializer` |
| `kafka-producer-value-serializer` | `SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER` | `...JsonSerializer` |
| `topic-save-in-id-pool` | `KAFKA_TOPICS_SAVE_IN_ID_POOL` | `save-in-id-pool` |
| `topic-update-id-pool-status` | `KAFKA_TOPICS_UPDATE_ID_POOL_STATUS` | `update-id-pool-status` |
| `topic-save-in-dispatch-log` | `KAFKA_TOPICS_SAVE_IN_DISPATCH_LOG` | `save-dispatch-id-log` |
| `topic-consumer-bulk-update` | `KAFKA_TOPICS_CONSUMER_BULK_UPDATE_TOPIC` | `beneficiary-id-gen-bulk-update-health-topic` |
| `topic-consumer-bulk-create` | `KAFKA_TOPICS_CONSUMER_BULK_CREATE_TOPIC` | `beneficiary-id-gen-bulk-create-health-topic` |

### ID pool generation and rate limits

| Key | Env var | Default |
|---|---|---|
| `id-pool-seq-code` | `ID_POOL_SEQ_CODE` | `id.pool.number.random` |
| `idgen-random-buffer` | `IDGEN_RANDOM_BUFFER` | `5` |
| `id-pool-generation-max-records-per-persist-batch` | `ID_POOL_GENERATION_MAX_RECORDS_PER_PERSIST_BATCH` | `100` |
| `id-pool-generation-max-size` | `ID_POOL_GENERATION_MAX_SIZE` | `20000` |
| `limit-id-user-device-per-day-enabled` | `LIMIT_ID_USER_DEVICE_PER_DAY_ENABLED` | `true` |
| `limit-id-user-device-per-day` | `LIMIT_ID_USER_DEVICE_PER_DAY` | `100` |
| `limit-id-user-device-total` | `LIMIT_ID_USER_DEVICE_TOTAL` | `6000` |
| `limit-id-user-device-per-day-expire-days` | `LIMIT_ID_USER_DEVICE_PER_DAY_EXPIRE_DAYS` | `10` |
| `limit-id-user-device-total-expire-days` | `LIMIT_ID_USER_DEVICE_TOTAL_EXPIRE_DAYS` | `30` |
| `redis-default-replenish-rate` | `SPRING_DATA_REDIS_DEFAULT_REPLENISHRATE` | `4` |
| `redis-default-burst-capacity` | `SPRING_DATA_REDIS_DEFAULT_BURSTCAPACITY` | `4` |

### Actuator / management

Health is served on the app port (`/<context>/health`), so the common probes apply directly.

| Key | Env var | Default |
|---|---|---|
| `management-server-port` | `MANAGEMENT_SERVER_PORT` | `8080` |
| `management-endpoints-web-exposure-include` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | `health,info` |
| `management-endpoint-health-probes-enabled` | `MANAGEMENT_ENDPOINT_HEALTH_PROBES_ENABLED` | `true` |
| `management-endpoint-health-show-details` | `MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS` | `never` |
| `management-health-db-enabled` | `MANAGEMENT_HEALTH_DB_ENABLED` | `false` |
| `management-health-redis-enabled` | `MANAGEMENT_HEALTH_REDIS_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_URL` / `DB_URL` | `egov-config` / `health-db-url` |
| `SPRING_DATASOURCE_USERNAME` / `PASSWORD` | secret `db` (via common) |
| `KAFKA_CONFIG_BOOTSTRAP_SERVER_CONFIG` / `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `MDMS_SERVICE_HOST` | `egov-service-host` / `mdms-service-v2` |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |
| `FLYWAY_LOCATIONS` | `egov-config` (init container) |

## Source

- Chart: [`charts/digit3/health/beneficiary-idgen`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/health/beneficiary-idgen)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/health/beneficiary-idgen/values.yaml)
