# employee-onboarding

Bulk employee onboarding service for DIGIT 3.0 - validates an uploaded file and provisions employees (Keycloak users + employee/individual records) in the background.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `employee-onboarding` (+ `employee-onboarding-db` migration) |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install employee-onboarding digit3-core/employee-onboarding -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
employee-onboarding:
  onboarding-workers: "8"
  onboarding-notify-enabled: "false"
  tenant-migration-enabled: "true"
```

This service reads its datasource via `DB_HOST`/`DB_USER` (not `SPRING_DATASOURCE`), so `appType` is left blank and the common chart does not inject the Spring env block. Shared endpoints (Kafka, Vault, service hosts, flyway) come from the `egov-config` / `egov-service-host` ConfigMaps and the `db` / `vault-approle` / `hmac-secret` secrets managed by `cluster-configs`.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `employee-onboarding` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `appType` | Blank so the common chart skips the Spring env block | `""` |
| `heap` | `JAVA_OPTS` (RAM percentage sizing) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |
| `healthChecks.enabled` | Enable liveness/readiness probes (no health route live yet) | `false` |
| `resources` | Container requests/limits | requests `500m` / `768Mi`, limits `1` / `2Gi` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.enabled` | Create the ingress | `true` |
| `ingress.gateway` | Route through the gateway | `true` |
| `ingress.context` | Gateway route segment | `employee-onboarding` |
| `context-path` | `SERVER_CONTEXT_PATH`; blank derives `/<ingress.context>` | `""` |

### Database

| Key | Env var | Description | Default |
|---|---|---|---|
| `db-host` | `DB_HOST` | Postgres host | `postgres.egov` |
| `db-port` | `DB_PORT` | Postgres port | `5432` |
| `db-name` | `DB_NAME` | Database name | `postgres` |
| `db-ssl-mode` | `DB_SSL_MODE` | TLS mode | `disable` |
| `dbSchema` | `FLYWAY_SCHEMAS` / `FLYWAY_DEFAULT_SCHEMA` | Migration schema | `public` |

### Downstream services and paths

| Key | Env var | Default |
|---|---|---|
| `individual-path` | `INDIVIDUAL_PATH` | `individuals/v3/individuals` |
| `individual-config-path` | `INDIVIDUAL_CONFIG_PATH` | `individuals/v3/configs` |
| `employee-path` | `EMPLOYEE_PATH` | `employee/v3/employees` |
| `boundary-relationship-path` | `BOUNDARY_RELATIONSHIP_PATH` | `boundary/v3/relationship` |
| `boundary-hierarchy-path` | `BOUNDARY_HIERARCHY_PATH` | `boundary/v3/hierarchy` |
| `idgen-template-path` | `IDGEN_TEMPLATE_PATH` | `idgen/v3/template` |
| `idgen-template-code` | `IDGEN_TEMPLATE_CODE` | `EmployeeCode` |
| `keycloak-base-url` | `KEYCLOAK_BASE_URL` | `http://keycloak.keycloak.svc.cluster.local:8080/keycloak/` |

### Onboarding tuning

| Key | Env var | Default |
|---|---|---|
| `onboarding-max-file-bytes` | `EMPLOYEE_ONBOARDING_MAX_FILE_BYTES` | `10485760` |
| `onboarding-max-rows` | `EMPLOYEE_ONBOARDING_MAX_ROWS` | `100000` |
| `onboarding-password-min-length` | `EMPLOYEE_ONBOARDING_PASSWORD_MIN_LENGTH` | `8` |
| `onboarding-denied-roles` | `EMPLOYEE_ONBOARDING_DENIED_ROLES` | `SUPERUSER` |
| `onboarding-validation-concurrency` | `EMPLOYEE_ONBOARDING_VALIDATION_CONCURRENCY` | `10` |
| `onboarding-kc-scan-max-users` | `EMPLOYEE_ONBOARDING_KC_SCAN_MAX_USERS` | `50000` |
| `onboarding-chunk-size` | `EMPLOYEE_ONBOARDING_CHUNK_SIZE` | `100` |
| `onboarding-workers` | `EMPLOYEE_ONBOARDING_WORKERS` | `4` |
| `onboarding-row-concurrency` | `EMPLOYEE_ONBOARDING_ROW_CONCURRENCY` | `10` |
| `onboarding-retry-delays-seconds` | `EMPLOYEE_ONBOARDING_RETRY_DELAYS_SECONDS` | `60,120,300,600,900,900,900,900` |
| `onboarding-wait-max-rows` | `EMPLOYEE_ONBOARDING_WAIT_MAX_ROWS` | `50` |
| `onboarding-wait-max-seconds` | `EMPLOYEE_ONBOARDING_WAIT_MAX_SECONDS` | `25` |
| `onboarding-unstarted-retention-days` | `EMPLOYEE_ONBOARDING_UNSTARTED_RETENTION_DAYS` | `7` |
| `onboarding-paused-retention-days` | `EMPLOYEE_ONBOARDING_PAUSED_RETENTION_DAYS` | `7` |
| `onboarding-row-pii-retention-days` | `EMPLOYEE_ONBOARDING_ROW_PII_RETENTION_DAYS` | `7` |
| `onboarding-result-retention-days` | `EMPLOYEE_ONBOARDING_RESULT_RETENTION_DAYS` | `90` |
| `onboarding-job-retention-days` | `EMPLOYEE_ONBOARDING_JOB_RETENTION_DAYS` | `365` |
| `onboarding-notify-enabled` | `EMPLOYEE_ONBOARDING_NOTIFY_ENABLED` | `true` |
| `onboarding-notify-topic` | `EMPLOYEE_ONBOARDING_NOTIFY_TOPIC` | `notify-send` |
| `onboarding-notify-template-code` | `EMPLOYEE_ONBOARDING_NOTIFY_TEMPLATE_CODE` | `employee-onboarding-welcome` |

### Vault / messaging / tracing

| Key | Env var | Default |
|---|---|---|
| `vault-enabled` | `VAULT_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `redis-host` | `REDIS_HOST` | `redis.backbone.svc.cluster.local` |
| `redis-port` | `REDIS_PORT` | `6379` |
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | `false` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | `employee-onboarding` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `true` |
| `otel-otlp-endpoint` | `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `http://open-telemetry-opentelemetry-collector.monitoring.svc.cluster.local:4318/v1/traces` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `INDIVIDUAL_HOST` | `egov-service-host` / `individual-java` |
| `EMPLOYEE_HOST` | `egov-service-host` / `employee-java` |
| `BOUNDARY_HOST` | `egov-service-host` / `boundary-java` |
| `IDGEN_HOST` | `egov-service-host` / `idgen-java` |
| `NOTIFY_HOST` | `egov-service-host` / `notify` |
| `VAULT_HOST` | `egov-service-host` / `vault` |
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `DB_USER` / `DB_PASSWORD` | secret `db` |
| `VAULT_ROLE_ID` / `VAULT_SECRET_ID` | secret `vault-approle` (optional) |
| `HMAC_SECRET` | secret `hmac-secret` (optional) |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/employee-onboarding`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/employee-onboarding)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/employee-onboarding/values.yaml)
