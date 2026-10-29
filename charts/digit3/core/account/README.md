# account

Tenant management and self-service tenant onboarding for DIGIT 3.0.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `account` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install account digit3-core/account -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
account:
  signup-enabled: "false"
  platform-auth-enabled: "true"
  access-token-lifespan: "3600"
  log-level: "INFO"
```

Shared infrastructure endpoints (database, Kafka, Redis, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets managed by the `cluster-configs` chart. App-specific settings (tenant topics, SMTP/OTP paths, session timeouts) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Env var | Description | Default |
|---|---|---|---|
| `replicas` | - | Number of pods | `1` |
| `image.repository` | - | Backend image name | `account` |
| `httpPort` | `SERVER_PORT` | Container + service port | `8080` (from common) |
| `heap` | `JAVA_OPTS` | JVM memory flags | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `accounts` |
| `context-path` | `CONTEXT_PATH` | App servlet context path; blank derives `/<ingress.context>` so the app and gateway route stay in sync | `""` -> `/accounts` |
| `canonical-api-prefix` | `CANONICAL_API_PREFIX` | Canonical API path prefix | `canonical` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |

### Keycloak & sessions

| Key | Env var | Description | Default |
|---|---|---|---|
| `citizen-broker-client-id` | `CITIZEN_BROKER_CLIENT_ID` | Citizen identity-broker client ID | `citizen-broker` |
| `access-token-lifespan` | `ACCESS_TOKEN_LIFESPAN` | Access-token lifespan (seconds) | `14400` |
| `sso-session-idle-timeout` | `SSO_SESSION_IDLE_TIMEOUT` | SSO session idle timeout (seconds) | `28800` |
| `sso-session-max-lifespan` | `SSO_SESSION_MAX_LIFESPAN` | SSO session max lifespan (seconds) | `36000` |

### SMTP

| Key | Env var | Description | Default |
|---|---|---|---|
| `smtp-starttls` | `SMTP_STARTTLS` | Use STARTTLS | `true` |
| `smtp-ssl` | `SMTP_SSL` | Use SSL | `false` |
| `smtp-auth` | `SMTP_AUTH` | SMTP authentication | `true` |

### Notification

| Key | Env var | Description | Default |
|---|---|---|---|
| `notify-send-path` | `NOTIFY_SEND_PATH` | Notify-service send path | `notify/v3/notifications` |
| `temp-password-template-code` | `NOTIFY_TEMP_PASSWORD_TEMPLATE_CODE` | Template code for temp-password notifications | `account-tenant-temp-password` |
| `notification-timeout-seconds` | `NOTIFICATION_TIMEOUT_SECONDS` | Notify client timeout (seconds) | `5` |
| `notification-enabled` | `NOTIFICATION_ENABLED` | Enable notifications | `true` |

### Platform auth & signup

| Key | Env var | Description | Default |
|---|---|---|---|
| `platform-auth-enabled` | `PLATFORM_AUTH_ENABLED` | Require platform auth | `false` |
| `platform-auth-realm` | `PLATFORM_AUTH_REALM` | Platform auth realm | `master` |
| `platform-auth-required-role` | `PLATFORM_AUTH_REQUIRED_ROLE` | Role required for platform auth | `SUPERADMIN` |
| `signup-enabled` | `SIGNUP_ENABLED` | Enable self-service signup | `true` |

### OTP

| Key | Env var | Description | Default |
|---|---|---|---|
| `otp-generate-path` | `OTP_GENERATE_PATH` | OTP generate path | `otp/v3/generate` |
| `otp-verify-path` | `OTP_VERIFY_PATH` | OTP verify path | `otp/v3/verify` |
| `otp-resend-path` | `OTP_RESEND_PATH` | OTP resend path | `otp/v3/resend` |
| `otp-config-path` | `OTP_CONFIG_PATH` | OTP config path | `otp/v3/config` |
| `otp-bootstrap-purposes` | `OTP_BOOTSTRAP_PURPOSES` | Purposes bootstrapped at startup | `login,registration` |
| `otp-timeout-seconds` | `OTP_TIMEOUT_SECONDS` | OTP client timeout (seconds) | `10` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table | `account_schema` |

### Kafka / PubSub topics

| Key | Env var | Default |
|---|---|---|
| `pubsub-enabled` | `PUBSUB_ENABLED` | `true` |
| `pubsub-type` | `PUBSUB_TYPE` | `kafka` |
| `topic-create-tenant` | `PUBSUB_TOPIC_CREATE_TENANT` | `account-create-tenant` |
| `topic-update-tenant` | `PUBSUB_TOPIC_UPDATE_TENANT` | `account-update-tenant` |
| `topic-delete-tenant` | `PUBSUB_TOPIC_DELETE_TENANT` | `account-delete-tenant` |
| `topic-create-tenant-config` | `PUBSUB_TOPIC_CREATE_TENANT_CONFIG` | `account-create-tenant-config` |
| `topic-update-tenant-config` | `PUBSUB_TOPIC_UPDATE_TENANT_CONFIG` | `account-update-tenant-config` |
| `topic-migration` | `PUBSUB_TOPIC_MIGRATION` | `account-migration` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `log-level` | `LOG_LEVEL` | `DEBUG` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `KAFKA_BROKERS` | `egov-config` / `kafka-brokers` |
| `REDIS_ADDRESS` | `egov-config` / `redis-address` |
| `REDIS_HOST` | `egov-config` / `redis-host` |
| `REDIS_PORT` | `egov-config` / `redis-port` |
| `KEYCLOAK_BASE_URL` | `egov-config` / `keycloak-base-url` |
| `KEYCLOAK_ADMIN_USER` / `KEYCLOAK_ADMIN_PASS` | secret `kc-admin` |
| `CITIZEN_BROKER_CLIENT_SECRET` | secret `citizen-broker-secret` |
| `EMPLOYEE_IAM_CLIENT_SECRET` | secret `employee-iam-secret` |
| `SMTP_HOST` | `egov-config` / `smtp-host` |
| `SMTP_PORT` | `egov-config` / `smtp-port` |
| `SMTP_FROM_NAME` | `egov-config` / `mail-sender-name` |
| `SMTP_FROM_ADDRESS` / `SMTP_USERNAME` / `SMTP_PASSWORD` | secret `egov-notification-mail` |
| `NOTIFY_BASE_URL` | `egov-service-host` / `notify` |
| `NOTIFICATION_PLATFORM_TENANT` / `OTP_PLATFORM_TENANT` | `egov-config` / `platform-tenant` |
| `OTP_BASE_URL` | `egov-service-host` / `otp` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/account`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/account)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/account/values.yaml)
