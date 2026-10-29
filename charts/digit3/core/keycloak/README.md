# keycloak

Keycloak for DIGIT 3.0 (custom citizen-registration SPI build). Identity and access management: issues tokens, registers citizens via OTP and emits Kafka events through the custom SPI listener.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `keycloak` (tag `keycloak-spi-custom-build-aardhya-78019f9`) |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install keycloak digit3-core/keycloak -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
keycloak:
  image:
    tag: "keycloak-spi-custom-build-aardhya-78019f9"   # Kargo (digit-core3-0) writes this
  hostname-strict: "true"
  otp-host: "http://otp:8080/"
```

Shared infrastructure endpoints (database URL, Kafka, OpenTelemetry) are **not** set here - they come from the `egov-config` ConfigMap and secrets (`kc-db`, `kc-admin`, `db`) managed by the `cluster-configs` chart. App-specific settings (OTP paths, features, telemetry toggles) live in this chart and are overridable per environment.

Before Keycloak starts, a `db-init` init container reads the JDBC URL from `egov-config/<dbInit.urlKey>`, waits for the server and, as the admin user, creates the `kc-db` role and the database owned by it. It is idempotent on every start.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `keycloak` |
| `image.tag` | Image tag (written per env by Kargo) | `keycloak-spi-custom-build-aardhya-78019f9` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `namespace` | Target namespace (`global.namespace` in the env file wins) | `digit3-core` |
| `cpu_requests` / `memory_requests` | Resource requests | `250m` / `512Mi` |
| `cpu_limits` / `memory_limits` | Resource limits | `500m` / `1024Mi` |
| `serviceMonitor.enabled` | Create a Prometheus ServiceMonitor | `false` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.enabled` | - | Create the ingress | `true` |
| `ingress.gateway` | - | Route through the gateway | `false` |
| `ingress.context` | - | Route segment | `keycloak` |
| `kc-http-relative-path` | `KC_HTTP_RELATIVE_PATH` | Keycloak HTTP relative path | `/keycloak` |
| `kc-proxy` | `KC_PROXY` | Reverse-proxy mode | `edge` |
| `hostname-strict` | `KC_HOSTNAME_STRICT` | Strict hostname resolution | `true` |

`KC_HOSTNAME` is derived as `https://<global.domain>/keycloak` so issuer and redirect URLs are correct behind the ingress.

### Server / features

| Key | Env var | Description | Default |
|---|---|---|---|
| `kc-features` | `KC_FEATURES` | Enabled Keycloak features | `token-exchange,admin-fine-grained-authz` |

### Database

`SPRING_DATASOURCE_DRIVER_CLASS_NAME` is fixed at `org.postgresql.Driver`.

| Key | Env var | Description | Default |
|---|---|---|---|
| `kc-db` | `KC_DB` | Database vendor | `postgres` |
| `db-port` | `DB_PORT` | Database port | `5432` |

### Events / Kafka

| Key | Env var | Description | Default |
|---|---|---|---|
| `events-listeners` | `KC_EVENTS_LISTENERS` | Event listener SPIs | `jboss-logging,register-citizen-listener` |
| `register-citizen-listener-enabled` | `KC_SPI_EVENTS_LISTENER_REGISTER_CITIZEN_LISTENER_ENABLED` | Enable the citizen-registration listener | `true` |

### Observability (OpenTelemetry)

The tracing/metrics block is gated: when `tracing-enabled` / `metrics-enabled` are off the env vars are not rendered, so adopting an existing deployment does not roll the pod. Use a real boolean in the env file (`tracing-enabled: false`); in Helm a non-empty string `"false"` is TRUTHY.

| Key | Env var | Description | Default |
|---|---|---|---|
| `tracing-enabled` | `KC_TRACING_ENABLED` / `OTEL_SERVICE_NAME`; drives `OTEL_TRACES_EXPORTER` (`otlp`/`none`) | Enable tracing | `true` |
| `metrics-enabled` | `KC_METRICS_ENABLED` | Enable metrics (management stays on default port 9000) | `true` |
| `otel-service-name` | `KC_TELEMETRY_SERVICE_NAME` / `OTEL_SERVICE_NAME` | Telemetry service name | `keycloak` |
| `kc-telemetry-protocol` | `KC_TELEMETRY_PROTOCOL` | Telemetry protocol | `http` |
| `otel-sampler-type` | `KC_TELEMETRY_SAMPLER_TYPE` | Trace sampler | `parentbased_always_on` |
| `otel-exporter-protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP exporter protocol (matches the :4318 collector port) | `http/protobuf` |

### OTP / citizen registration

`*_DEFAULT_OTP` is a NON-PRODUCTION convenience; override `otp-default`, never ship a fixed OTP.

| Key | Env var | Description | Default |
|---|---|---|---|
| `otp-host` | `OTP_HOST` | OTP service base URL | `http://otp:8080/` |
| `otp-generate-path` | `OTP_GENERATE_PATH` | Generate path | `otp/v3/generate` |
| `otp-resend-path` | `OTP_RESEND_PATH` | Resend path | `otp/v3/resend` |
| `otp-verify-path` | `OTP_VERIFY_PATH` | Verify path | `otp/v3/verify` |
| `otp-invalidate-path` | `OTP_INVALIDATE_PATH` | Invalidate path | `otp/v3/invalidate` |
| `otp-email-purpose` | `OTP_EMAIL_PURPOSE` | Email OTP purpose | `login` |
| `otp-sms-purpose` | `OTP_SMS_PURPOSE` | SMS OTP purpose | `login` |
| `otp-registration-purpose` | `OTP_REGISTRATION_PURPOSE` | Registration OTP purpose | `registration` |
| `otp-default` | `OTP_EMAIL_DEFAULT_OTP` / `OTP_SMS_DEFAULT_OTP` / `OTP_REGISTRATION_DEFAULT_OTP` | Fixed OTP (non-prod only) | `123456` |
| `otp-registration-roles` | `OTP_REGISTRATION_ROLES` | Roles granted on self-registration | `CITIZEN` |
| `email-destination-attribute` | `KEYCLOAK_EMAIL_DESTINATION_ATTRIBUTE` | User attribute holding email | `email` |
| `sms-destination-attribute` | `KEYCLOAK_SMS_DESTINATION_ATTRIBUTE` | User attribute holding mobile | `mobileNumber` |
| `http-client-connect-timeout-ms` | `HTTP_CLIENT_CONNECT_TIMEOUT_MS` | OTP client connect timeout | `3000` |
| `http-client-request-timeout-ms` | `HTTP_CLIENT_REQUEST_TIMEOUT_MS` | OTP client request timeout | `5000` |

### Database bootstrap (db-init)

| Key | Description | Default |
|---|---|---|
| `dbInit.enabled` | Run the db-init init container | `true` |
| `dbInit.urlKey` | `egov-config` key holding the JDBC URL | `keycloak-db-url` |
| `dbInit.image` | psql image (>= 15 for `\getenv`) | `postgres:17-alpine` |
| `dbInit.adminUser` | Admin role (needs CREATEROLE + CREATEDB) | `postgres` |
| `dbInit.adminSecret.name` / `.key` | Secret holding the admin password | `db` / `superuserPassword` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `KC_DB_URL` | `egov-config` / `keycloak-db-url` (`dbInit.urlKey`) |
| `KC_DB_USERNAME` / `KC_DB_PASSWORD` | secret `kc-db` |
| `KEYCLOAK_ADMIN` / `KEYCLOAK_ADMIN_PASSWORD` | secret `kc-admin` |
| `KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `KC_TELEMETRY_ENDPOINT` | `egov-config` / `otel-exporter-otlp-endpoint` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `egov-config` (common otel block) |
| `PGPASSWORD` (db-init admin) | secret `db` / `superuserPassword` |

## Source

- Chart: [`charts/digit3/core/keycloak`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/keycloak)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/keycloak/values.yaml)
