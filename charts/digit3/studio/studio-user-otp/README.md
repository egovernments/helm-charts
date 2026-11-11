# studio-user-otp

User OTP service for DIGIT Studio. Issues and validates one-time passwords for Studio sign-in and
verification flows, backed by Redis and the platform OTP/user/localization services.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `studio-user-otp` |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-user-otp digit3-studio/studio-user-otp -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
studio-user-otp:
  otp-host: "http://otp-java.egov:8080/otp"
  redis-host: "redis.backbone"
```

Shared infrastructure (database, Kafka, timezone, OpenTelemetry) is injected by `common` for
`appType: java`. This service has no DB migrations of its own; it stores OTPs in Redis.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `studio-user-otp` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx192m -Xms192m` |
| `cpu_requests` / `memory_requests` | Resource requests | `200m` / `512Mi` |
| `cpu_limits` / `memory_limits` | Resource limits | `200m` / `512Mi` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `studio-user-otp` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | App context path; blank derives `/<ingress.context>` | `""` -> `/studio-user-otp` |

### Redis (OTP store)

| Key | Env var | Default |
|---|---|---|
| `redis-host` | `SPRING_DATA_REDIS_HOST` | `redis.backbone` |
| `redis-port` | `SPRING_DATA_REDIS_PORT` | `6379` |

### External services

| Key | Env var | Default |
|---|---|---|
| `otp-host` | `OTP_HOST` | `http://otp-java.egov:8080/otp` |
| `topic-sms` | `SMS_TOPIC` | `egov.core.notification.sms` |
| `flag-digitstudio-url` | `FLAG_DIGITSTUDIO_URL` | `true` |
| `security-basic-enabled` | `SECURITY_BASIC_ENABLED` | `false` |
| `management-security-enabled` | `MANAGEMENT_SECURITY_ENABLED` | `false` |

`USER_HOST`, `EGOV_LOCALISATION_HOST` and `EGOV_MDMS_HOST` resolve from the `egov-service-host`
ConfigMap (keys `egov-user`, `egov-localization`, `egov-mdms-service`).

### Health checks

Liveness and readiness probe `/{ingress.context}/health` (`/studio-user-otp/health`).

## Source

- Chart: [`charts/digit3/studio/studio-user-otp`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-user-otp)
