# localization-management

Localization management service for DIGIT 3.0 - bulk upload/download of localization messages on top of the core localization service.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `localization-management` (+ `localization-management-db` migration) |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install localization-management digit3-core/localization-management -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
localization-management:
  localization-batch-size: "200"
  localization-max-rows: "100000"
```

This service reads its datasource via `DB_HOST`/`DB_USER` (not `SPRING_DATASOURCE`), so `appType` is left blank and the common chart does not inject the Spring env block. Shared endpoints (database name, service hosts, flyway) come from the `egov-config` / `egov-service-host` ConfigMaps and the `db` secret managed by `cluster-configs`.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `localization-management` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `appType` | Blank so the common chart skips the Spring env block | `""` |
| `heap` | `JAVA_OPTS` (RAM percentage sizing) | `-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=70.0` |
| `healthChecks.enabled` | Enable liveness/readiness probes (Spring actuator) | `true` |
| `healthChecks.livenessProbePath` | Liveness path under the context | `/actuator/health/liveness` |
| `healthChecks.readinessProbePath` | Readiness path under the context | `/actuator/health/readiness` |
| `resources` | Container requests/limits | requests `100m` / `512Mi`, limits `1` / `1Gi` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.enabled` | Create the ingress | `true` |
| `ingress.gateway` | Route through the gateway | `true` |
| `ingress.context` | Gateway route segment | `localization-management` |
| `context-path` | `SERVER_CONTEXT_PATH`; blank derives `/<ingress.context>` | `""` |

### Database

| Key | Env var | Description | Default |
|---|---|---|---|
| `db-host` | `DB_HOST` | Postgres host | `postgres.egov` |
| `db-port` | `DB_PORT` | Postgres port | `5432` |
| `db-ssl-mode` | `DB_SSL_MODE` | TLS mode | `disable` |
| `dbSchema` | `FLYWAY_SCHEMAS` / `FLYWAY_DEFAULT_SCHEMA` | Migration schema | `public` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `filestore-module` | `FILESTORE_MODULE` | Filestore module for uploads | `localization-management` |
| `digit-client-id` | `DIGIT_CLIENT_ID` | DIGIT client id | `localization-management` |
| `localization-batch-size` | `LOCALIZATION_BATCH_SIZE` | Rows per upsert batch | `100` |
| `localization-max-rows` | `LOCALIZATION_MAX_ROWS` | Max rows per upload | `50000` |
| `localization-max-file-bytes` | `LOCALIZATION_MAX_FILE_BYTES` | Max upload size in bytes | `26214400` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.enabled` | - | Run the flyway DB migration init container | `true` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table | `localization_management_schema` |
| `initContainers.dbMigration.image.repository` | - | Migration image name | `localization-management-db` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `DB_NAME` | `egov-config` / `db-name` |
| `DB_USER` / `DB_PASSWORD` | secret `db` |
| `EGOV_FILESTORE_SERVICE_HOST` | `egov-service-host` / `filestore-java` |
| `EGOV_LOCALIZATION_HOST` | `egov-service-host` / `localization-java` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/localization-management`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/localization-management)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/localization-management/values.yaml)
