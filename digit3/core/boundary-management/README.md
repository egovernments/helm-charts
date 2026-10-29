# boundary-management

Boundary Management utility (Node.js/TypeScript) - Excel template generate/process front for the digit3 boundary service.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `boundary-management` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install boundary-management digit3-core/boundary-management -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
boundary-management:
  logLevel: "debug"
  defaultLocale: "en_IN"
  localization-module: "hcm-admin-schemas"
  dbSchema: "public"
```

Shared infrastructure endpoints (database host, Kafka, Redis, service FQDN) are **not** set here - they come from the `egov-config` / `egov-service-host` ConfigMaps and secrets managed by the `cluster-configs` chart. App-specific settings (locale, modules, bulk-relationship tuning) live in this chart and are overridable per environment.

This is a Node.js app: `appType` is left blank so the common chart does not inject the Spring env block. Defaults in the app's `src/server/config/index.ts` already target the digit3 paths, so only hosts and app-level toggles are set here.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `boundary-management` |
| `httpPort` | Container + service port (`APP_PORT`) | `8080` (from common) |
| `appType` | Blank so the common chart skips the Spring env block | `""` |
| `tracing-enabled` | Enable app tracing | `false` |
| `healthChecks.enabled` | Enable liveness/readiness probes (no dedicated health route yet) | `false` |
| `resources` | Container requests/limits; memory limit sits above the image's 2048 MB Node heap cap | requests `150m` / `512Mi`, limits `1` / `2.5Gi` |

### Routing

The servlet context path is handled by the application (`/boundary-management`); the gateway route is `ingress.context`.

| Key | Description | Default |
|---|---|---|
| `ingress.enabled` | Create the ingress | `true` |
| `ingress.gateway` | Route through the gateway | `true` |
| `ingress.context` | Gateway route segment | `boundary-management` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `logLevel` | `APP_LOG_LEVEL` | Application log level | `info` |
| `maxDebugChar` | `APP_MAX_DEBUG_CHAR` | Max characters logged per debug line | `1000` |
| `digit-client-id` | `DIGIT_CLIENT_ID` | DIGIT client id | `boundary-management` |
| `defaultLocale` | `LOCALE` | Default locale | `en_IN` |
| `localization-module` | `LOCALIZATION_MODULE` | Localization module | `hcm-admin-schemas` |
| `filestoreModule` | `FILESTORE_MODULE` | Filestore document category uploads are filed under; must exist for the tenant | `boundary` |
| `defaultTenantId` | `DEFAULT_TENANT_ID` | Tenant used when a request carries no `X-Tenant-Id`; emitted only when set | `""` |

### Database

| Key | Env var | Description | Default |
|---|---|---|---|
| `dbSchema` | `DB_SCHEMA` | Database schema | `public` |
| `db-ssl` | `DB_SSL` | TLS to the database | `false` |
| `persistence-mode` | `PERSISTENCE_MODE` | Job status persistence driver; `db` writes rows straight to Postgres | `db` |

### Bulk relationship

| Key | Env var | Description | Default |
|---|---|---|---|
| `bulk-relationship-enabled` | `BULK_RELATIONSHIP_ENABLED` | Enable bulk relationship processing | `true` |
| `bulkRelationshipChunkSize` | `BULK_RELATIONSHIP_CHUNK_SIZE` | Chunk size; must not exceed the boundary service's `BULK_RELATIONSHIP_MAX_SIZE` (1000) | `1000` |
| `confirm-parent-enabled` | `CONFIRM_PARENT_ENABLED` | Confirm parent on relationship create | `false` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.enabled` | - | Run the flyway DB migration init container | `true` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table | `boundary_management_schema` |
| `initContainers.dbMigration.image.repository` | - | Migration image name | `boundary-management-db` |
| `dbSchema` | `SCHEMA_NAME` | Comma-separated schema list `migrate.sh` loops over; mandatory (unset migrates nothing) | `public` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `EGOV_HOST` | `egov-config` / `egov-services-fqdn-name` |
| `EGOV_BOUNDARY_HOST` | `egov-service-host` / `boundary-java` |
| `EGOV_FILESTORE_SERVICE_HOST` | `egov-service-host` / `filestore-java` |
| `EGOV_LOCALIZATION_HOST` | `egov-service-host` / `localization-java` |
| `EGOV_MDMS_HOST` / `EGOV_MDMS_V2_HOST` | `egov-service-host` / `mdms-v2` |
| `KAFKA_BROKER_HOST` | `egov-config` / `kafka-brokers` |
| `REDIS_HOST` | `egov-config` / `redis-host` |
| `REDIS_PORT` | `egov-config` / `redis-port` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/boundary-management`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/boundary-management)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/boundary-management/values.yaml)
