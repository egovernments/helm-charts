# studio-hrms

Employee / HRMS management for DIGIT Studio. Creates and searches employees, wires them to the
user, individual, idgen and boundary services, and persists into the `studio` database schema.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `studio-hrms` (migrations: `studio-hrms-db`) |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-hrms digit3-studio/studio-hrms -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (datasource, Kafka, Tomcat, timezone, OTel).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named `studio-hrms` in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml
studio-hrms:
  kafka-consumer-group: "studio-employee-group1"
  hrms-dev-mode: "false"
```

Shared infrastructure (database credentials, Kafka brokers, timezone, Tomcat tuning, OpenTelemetry)
is injected by `common` for `appType: java` from the `egov-config` ConfigMap and `db` secret - do not
set it here. This chart runs against the **`studio`** schema, so `SPRING_DATASOURCE_URL` is overridden
(via `extraEnv.app`) to `egov-config/studio-db-url`; the Flyway init container uses the schema-less
`studio-db-url-no-schema` with `SCHEMA_NAME=studio` to work around the image double-appending
`?currentSchema`.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `studio-hrms` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx256m -Xms256m` |
| `java-args` | Extra JVM args (`JAVA_ARGS`) | `-Dspring.profiles.active=monitoring` |
| `cpu_requests` / `memory_requests` | Resource requests | `300m` / `512Mi` |
| `cpu_limits` / `memory_limits` | Resource limits | `300m` / `512Mi` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `studio-hrms` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | Servlet context; blank derives `/<ingress.context>` | `""` -> `/studio-hrms` |

### Service endpoints

| Key | Env var | Default |
|---|---|---|
| `egov-mdms-search-endpoint` | `EGOV_MDMS_SEARCH_ENDPOINT` | `/mdms-v2/v1/_search` |
| `egov-mdmslegacy-search-endpoint` | `EGOV_MDMSLEGACY_SEARCH_ENDPOINT` | `mdms-v2/v1/_search` |
| `egov-filestore-url-endpoint` | `EGOV_FILESTORE_URL_ENDPOINT` | `/filestore/v1/files/url` |
| `egov-localization-search-endpoint` | `EGOV_LOCALIZATION_SEARCH_ENDPOINT` | `/localization/messages/v1/_search` |
| `egov-idgen-createpath` | `EGOV_SERVICES_EGOV_IDGEN_CREATEPATH` | `/egov-idgen/id/_generate` |
| `egov-idgen-emp-code-name` | `EGOV_SERVICES_EGOV_IDGEN_EMP_CODE_NAME` | `employee.code` |
| `egov-idgen-emp-code-format` | `EGOV_SERVICES_EGOV_IDGEN_EMP_CODE_FORMAT` | `EMP_[SEQ_EMPLOYEE_CODE]` |
| `egov-user-search-endpoint` | `EGOV_USER_SEARCH_ENDPOINT` | `/user/v1/_search` |
| `egov-user-create-endpoint` | `EGOV_USER_CREATE_ENDPOINT` | `/user/users/_createnovalidate` |
| `egov-user-update-endpoint` | `EGOV_USER_UPDATE_ENDPOINT` | `/user/users/_updatenovalidate` |
| `egov-individual-create-endpoint` | `EGOV.INDIVIDUAL.CREATE.ENDPOINT` | `/studio-individual/v1/_create` |
| `egov-individual-update-endpoint` | `EGOV.INDIVIDUAL.UPDATE.ENDPOINT` | `/studio-individual/v1/_update` |
| `egov-individual-search-endpoint` | `EGOV.INDIVIDUAL.SEARCH.ENDPOINT` | `/studio-individual/v1/_search` |
| `egov-individual-delete-endpoint` | `EGOV.INDIVIDUAL.DELETE.ENDPOINT` | `/studio-individual/v1/_delete` |

Service **hosts** (`EGOV_MDMS_HOST`, `EGOV_FILESTORE_HOST`, `EGOV_LOCALIZATION_HOST`, `EGOV_IDGEN_HOST`,
`EGOV_BOUNDARY_HOST`, `EGOV_USER_HOST`, `EGOV_OTP_HOST`, `EGOV_INDIVIDUAL_HOST`) come from the
`egov-service-host` ConfigMap managed by `cluster-configs`.

### Kafka

| Key | Env var | Default |
|---|---|---|
| `kafka-consumer-group` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | `studio-employee-group1` |
| `topic-save-service` | `KAFKA_TOPICS_SAVE_SERVICE` | `save-hrms-employee-studio` |
| `topic-update-service` | `KAFKA_TOPICS_UPDATE_SERVICE` | `update-hrms-employee-studio` |
| `topic-notification-sms` | `KAFKA_TOPICS_NOTIFICATION_SMS` | `egov.core.notification.sms` |

### Feature flags

| Key | Env var | Default |
|---|---|---|
| `data-sync-employee-required` | `EGOV_SERVICES_DATA_SYNC_EMPLOYEE_REQUIRED` | `false` |
| `hrms-location-integration-enabled` | `EGOV_HRMS_LOCATION_INTEGRATION_ENABLED` | `false` |
| `hrms-dev-mode` | `EGOV_HRMS_DEV_MODE` | `true` |
| `hrms-employee-app-link` | `EGOV_HRMS_EMPLOYEE_APP_LINK` | egov-micro-dev login URL |
| `hrms-employee-default-password` | `EGOV_HRMS_EMPLOYEE_DEFAULT_PASSWORD` | `eGov@123` |
| `security-basic-enabled` | `SECURITY_BASIC_ENABLED` | `false` |
| `management-security-enabled` | `MANAGEMENT_SECURITY_ENABLED` | `false` |

### Migrations (init container)

| Key | Description | Default |
|---|---|---|
| `initContainers.dbMigration.schemaTable` | Flyway history table | `egov_hrms_schema` |
| (`SCHEMA_NAME`) | Target schema (fixed) | `studio` |

## Source

- Chart: [`charts/digit3/studio/studio-hrms`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-hrms)
