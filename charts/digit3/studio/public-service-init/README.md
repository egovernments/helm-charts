# public-service-init

Service-definition initializer for DIGIT Studio's `public-service`. Seeds and reconciles service and business-service configuration over the DIGIT (v1) APIs, and persists its state in PostgreSQL.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `public-service-init` (+ `public-service-init-db` flyway init) |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install public-service-init digit3-studio/public-service-init -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
public-service-init:
  service-module-name: "Studio"
  studio-citizen-role: "STUDIO_CITIZEN"
  tracing-enabled: true
```

Shared infrastructure (the Spring datasource, Kafka brokers, Hikari pool, Tomcat, timezone and OpenTelemetry) is injected automatically by `common` because `appType: java` - do **not** redefine it here. Sibling-service hosts come from the `egov-service-host` ConfigMap and DB/Keycloak credentials from secrets, both managed by `cluster-configs`. The app-specific endpoint paths (v1), Kafka topics and feature flags below live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `public-service-init` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx256m -Xms118m` |
| `ingress.context` | Gateway route segment | `public-service-init` |
| `tracing-enabled` | Inject OpenTelemetry env (via common) | `false` |

### Database (app reads `DB_*` directly)

| Key | Env var | Default |
|---|---|---|
| `db-port` | `DB_PORT` | `5432` |
| `db-sslmode` | `DB_SSLMODE` | `disable` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | `public_service_init_schema` |

### Sibling-service endpoint paths (v1)

| Key | Env var | Default |
|---|---|---|
| `mdms-service-host` | `MDMS_SERVICE_HOST` | `http://mdms-v2.egov:8080/` |
| `workflow-transition-path` | `WORKFLOW_TRANSITION_PATH` | `egov-workflow-v2/egov-wf/process/_transition` |
| `workflow-search-path` | `WORKFLOW_SEARCH_PATH` | `egov-workflow-v2/egov-wf/process/_search` |
| `wf-business-service-create-url` / `-search-path` / `-update-path` | `WF_BUSINESS_SERVICE_*` | `egov-workflow-v2/egov-wf/businessservice/_*` |
| `individual-create-endpoint` / `-update-` / `-search-` | `INDIVIDUAL_*_ENDPOINT` | `studio-individual/v1/_create` etc. |
| `mdms-search-endpoint` | `MDMS_SEARCH_ENDPOINT` | `mdms-v2/v1/_search` |
| `mdms-v2-search-endpoint` | `MDMS_V2_SEARCH_ENDPOINT` | `mdms-v2/v2/_search` |
| `mdms-search-v2-endpoint` | `MDMS_SEARCH_V2_ENDPOINT` | `mdms-v2/v2` |
| `mdms-v2-create-endpoint` / `-update-` | `MDMS_V2_CREATE_ENDPOINT` / `_UPDATE_` | `mdms-v2/v2/_create` / `_update` |
| `demand-create-endpoint` | `DEMAND_CREATE_ENDPOINT` | `billing-service/demand/_create` |
| `bill-fetch-endpoint` | `BILL_FETCH_ENDPOINT` | `billing-service/bill/v2/_fetchbill` |
| `idgen-service-generate-url` | `IDGEN_SERVICE_GENERATE_URL` | `egov-idgen/id/_generate` |
| `localization-context-path` | `LOCALIZATION_CONTEXT_PATH` | `localization/messages/v1` |
| `localization-search-endpoint` / `-upsert-` | `LOCALIZATION_SEARCH_ENDPOINT` / `_UPSERT_` | `/_search` / `/_upsert` |
| `service-request-create-endpoint` / `-search-` / `-update-` | `SERVICE_REQUEST_*_ENDPOINT` | `studio-service-request/service/definition/v1/_*` |

### MDMS master / module config

| Key | Env var | Default |
|---|---|---|
| `service-master-name` | `SERVICE_MASTER_NAME` | `ServiceConfiguration` |
| `service-module-name` | `SERVICE_MODULE_NAME` | `Studio` |
| `mdms-mapping` | `MDMS_MAPPING` | documents/idgen/bill schema-code JSON |
| `taxhead` / `taxperiod` / `business-service` | `TAXHEAD` / `TAXPERIOD` / `BusinessService` | `TaxHeadMaster` / `TaxPeriod` / `BusinessService` |
| `localization-module` | `LOCALIZATION_MODULE` | `rainmaker-studio-` |

### Kafka topics

| Key | Env var | Default |
|---|---|---|
| `save-public-service-application-topic` / `update-` (+ `-indexer`) | `*_PUBLIC_SERVICE_APPLICATION_TOPIC(_INDEXER)` | `save/update-public-service-application[-indexer]` |
| `send-sms-topic` / `send-email-topic` | `SEND_SMS_TOPIC` / `SEND_EMAIL_TOPIC` | `egov.core.notification.sms` / `.email` |
| `kafka-topics-payment-create-name` | `KAFKA_TOPICS_PAYMENT_CREATE_NAME` | `egov.collection.payment-create` |
| `kafka-payment-consumer-enabled` | `KAFKA_PAYMENT_CONSUMER_ENABLED` | `true` |
| `save-public-service` / `update-public-service` | `SAVE_PUBLIC_SERVICE` / `UPDATE_PUBLIC_SERVICE` | `save/update-public-service` |
| `save-public-service-process` | `SAVE_PUBLIC_SERVICE_PROCESS` | `save-public-service-process` |
| `save-service-version-config-mapping` | `SAVE_SERVICE_VERSION_CONFIG_MAPPING` | `save-service-version-config-mapping` |
| `delete-business-service-topic` | `DELETE_BUSINESS_SERVICE_TOPIC` | `delete-business-service-topic` |

### Misc

| Key | Env var | Default |
|---|---|---|
| `notification-locale` | `NOTIFICATION_LOCALE` | `en_IN` |
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | `true` |
| `studio-citizen-role` | `STUDIO_CITIZEN_ROLE` | `STUDIO_CITIZEN` |

## Sourced from the platform (not set in this chart)

Injected from `cluster-configs` resources, which must exist in the target namespace.

| Env var | Source |
|---|---|
| `DB_HOST` / `DB_NAME` | `egov-config` / `db-host`, `db-name` |
| `DB_USER` / `DB_PASSWORD` | secret `db` / `username`, `password` |
| `KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `WORKFLOW_HOST` | `egov-service-host` / `egov-workflow-v2` |
| `INDIVIDUAL_SERVICE_HOST` | `egov-service-host` / `individual` |
| `BILLING_SERVICE_HOST` | `egov-service-host` / `billing-service` |
| `IDGEN_SERVICE_HOST` | `egov-service-host` / `egov-idgen` |
| `LOCALIZATION_SERVICE_HOST` | `egov-service-host` / `egov-localization` |
| `SERVICE_REQUEST_HOST` | `egov-service-host` / `service-request` |
| `STUDIO_PDF_HELPER_HOST` | `egov-service-host` / `studio-pdf-helper-service` |
| `KEYCLOAK_HOST` | `egov-service-host` / `keycloak` (optional) |
| `STUDIO_KEYCLOAK_CLIENT_SECRET` | secret `studio-keycloak` / `client-secret` (optional) |
| Spring datasource / Kafka / Tomcat / timezone / OTel | injected by `common` (`appType: java`) |

## Source

- Chart: [`charts/digit3/studio/public-service-init`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/public-service-init)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/studio/public-service-init/values.yaml)
