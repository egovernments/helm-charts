# public-service

Citizen-facing public service application API for DIGIT Studio. Orchestrates workflow transitions, billing/demands, ID generation, localization and notifications over the DIGIT 3.0 (v3) APIs, and persists application data in PostgreSQL.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `public-service` (+ `public-service-db` flyway init) |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install public-service digit3-studio/public-service -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
public-service:
  service-module-name: "Studio"
  localization-module: "rainmaker-studio-"
  tracing-enabled: true
```

Shared infrastructure (the Spring datasource, Kafka brokers, Hikari pool, Tomcat, timezone and OpenTelemetry) is injected automatically by `common` because `appType: java` - do **not** redefine it here. Sibling-service hosts come from the `egov-service-host` ConfigMap and DB/Keycloak credentials from secrets, both managed by `cluster-configs`. The app-specific endpoint paths, Kafka topics and feature flags below live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `public-service` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-Xmx256m -Xms118m` |
| `ingress.context` | Gateway route segment | `public-service` |
| `tracing-enabled` | Inject OpenTelemetry env (via common) | `false` |

### Database (app reads `DB_*` directly)

| Key | Env var | Default |
|---|---|---|
| `db-port` | `DB_PORT` | `5432` |
| `db-sslmode` | `DB_SSLMODE` | `disable` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | `public_service_schema` |

### Workflow

| Key | Env var | Default |
|---|---|---|
| `workflow-transition-path` | `WORKFLOW_TRANSITION_PATH` | `workflow/v3/transition` |
| `workflow-search-path` | `WORKFLOW_SEARCH_PATH` | `workflow/v3/transition` |
| `wf-business-service-create-url` | `WF_BUSINESS_SERVICE_CREATE_URL` | `egov-workflow-v2/egov-wf/businessservice/_create` |
| `wf-business-service-search-path` | `WF_BUSINESS_SERVICE_SEARCH_PATH` | `egov-workflow-v2/egov-wf/businessservice/_search` |
| `wf-business-service-update-path` | `WF_BUSINESS_SERVICE_UPDATE_PATH` | `egov-workflow-v2/egov-wf/businessservice/_update` |
| `workflow-escalate-path` | `WORKFLOW_ESCALATE_PATH` | `workflow/v3/auto` |
| `auto-escalation-scheduler-enabled` | `AUTO_ESCALATION_SCHEDULER_ENABLED` | `true` |
| `auto-escalation-cron-schedule` | `AUTO_ESCALATION_CRON_SCHEDULE` | `0 * * * *` |
| `auto-escalation-system-user-uuid` | `AUTO_ESCALATION_SYSTEM_USER_UUID` | `069bcd98-...` |

### Sibling-service endpoint paths

| Key | Env var | Default |
|---|---|---|
| `mdms-service-host` | `MDMS_SERVICE_HOST` | `http://mdms-v2.egov:8080/` |
| `individual-create-endpoint` / `-update-` / `-search-` | `INDIVIDUAL_*_ENDPOINT` | `individuals/v3/individuals` |
| `boundary-search-endpoint` | `BOUNDARY_SEARCH_ENDPOINT` | `boundary/v3/boundaries` |
| `mdms-search-endpoint` | `MDMS_SEARCH_ENDPOINT` | `mdms-v2/v1/_search` |
| `mdms-v2-search-endpoint` / `mdms-search-v2-endpoint` | `MDMS_V2_SEARCH_ENDPOINT` / `MDMS_SEARCH_V2_ENDPOINT` | `mdms-v2/v2` |
| `mdms-v2-create-endpoint` / `-update-` | `MDMS_V2_CREATE_ENDPOINT` / `_UPDATE_` | `mdms-v2/v2/_create` / `_update` |
| `demand-create-endpoint` / `bill-fetch-endpoint` | `DEMAND_CREATE_ENDPOINT` / `BILL_FETCH_ENDPOINT` | `billing/v3/demands` |
| `idgen-service-generate-url` | `IDGEN_SERVICE_GENERATE_URL` | `idgen/v3/generate` |
| `localization-context-path` | `LOCALIZATION_CONTEXT_PATH` | `localization/v3/messages` |
| `localization-search-endpoint` | `LOCALIZATION_SEARCH_ENDPOINT` | `""` |
| `localization-upsert-endpoint` | `LOCALIZATION_UPSERT_ENDPOINT` | `/_upsert` |
| `service-request-create-endpoint` / `-search-` / `-update-` | `SERVICE_REQUEST_*_ENDPOINT` | `studio-service-request/service/definition/v1/_*` |
| `public-service-search-endpoint` | `PUBLIC_SERVICE_SEARCH_ENDPOINT` | `public-service-init/v1/service` |

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
| `save-public-service-application-topic` | `SAVE_PUBLIC_SERVICE_APPLICATION_TOPIC` | `save-public-service-application` |
| `update-public-service-application-topic` | `UPDATE_PUBLIC_SERVICE_APPLICATION_TOPIC` | `update-public-service-application` |
| `save-public-service-application-topic-indexer` | `..._INDEXER` | `save-public-service-application-indexer` |
| `update-public-service-application-topic-indexer` | `..._INDEXER` | `update-public-service-application-indexer` |
| `send-sms-topic` | `SEND_SMS_TOPIC` | `egov.core.notification.sms` |
| `send-email-topic` | `SEND_EMAIL_TOPIC` | `egov.core.notification.email` |
| `kafka-topics-payment-create-name` | `KAFKA_TOPICS_PAYMENT_CREATE_NAME` | `billing-create-payment` |
| `kafka-payment-consumer-enabled` | `KAFKA_PAYMENT_CONSUMER_ENABLED` | `true` |
| `save-public-service` / `update-public-service` | `SAVE_PUBLIC_SERVICE` / `UPDATE_PUBLIC_SERVICE` | `save-public-service` / `update-public-service` |
| `save-public-service-process` | `SAVE_PUBLIC_SERVICE_PROCESS` | `save-public-service-process` |
| `save-service-version-config-mapping` | `SAVE_SERVICE_VERSION_CONFIG_MAPPING` | `save-service-version-config-mapping` |
| `delete-business-service-topic` | `DELETE_BUSINESS_SERVICE_TOPIC` | `delete-business-service-topic` |

### Misc

| Key | Env var | Default |
|---|---|---|
| `notification-locale` | `NOTIFICATION_LOCALE` | `en_IN` |
| `notification-time-zone` | `NOTIFICATION_TIME_ZONE` | `Asia/Kolkata` |
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | `true` |
| `plugin-trusted-hosts` | `PLUGIN_TRUSTED_HOSTS` | `studio-calculator-service` |
| `default-limit` / `max-limit` / `default-offset` | `DEFAULT_LIMIT` / `MAX_LIMIT` / `DEFAULT_OFFSET` | `100` / `100000` / `0` |

## Sourced from the platform (not set in this chart)

Injected from `cluster-configs` resources, which must exist in the target namespace.

| Env var | Source |
|---|---|
| `DB_HOST` / `DB_NAME` | `egov-config` / `db-host`, `db-name` |
| `DB_USER` / `DB_PASSWORD` | secret `db` / `username`, `password` |
| `KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `STATE_LEVEL_TENANT_ID` | `egov-config` / `egov-state-level-tenant-id` |
| `WORKFLOW_HOST` | `egov-service-host` / `egov-workflow-v2` |
| `INDIVIDUAL_SERVICE_HOST` | `egov-service-host` / `individual-java` |
| `BOUNDARY_SERVICE_HOST` | `egov-service-host` / `boundary-service` |
| `BILLING_SERVICE_HOST` | `egov-service-host` / `billing-service` |
| `IDGEN_SERVICE_HOST` | `egov-service-host` / `egov-idgen` |
| `LOCALIZATION_SERVICE_HOST` | `egov-service-host` / `egov-localization` |
| `SERVICE_REQUEST_HOST` | `egov-service-host` / `service-request` |
| `DIGIT3_ACCOUNT_HOST` | `egov-service-host` / `account-java` |
| `KEYCLOAK_HOST` | `egov-service-host` / `keycloak` |
| `PUBLIC_SERVICE_HOST` | `egov-service-host` / `public-service-init` |
| `STUDIO_KEYCLOAK_CLIENT_SECRET` | secret `studio-keycloak` / `client-secret` (optional) |
| Spring datasource / Kafka / Tomcat / timezone / OTel | injected by `common` (`appType: java`) |

## Source

- Chart: [`charts/digit3/studio/public-service`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/public-service)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/studio/public-service/values.yaml)
