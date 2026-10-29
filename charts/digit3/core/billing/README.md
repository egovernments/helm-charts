# billing

Billing, demand and payment management for DIGIT 3.0.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `billing` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install billing digit3-core/billing -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
billing:
  demand-enable-arrears: "false"
  bulk-bill-consumer-batch-size: "50"
  pdf-fail-on-error: "true"
  log-level: "INFO"
```

Shared infrastructure endpoints (database, Kafka, OpenTelemetry) and internal service hosts are **not** set here - they come from the `egov-config` / `egov-service-host` ConfigMaps and secrets managed by the `cluster-configs` chart. App-specific settings (demand flags, bulk-bill topics, client paths) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Env var | Description | Default |
|---|---|---|---|
| `replicas` | - | Number of pods | `1` |
| `image.repository` | - | Backend image name | `billing` |
| `httpPort` | `SERVER_PORT` | Container + service port | `8080` (from common) |
| `heap` | `JAVA_OPTS` | JVM memory flags | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Gateway route segment | `billing` |

The servlet context path is fixed in the application image (`/billing`); the gateway route is `ingress.context`.

### App config

| Key | Env var | Description | Default |
|---|---|---|---|
| `demand-enable-arrears` | `BILLING_DEMAND_ENABLE_ARREARS` | Carry unpaid arrears into new demands | `true` |
| `max-instrument-date-age-days` | `BILLING_MAX_INSTRUMENT_DATE_AGE_DAYS` | Max age of a payment instrument date (days) | `90` |
| `bulk-bill-consumer-batch-size` | `BILLING_BULK_BILL_CONSUMER_BATCH_SIZE` | Bulk-bill consumer batch size | `10` |
| `pdf-fail-on-error` | `BILLING_PDF_FAIL_ON_ERROR` | Fail bill generation if PDF/filestore errors (PDF backfilled otherwise) | `false` |

### Bulk-bill topics

| Key | Env var | Default |
|---|---|---|
| `topic-bulk-bill-generation` | `BILLING_TOPICS_BULK_BILL_GENERATION` | `bulk-bill-generator` |
| `topic-bulk-bill-generation-dlq` | `BILLING_TOPICS_BULK_BILL_GENERATION_DLQ` | `bulk-bill-generator-dlq` |

### Internal service clients

Hosts come from `egov-service-host` (see the platform table); the paths below are set in this chart.

| Key | Env var | Default |
|---|---|---|
| `idgen-generate-path` | `BILLING_IDGEN_GENERATE_PATH` | `idgen/v3/generate` |
| `idgen-bulk-generate-path` | `BILLING_IDGEN_BULK_GENERATE_PATH` | `idgen/v3/generate/bulk` |
| `idgen-bill-number-template` | `BILLING_IDGEN_BILL_NUMBER_TEMPLATE` | `BillNumber` |
| `idgen-receipt-number-template` | `BILLING_IDGEN_RECEIPT_NUMBER_TEMPLATE` | `ReceiptNumber` |
| `idgen-transaction-number-template` | `BILLING_IDGEN_TRANSACTION_NUMBER_TEMPLATE` | `TransactionNumber` |
| `apportion-bill-path` | `BILLING_APPORTION_BILL_PATH` | `apportion/v3/bills` |
| `filestore-upload-path` | `BILLING_FILESTORE_UPLOAD_PATH` | `filestore/v3/files/upload` |
| `filestore-download-path` | `BILLING_FILESTORE_DOWNLOAD_PATH` | `filestore/v3/files/{id}` |

### Messaging

| Key | Env var | Description | Default |
|---|---|---|---|
| `pubsub-type` | `TRACER_PUBSUB_TYPE` | PubSub transport (kafka required; account publishes to kafka only) | `kafka` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Tenant-migration consumer group | `billing` |

### Datasource / migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `spring-flyway-enabled` | `SPRING_FLYWAY_ENABLED` | App-boot Flyway; off because the init container runs migrations | `false` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `billing_schema` |

### Observability

| Key | Env var | Default |
|---|---|---|
| `metrics-export-enabled` | `MANAGEMENT_DEFAULTS_METRICS_EXPORT_ENABLED` | `false` |
| `management-endpoints-exposure` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | `health,info` |
| `tracing-sampling-probability` | `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `1.0` |
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | `false` |
| `log-level` | `LOGGING_LEVEL_ORG_DIGIT` | `DEBUG` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `BILLING_IDGEN_HOST` | `egov-service-host` / `idgen` |
| `BILLING_APPORTION_HOST` | `egov-service-host` / `apportion` |
| `BILLING_FILESTORE_HOST` | `egov-service-host` / `filestore` |
| `ACCOUNTS_HOST` | `egov-service-host` / `account` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/billing`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/billing)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/billing/values.yaml)
