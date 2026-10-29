# pg-service

Payment gateway integration (initiate, callback, verify) for DIGIT 3.0. Fronts external gateways (Stripe, Axis, PayU, Paytm, PhonePe), records transactions in PostgreSQL and runs Quartz reconciliation jobs.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `pg-service` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install pg-service digit3-core/pg-service -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
pg-service:
  stripe: true
  axis: false
  reconcile-interval-mins: "15"
  log-level: "INFO"
```

Shared infrastructure endpoints (database, Kafka, OpenTelemetry) and the collaborating service hosts (idgen, billing, registry, individual) are **not** set here - they come from the `egov-config` and `egov-service-host` ConfigMaps and the secrets managed by the `cluster-configs` chart. App-specific settings (gateway toggles, topics, paths) live in this chart and are overridable per environment.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `pg-service` |
| `appType` | Runtime type | `java` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `heap` | JVM heap flags (`JAVA_OPTS`) | `-XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0` |
| `java-args` | Extra JVM args (`JAVA_ARGS`) | `-Dspring.profiles.active=monitoring` |
| `java-debug` | Enable debug (`JAVA_ENABLE_DEBUG`) | `true` |

### Routing

The servlet context path is fixed in the application image (`/pg-service`); the gateway route is `ingress.context`.

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `pg-service` |

### Datasource / migrations

Migration runs in an init container; app-boot Flyway is off.

| Key | Env var | Description | Default |
|---|---|---|---|
| `flyway-enabled` | `SPRING_FLYWAY_ENABLED` | App-boot Flyway (kept off) | `false` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` / `TENANT_MIGRATION_SCHEMA_TABLE` | Flyway history table | `pg_service_schema` |

### Reconciliation

| Key | Env var | Description | Default |
|---|---|---|---|
| `reconcile-interval-mins` | `PG_EARLYRECONCILEJOBRUNINTERVAL_MINS` | Early-reconcile job interval (minutes) | `15` |
| `reconciliationSystemUserUuid` | `PG_RECONCILIATION_SYSTEM_USER_UUID` | Seeded system user UUID for reconciliation; install-specific | `b405e4d0-99ec-41b1-92e7-2ebda0321e0d` |

### Canonical API

| Key | Env var | Description | Default |
|---|---|---|---|
| `canonical-path` | `PG_SERVICE_CANONICAL_PATH` | Path segment of the canonical twin endpoints | `canonical` |

### Transaction events / messaging

| Key | Env var | Description | Default |
|---|---|---|---|
| `message-broker-enabled` | `MESSAGE_BROKER_ENABLED` | Publish transaction events | `false` |
| `topic-create-txn` | `MESSAGING_BROKER_TOPIC_CREATE_TXN` | Create-transaction topic | `create-pg-txn` |
| `topic-update-txn` | `MESSAGING_BROKER_TOPIC_UPDATE_TXN` | Update-transaction topic | `update-pg-txn` |
| `pubsub-type` | `TRACER_PUBSUB_TYPE` | Messaging backend (kafka required) | `kafka` |

### Internal service clients

Collaborating service hosts come from `egov-service-host` (see the platform table); only the relative paths are set here.

| Key | Env var | Description | Default |
|---|---|---|---|
| `idgen-generate-path` | `IDGEN_GENERATE_ID_PATH` | ID generation path | `idgen/v3/generate` |
| `idgen-txn-template-code` | `IDGEN_TXN_ID_TEMPLATE_CODE` | Transaction ID template code | `TxnID` |
| `billing-payments-create-path` | `BILLING_PAYMENTS_CREATE_PATH` | Billing create-payment path | `billing/v3/payments` |
| `billing-payments-validate-path` | `BILLING_PAYMENTS_VALIDATE_PATH` | Billing validate-payment path | `billing/v3/payments/validate` |
| `registry-data-search-path` | `REGISTRY_DATA_SEARCH_PATH` | Registry data search path (AXIS bank-account lookup) | `registry/v3/{schemaCode}/data/_search` |
| `registry-bank-account-schema` | `REGISTRY_BANK_ACCOUNT_SCHEMA` | Registry schema code for bank accounts | `bank-account` |
| `individual-create-path` | `INDIVIDUAL_CREATE_PATH` | Individual create path | `individual/v3/individuals` |
| `individual-search-path` | `INDIVIDUAL_SEARCH_PATH` | Individual search path | `individual/v3/individuals` |
| `user-create-enabled` | `PG_IS_USER_CREATE_ENABLED` | Create user on payment | `false` |

### Tenant / schema separation

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-migration-enabled` | `TENANT_MIGRATION_ENABLED` | Consume tenant-migration events | `true` |
| `schema-separation-mode` | `SCHEMA_SEPARATION_MODE` | Per-tenant schema isolation | `true` |
| `tenant-migration-consumer-group` | `TENANT_MIGRATION_CONSUMER_GROUP` | Migration consumer group | `pg-service` |

### Payment gateways

Each gateway is advertised only when its toggle is `true` **and** its credentials resolve. Gateway credentials come from the `egov-pg-service` secret (see the platform table). When a toggle is `false`, only `<GATEWAY>_ACTIVE=false` is set and the other keys are skipped.

| Key | Env var | Description | Default |
|---|---|---|---|
| `stripe` | `STRIPE_ACTIVE` | Enable Stripe | `true` |
| `stripe-currency` | `STRIPE_CURRENCY` | Stripe currency | `inr` |
| `axis` | `AXIS_ACTIVE` | Enable Axis | `false` |
| `axis-currency` | `AXIS_CURRENCY` | Axis currency | `INR` |
| `axis-locale` | `AXIS_LOCALE` | Axis locale | `en_IN` |
| `axis-vpc-version` | `AXIS_MERCHANT_VPC_VERSION` | Axis VPC version | `1` |
| `axis-vpc-command-pay` | `AXIS_MERCHANT_VPC_COMMAND_PAY` | Axis pay command | `pay` |
| `axis-vpc-command-status` | `AXIS_MERCHANT_VPC_COMMAND_STATUS` | Axis status command | `queryDR` |
| `axis-url-debit` | `AXIS_URL_DEBIT` | Axis debit URL | `https://migs.mastercard.co.in/vpcpay` |
| `axis-url-status` | `AXIS_URL_STATUS` | Axis status URL | `https://migs.mastercard.co.in/vpcdps` |
| `payu` | `PAYU_ACTIVE` | Enable PayU | `false` |
| `payu-url` | `PAYU_URL` | PayU pay URL | `""` |
| `payu-url-status` | `PAYU_URL_STATUS` | PayU status URL | `""` |
| `paytm` | `PAYTM_ACTIVE` | Enable Paytm | `false` |
| `phonepe` | `PHONEPE_ACTIVE` | Enable PhonePe | `false` |

### Observability

| Key | Env var | Description | Default |
|---|---|---|---|
| `tracing-export-enabled` | `MANAGEMENT_TRACING_EXPORT_ENABLED` | Export spans | `false` |
| `metrics-export-enabled` | `MANAGEMENT_OTLP_METRICS_EXPORT_ENABLED` | Export OTLP metrics | `false` |
| `management-endpoints-exposure` | `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | Exposed actuator endpoints | `health,info` |
| `tracing-sampling-probability` | `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | Trace sampling probability | `1.0` |
| `log-level` | `LOGGING_LEVEL_ORG_EGOV_PG` | App log level | `DEBUG` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config` / `kafka-brokers` |
| `IDGEN_HOST` | `egov-service-host` / `idgen` |
| `BILLING_HOST` | `egov-service-host` / `billing` |
| `REGISTRY_HOST` | `egov-service-host` / `registry` |
| `INDIVIDUAL_HOST` | `egov-service-host` / `individual` |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `egov-config` / `otel-tracing-export-otlp-endpoint` |
| `STRIPE_SECRET_KEY` | secret `egov-pg-service` / `stripe-secret-key` (optional) |
| `AXIS_MERCHANT_ID` / `AXIS_MERCHANT_SECRET_KEY` / `AXIS_MERCHANT_USER` / `AXIS_MERCHANT_PWD` / `AXIS_MERCHANT_ACCESS_CODE` | secret `egov-pg-service` |
| `PAYU_MERCHANT_KEY` / `PAYU_MERCHANT_SALT` | secret `egov-pg-service` |
| `PAYTM_MERCHANT_ID` / `PAYTM_MERCHANT_SECRET_KEY` | secret `egov-pg-service` |
| `PHONEPE_MERCHANT_ID` / `PHONEPE_MERCHANT_SECRET_KEY` / `PHONEPE_MERCHANT_SECRET_INDEX` | secret `egov-pg-service` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/pg-service`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/pg-service)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/pg-service/values.yaml)
