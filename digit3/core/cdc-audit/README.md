# cdc-audit

Tamper-evident change-data-capture audit trail (Java / Spring Boot). Captures row changes from Postgres (Debezium embedded engine or a Debezium Kafka topic), chains a SHA-256 hash per tenant schema, signs it with a per-tenant Vault transit key, and stores it. A small REST API verifies stored events.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `cdc-audit` (+ `cdc-audit-db` migration init image) |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install cdc-audit digit3-core/cdc-audit -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
cdc-audit:
  cdc-source: "KAFKA"
  cdc-debezium-slot-name: "cdc_audit_slot_uat"
  cdc-vault-base-url: "http://vault.vault:8200"
```

This is a Spring Boot (java) app: `appType: java`, so the common chart injects the shared Spring env block. The audit **storage** database is `SPRING_DATASOURCE_*` from common (the platform DB). The app's own Flyway is off (`SPRING_FLYWAY_ENABLED=false` from common); the `cdc-audit-db` init container owns the migration. CDC source, Debezium, Vault and storage settings are set in this chart and overridable per environment.

The verify API has no public gateway route (`ingress.enabled: false`): auditors reach it in-cluster via the Service at `http://cdc-audit.<namespace>:8080/api/v1/verify/{tenant}/{id}`.

## Prerequisites (operator)

- **Source Postgres** must run with `wal_level=logical` and provide a replication user; the service creates/uses the slot (`cdc-debezium-slot-name`) and publication (`cdc-debezium-publication-name`) with `pgoutput`.
- **Vault transit** must be enabled, and a secret (default name `cdc-audit`, key `vault-token`) must hold the Vault token. It is never set in chart values.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `cdc-audit` |
| `httpPort` | Container + service port | `8080` (from common) |
| `appType` | `java` so the common chart injects the Spring env block | `"java"` |
| `tracing-enabled` | Enable app tracing (the app does not use the tracer) | `false` |
| `healthChecks.enabled` | Liveness/readiness probes (verify API only, no health contract) | `false` |
| `resources` | Container requests/limits; image heap default is `-Xmx64m` | requests `50m` / `128Mi`, limits `200m` / `256Mi` |

### CDC source and Debezium

| Key | Env var | Description | Default |
|---|---|---|---|
| `cdc-source` | `CDC_SOURCE` | `ENGINE` (embedded Debezium) or `KAFKA` | `ENGINE` |
| `cdc-debezium-slot-name` | `CDC_DEBEZIUM_SLOT_NAME` | Logical replication slot | `cdc_audit_slot` |
| `cdc-debezium-plugin-name` | `CDC_DEBEZIUM_PLUGIN_NAME` | Logical decoding plugin | `pgoutput` |
| `cdc-debezium-publication-name` | `CDC_DEBEZIUM_PUBLICATION_NAME` | Publication name | `cdc_audit_pub` |
| `cdc-debezium-server-name` | `CDC_DEBEZIUM_DATABASE_SERVER_NAME` | Logical server name | `digit` |
| `cdc-debezium-offset-storage-file` | `CDC_DEBEZIUM_OFFSET_STORAGE_FILE` | Embedded-engine offset file (writable path) | `/tmp/cdc-offsets.dat` |

### Vault transit signing

| Key | Env var | Description | Default |
|---|---|---|---|
| `cdc-vault-base-url` | `CDC_VAULT_BASE_URL` | Vault address | `http://vault.vault:8200` |
| `cdc-vault-transit-mount` | `CDC_VAULT_TRANSIT_MOUNT` | Transit secrets-engine mount | `transit` |
| `cdc-vault-key-type` | `CDC_VAULT_KEY_TYPE` | Signing key type | `ecdsa-p256` |
| `vault-token-secret` | - | Secret holding the Vault token (`CDC_VAULT_TOKEN`) | `cdc-audit` |
| `vault-token-key` | - | Key within that secret | `vault-token` |

### Storage

| Key | Env var | Description | Default |
|---|---|---|---|
| `cdc-storage-type` | `CDC_STORAGE_TYPE` | `postgres` (cdc_events per tenant schema) or `rolling-local` | `postgres` |

### Migrations (init container)

| Key | Env var | Description | Default |
|---|---|---|---|
| `initContainers.dbMigration.enabled` | - | Run the flyway DB migration init container | `true` |
| `initContainers.dbMigration.schemaTable` | `SCHEMA_TABLE` | Flyway history table | `cdc_audit_schema` |
| `initContainers.dbMigration.image.repository` | - | Migration image name | `cdc-audit-db` |

## Sourced from the platform (not set in this chart)

Injected by the common chart from `cluster-configs` resources; these must exist in the target namespace.

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_URL` | `egov-config` / `db-url` (audit storage DB) |
| `SPRING_DATASOURCE_USERNAME` / `SPRING_DATASOURCE_PASSWORD` | secret `db` |
| `CDC_DEBEZIUM_DATABASE_HOSTNAME` / `_PORT` / `_DBNAME` | `egov-config` / `db-host`, `db-port`, `db-name` (source DB) |
| `CDC_DEBEZIUM_DATABASE_USER` / `_PASSWORD` | secret `db` |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/core/cdc-audit`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/cdc-audit)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/cdc-audit/values.yaml)
- App source: [`digitnxt/digit3` src/utilities/cdc-audit](https://github.com/digitnxt/digit3/tree/master/src/utilities/cdc-audit)
