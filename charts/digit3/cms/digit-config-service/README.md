# digit-config-service

Schema-validated, tenant-scoped configuration store for the CMS notification stack: template
bindings, provider details (stored encrypted) and channel toggles, validated against MDMS v2
schemas.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `digit-config-service` (+ `digit-config-service-db` flyway init) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install digit-config-service digit3-cms/digit-config-service -n egov -f values.yaml
```

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`digit-config-service` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
digit-config-service:
  default-limit: "200"
```

## Parameters

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `config-service` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | Blank derives `/<ingress.context>` | `""` -> `/config-service` |
| `default-offset` / `default-limit` | `CONFIG_DEFAULT_OFFSET` / `CONFIG_DEFAULT_LIMIT` | Search paging defaults | `0` / `100` |
| `mdms-schema-search-path` | `MDMS_V2_SCHEMA_SEARCH_PATH` | MDMS v2 schema search | `/mdms-v2/schema/v1/_search` |
| `mdms-validation-enabled` | `MDMS_V2_VALIDATION_ENABLED` | Validate config against MDMS schemas | `true` |
| `encryption-service-enabled` | `ENCRYPTION_SERVICE_ENABLED` | Encrypt provider secrets via egov-enc-service | `true` |
| `enc-encrypt-endpoint` / `enc-decrypt-endpoint` | `EGOV_ENC_ENCRYPT_ENDPOINT` / `_DECRYPT_ENDPOINT` | egov-enc-service paths | `/egov-enc-service/crypto/v1/_encrypt` / `_decrypt` |
| `http-connection-timeout` / `http-read-timeout` | `HTTP_CLIENT_CONNECTION_TIMEOUT` / `_READ_TIMEOUT` | Outbound HTTP (ms) | `30000` / `60000` |
| `topic-audit` | `KAFKA_TOPIC_AUDIT` | Audit topic | `egov.core.audit` |
| `tracer-*` | `TRACER_OPENTRACING_ENABLED`, `TRACER_DETAILED_TRACING_ENABLED`, `TRACER_ERRORS_SEND_TO_KAFKA` | Legacy tracer | `false` |
| `heap` / `java-args` | `JAVA_OPTS` / `JAVA_ARGS` | JVM options | `-Xmx192m -Xms192m` / monitoring profile |
| `memory_limits` | - | Container memory limit | `1024Mi` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_*`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, timezone, pool | `common` java block (`egov-config`, secret `db`) |
| `STATE_LEVEL_TENANT_ID` / `KAFKA_BROKER_HOST` | `egov-config`: `egov-state-level-tenant-id` / `kafka-brokers` |
| `MDMS_V2_HOST` / `EGOV_ENC_HOST` | `egov-service-host`: `mdms-service-v2` / `egov-enc-service` |
| `DB_URL` / `FLYWAY_*` | `egov-config`, secret `db` (init container) |

## Source

- Chart: [`charts/digit3/cms/digit-config-service`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/digit-config-service)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/digit-config-service/values.yaml)
