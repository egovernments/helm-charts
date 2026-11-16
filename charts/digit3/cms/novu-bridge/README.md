# novu-bridge

Consumes CMS domain events (complaints, core SMS such as login OTPs) from Kafka, resolves them
against user preferences, localization and MDMS, and dispatches SMS / email / WhatsApp through
Novu. Also hosts the provider-management and delivery-receipt APIs.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `novu-bridge` (+ `novu-bridge-db` flyway init) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install novu-bridge digit3-cms/novu-bridge -n egov -f values.yaml
```

> Install/upgrade **before** `pgr-services`, from the same build. Needs a Novu deployment
> (`novu-base-url`) and its API key.

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`novu-bridge` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
novu-bridge:
  core-sms-country-code: "+254"
  tenant-accounts:
    enabled: true
```

## Parameters

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.gateway` | - | Not behind the gateway; exact paths go straight to the bridge | `false` |
| `ingress.publicPaths` | - | Exact public routes under `/<context>/novu-adapter/v1/`. Everything else stays internal | logs, integrations, preferences, providers/..., receipts/..., contract/..., config/source, dispatch/_resolve |
| `ingress.context` | - | Route segment | `novu-bridge` |
| `context-path` | `SERVER_SERVLET_CONTEXT_PATH` | Blank derives `/<ingress.context>` | `""` -> `/novu-bridge` |

### Kafka and messages

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-input-topics` | `NOVU_BRIDGE_KAFKA_INPUT_TOPICS` | Event topics (the envelope's eventType decides handling) | `complaints.domain.events,notifications.events` |
| `core-sms-topic` | `NOVU_BRIDGE_CORE_SMS_TOPIC` | DIGIT core SMS topic | `egov.core.notification.sms` |
| `dlq-topic` | `NOVU_BRIDGE_KAFKA_DLQ_TOPIC` | Dead-letter topic | `novu-bridge.dlq` |
| `max-retries` | `NOVU_BRIDGE_MAX_RETRIES` | Retries before DLQ | `3` |
| `default-locale` | `NOVU_BRIDGE_DEFAULT_LOCALE` | Fallback locale | `en_IN` |
| `core-sms-country-code` | `NOVU_BRIDGE_CORE_SMS_COUNTRY_CODE` | Last-resort country code; blank skips numbers without one | `""` |
| `dispatch-log-enabled` | `NOVU_BRIDGE_DISPATCH_LOG_ENABLED` | Persist the dispatch log | `true` |
| `receipts-secret` | `NOVU_BRIDGE_RECEIPTS_SECRET` | Delivery-receipt webhook secret | `""` |

### Preferences and lookups

| Key | Env var | Default |
|---|---|---|
| `preference-enabled` | `NOVU_BRIDGE_PREFERENCE_ENABLED` | `true` |
| `preference-check-path` | `NOVU_BRIDGE_PREFERENCE_CHECK_PATH` | `/user-preference/v1/_search` |
| `preference-code` | `NOVU_BRIDGE_PREFERENCE_CODE` | `USER_NOTIFICATION_PREFERENCES` |
| `user-search-path` | `NOVU_BRIDGE_USER_SEARCH_PATH` | `/user/_search` |
| `mdms-search-path` | `NOVU_BRIDGE_MDMS_SEARCH_PATH` | `/mdms-v2/v2/_search` |

### Novu

| Key | Env var | Description | Default |
|---|---|---|---|
| `novu-base-url` | `NOVU_BASE_URL` | Novu API | `http://novu-api.novu:3000` |
| `novu-api-key` | `NOVU_API_KEY` | Blank -> secret `novu-bridge-novu-api-key/api-key` (optional) | `""` |
| `digit-worker-providers` | `NOVU_BRIDGE_DIGIT_WORKER_PROVIDERS` | Must match the Novu worker's DIGIT SMS providers; `global.novuWorkerDigitProviders` wins when set | `true` |
| `provider-admin-tenants` | `NOVU_BRIDGE_PROVIDER_ADMIN_TENANTS` | Extra state tenants whose admins manage providers | `""` |

### Per-tenant Novu accounts (off by default)

| Key | Env var | Description | Default |
|---|---|---|---|
| `tenant-accounts.enabled` | `NOVU_BRIDGE_TENANT_ACCOUNTS_ENABLED` | One Novu organization per root tenant | `false` |
| `tenant-accounts.secretName` | admin email/password, encryption keys, internal tokens | Secret keys: `novu-admin-email`, `novu-admin-password`, `encryption-key`, `encryption-key-previous` (opt.), `internal-admin-token`, `internal-send-token` (opt.) | `novu-bridge-tenant-accounts` |
| `tenant-accounts.environment` | `NOVU_BRIDGE_TENANT_ACCOUNTS_ENVIRONMENT` | Novu environment | `Development` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_*`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, timezone, pool | `common` java block |
| `KAFKA_BROKER_HOST` / `NOVU_BRIDGE_CORE_SMS_DEFAULT_TENANT` | `egov-config`: `kafka-brokers` / `state-level-tenant-id` |
| `NOVU_BRIDGE_PREFERENCE_HOST` / `_USER_HOST` / `_LOCALIZATION_HOST` / `_MDMS_HOST` | `egov-service-host`: `digit-user-preferences-service`, `egov-user`, `egov-localization`, `mdms-service-v2` |
| `DB_URL` / `FLYWAY_*` | `egov-config`, secret `db` (init container) |

## Source

- Chart: [`charts/digit3/cms/novu-bridge`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/novu-bridge)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/novu-bridge/values.yaml)
