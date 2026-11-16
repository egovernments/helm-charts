# xstate-chatbot

WhatsApp chatbot (Node.js, XState) that lets citizens file and track complaints. It creates
citizens and files PGR complaints through a dedicated service account.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `xstate-chatbot` (+ `xstate-chatbot-db` flyway init) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install xstate-chatbot digit3-cms/xstate-chatbot -n egov -f values.yaml
```

Create the `chatbot` and `egov-location` Secrets first (see below). **Keep `replicas: 1`**:
conversation state is last-writer-wins per citizen, so a second replica can lose a step.

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`xstate-chatbot` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
xstate-chatbot:
  default-country-code: "+258"
  default-mobile-regex: "^8[0-9]{8}$"
  mobile-number-length: "9"
```

## Parameters

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `xstate-chatbot` |
| `ingress.ip-whitelist` | - | `whitelist-source-range` for the webhook | `0.0.0.0/0` |
| `context-path` | `CONTEXT_PATH` | Blank derives `/<ingress.context>` | `""` -> `/xstate-chatbot` |
| `default-country-code` | `DEFAULT_COUNTRY_CODE` | Fallback when MDMS has no number rule (required) | `+91` |
| `default-mobile-regex` | `DEFAULT_MOBILE_REGEX` | Fallback mobile regex (required) | `^[6-9][0-9]{9}$` |
| `mobile-number-length` | `MOBILE_NUMBER_LENGTH` | Fallback length (required) | `10` |
| `flow-reset-keywords` | `FLOW_RESET_KEYWORDS` | Messages that restart the flow | `Hi,Hey,Hello,mseva,Start,missedCall` |
| `tracer-opentracing-enabled` | `TRACER_OPENTRACING_ENABLED` | Legacy tracer | `true` |
| `whatsapp-provider` | `WHATSAPP_PROVIDER` | Optional; blank = unset | `""` |
| `valuefirst-whatsapp-number` | `WHATSAPP_BUSINESS_NUMBER` | Optional | `""` |
| `valuefirst-send-message-url` | `VALUEFIRST_SEND_MESSAGE_URL` | Optional | `""` |
| `valuefirst-notification-*-templateid` | `VALUEFIRST_NOTIFICATION_*_TEMPLATEID` | Optional: assigned, resolved, rejected, reassigned, commented, welcome, root | `""` |
| `contact-card-whatsapp-number` / `-name` | `CONTACT_CARD_WHATSAPP_NUMBER` / `_NAME` | Optional | `""` |
| `bill-supported-modules` | `BILL_SUPPORTED_MODULES` | Optional | `""` |
| `pgr-version` / `pgr-update-topic` | `PGR_VERSION` / `PGR_UPDATE_TOPIC` | Optional | `""` |
| `information-image-filestoreid` | `INFORMATION_IMAGE_FILESTORE_ID` | Optional | `""` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `KAFKA_BOOTSTRAP_SERVER`, `DB_HOST`, `DB_NAME` | `egov-config`: `kafka-brokers`, `db-host`, `db-name` |
| `DB_USER` / `DB_PASSWORD` | secret `db`: `username` / `password` |
| `EGOV_SERVICES_HOST`, `EXTERNAL_HOST`, `URL_SHORTNER_HOST` | `egov-config`: `egov-services-fqdn-name` |
| `ROOT_TENANTID` | `egov-config`: `egov-state-level-tenant-id` |
| `USER_SERVICE_HOST` | `egov-service-host`: `egov-user-chatbot` |
| `MDMS_HOST`, `PGR_SERVICE_HOST`, `LOCALIZATION_SERVICE_HOST`, `EGOV_FILESTORE_SERVICE_HOST`, `BILL_SERVICE_HOST`, `COLLECTION_SERVICE_HOST` | `egov-service-host`: `zuul` (the gateway) |
| `USER_SERVICE_ACCOUNT_USERNAME` / `_PASSWORD` | secret `chatbot`: `service-account-username` / `service-account-password` |
| `VALUEFIRST_USERNAME` / `_PASSWORD` | secret `chatbot`: `valuefirst-username` / `valuefirst-password` |
| `GOOGLE_MAPS_API_KEY` | secret `egov-location`: `gmapskey` |

## Source

- Chart: [`charts/digit3/cms/xstate-chatbot`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/xstate-chatbot)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/xstate-chatbot/values.yaml)
