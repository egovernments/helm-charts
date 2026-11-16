# pgr-services

Public Grievance Redressal (PGR): the complaint service of the Citizen Complaint Resolution
System. It files, assigns, resolves and reopens complaints, publishes notification events,
serves the public analytics dashboard and runs the optional self-service tenant onboarding.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `pgr-services` (+ `pgr-services-db` flyway init) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install pgr-services digit3-cms/pgr-services -n egov -f values.yaml
```

> **Release together.** `pgr-services` and `novu-bridge` must run the same build (thin
> notification events). Upgrade `novu-bridge` **first**: an older bridge dead-letters thin events.

## How configuration works

Every parameter below is overridable. Values resolve lowest to highest priority:

1. **common chart defaults** - shared platform defaults.
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named `pgr-services` in `environments/<env>.yaml` (wins).

```yaml
# environments/<env>.yaml
pgr-services:
  notification-email-enabled: "true"
  memory_limits: "1Gi"
```

Shared infrastructure (database, Kafka, timezone, Tomcat, pool) comes from `common`'s java block
(`egov-config` + the `db` secret), not from this chart.

## Deployment behaviour

| Key | Description | Default |
|---|---|---|
| `deploymentStrategy` | Applied on top of `common.deployment`. `Recreate` so an old and a new pod never publish the same notification twice | `{type: Recreate}` |
| `replicas` | Pods | `1` |
| `heap` / `java-args` / `java-enable-debug` | `JAVA_OPTS` / `JAVA_ARGS` / `JAVA_ENABLE_DEBUG` | `-Xmx192m -Xms192m` / monitoring profile / `false` |
| `memory_limits` | Container memory limit | `768Mi` |

## Parameters

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route. The app hardcodes `server.servlet.context-path`, so this must stay `pgr-services` | `pgr-services` |
| `publicAnalytics.enabled` | - | Exact-path anonymous routes for the public dashboard, straight to the service | `true` |
| `publicAnalytics.paths` | - | Paths under `/pgr-services/v2/analytics/public/` | `packs, _query, catalog/_search, _options` |
| `publicAnalytics.limitRpm` / `maxBodySize` | - | Site-wide nginx flood guard / body limit | `6000` / `1m` |

### Service

| Key | Env var | Description | Default |
|---|---|---|---|
| `kafka-consumer-group` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | Kafka consumer group | `egov-pgr-services` |
| `mdms-host` | `EGOV_MDMS_HOST` | Blank -> `egov-service-host/mdms-service-v2` | `""` |
| `mdms-search-endpoint` | `EGOV_MDMS_SEARCH_ENDPOINT` | MDMS search path | `/mdms-v2/v1/_search` |
| `gateway-host` | `EGOV_GATEWAY_HOST` | Origin for workspace-settings writes (admin's token). Blank = unset; workspace writes then fail | `http://gateway.egov:8080` |
| `security-basic-enabled` / `management-security-enabled` | `SECURITY_BASIC_ENABLED` / `MANAGEMENT_SECURITY_ENABLED` | Spring security toggles | `false` |
| `tracer-request-logging-enabled` | `TRACER_REQUESTLOGGINGENABLED` | Tracer request logging | `true` |
| `time-before-closing-complaint` | `PGR_COMPLAIN_IDLE_TIME` | Idle-close window (ms). Leave blank: MDMS `REOPENSLA` per tenant applies | `""` (unset) |
| `central-instance-enabled` | `IS_ENVIRONMENT_CENTRAL_INSTANCE`, `STATE_LEVEL_TENANTID_LENGTH` | Central-instance (multi-state) mode | `false` |
| `pgr-ismultischema-enabled` / `pgr-schemas` | `SCHEMA_NAME` (init) | Migrate into state-level schemas | `false` / `""` |

### Notifications

| Key | Env var | Default |
|---|---|---|
| `notification-sms-enabled` | `NOTIFICATION_SMS_ENABLED` | `true` |
| `notification-email-enabled` | `NOTIFICATION_EMAIL_ENABLED` | `false` |
| `new-complaint-enabled` | `NEW_COMPLAINT_ENABLED` | `true` |
| `reassign-complaint-enabled` | `REASSIGN_COMPLAINT_ENABLED` | `true` |
| `reopen-complaint-enabled` | `REOPEN_COMPLAINT_ENABLED` | `true` |
| `comment-by-employee-notif-enabled` | `COMMENT_BY_EMPLOYEE_NOTIF_ENABLED` | `false` |
| `notification-allowed-status` | `NOTIFICATION_ALLOWED_ON_STATUS` | `open,assigned,rejected,resolved` |
| `pgr-app-playstore-link` | `EGOV_PGR_APP_PLAYSTORE_LINK` | `https://goo.gl/ArDyQ1` |
| `usr-events-notification-enabled` | `EGOV_USR_EVENTS_NOTIFICATION_ENABLED` | `true` |
| `usr-events-create-topic` | `EGOV_USR_EVENTS_CREATE_TOPIC` | `persist-user-events-async` |
| `usr-events-rate-link` / `usr-events-reopen-link` | `EGOV_USR_EVENTS_RATE_LINK` / `..._REOPEN_LINK` | citizen OTP-login deep links |
| `usr-events-rate-code` / `usr-events-reopen-code` | `EGOV_USR_EVENTS_RATE_CODE` / `..._REOPEN_CODE` | `RATE` / `REOPEN` |

### Self-service onboarding (off by default)

| Key | Env var | Description | Default |
|---|---|---|---|
| `onboarding.runnerEnabled` | `PGR_ONBOARDING_RUNNER_ENABLED` | Turn on the signup runner | `false` |
| `onboarding.identityBffUrl` | `PGR_ONBOARDING_IDENTITY_BFF_URL` | Identity BFF base URL (required when on) | `""` |
| `onboarding.provisionerUsername` / `provisionerTenantId` | `DIGIT_PROVISIONER_USERNAME` / `_TENANT_ID` | Provisioner employee on a ROOT tenant (required when on) | `""` |
| `onboarding.oauthClientId` | `DIGIT_OAUTH_CLIENT_ID` | OAuth client | `egov-user-client` |
| `onboarding.secretName` | - | Secret with `provisioner-password`, `onboarding-token`, `oauth-client-secret` (optional) | `pgr-onboarding` |
| `onboarding.notificationDefaultsEnabled` | `PGR_ONBOARDING_NOTIFICATION_DEFAULTS_ENABLED` | Seed notification schemas/templates for a new workspace | `true` |
| `onboarding.notificationAccountUrl` | `PGR_ONBOARDING_NOTIFICATION_ACCOUNT_URL` | novu-bridge internal admin API. Blank = step disabled | `""` |
| `onboarding.notificationAccountSecretName` | `PGR_ONBOARDING_NOTIFICATION_ACCOUNT_TOKEN` | Secret holding novu-bridge's `internal-admin-token` | `novu-bridge-tenant-accounts` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_*`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `APP_TIMEZONE`, Tomcat/pool | `common` java block (`egov-config`, secret `db`) |
| `STATE_LEVEL_TENANT_ID` / `EGOV_UI_APP_HOST` | `egov-config`: `state-level-tenant-id` / `egov-services-fqdn-name` |
| `EGOV_*_HOST` | `egov-service-host`: `egov-idgen`, `egov-url-shortening`, `egov-workflow-v2`, `mdms-service-v2`, `egov-localization`, `egov-accesscontrol`, `egov-searcher`, `egov-filestore`, `egov-user`, `boundary-service`, `egov-hrms`, `egov-location`, `egov-enc-service` (onboarding only) |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/cms/pgr-services`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/pgr-services)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/pgr-services/values.yaml)
