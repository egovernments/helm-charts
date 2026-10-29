# cluster-configs

Shared platform configuration for a DIGIT 3.0 product. One install per product, scoped to the product namespace, renders the ConfigMaps, secret shells, namespaces, and root ingress that every service in that product consumes.

| | |
|---|---|
| **Project / Module** | `digit3` / `cluster-configs` |
| **Chart type** | application |

## What it renders

Objects render dynamically from the lists in `values.yaml`; adding an entry (here or in an env values file) adds an object with no chart change. Items carry only `name` + `namespace` by default, so nothing renders for an item until a values file supplies `data`.

| Resource | Template | Purpose |
|---|---|---|
| `egov-config` ConfigMap | `templates/configmaps.yaml` | Shared platform config read by all services: DB datasource + Hikari pool, Kafka brokers, Redis, OpenTelemetry endpoints, SMTP/SMS hosts, Flyway location, Keycloak. Consumed by common's `extraEnv.java` / `extraEnv.otel` and by each service's own env. |
| `egov-service-host` ConfigMap | `templates/configmaps.yaml` | Inter-service hostnames (one key per service, e.g. `filestore`, `workflow`), so services address each other by cluster URL. |
| Secret shells | `templates/secrets.yaml` | Named, empty secret placeholders (`db`, `minio`, `kc-admin`, and so on). The chart defines name, namespace, and type; the secret data is supplied per environment (values file or out of band). Values are base64-encoded automatically; `docker-registry-secret` is rendered as a `kubernetes.io/dockerconfigjson` secret. |
| Namespaces | `templates/namespaces.yaml` | Optional. Created only when `cluster-configs.namespaces.create: true`; one Namespace per entry in `values`. |
| Root ingress | `templates/root-ingress.yaml` | Optional. Created only when `cluster-configs.root-ingress.enabled: true`; routes the domain root `/` to a UI service. |

The ConfigMap and Secret renderers iterate their respective maps and emit one object per namespace in each item's `namespace` list. A helper (`cc.checkUnique`) fails the render if two items resolve to the same name in the same namespace, or if an item mixes a `data:` map with top-level data keys.

## Install

```bash
helm repo add digit3-cluster-configs https://helm.digit.org/digit3/cluster-configs
helm repo update
helm install cluster-configs digit3-cluster-configs/cluster-configs -n digit3-core -f values.yaml
```

## Parameters

All keys live under the top-level `cluster-configs:` map in `values.yaml`.

### Namespaces

| Key | Description | Default |
|---|---|---|
| `namespaces.create` | Create the listed namespaces | `false` |
| `namespaces.values` | Namespace names to create | `[]` |

### Root ingress

| Key | Description | Default |
|---|---|---|
| `root-ingress.enabled` | Render the root `/` ingress | `false` |
| `root-ingress.name` | Ingress name | `root-ingress` |
| `root-ingress.namespace` | Ingress namespace | `egov` |
| `root-ingress.cert-issuer` | cert-manager cluster issuer | `letsencrypt-staging` |
| `root-ingress.appRoot` | App-root path annotation target | `digit-ui` |
| `root-ingress.serviceName` | Backend service for `/` | `digit-ui` |
| `root-ingress.servicePort` | Backend service port | `80` |

### egov-config ConfigMap

Rendered into namespace `egov` by default (`configmaps.egov-config.namespace`), name `egov-config`.

#### Database and Hikari pool

| Key | Description | Default |
|---|---|---|
| `db-host` | PostgreSQL host | `postgresql.backbone` |
| `db-port` | PostgreSQL port | `5432` |
| `db-name` | Database name | `postgres` |
| `db-ssl-mode` | JDBC SSL mode | `disable` |
| `db-url` | JDBC datasource URL | `jdbc:postgresql://postgresql.backbone:5432/postgres?sslmode=disable` |
| `db-pool-max-size` | Hikari max pool size | `20` |
| `db-pool-min-idle` | Hikari minimum idle | `2` |
| `db-pool-connection-timeout-ms` | Hikari connection timeout (ms) | `1000` |

#### Flyway

| Key | Description | Default |
|---|---|---|
| `flyway-locations` | Migration script location | `filesystem:/flyway/sql` |

#### Kafka

| Key | Description | Default |
|---|---|---|
| `kafka-brokers` | Kafka bootstrap servers | `kafka-controller-headless.backbone:9092` |

#### Platform and web server

| Key | Description | Default |
|---|---|---|
| `timezone` | Application timezone | `Asia/Kolkata` |
| `spring-jpa-show-sql` | Log SQL from JPA | `false` |
| `server-tomcat-threads-max` | Embedded Tomcat max threads | `200` |
| `server-tomcat-max-connections` | Embedded Tomcat max connections | `8192` |
| `platform-tenant` | Default platform tenant | `default` |

#### OpenTelemetry

| Key | Description | Default |
|---|---|---|
| `otel-enabled` | Enable tracing | `false` |
| `otel-exporter-otlp-endpoint` | Bare `host:port` exporter endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`) | `open-telemetry-opentelemetry-collector.monitoring:4318` |
| `otel-tracing-export-otlp-endpoint` | Full traces URL (`MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT`) | `http://open-telemetry-opentelemetry-collector.monitoring:4318/v1/traces` |

#### Redis

| Key | Description | Default |
|---|---|---|
| `redis-address` | Redis `host:port` | `redis.backbone:6379` |
| `redis-host` | Redis host | `redis.backbone` |
| `redis-port` | Redis port | `6379` |

#### SMTP and SMS

Credentials stay in secrets; only hosts/URLs live here.

| Key | Description | Default |
|---|---|---|
| `smtp-host` | SMTP host | `smtp.gmail.com` |
| `smtp-port` | SMTP port | `587` |
| `mail-sender-name` | Mail sender display name | `eGov Foundation` |
| `sms-provider-url` | SMS provider URL | `http://api.smscountry.com/SMSCwebservice_bulk.aspx` |

#### Keycloak

| Key | Description | Default |
|---|---|---|
| `keycloak-enabled` | Enable Keycloak integration | `false` |
| `keycloak-base-url` | Keycloak base URL | `http://keycloak.keycloak.svc.cluster.local:8080/keycloak` |
| `keycloak-db-url` | Keycloak database JDBC URL | `jdbc:postgresql://postgresql.backbone:5432/keycloak` |

### egov-service-host ConfigMap

`configmaps.egov-service-host.data` is a map of service name to cluster URL. Rendered into namespace `egov` by default, name `egov-service-host`. Defaults:

| Key | Default |
|---|---|
| `account` | `http://account.egov:8080/` |
| `apportion` | `http://apportion.egov:8080/` |
| `billing` | `http://billing.egov:8080/` |
| `boundary` | `http://boundary.egov:8080/` |
| `boundary-management` | `http://boundary-management.egov:8080/` |
| `employee` | `http://employee.egov:8080/` |
| `filestore` | `http://filestore.egov:8080/` |
| `idgen` | `http://idgen.egov:8080/` |
| `individual` | `http://individual.egov:8080/` |
| `localization` | `http://localization.egov:8080/` |
| `mdms-v2` | `http://mdms-v2.egov:8080/` |
| `notification` | `http://notification.egov:8080/` |
| `notify` | `http://notify.egov:8080/` |
| `otp` | `http://otp.digit3-core:8080/` |
| `pg-service` | `http://pg-service.egov:8080/` |
| `registry` | `http://registry.egov:8080/` |
| `template-config` | `http://template-config.egov:8080/` |
| `url-shortener` | `http://url-shortener.egov:8080/` |
| `workflow` | `http://workflow.egov:8080/` |
| `vault` | `http://vault.vault:8200/` |

### Secrets

Each entry under `secrets` is a shell: `name`, `namespace` (list), optional `type`, and `data` supplied per environment. A secret renders only once it has `data`.

| Secret | Expected keys | Default namespace | Type |
|---|---|---|---|
| `db` | `username`, `password`, `flyway-username`, `flyway-password` | `egov` | Opaque |
| `minio` | `accesskey`, `secretkey` | `egov` | Opaque |
| `kc-admin` | `username`, `password` | `egov` | Opaque |
| `kc-db` | `username`, `password` | `keycloak` | Opaque |
| `vault-approle` | `role-id`, `secret-id` | `egov` | Opaque |
| `hmac-secret` | `hmac-secret` | `egov` | Opaque |
| `employee-iam-secret` | `employee-iam-client-secret` | `egov` | Opaque |
| `citizen-broker-secret` | `citizen-broker-client-secret` | `egov` | Opaque |
| `egov-notification-mail` | `mailsenderusername`, `mailsenderpassword` | `egov` | Opaque |
| `egov-notification-sms` | `username`, `password`, optional `secure-key` | `egov` | Opaque |
| `egov-pg-service` | `stripe-secret-key` + optional payment-gateway keys | `egov` | Opaque |
| `docker-registry-secret` | `dockerconfigjson` (used as `imagePullSecrets` by all services) | `egov` | `kubernetes.io/dockerconfigjson` |

## Source

- Chart: [`charts/digit3/cluster-configs`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cluster-configs)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cluster-configs/values.yaml)
