# digit-user-preferences-service

Stores each user's notification consent and language settings for the Citizen Complaint
Resolution System. `novu-bridge` consults it before sending a message.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `digit-user-preferences-service` (+ `-db` flyway init) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install digit-user-preferences-service digit3-cms/digit-user-preferences-service -n egov -f values.yaml
```

The in-cluster Service is `digit-user-preferences-service`, which `novu-bridge` reaches through the
`egov-service-host` key of the same name (e.g. `http://digit-user-preferences-service.egov:8080/`).

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`digit-user-preferences-service` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
digit-user-preferences-service:
  db-ssl-mode: "disable"
```

## Parameters

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `user-preference` |
| `context-path` | `SERVER_CONTEXT_PATH` | API base path. Blank derives `/<ingress.context>` | `""` -> `/user-preference` |
| `db-ssl-mode` | `DB_SSL_MODE` | JDBC sslmode, applied as a driver property | `require` |
| `enforce-ownership` | `ENFORCE_OWNERSHIP` | A citizen may only read/write their own record | `true` |
| `privileged-roles` | `PRIVILEGED_ROLES` | Roles that may act on another user's record within their tenant | `SUPERUSER,ACCOUNT_ADMIN,SYSTEM` |
| `flyway-validate-on-migrate` | `FLYWAY_VALIDATE_ON_MIGRATE` (init) | Off: an early migration was edited after release (checksum mismatch) | `false` |
| `heap` / `java-args` | `JAVA_OPTS` / `JAVA_ARGS` | JVM options | `-Xmx192m -Xms192m` / monitoring profile |
| `healthChecks.*ProbePath` | - | Actuator `/health` is at the container **root**, not under the context; probes are given in full | `/health` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `SPRING_DATASOURCE_*`, Kafka, timezone, pool | `common` java block (`egov-config`, secret `db`) |
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: [`charts/digit3/cms/digit-user-preferences-service`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/digit-user-preferences-service)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/digit-user-preferences-service/values.yaml)
