# project-factory

Campaign setup service (Node 20 / TypeScript): generates projects, facilities, resources
and boundary data from a campaign definition.

| | |
|---|---|
| **Project / Module** | `digit3` / `hcm` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `project-factory` (+ `project-factory-db` flyway init image) |
| **Context path** | `/project-factory` (env `CONTEXT_PATH`) |
| **Port** | `8080` (`APP_PORT`; the Dockerfile `EXPOSE 3000` is stale) |

## Install

```bash
helm repo add digit3-hcm https://helm.digit.org/digit3/hcm
helm repo update
helm install project-factory digit3-hcm/project-factory -n hcm-3-0 -f values.yaml
```

## Node service note

This is the first Node chart in `digit3`. It is **not** `appType: java`, so `common` does not
inject the Java datasource/Kafka/pool env; this chart wires the app's own `process.env`
config (the app reads everything from the environment at startup). A `resources` block is set
explicitly (Node heap is 3072m, so memory limit is 4Gi).

## Required before it starts

| Need | Where | Note |
|---|---|---|
| `BASE_SECRET` | `project-factory` Secret, key `base-secret` | **app throws at startup if missing** - create it in `environments/digit-3.0/hcm-3.0-secrets.yaml` |
| DB password | `db` Secret, key `password` | own Postgres (`eg_cm_campaign_*` in schema `egov`) |
| Kafka broker | `egov-config`, key `kafka-brokers` | injected |

## Parameters

| Key | Env var | Default |
|---|---|---|
| `app-port` | `APP_PORT` | `8080` |
| `egov-host` | `EGOV_HOST` | `https://unified-dev.digit.org/` |
| `db-host` | `DB_HOST` | `postgresql.backbone-3-0` |
| `db-schema` | `DB_SCHEMA` | `egov` |
| `module-name` | `MODULE_NAME` | `HCM-ADMIN-CONSOLE` |
| `locale` | `LOCALE` | `en_MZ` |
| `user-main-boundary` | `USER_MAIN_BOUNDARY` | `mz` |
| `cmp-idgen-format` | `CMP_IDGEN_FORMAT` | `CMP-[cy:yyyy-MM-dd]-[SEQ_EG_CMP_ID]` |

The app reads ~18 `EGOV_*_HOST` URLs for other services; their defaults already point at
`unified-dev.digit.org`, correct for that cluster. Override them in the environment file to
route in-cluster.

## Verify in the environment

`healthChecks` is **disabled** by default: the app uses `@godaddy/terminus` and the exact
probe route is not declared in config. Confirm the terminus health route and enable
`healthChecks` with that path before relying on readiness. Country/locale values
(`locale`, `user-main-boundary`, attendance topics) are environment-specific.
