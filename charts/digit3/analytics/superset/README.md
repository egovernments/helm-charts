# superset

Apache Superset for DIGIT analytics: gunicorn web behind an nginx sidecar,
Celery worker and beat, and an init job that runs the database migrations. The
image ships the Postgres and ClickHouse drivers, and GitHub OAuth with
team-to-role mapping is optional.

## Install

```bash
helm repo add digit3-analytics https://helm.digit.org/digit3/analytics
helm repo update
helm install superset digit3-analytics/superset -f <environment>.yaml
```

With Argo CD, point an Application at the chart and pass the environment file
as a value file. No CLI flags are needed.

## Environment overrides

Every value can be overridden from the environment file under a top-level
`superset:` key, the same convention the DIGIT charts use:

```yaml
global:
  domain: health-demo-v2.digit.org
superset:
  namespace: analytics
  database:
    host: <server>.postgres.database.azure.com
  redis:
    celeryDb: "2"
    resultsDb: "3"
  initJob:
    adminEmail: admin@example.org
  ingress:
    enabled: true
    hostName: analytics.health-demo-v2.digit.org
```

`templates/_helpers.tpl` (`superset.init`) merges that block over
`values.yaml` in every template. Empty strings, `false` and `0` override as
expected. Maps merge key by key, and lists (`tolerations`, `extraEnvFrom`, …)
replace the default whole.

## Login

| `auth.oauth.enabled` | `auth.passwordLogin.enabled` | Login page |
|---|---|---|
| `false` | (ignored) | Username and password only (Superset's own page) |
| `true` | `true` (default) | A **Sign in with GitHub** button and a username/password form on one page |
| `true` | `false` | GitHub only |

GitHub sign-in is scoped by `auth.oauth.githubOrg`, and `auth.oauth.teamRoles`
maps teams to roles. A user in none of the mapped teams is refused.

A password sign-in checks the password against Superset's own users. Those are
the accounts an admin creates under *Settings → List Users*, where the form
requires a password, and the `initJob` admin. Users who registered through
GitHub have no password and must use GitHub.

Both can be offered on one page because `AUTH_TYPE` stays `AUTH_OAUTH`, so the
GitHub flow, team mapping and self-registration are unchanged. `/login/` is
replaced by `files/login.html`, which the ConfigMap ships next to
`superset_config.py`, and the password form posts to that same route. Superset's
React login page can't do this: it shows either the provider buttons or the
password form, depending on `AUTH_TYPE`.

## Prerequisites

1. **Metadata database.** Create an empty Postgres database and role, for example `superset`.
2. **Secret.** Create the secret named by `secrets.name` (default `superset-secret`)
   in `namespace`, or set `secrets.create: true` and pass `secrets.data`. Keys:

   | Key | Required | Notes |
   |---|---|---|
   | `SECRET_KEY` | yes | Flask secret key. Keep it stable: it encrypts stored DB passwords. |
   | `SQLALCHEMY_DATABASE_URI` | yes | `postgresql://user:pass@host:5432/superset` |
   | `DB_HOST`, `DB_PORT` | yes | Used by the wait-for-postgres init container. |
   | `REDIS_HOST`, `REDIS_PORT` | yes | Used by the wait-for-redis init container. |
   | `REDIS_PASSWORD` | no | Set it when Redis requires auth. |
   | `ADMIN_PASSWORD` | no | When set, the init job creates `initJob.adminUsername` (needs `initJob.adminEmail`). |
   | `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET` | with `auth.oauth.enabled` | GitHub OAuth app. The callback URL is `https://<host><contextPath>/oauth-authorized/github`. |

## Compared with the upstream apache/superset chart

The upstream chart (`apache.github.io/superset`, 0.22.x) was the reference.
This chart now covers these upstream features:

| Upstream | Here |
|---|---|
| `supersetNode` / `supersetWorker` / `supersetCeleryBeat` | `web` / `worker` / `beat` |
| `init` (db upgrade, init, create admin, examples) | `initJob`: an Argo CD Sync hook that is safe to rerun, and creates the admin only when it is missing |
| `bootstrapScript` (pip installs at pod start) | Drivers baked into the image instead (see `image/`), so pods start without PyPI |
| `configOverrides` | `configOverrides` |
| `featureFlags` | `features` and `extraFeatureFlags` |
| `extraEnv`, `envFromSecrets` | `extraEnv`, `extraEnvFrom` |
| `imagePullSecrets`, `nodeSelector`, `tolerations`, `affinity`, `priorityClassName` | Same names, applied to every pod |
| `supersetNode.autoscaling`, `supersetWorker.autoscaling` | `web.autoscaling`, `worker.autoscaling` |
| `supersetNode.podDisruptionBudget` | `web.podDisruptionBudget` |
| Redis password | `REDIS_PASSWORD` in the secret |
| `ingress` | `ingress`, with `hostName` or `global.domain` and cert-manager TLS |

These are additions not in upstream:

- GitHub OAuth with org and team→role mapping (`auth.oauth`), optionally next to username/password login on the same page (`auth.passwordLogin`).
- An nginx sidecar that serves static assets and preserves the `X-Forwarded-*` headers from the ingress.
- Config checksums that roll the pods when the rendered config changes.

These upstream features are intentionally left out:

- **Bundled Postgres/Redis:** use the platform Postgres and the backbone Redis.
- **Celery Flower, the websocket server for GAA, and the MCP server:** not used.
- **`httproute`.**

## Image

`egovio/superset:<tag>` is built from `image/Dockerfile`, which takes the pinned
`ghcr.io/care-ecosystem/superset` base (Superset master, ahead of 6.1.0) and
bakes in `psycopg2-binary`, `clickhouse-connect`, `lz4`, `backports.zstd`,
`authlib` and `joserfc`. See `image/README.md` to rebuild and verify it. Because
the base is newer than Superset 6.1.0, a 6.1.0 metadata database upgrades in
place: the init job's `superset db upgrade` migrates it forward.

## Source

Migrated from `egovernments/DIGIT-DevOps` (`unified-dev`)
`deploy-as-code/helm/charts/care/superset`, and made environment-neutral:

- The namespace, branding, admin email and context path are no longer CARE-specific.
- The image tag and pull secrets are configurable.
- The `digit3/common` ingress was replaced with a self-contained template.
