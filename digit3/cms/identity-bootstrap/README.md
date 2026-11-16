# identity-bootstrap

Idempotent one-time setup for the CMS identity stack, run as a Helm `post-install` /
`post-upgrade` hook Job. It does the identity prerequisites that would otherwise be manual:

1. **secrets** - creates `identity-secrets` and `pgr-onboarding`, generating every missing key and
   never rewriting a present one.
2. **database** - creates Keycloak's role and database on the platform PostgreSQL (only when
   Keycloak cannot already sign in).
3. **accounts** - creates the `ACCOUNT_ADMIN` employee `identity-bff` signs in as and the PGR
   onboarding provisioner through egov-user, then verifies both can sign in with their roles.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application (standalone - no `common` dependency) |
| **Images** | `curlimages/curl:8.10.1`, `postgres:16-alpine` |

## Install

Install it **before** Keycloak and `identity-bff`; Helm waits for the hook to finish.

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install identity-bootstrap digit3-cms/identity-bootstrap -n egov -f values.yaml
```

Then wire the consumers to the Secrets it owns:

```yaml
identity-bff:
  secret:
    existingSecret: identity-secrets
pgr-services:
  onboarding:
    secretName: pgr-onboarding
```

## Permissions

A hook-scoped ServiceAccount/Role/RoleBinding (deleted when the hooks finish) may `create`
Secrets and `get`/`patch` only the two it owns. The Job runs as a non-root user, and passwords
never appear on a command line (they are passed via stdin or environment).

## Parameters

| Key | Description | Default |
|---|---|---|
| `images.curl` / `images.psql` | Job images | `curlimages/curl:8.10.1` / `postgres:16-alpine` |
| `identitySecret` | Secret for Keycloak + identity-bff | `identity-secrets` |
| `onboardingSecret` | Secret for pgr-services onboarding (values copied from `identitySecret`) | `pgr-onboarding` |
| `smtpAuth` | Require operator-supplied `keycloak-smtp-password` | `false` |
| `googleSignin` | Require operator-supplied `keycloak-google-client-secret` | `false` |
| `keycloakDb.host` | Keycloak DB host; empty = `egov-config/db-host` (with `db-name`) | `""` |
| `keycloakDb.port` / `database` / `username` | Keycloak DB | `5432` / `keycloak` / `keycloak` |
| `platformDbSecret` / `platformDbUserKey` / `platformDbPasswordKey` | Postgres user that creates the role/DB | `db` / `username` / `password` |
| `dbSslMode` | `PGSSLMODE` | `prefer` |
| `egovUserHost` | egov-user, in-cluster (never via the gateway) | `http://egov-user.egov:8080` |
| `adminUsername` / `adminTenant` / `adminMobile` | identity-bff's ACCOUNT_ADMIN employee; empty tenant = state tenant | `IDENTITY_ADMIN` / `""` / `7777777776` |
| `provisionerUsername` / `provisionerTenant` / `provisionerMobile` | PGR onboarding provisioner | `PGR_PROVISIONER` / `""` / `7777777777` |
| `activeDeadlineSeconds` | Ceiling for the whole Job (keep below the helm `--timeout`) | `900` |

Keep `adminUsername` / `adminTenant` in step with identity-bff's `config.DIGIT_ADMIN_USERNAME` /
`DIGIT_ADMIN_TENANT_ID`, and the provisioner with pgr-services' `onboarding.provisioner*`.

## Generated Secret keys

`identity-secrets`: `keycloak-db-password`, `keycloak-admin-password`,
`keycloak-{bff,admin,employee,citizen,magic-link}-client-secret`, `identity-control-plane-token`,
`identity-onboarding-token`, `identity-citizen-otp-secret`, `identity-session-introspection-token`,
`digit-admin-password`, `digit-provisioner-password`.
`pgr-onboarding`: `provisioner-password`, `onboarding-token` (copied), `oauth-client-secret` (empty).

## Sourced from the platform (not set in this chart)

| Value | Source |
|---|---|
| DB host / name (when `keycloakDb.host` is empty) | `egov-config`: `db-host` / `db-name` |
| State tenant | `egov-config`: `state-level-tenant-id` |
| Platform DB credentials | secret `db` |

## Source

- Chart: [`charts/digit3/cms/identity-bootstrap`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/identity-bootstrap)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/identity-bootstrap/values.yaml)
