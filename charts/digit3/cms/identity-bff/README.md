# identity-bff

The Identity BFF (Node.js): browser sign-in through Keycloak, tenant choice and the user-scoped
DIGIT token, served at `/identity/v1`. Every `digit-ui` and `configurator` sign-in goes through
it, so a CMS deployment without it (and Keycloak) has no working sign-in.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Image** | `identity-bff` (port 3000) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install identity-bff digit3-cms/identity-bff -n egov -f values.yaml \
  --set global.domain=<your-domain> --set secret.existingSecret=<secret-name>
```

Needs: a Keycloak reachable at `identity.keycloak` (service/namespace/port) with the BFF's clients
configured, Redis (`REDIS_HOST`, default `redis.backbone`), and DIGIT's egov-user, egov-otp and
MDMS. The chart **refuses to render** without `global.domain` (or `identity.publicUrl`) and
without the required secrets.

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`identity-bff` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
identity-bff:
  identity:
    keycloak:
      namespace: keycloak
  config:
    IDENTITY_OTP_SENDER: "http"
    IDENTITY_OTP_SENDER_URL: "http://..."
```

Every non-secret BFF setting is under `config:`, keyed by its environment variable; an empty value
is not passed (the BFF's own default applies).

## Parameters

### Identity (derived settings)

| Key | Drives | Default |
|---|---|---|
| `identity.realm` | `KEYCLOAK_*_REALM`, issuer and JWKS URLs | `digit` |
| `identity.publicUrl` | `KEYCLOAK_ISSUER`, `IDENTITY_REDIRECT_URI`, cookie `Secure` | `""` -> `https://<global.domain>` |
| `identity.postLoginRedirect` | `IDENTITY_POST_LOGIN_REDIRECT` | `""` -> `<publicUrl>/configurator/login` |
| `identity.allowedOrigins` | `IDENTITY_ALLOWED_ORIGINS` | `""` -> `<publicUrl>` |
| `identity.keycloak.service` / `namespace` / `httpPort` | `KEYCLOAK_OIDC_BACKCHANNEL_URL`, `KEYCLOAK_JWKS_URI`, `KEYCLOAK_ADMIN_URL` | `keycloak` / release ns / `8180` |

These derived variables are refused under `config:` - set `identity.*` instead.

### Main `config` keys (see `values.yaml` for the full list)

| Key | Description | Default |
|---|---|---|
| `KEYCLOAK_*_CLIENT_ID` | Keycloak client ids (bff, magic-link, employee, citizen, admin) | `digit-identity-bff`, ... |
| `IDENTITY_COOKIE_SAME_SITE` / `IDENTITY_TRUST_PROXY_HOPS` | Session cookie / proxy hops | `Lax` / `1` |
| `IDENTITY_STAFF_CREDENTIAL_MODE` | `rotate` or `derived` | `rotate` |
| `IDENTITY_OTP_SENDER` / `_URL` | Citizen OTP channel (`http` in production, `log` dev only) | `""` (no channel) |
| `CITIZEN_LOGIN_PASSWORD_OTP_FIXED_ENABLED` | Fixed OTP for any number - development only | `false` |
| `IDENTITY_WORKSPACE_TRIAL_DAYS` | Self-serve trial length; `0` = trials off | `0` |
| `DIGIT_GATEWAY_HOST` | Gateway for business calls | `http://gateway.egov:8080` |
| `DIGIT_USER_SERVICE_URL` / `DIGIT_OTP_CREATE_URL` | egov-user / egov-otp directly (never via a public gateway) | in-cluster `egov` URLs |
| `DIGIT_MDMS_*_URL` | MDMS v2 endpoints (via gateway) | `http://gateway.egov:8080/mdms-v2/...` |
| `DIGIT_ADMIN_USERNAME` / `DIGIT_ADMIN_TENANT_ID` | ACCOUNT_ADMIN employee; blank tenant -> `egov-config/state-level-tenant-id` | `ADMIN` / `""` |
| `ONBOARDING_WORKER_ENABLED` / `PGR_ONBOARDING_WORKER_URL` | Onboarding worker | `false` / `http://pgr-services.egov:8080/pgr-services` |
| `REDIS_HOST` / `REDIS_PORT` / `CACHE_PREFIX` | Sessions, leases, audit stream | `redis.backbone` / `6379` / `digit-identity` |

### Secrets

| Key | Description | Default |
|---|---|---|
| `secret.existingSecret` | **Preferred.** A Secret created out of band | `""` |
| `secret.values` | Escape hatch: the chart renders `<release>-secrets`; placeholders are refused | `{}` |

Required keys: `keycloak-bff-client-secret`, `keycloak-admin-client-secret`,
`keycloak-employee-client-secret`, `keycloak-citizen-client-secret`, `digit-admin-password`.
Optional: `keycloak-magic-link-client-secret`, `identity-control-plane-token`,
`identity-session-introspection-token`, `identity-onboarding-token`, `pgr-onboarding-worker-token`,
`identity-citizen-otp-secret`, `identity-credential-keys`, `identity-credential-key-current`,
`digit-provisioner-password`, `identity-surfaces-json`, `novu-bridge-send-token`.

### Health checks

`/livez` (liveness) and `/readyz` (readiness, checks Redis, Keycloak, DIGIT; 12 s timeout), given
in full under `healthChecks` - they are not under the ingress context.

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `DIGIT_ADMIN_TENANT_ID` (when `config.DIGIT_ADMIN_TENANT_ID` is blank) | `egov-config`: `state-level-tenant-id` |
| Keycloak client secrets, admin password, tokens | the Secret above |

## Source

- Chart: [`charts/digit3/cms/identity-bff`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/identity-bff)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/identity-bff/values.yaml)
