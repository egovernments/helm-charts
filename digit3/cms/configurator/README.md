# configurator

The CMS Configurator: a static SPA (nginx) for tenant onboarding and management. It is served
at `/configurator/` and calls the same-origin gateway with each user's own token.

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Image** | `configurator` (nginx) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install configurator digit3-cms/configurator -n egov -f values.yaml
```

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`configurator` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
configurator:
  runtimeConfig:
    STATE_TENANT_ID: "pg"
```

The chart renders `runtimeConfig` into `/configurator/config.js` (a ConfigMap mounted over the
image's copy), so one image serves every deployment. A config change rolls the pods via a
`checksum/config` annotation.

## Parameters

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Route segment (plain nginx ingress, not the gateway) | `configurator` |
| `httpPort` | Container + service port | `80` |
| `healthChecks.*ProbePath` | common prepends the context -> `/configurator/` | `/` |
| `runtimeConfig.STATE_TENANT_ID` | Login form's tenant | `""` (SPA default `pg`) |
| `runtimeConfig.OVERPASS_URL` | Boundary-polygon Overpass endpoint | `""` (overpass-api.de) |
| `runtimeConfig.TURBOPASS_URL` | Turbopass base for boundary search | `""` (same-origin `/turbopass`) |
| `runtimeConfig.BOUNDARY_SEARCH_LIMIT` | boundary-service page cap | `""` (300) |
| `runtimeConfig.SENTRY_DSN` / `POSTHOG_KEY` / `POSTHOG_HOST` | Console telemetry | `""` (off) |
| `resources` | nginx footprint | `10m` / `24Mi`, limit `64Mi` |

## Sourced from the platform (not set in this chart)

Nothing: the SPA is static. Sign-in goes through `identity-bff`.

## Source

- Chart: [`charts/digit3/cms/configurator`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/configurator)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/configurator/values.yaml)
