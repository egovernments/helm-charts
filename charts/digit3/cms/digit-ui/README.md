# digit-ui

The DIGIT citizen and employee web UI for the Citizen Complaint Resolution System: a static
SPA served by nginx at tenant-scoped URLs (`/<tenant-slug>/digit-ui/...`).

| | |
|---|---|
| **Project / Module** | `digit3` / `cms` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Image** | `digit-ui-esbuild` (nginx) |

## Install

```bash
helm repo add digit3-cms https://helm.digit.org/digit3/cms
helm repo update
helm install digit-ui digit3-cms/digit-ui -n egov -f values.yaml
```

## How configuration works

Values resolve lowest to highest priority: **common defaults** < **this chart's `values.yaml`** <
the **`digit-ui` block in `environments/<env>.yaml`** (wins).

```yaml
# environments/<env>.yaml
digit-ui:
  globalConfigs:
    stateTenantId: "pg"
    mapTenant: "pg"
```

`globalConfigs.js` is rendered by this chart into the `digit-ui-config` ConfigMap (with the nginx
`default.conf` that injects it into `<head>`), so the UI config ships with the release instead of
being uploaded to object storage by hand. A missing key means a blank page with HTTP 200.

## Routing

| Ingress | Path | Purpose |
|---|---|---|
| `digit-ui-tenant-routes` | `/<slug>/digit-ui/(.*)` -> `/digit-ui/$2` | Canonical tenant-scoped UI |
| `digit-ui-tenant-root-redirect` | `/<slug>/digit-ui` | 302 to the trailing-slash URL (answered by the image's nginx) |
| `digit-ui-static-assets` | `/digit-ui/*.{js,css,png,...}` | Bundle assets load from the tenantless prefix |
| `digit-ui-tenantless-redirect` | `/digit-ui` | Only when `ingress.defaultTenantSlug` is set |
| `digit-ui` | `/digit-ui` | Legacy; only when `ingress.legacyPathEnabled` |

## Parameters

| Key | Description | Default |
|---|---|---|
| `ingress.context` | UI base path | `digit-ui` |
| `ingress.legacyPathEnabled` | Old tenantless `/digit-ui` route | `false` |
| `ingress.defaultTenantSlug` | Redirect `/digit-ui/...` to `/<slug>/digit-ui/...` | `""` (off) |
| `httpPort` | Container + service port | `80` |
| `globalConfigs.enabled` | Serve `globalConfigs.js` from this chart | `true` |
| `globalConfigs.stateTenantId` / `mapTenant` | State tenant | `pg` |
| `globalConfigs.contextPath` | `CONTEXT_PATH` | `digit-ui` |
| `globalConfigs.gmapsApiKey` | Google Maps key | `""` |
| `globalConfigs.localeRegion` / `localeDefault` | Locale | `IN` / `en` |
| `globalConfigs.mdmsContext` / `hrmsContext` | Service context paths | `mdms-v2` / `egov-hrms` |
| `globalConfigs.pgrBoundaryHighestLevel` / `pgrBoundaryLowestLevel` | Complaint boundary levels | `District` / `Ward` |
| `globalConfigs.boundaryType` / `hierarchyType` / `complaintHierarchyType` | Boundary hierarchy | `Ward` / `ADMIN` / `""` |
| `globalConfigs.mapCenter` | Map centre | India |
| `globalConfigs.coreMobileConfigs` | Country code + mobile regex | `+91`, `^[6-9][0-9]{9}$` |
| `globalConfigs.invalidEmployeeRoles` / `employeeModuleDenylist` | Role / module filters | `[SYSTEM]` / `[IM]` |
| `globalConfigs.dashboardMetricsEnabled` / `citizenBannerEnabled` | Feature flags | `false` |
| `globalConfigs.*` (others) | finEnv, footer logos, digitHomeURL, assetS3Bucket, configModuleName, centralInstanceEnabled, corePostalConfigs | see `values.yaml` |
| `custom-js-injection` | Legacy: your own `sub_filter.conf` in a `digit-ui` ConfigMap (with `globalConfigs.enabled: false`) | `""` |

## Sourced from the platform (not set in this chart)

Nothing: the SPA is static. Sign-in goes through `identity-bff`.

## Source

- Chart: [`charts/digit3/cms/digit-ui`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/cms/digit-ui)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/cms/digit-ui/values.yaml)
