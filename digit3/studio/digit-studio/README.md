# digit-studio

DIGIT Studio web UI for DIGIT 3.0 - the low-code studio frontend, served as static assets by nginx.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Image** | `digit-studio` (nginx) |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install digit-studio digit3-studio/digit-studio -n studio -f values.yaml
```

## How configuration works

Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the defaults listed below.
3. **the environment file** - a block named `digit-studio` in `environments/<env>.yaml` (highest).

This is a static frontend (nginx), so it has no application env of its own. Runtime configuration
(API host, flags) is injected into `index.html` by nginx `sub_filter`, driven by the
`digit-studio-js-injection` ConfigMap that the `cluster-configs` chart manages.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Image name | `digit-studio` |
| `httpPort` | Container + service port | `80` |
| `appType` | `""` - nginx frontend (no shared java env injected) | `""` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Route segment (served at `/digit-studio`) | `digit-studio` |
| `ingress.gateway` | Route via the kong gateway. Frontend uses the plain nginx ingress, so `false` | `false` |

### Runtime config injection

| Key | Description | Default |
|---|---|---|
| `extraVolumes` / `extraVolumeMounts` | Mount the `digit-studio-js-injection` ConfigMap at `/etc/nginx/conf.d/sub_filter.conf` | (set) |

### Health checks

| Key | Description | Default |
|---|---|---|
| `healthChecks.livenessProbePath` / `readinessProbePath` | Probe path; common prepends the context -> `/digit-studio/` | `/` |

## Sourced from the platform (not set in this chart)

| Resource | Source |
|---|---|
| `digit-studio-js-injection` ConfigMap | `cluster-configs` |

## Source

- Chart: [`charts/digit3/studio/digit-studio`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/digit-studio)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/studio/digit-studio/values.yaml)
