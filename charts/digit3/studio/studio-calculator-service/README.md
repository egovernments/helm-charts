# studio-calculator-service

Pluggable calculation service for DIGIT Studio. `public-service` delegates fee/amount calculation to
it as a trusted plugin host (see `public-service`'s `PLUGIN_TRUSTED_HOSTS`).

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Image** | `studio-calculator-service` |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-calculator-service digit3-studio/studio-calculator-service -n studio -f values.yaml
```

## How configuration works

Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the defaults listed below.
3. **the environment file** - a block named `studio-calculator-service` in `environments/<env>.yaml` (highest).

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Image name | `studio-calculator-service` |
| `httpPort` | Container + service port (`SERVER_PORT`) | `8080` (from common) |
| `appType` | `""` - manages its own env | `""` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `studio-calculator-service` |
| `context-path` | `CONTEXT_PATH` | App context path; blank derives `/<ingress.context>` | `""` -> `/studio-calculator-service` |

### Observability

| Key | Description | Default |
|---|---|---|
| `tracing-enabled` | Inject OpenTelemetry env from `egov-config` | `false` |

## Source

- Chart: [`charts/digit3/studio/studio-calculator-service`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-calculator-service)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/studio/studio-calculator-service/values.yaml)
