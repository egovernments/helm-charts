# studio-pdf

PDF generation service for DIGIT Studio (Node.js). Renders receipts/certificates for Studio public
services, using the platform `pdf-service`, `filestore`, workflow, billing and collection services.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Image** | `studio-pdf` (Node.js) |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-pdf digit3-studio/studio-pdf -n studio -f values.yaml
```

## How configuration works

Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the defaults listed below.
3. **the environment file** - a block named `studio-pdf` in `environments/<env>.yaml` (highest).

This is a Node.js service, so `common` does **not** inject the shared java datasource/kafka env; the
chart wires `DB_*` and `KAFKA_BROKER_HOST` directly from `egov-config` / the `db` secret. Service
hosts come from the `egov-service-host` ConfigMap managed by `cluster-configs`.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Image name | `studio-pdf` |
| `httpPort` | Container + service port | `8080` (from common) |
| `appType` | `""` - Node.js service | `""` |

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `studio-pdf` |
| `context-path` | `CONTEXT_PATH` | App context path; blank derives `/<ingress.context>` | `""` -> `/studio-pdf` |

### Database

| Key | Env var | Description | Default |
|---|---|---|---|
| `db-port` | `DB_PORT` | PostgreSQL port | `5432` |

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `DB_HOST` / `DB_NAME` | `egov-config` / `db-host`, `db-name` |
| `DB_USER` / `DB_PASSWORD` | secret `db` / `username`, `password` |
| `KAFKA_BROKER_HOST` | `egov-config` / `kafka-brokers` |
| `EGOV_HOST` | `egov-config` / `egov-services-fqdn-name` |
| `EGOV_*_HOST` (mdms, pdf, user, workflow, filestore, localization, public-service, studio-pdf-helper, collection, billing) | `egov-service-host` |

## Source

- Chart: [`charts/digit3/studio/studio-pdf`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-pdf)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/studio/studio-pdf/values.yaml)
