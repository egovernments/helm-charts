# studio-pdf-helper-service

PDF configuration helper for DIGIT Studio. Generates and commits pdf-service config (templates and
QR definitions) to the configs repository through a GitHub App, so Studio-authored documents render
through the platform `pdf-service`.

| | |
|---|---|
| **Project / Module** | `digit3` / `studio` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `studio-pdf-helper-service` |

## Install

```bash
helm repo add digit3-studio https://helm.digit.org/digit3/studio
helm repo update
helm install studio-pdf-helper-service digit3-studio/studio-pdf-helper-service -n studio -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **common chart defaults** - shared platform defaults (lowest).
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
studio-pdf-helper-service:
  github-config-branch: "DIGIT-LTS"
  github-devops-branch: "digit-lts"
```

This is **not** a java-spring service (`appType: ""`), so `common` injects no datasource or Kafka
block; the chart carries its own `DB_*`, Kafka and GitHub-App env. Database and Kafka endpoints
still come from the `egov-config` ConfigMap and `db` secret; the GitHub App credentials come from the
`github-config` secret.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `replicas` | Number of pods | `1` |
| `image.repository` | Backend image name | `studio-pdf-helper-service` |
| `httpPort` | Container + service port | `8080` (from common) |
| `resources` | Requests/limits block | `50m`/`256Mi` .. `100m`/`512Mi` |

### Routing

| Key | Description | Default |
|---|---|---|
| `ingress.context` | Gateway route segment | `studio-pdf-helper-service` |

### Database

| Key | Env var | Default |
|---|---|---|
| `db-port` | `DB_PORT` | `5432` |
| `db-sslmode` | `DB_SSLMODE` | `disable` |

`DB_HOST` / `DB_NAME` resolve from `egov-config` (`db-host` / `db-name`); `DB_USER` / `DB_PASSWORD`
from the `db` secret.

### Kafka

| Key | Env var | Default |
|---|---|---|
| `topic-save-pdf-qr-generator` | `SAVE_PDF_QR_GENERATOR` | `save-pdf-qr-generator` |

`KAFKA_BOOTSTRAP_SERVERS` resolves from `egov-config/kafka-brokers`.

### GitHub integration

| Key | Env var | Default |
|---|---|---|
| `github-owner` | `GITHUB_OWNER` | `egovernments` |
| `github-config-repo` | `GITHUB_CONFIG_REPO` | `configs` |
| `github-config-branch` | `GITHUB_CONFIG_BRANCH` | `DIGIT-LTS` |
| `github-config-base-path` | `GITHUB_CONFIG_BASE_PATH` | `pdf-service` |
| `github-devops-owner` | `GITHUB_DEVOPS_OWNER` | `egovernments` |
| `github-devops-repo` | `GITHUB_DEVOPS_REPO` | `DIGIT-DevOps` |
| `github-devops-branch` | `GITHUB_DEVOPS_BRANCH` | `digit-lts` |
| `github-devops-file-path` | `GITHUB_DEVOPS_FILE_PATH` | `deploy-as-code/helm/environments/test-lts.yaml` |
| `kubernetes-namespace` | `KUBERNETES_NAMESPACE` | `egov` |

`GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY`, `KUBE_API_URL` and
`KUBE_TOKEN` come from the `github-config` secret.

## Source

- Chart: [`charts/digit3/studio/studio-pdf-helper-service`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/studio/studio-pdf-helper-service)
