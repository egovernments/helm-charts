# kong

The Cloud-Native Ingress and API-management gateway for DIGIT 3.0. This chart is the upstream Kong Helm chart with a DIGIT-specific `values.yaml`: it runs the `egovio/kong` build (bundled DIGIT plugins), connects to the platform PostgreSQL, and fronts every DIGIT service route.

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | vendored upstream Kong chart (no `common` dependency) |
| **Backend image** | `egovio/kong` (tag `master-9b942c2`) |

The chart sets `fullnameOverride: "kong"`, so the proxy Service renders as **`kong-proxy`** - the name the common ingress targets by default via `ingress.gatewayServiceName`. To adopt a legacy `kong-kong` deployment, set `ingress.gatewayServiceName` on the common side instead.

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install kong digit3-core/kong -n digit3-core -f values.yaml
```

## How configuration works

This chart exposes the full upstream Kong parameter surface. Values resolve from lowest to highest priority:

1. **upstream chart defaults** - Kong's own `values.yaml` defaults (lowest).
2. **this chart's `values.yaml`** - the DIGIT defaults listed below.
3. **the environment file** - a `kong` block in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
kong:
  replicaCount: 2
  image:
    tag: "master-9b942c2"
  autoscaling:
    maxReplicas: 8
```

Database connection details (host, name, credentials) are **not** hard-coded here - they are pulled from the `egov-config` ConfigMap and the `db` secret managed by the `cluster-configs` chart. The built-in PostgreSQL sub-chart is disabled (`postgresql.enabled: false`); Kong uses the external platform database.

## Parameters

### Deployment

| Key | Description | Default |
|---|---|---|
| `fullnameOverride` | Release name override; renders the proxy Service as `kong-proxy` | `kong` |
| `namespace` | Namespace for chart resources | `egov` |
| `global.containerRegistry` | Image registry | `egovio` |
| `deployment.kong.enabled` | Run Kong itself (vs controller-only) | `true` |
| `replicaCount` | Pod count (ignored when autoscaling is on) | `1` |
| `image.repository` | Kong image (DIGIT build with gateway customizations) | `egovio/kong` |
| `image.tag` | Image tag (validated for 3.0.1) | `master-9b942c2` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |

### Autoscaling

| Key | Description | Default |
|---|---|---|
| `autoscaling.enabled` | Enable HorizontalPodAutoscaler | `true` |
| `autoscaling.minReplicas` | Minimum replicas (kept at 1 so exactly one pod runs the first-boot migration sequence) | `1` |
| `autoscaling.maxReplicas` | Maximum replicas | `5` |
| `autoscaling.targetCPUUtilizationPercentage` | CPU target | `80` |

### Kong configuration (`env`)

Keys under `env` become `KONG_*` environment variables and take precedence over other sections.

| Key | Description | Default |
|---|---|---|
| `env.database` | Datastore mode | `postgres` |
| `env.pg_port` | PostgreSQL port | `5432` |
| `env.router_flavor` | Router (traditional routes from the ingress controller) | `traditional` |
| `env.nginx_worker_processes` | nginx worker processes | `2` |
| `env.prefix` | Kong prefix dir (writable emptyDir; image default `/kong` is not writable for non-root) | `/kong_prefix` |
| `env.plugins` | Enabled plugins, including custom DIGIT plugins baked into `egovio/kong` | `bundled,dynamic-jwt,keycloak-rbac,header-enrichment,metadata-enrichment,token-revocation` |
| `env.real_ip_header` | Real IP header | `X-Real-IP` |
| `env.real_ip_recursive` | Recursive real-IP resolution | `on` |
| `env.trusted_ips` | Trusted proxy IPs | `0.0.0.0/0` |
| `env.proxy_access_log` / `env.proxy_error_log` | Proxy log sinks | `/dev/stdout` / `/dev/stderr` |

Database connection keys (`env.pg_host`, `env.pg_user`, `env.pg_password`, `env.pg_database`) are platform-sourced - see below.

### Proxy

| Key | Description | Default |
|---|---|---|
| `proxy.enabled` | Create the proxy Service | `true` |
| `proxy.type` | Service type | `ClusterIP` |
| `proxy.http.servicePort` / `proxy.http.containerPort` | Plaintext proxy port (common ingress routes to `:8000`) | `8000` / `8000` |
| `proxy.tls.servicePort` / `proxy.tls.containerPort` | TLS proxy port | `443` / `8443` |
| `proxy.labels.enable-metrics` | Metrics scrape label | `true` |
| `proxy.resources.requests` | Proxy requests | `250m` / `512Mi` |
| `proxy.resources.limits` | Proxy limits | `1000m` / `1Gi` |

### Admin / status API

| Key | Description | Default |
|---|---|---|
| `admin.enabled` | Create the admin Service (ClusterIP only; used by the DIGIT kong bootstrap to program routes) | `true` |
| `admin.type` | Service type | `ClusterIP` |
| `admin.http.servicePort` / `containerPort` | Plaintext admin port | `8001` / `8001` |
| `admin.tls.servicePort` / `containerPort` | TLS admin port | `8444` / `8444` |
| `admin.ingress.enabled` | Expose admin API via ingress | `false` |
| `status.enabled` | Enable status listener (internal only) | `true` |
| `status.http.containerPort` | Status port | `8100` |

### Migrations (init container)

Migrations run from an `extraInitContainers` definition (`kong migrations bootstrap && up && finish`) against the external database, reading `KONG_DATABASE` / `KONG_PG_*` from `egov-config` and the `db` secret. The upstream migration Jobs are off.

| Key | Description | Default |
|---|---|---|
| `migrations.preUpgrade` | Run pre-upgrade migration Job | `false` |
| `migrations.postUpgrade` | Run post-upgrade migration Job | `false` |
| `waitImage.enabled` | Wait for the database before starting Kong | `true` |

### Ingress controller / sub-charts

| Key | Description | Default |
|---|---|---|
| `ingressController.enabled` | Run the Kong Ingress Controller | `false` |
| `postgresql.enabled` | Deploy the bundled PostgreSQL sub-chart (external DB used instead) | `false` |
| `certificates.enabled` | cert-manager integration | `false` |
| `enterprise.enabled` | Kong Enterprise features | `false` |

### Security context / scheduling

| Key | Description | Default |
|---|---|---|
| `containerSecurityContext.readOnlyRootFilesystem` | Read-only root FS (off; `egovio/kong` writes to `/kong` at startup) | `false` |
| `containerSecurityContext.runAsUser` | Run-as UID | `1000` |
| `containerSecurityContext.runAsNonRoot` | Disallow root | `true` |
| `containerSecurityContext.capabilities.drop` | Dropped capabilities | `ALL` |
| `podAnnotations` | Pod annotations (mesh gateway hints) | `kuma.io/gateway: enabled`, `traffic.sidecar.istio.io/includeInboundPorts: ""` |
| `terminationGracePeriodSeconds` | Pod termination grace period | `30` |
| `serviceMonitor.enabled` | Create a Prometheus ServiceMonitor | `false` |

## Sourced from the platform (not set in this chart)

These are injected from `cluster-configs` resources and must exist in the target namespace. They feed both the proxy `env` and the `kong-migration` init container.

| Env var | Source |
|---|---|
| `KONG_PG_HOST` / `pg_host` | `egov-config` / `db-host` |
| `KONG_PG_DATABASE` / `KONG_DATABASE` / `pg_database` | `egov-config` / `db-name` |
| `KONG_PG_USER` / `pg_user` | secret `db` / `username` |
| `KONG_PG_PASSWORD` / `pg_password` | secret `db` / `password` |

## Source

- Chart: [`charts/digit3/core/kong`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/kong)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/kong/values.yaml)
- Upstream chart: [Kong/charts - charts/kong](https://github.com/Kong/charts/tree/main/charts/kong)
