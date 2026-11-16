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

## Gateway-sync (declarative routing)

Optional, disabled by default (`gatewaySync.enabled: false`). Makes Kong routing
declarative and self-healing instead of hand-seeded via the Admin API: a reconciler
keeps Kong services/routes/plugins in sync with **per-Service annotations** - the
single source of truth. It is **tag-scoped** (`gateway-sync`) and only ever touches
entities carrying that tag, so the core team's hand-made routes are never modified or
deleted.

There is deliberately **no central route list** in this chart. Each service declares
its own routing via annotations on its backing Service (in that service's own chart),
so routing is defaulted and overridden per service - no cross-service list to merge.

**What it ships** (all gated on `gatewaySync.enabled`):

| Object | Purpose |
|---|---|
| ConfigMap `kong-gateway-sync` | Global config (`config.json`) + the reconciler script |
| CronJob `kong-gateway-sync` | Runs the reconciler every `gatewaySync.schedule` |
| Job `kong-gateway-sync-init` | Helm `post-install,post-upgrade` hook; one reconcile on deploy |
| ServiceAccount + ClusterRole/Binding | `get,list,watch` on `services` (discovery + host/port) |

The reconciler is a pure-stdlib Python script (no pip/custom image - runs on any
`python3 >= 3.8` image). Each pass: discovers annotated Services, builds the desired
Kong entities (upstream host/port looked up from the **real** k8s Service, which fixes
the hostname/plural/hyphen class of bugs), upserts them by name, and prunes tagged
entities no longer desired (Service or annotation removed).

### The annotation contract

Put these on the backing **Service** (in that service's own chart). Default tier is
PROTECTED; a path is OPEN or MIXED only if listed. `<prefix>` is `gatewaySync.annotationPrefix`
(default `gateway`).

| Annotation | Meaning |
|---|---|
| `gateway/route-path` | **Required** marker + main route path. For digit3 charts this is **auto-emitted** by the common Service template from `ingress.context` whenever `ingress.gateway: true` - no need to set it by hand (a `gateway: false` service that still needs a gateway route, like keycloak, sets it explicitly). |
| `gateway/route-auth` | Main route tier: `open` (no auth), `mixed` (optional auth), or `protected` (full auth). Default `protected`. |
| `gateway/open-paths` | Comma-separated no-auth paths - per-path exceptions (must out-match the main route) |
| `gateway/mixed-paths` | Comma-separated optional-auth paths (no `keycloak-rbac`) |
| `gateway/protected-paths` | Comma-separated full-auth paths - protect specific writes on an otherwise `open`/`mixed` main route |
| `gateway/methods` | Optional HTTP method allowlist (default: all) |

The main route's plugin stack comes from `route-auth`; `open-paths` / `mixed-paths` /
`protected-paths` layer per-path exceptions on top (e.g. an `open` main route can still
protect a specific write via `protected-paths`). Keycloak is the canonical `route-auth: open`
case - it is the auth server, so `/keycloak` carries no auth plugins.

Path entries support two extras:
- **Per-path methods**: a `PATH (M1,M2)` suffix scopes that route to those HTTP methods,
  e.g. `account` keeps `/accounts/v3/config` open for reads but protects writes with
  `gateway/protected-paths: "/accounts/v3/config (POST,PUT)"`.
- **Regex paths**: a value starting with `~` is a Kong regex route, created with a high
  `regex_priority` (`gatewaySync.regexPriority`) so it out-matches the protected main
  route, e.g. `gateway/open-paths: "~/workflow/v3/process/definition/[^/]+$"`.

Because `gateway/route-path` comes for free from `ingress.context`, every `gateway: true`
digit3 service is already discoverable; a service only adds `service.additionalAnnotations`
when it has open or mixed paths:

```yaml
# in the account service chart's values.yaml, overridable per env
service:
  additionalAnnotations: |
    gateway/open-paths: "/accounts/v3/config,/accounts/v3/canonical/config"
```

Every route is pinned to `gatewaySync.domain` as its host (required: in the traditional
router a host match outranks path length), with `strip_path: false`.

### Gateway-sync parameters

| Key | Description | Default |
|---|---|---|
| `gatewaySync.enabled` | Create the reconciler + config | `false` |
| `gatewaySync.domain` | Public API host every route matches on | `test-lts.digit.org` |
| `gatewaySync.schedule` | CronJob cadence | `*/2 * * * *` |
| `gatewaySync.runOnInstall` | Run one reconcile on deploy (hook Job) | `true` |
| `gatewaySync.kongAdminUrl` | Kong Admin API | `http://kong-kong-admin.egov.svc.cluster.local:8001` |
| `gatewaySync.keycloakBaseUrl` | Keycloak base URL (injected into auth plugins) | `http://keycloak.keycloak.svc.cluster.local:8080/keycloak` |
| `gatewaySync.clientId` | keycloak-rbac client id | `auth-server` |
| `gatewaySync.redis.host` / `.port` | token-revocation Redis | `redis.backbone.svc.cluster.local` / `6379` |
| `gatewaySync.namespaces` | Namespaces discovered for annotated Services (empty = cluster-wide) | `[egov]` |
| `gatewaySync.annotationPrefix` | Annotation key prefix | `gateway` |
| `gatewaySync.tag` | Ownership tag (reconciler scope) | `gateway-sync` |
| `gatewaySync.image.repository` / `.tag` | Reconciler image (any python3 image) | `python` / `3.12-slim` |
| `gatewaySync.protectedPlugins` / `.mixedPlugins` | Plugin stack per tier | see values |
| `gatewaySync.mixedAnonymousFlag` | `dynamic-jwt` delta for MIXED (optional-auth) | `{}` |
| `gatewaySync.pluginConfigs` | Auth plugin configs (keycloak/redis/client injected) | see values |

### Before enabling

- **Annotations are seeded from the live Kong config** (verified against digit-lts). All 22
  `gateway: true` core services are covered: the 16 plain-`protected` ones need nothing
  (route-path auto-emits from `ingress.context` and the default tier is protected), and the
  reconciler-produced routes were diffed against live - they match for every core service.
  Non-default routing is annotated per service: `account` (open main + method-split protected
  writes), `localization`, `mdms-v2`, `workflow` (regex), `keycloak` (open). `individual`'s
  `ingress.context` was corrected to `individuals` (plural) to match the live service.
- **Still outside this chart/PR**: the studio-module services (separate release) and the
  services with no chart in this repo yet (calculator, license-certificate, metabase, pdf-v3,
  schema-registry, ui-access-control, vc, inbox) - add their annotations when those land.
- **Set `gatewaySync.mixedAnonymousFlag`** once the custom `dynamic-jwt` plugin's
  optional/anonymous flag is confirmed; until then MIXED routes validate as protected
  (require a valid JWT), not as open.

## Source

- Chart: [`charts/digit3/core/kong`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/core/kong)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/core/kong/values.yaml)
- Upstream chart: [Kong/charts - charts/kong](https://github.com/Kong/charts/tree/main/charts/kong)
