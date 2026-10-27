# Onboarding a new chart

This walks through adding a new chart to the registry. Two kinds of chart exist, and they are
onboarded differently:

- **A DIGIT service chart** (the common case) - a thin chart that depends on the project's
  `common` library, e.g. a service under `digit3/core`. Sections 1-7 cover this.
- **A vendored upstream chart** - a third-party chart copied in wholesale (e.g. `kong`, `vault`,
  `postgresql`). It does **not** depend on `common`. See [Vendored upstream charts](#vendored-upstream-charts).

Prerequisite: the project's `common` library is already published. For `digit3` it is, currently at
**`3.1.0`** - check `https://helm.digit.org/digit3/common` for the latest and pin that.

## 0. Where service charts come from

Most `digit3` service charts are migrated from DIGIT-DevOps, then adapted with the conventions below:

- modulith services: branch `modulith`, `deploy-as-code/helm/charts/digit3/<service>`
- a few LTS-only services (e.g. `notify`, `mdms-v2`): branch `digit-lts`,
  `deploy-as-code/helm/charts/core-services/<service>`

A brand-new chart follows the same shape.

## 1. Create the chart directory

```
charts/digit3/core/<service>/
  Chart.yaml
  values.yaml
  templates/
    deployment.yaml   # {{- template "common.deployment" . -}}
    service.yaml      # {{- template "common.service" . -}}
    ingress.yaml      # {{- template "common.ingress" . -}}
```

Service charts are thin - the templates just include the `common` library helpers. Almost all of
the real configuration is in `values.yaml`.

## 2. Write `Chart.yaml`

```yaml
apiVersion: v2
name: <service>                        # MUST match the directory and the registry name.
                                       # Strip any leftover "digit-" prefix from the source
                                       # Chart.yaml name, or the chart publishes mis-named.
description: <one short line>          # a real sentence, not "A Helm chart for Kubernetes".
                                       # e.g. "Formatted, unique ID generation for DIGIT 3.0"
type: application

version: 3.0.0                         # digit3 seed baseline. First publish uses this as-is;
                                       # later releases bump it from the registry, not by hand.

appVersion: "flyway-branch-18c74fe"    # the REAL image tag. Source charts ship the helm-create
                                       # default (1.16.0) - replace it. In prod mode this drives
                                       # BOTH the service image and the <service>-db init-image.

dependencies:
  - name: common
    version: 3.1.0                     # a common version that is ALREADY published.
    repository: https://helm.digit.org/digit3/common
```

Key points:

- **`name`** equals the directory and the registry name. A `digit-<name>` left over from the source
  chart publishes a mis-named chart - strip the prefix.
- **`description`** is shown on the registry website; give it a short, relevant one-liner.
- **`version: 3.0.0`** is the seed; first publish takes it as-is. You will not edit it again by hand.
- **`appVersion`** is the image tag. Do not leave the scaffold default `1.16.0`.
- **`dependencies[].version`** must be a `common` version that already exists in the registry.

## 3. Write `values.yaml`

### 3a. Image tag - the `global.mode` mechanism

`global.mode` (default **`prod`**, set in `common`) decides the tag:

- **prod** (default): the image tag is always `Chart.AppVersion`. You do **not** set `image.tag`.
- **dev**: uses `.Values.image.tag` if set, else falls back to `appVersion`. Environment files set
  `global.mode: dev` and an `image.tag` to pin a specific build.

```yaml
image:
  repository: "<service>"
initContainers:
  dbMigration:
    enabled: true
    schemaTable: "<service>_schema"
    image:
      repository: "<service>-db"   # tag also comes from appVersion in prod mode
```

### 3b. Ingress - the gateway

```yaml
ingress:
  enabled: true
  gateway: true          # route through the API gateway (renamed from the old `zuul`)
  context: "<service>"   # NO "digit-" prefix; drives the /<context> route + gateway/route-path
```

The ingress backend resolves to the **`kong-proxy`** service (from the `kong` chart), and the
`gateway/route-path` annotation is taken from `ingress.context`.

### 3c. What `common` gives a `java` service for free

Set `appType: "java"` and `common` injects a shared **`extraEnv.java`** block into the container.
**Do not redefine any of these at the service level** - they are centralised so every service is
configured the same way, from `egov-config` (a ConfigMap) and the `db` Secret:

| Injected env | From |
|--------------|------|
| `SPRING_DATASOURCE_URL` | `egov-config/db-url` |
| `SPRING_DATASOURCE_USERNAME` / `PASSWORD` | `db` Secret |
| `SPRING_DATASOURCE_DRIVER_CLASS_NAME` | `org.postgresql.Driver` |
| `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` / `MINIMUM_IDLE` / `CONNECTION_TIMEOUT` | `egov-config/db-pool-*` |
| `FLYWAY_ENABLED=false`, `SPRING_FLYWAY_ENABLED` | app-side flyway off (the db-migration init container owns migrations) |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `egov-config/kafka-brokers` |
| `APP_TIMEZONE` | `egov-config/timezone` |
| `SPRING_JPA_SHOW_SQL` | `egov-config/spring-jpa-show-sql` |
| `MANAGEMENT_ENDPOINTS_WEB_BASE_PATH=/` | actuator served at the context root (so health is `/<context>/health`, not `/<context>/actuator/health`) |
| `SERVER_TOMCAT_THREADS_MAX` / `MAX_CONNECTIONS` | `egov-config/server-tomcat-*` |
| `IS_ENVIRONMENT_CENTRAL_INSTANCE=false` | |

So a new java service does **not** set its own datasource, Kafka broker, timezone, flyway or pool
env - it inherits them. Spring Boot reads these names directly (relaxed binding), so they apply even
when the app's `application.properties` constructs its own defaults.

Tracing is separate: set **`tracing-enabled: true`** (or `global.tracing-enabled`) and `common`
also injects `OTEL_ENABLED` + `OTEL_EXPORTER_OTLP_ENDPOINT` from `egov-config` (off unless the
`otel-enabled` key is flipped).

### 3d. Service-specific env

Put the env that is genuinely unique to your service in the `env` block (service topics, feature
flags, downstream service hosts, etc.). Two optional `common` hooks:

- **`extraEnv.app`** - an extra block injected after the shared ones.
- **`extraEnv.promote`** - injected **last**, so its entries override any earlier duplicate. Leave
  unset unless you specifically need a last-word override.

Keep `values.yaml` visually grouped: a single-rule section divider per block
(`# ── Container env ──`), env grouped by concern (Kafka / Redis / Observability / …), minimal
comments, plain hyphens (not em-dashes).

### 3e. Context path (`SERVER_CONTEXT_PATH` / `CONTEXT_PATH`)

The app serves at its **codebase default** context path. Rule for the Helm context-path env:

1. Take the chart's context-path env value and strip any `digit-` prefix.
2. If it **equals the codebase default**, **remove the env** - the app already defaults there
   (e.g. `filestore`, `pg-service`, `idgen`, `notification`, `otp`, `workflow`, …).
3. If it **differs**, **keep the env** with the stripped value, so the served path matches
   `ingress.context`. Known exceptions where the code default is not the plain name and the chart
   sets the env to realign it: `account` → `/account` (code default `/accounts`),
   `employee` → `/employee` (code default `/employee-java`), `individual` → `/individual`.

The env-var name varies by service (`CONTEXT_PATH`, `SERVER_CONTEXT_PATH`) - match the source.

### 3f. Health checks

Enable in the service values and use a plain `/health` path for both probes:

```yaml
healthChecks:
  enabled: true
  livenessProbePath: "/health"
  readinessProbePath: "/health"
```

`common` prepends `ingress.context`, so the kubelet hits the app's real served path
`/<context>/health` (e.g. `localization` → `/localization/health`). Because `common` sets the
actuator base-path to `/` (see 3c), `/health` is the actuator health endpoint - do **not** put the
context or `/actuator` into the value yourself, or the path double-prefixes.

## 4. Shared config and secrets (`cluster-configs`)

Services read shared config from the `egov-config` ConfigMap, internal URLs from `egov-service-host`,
and credentials from named Secrets - all defined once in `charts/digit3/cluster-configs/values.yaml`
(installed once per product namespace, before the services). If your service needs:

- **a new shared config key** (something other services also use, or a datasource/pool/tracing
  default) → add it under `configmaps.egov-config.data` with a sensible default.
- **an internal service URL** → add it under `configmaps.egov-service-host.data`.
- **a new Secret** (credentials) → declare it under `secrets:` (name + namespace; values come from
  the environment's SOPS file, not from here).

Config that is specific to one environment is **not** set here - it is overridden per environment in
`environments/<env>.yaml`. `cluster-configs` only carries safe, platform-neutral defaults.

## 5. Validate locally

`helm template`/`lint` need the `common` dependency present. Either pull it from the registry:

```bash
cd charts/digit3/core/<service>
helm dependency build .      # pulls the pinned common from the registry
helm lint .
helm template test . --set global.namespace=core
rm -rf charts Chart.lock     # do NOT commit the vendored dependency; CI rebuilds it
```

…or, to render without network, vendor the in-repo `common` just for the check:

```bash
mkdir -p charts/digit3/core/<service>/charts
cp -r charts/digit3/common charts/digit3/core/<service>/charts/common
helm template test charts/digit3/core/<service> --set global.namespace=core
rm -rf charts/digit3/core/<service>/charts     # throwaway; never commit it
```

Confirm it renders, the ingress backend is `kong-proxy`, and the health probe path is
`/<context>/health`.

## 6. Open the PR and release

Branch off the latest `master`, commit only your chart(s), open the PR. A workflow appends a
**Release selection** form to the PR description; tick one `major`/`minor`/`patch` (Project and
Module auto-tick). Get `require-label` + `lint-template` green, then merge. **One module per PR**
(several services under the same module in one PR is fine).

> Do **not** hand-edit the PR description in a way that deletes the `<!-- RELEASE-FORM -->` block -
> it is managed by the workflow. If you wipe it, push any commit (or reopen the PR) and it is
> re-injected, preserving your ticks.

- **First publish seeds from `Chart.yaml` version** (`3.0.0`); the Release type is still required but
  does not change that first number.
- **Later releases bump the published version** per the Release type, read from the registry
  `index.yaml` (not from `Chart.yaml`).

## 7. Verify after merge

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm search repo digit3-core            # your chart appears
```

Check that `https://helm.digit.org/digit3/core/` lists it with its description and a changelog entry.

---

## Vendored upstream charts

Infrastructure charts that are not DIGIT services (`kong`, `vault`, `postgresql`) are **vendored**:
the upstream chart is copied in wholesale and lightly adapted. They do **not** depend on `common`.

1. **Copy the upstream chart** (`Chart.yaml`, `values.yaml`, `templates/`, any `monitoring/` data)
   into `charts/digit3/<module>/<name>/`.
2. **Rename + reversion** in `Chart.yaml`: `name: <name>`, `version: 3.0.0` (this repo's convention,
   not the upstream chart version), `appVersion:` = the upstream application version.
3. **Record provenance** so the source is easy to map and re-sync later, as annotations:

   ```yaml
   annotations:
     digit.org/upstream-chart: hashicorp/vault-helm   # or cloudnative-pg/cluster, etc.
     digit.org/upstream-chart-version: "0.29.1"       # the upstream CHART version
     digit.org/upstream-app-version: "1.18.1"         # the upstream APP version
   ```
4. **Adapt the defaults** to platform-neutral values (namespace, storage class left to the cluster
   default, public ingress off, etc.); keep environment-specific values for `environments/<env>.yaml`.
5. Validate with `helm lint` / `helm template` (no `common` vendoring needed - these are standalone)
   and release like any other chart (module = its directory, e.g. `backbone`).

Current examples: `core/kong` (Kong), `backbone/vault` (vault-helm `0.29.1`),
`backbone/postgresql` (cloudnative-pg/cluster `0.9.0`, PostgreSQL 18).

---

## Adding a new module

No configuration is needed for the normal (PR-merge) flow - the module is detected from the path.

1. Create `charts/<project>/<new-module>/<service>/...` as above.
2. Open a PR (one module per PR) and release.
3. The module directory, its `index.yaml`, `changelog.yaml`, and the `<project>/modules.json`
   entry are created automatically, and it appears on the project page.

Optional: to make the new module selectable in the **manual** `workflow_dispatch` run, add it to the
`module` `options` list in `.github/workflows/release.yaml`.

## Adding a new project

1. Create `charts/<new-project>/common/` (the library) and publish it first (its own PR,
   module = `common`). Seed its `Chart.yaml` `version`.
2. Create `charts/<new-project>/<module>/<service>/...` with `dependencies[].repository` pointing at
   `https://helm.digit.org/<new-project>/common`.
3. Release `common` first, then the services (dependency ordering).
4. `projects.json` and the project page are generated automatically; the project appears on
   `helm.digit.org`.

Optional: add the new project to the `project` `options` list in `release.yaml` for manual dispatch.
