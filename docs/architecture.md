# Architecture

## Layout: project -> module -> service

```
charts/
  <project>/                 PROJECT   (e.g. digit3)
    common/                  per-project library chart (shared templates/helpers)
    <module>/                MODULE    (e.g. core, health, urban, frontend, backbone)
      <service>/             SERVICE CHART (e.g. localization)
        Chart.yaml
        values.yaml
        templates/
```

Live example:

```
charts/digit3/common/
charts/digit3/core/localization/
```

- A **project** groups everything for one product line (first project: `digit3`).
- A **module** is a logical grouping of services and maps 1:1 to a **Helm repository**.
- A **service** is a single deployable chart.
- **`common`** is a library chart (one per project). Service charts depend on it.

## The registry (gh-pages -> helm.digit.org)

Two branches drive everything:

| Branch | Role |
|--------|------|
| `master` | Source of truth: chart sources, the CI workflows, and the registry website templates. |
| `gh-pages` | The published site. Served at `https://helm.digit.org` via the `CNAME` file. Machine-managed by CI; do not hand-edit except for one-off recovery. |

Each **module** is an independent Helm repository. Its index lives at the module root and the
chart packages are nested one level deeper, under the chart name:

```
https://helm.digit.org/<project>/<module>/index.yaml                         <- the repo index
https://helm.digit.org/<project>/<module>/<chart>/<chart>-<version>.tgz      <- the package
```

Concretely:

```
https://helm.digit.org/digit3/core/index.yaml
https://helm.digit.org/digit3/core/localization/localization-1.0.0.tgz
https://helm.digit.org/digit3/common/index.yaml
https://helm.digit.org/digit3/common/common/common-1.0.0.tgz
```

> Every chart, including `common`, is nested under its own name
> (`<module>/<chart>/<chart>-<version>.tgz`). This keeps the rule uniform.

## Consuming charts

Add a module as a Helm repo, then install any chart in it:

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm search repo digit3-core
helm install my-release digit3-core/localization -n core -f values.yaml
```

The per-project library is a repo too:

```bash
helm repo add digit3-common https://helm.digit.org/digit3/common
```

## The `common` dependency

Service charts declare `common` as a dependency pulled from the registry:

```yaml
# charts/digit3/core/localization/Chart.yaml
dependencies:
  - name: common
    version: 3.1.1
    repository: https://helm.digit.org/digit3/common
```

At release time CI runs `helm dependency build`, which downloads the pinned `common` version
from the registry. This means **`common` must be published before any chart that depends on it**,
and a dependent's `version:` must point at a `common` version that already exists in the registry.

### What `common` provides

`common` is more than shared templates - it **centralises runtime config** so every service is wired
the same way. For a chart with `appType: "java"` it injects (via `extraEnv.java`) the datasource
(`SPRING_DATASOURCE_*` from `egov-config`/the `db` Secret), the Hikari pool, the Kafka brokers, the
timezone, flyway/JPA/Tomcat settings and the actuator base-path - all read from the `egov-config`
ConfigMap and the `db` Secret. A separate `extraEnv.otel` block adds OpenTelemetry env when tracing
is enabled. Services therefore **do not** redefine their datasource, broker or pool; they inherit
them. See [onboarding-a-service.md](onboarding-a-service.md) for the exact fields.

A service's **own** env (topics, feature flags, consumer groups, …) is wired to be overridable: each
value is a short top-level key referenced as `{{ index .Values "<key>" | quote }}`, resolved through
a three-tier merge - `common` defaults < the chart's `values.yaml` < the `environments/<env>.yaml`
block named after the chart (highest). App-specific values live in the chart; shared infrastructure
stays in `egov-config`. See [onboarding-a-service.md](onboarding-a-service.md#3d-service-specific-env-and-making-it-overridable).

### Shared config: `cluster-configs`

The values those env vars read from are defined in one place - the `cluster-configs` chart
(`charts/digit3/cluster-configs`). It renders the shared `egov-config` ConfigMap (db URL, pool,
Kafka brokers, timezone, OTel toggle, Keycloak), the `egov-service-host` ConfigMap (internal service
URLs), and declares the named Secrets (`db`, `minio`, …). It is installed once per product namespace,
before the services. Platform-neutral defaults live here; environment-specific values are overridden
in `environments/<env>.yaml`.

### Image tags: the `global.mode` mechanism

`common.image` resolves the tag from `global.mode` (default `prod`, set in `common`):

- **prod** (default): the image tag is always the chart's `Chart.AppVersion`. `image.tag` in
  `values.yaml` is ignored. Set `appVersion` to the real CI build tag (currently `dev-flyway-<build>`, e.g. `dev-flyway-7068862`); it
  drives both the service image and the `<service>-db` init-image.
- **dev**: uses `.Values.image.tag` if set, else falls back to `appVersion`.

So in normal operation you set `appVersion` and leave `image.tag` out. To override per environment,
install with `global.mode=dev` and an `image.tag`:

```bash
helm install my-release digit3-core/localization \
  --set global.mode=dev --set image.tag=my-xyz
```

See [onboarding-a-service.md](onboarding-a-service.md) for the exact fields.

## The registry website

The site at `helm.digit.org` is a 3-tier static UI, generated from discovery files and the
chart indexes:

| Level | Page template (on `master`) | Served as | Reads |
|-------|------------------------------|-----------|-------|
| Projects | `index.html` | `/index.html` | `projects.json` |
| Modules | `project-index.html` | `/<project>/index.html` | `<project>/modules.json` + each module's `index.yaml` |
| Charts | `module-index.html` | `/<project>/<module>/index.html` | `<project>/<module>/index.yaml` + `changelog.yaml` |
| Chart detail | `chart-index.html` | `/<project>/<module>/<chart>/index.html` | the chart's `README.md` (rendered as the page) + the module `index.yaml` (version history) |

On every release run CI (re)generates and copies into `gh-pages`, automatically: `projects.json`,
each `<project>/modules.json`, the four HTML page templates, each module's `changelog.yaml`, the
`favicon.png`, and each chart's `README.md` (placed next to its package so the chart-detail page can
render it). You never edit them by hand.

## What is NOT used for versioning

- There are **no per-service git tags**. The previous version of a chart is read from the
  published `index.yaml`. A single tag `"<project>/<module>/PR-<number>"` is cut per release run
  purely for traceability back to the PR.
- Release type is **not** stored as a label on merged history; it is chosen per PR via the form
  and recorded in the module `changelog.yaml`.
