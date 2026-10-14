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
    version: 1.0.0
    repository: https://helm.digit.org/digit3/common
```

At release time CI runs `helm dependency build`, which downloads the pinned `common` version
from the registry. This means **`common` must be published before any chart that depends on it**,
and a dependent's `version:` must point at a `common` version that already exists in the registry.

### Image tags come from values, defaulted to the service's appVersion

The `common` library's `common.image` helper **requires** an image tag (`required "Tag is mandatory"`).
It does not fall back to `appVersion` on its own. The convention is:

- set the chart's `appVersion` to the image tag you want (e.g. `flyway-branch-18c74fe`), and
- set `image.tag` (and any init-container image tag) in `values.yaml` to the same value.

Both are plain `values.yaml` defaults, so a values file supplied at install time overrides them:

```bash
helm install my-release digit3-core/localization -f my-values.yaml   # my-values.yaml image.tag wins
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

`projects.json`, each `<project>/modules.json`, the three HTML pages, and each module's
`changelog.yaml` are (re)generated and copied into `gh-pages` automatically on every release run.
You never edit them by hand.

## What is NOT used for versioning

- There are **no per-service git tags**. The previous version of a chart is read from the
  published `index.yaml`. A single tag `"<project>/<module>/PR-<number>"` is cut per release run
  purely for traceability back to the PR.
- Release type is **not** stored as a label on merged history; it is chosen per PR via the form
  and recorded in the module `changelog.yaml`.
