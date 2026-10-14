# digit3 (project)

This directory is a **project**. Its layout is:

```
charts/
  digit3/                 <- PROJECT
    common/               <- shared library chart for this project
    <module>/             <- MODULE  (core, health, urban, frontend, backbone, ...)
      <service>/          <- SERVICE CHART
        Chart.yaml
        values.yaml
        templates/
```

## How it is published

On a PR merge to `master`, every changed chart is released to the registry:

```
https://helm.digit.org/<project>/<module>/<service>/<service>-<version>.tgz
```

The Helm repo index for a module lives at:

```
https://helm.digit.org/<project>/<module>/index.yaml
```

so you consume a module as a repo:

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install egov-user digit3-core/egov-user -n core -f values.yaml
```

## Releasing

1. Put / change your chart under `charts/digit3/<module>/<service>/`.
2. Open a PR to `master`.
3. Add **exactly one** label: `release:major` | `release:minor` | `release:patch`
   (the **Require release label** check blocks merge until you do).
4. Merge. The chart is auto-versioned, packaged and published.

### Versioning
- First publish of a chart: the `version:` in its `Chart.yaml` is used as the seed.
- Every release after that: the previous version is read from the published
  `index.yaml` and bumped per the PR label. No per-service git tags are created.

### Traceability
One tag per release run: `<project>/<module>/PR-<number>` (or `.../dispatch-<run>` for
manual `workflow_dispatch` re-publishes).

## Dependency on `common`
Service charts depend on this project's library chart:

```yaml
dependencies:
  - name: common
    version: ">=0.1.0"
    repository: "https://helm.digit.org/digit3/common"
```
