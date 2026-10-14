# Onboarding a new service

This walks through adding a new service chart to an existing module. Sections at the end cover
adding a new module and a new project.

Prerequisite: the project's `common` library is already published (for `digit3` it is). If you
are standing up a brand-new project, publish its `common` first (see "New project" below).

## 1. Create the chart directory

Add your chart under the correct module:

```
charts/<project>/<module>/<service>/
  Chart.yaml
  values.yaml
  templates/
    deployment.yaml
    service.yaml
    ingress.yaml
    ...
```

Example: `charts/digit3/core/localization/`.

## 2. Write `Chart.yaml`

```yaml
apiVersion: v2
name: <service>
description: <one line>
type: application

# First publish takes THIS version as-is (the release type does not change it).
# Pick the version you want the chart to start at.
version: 1.0.0

# appVersion doubles as the default container image tag (see values.yaml).
appVersion: "<image-tag>"        # e.g. flyway-branch-18c74fe

dependencies:
  - name: common
    version: <published-common-version>     # must already exist in the registry, e.g. 1.0.0
    repository: https://helm.digit.org/<project>/common
```

Key points:

- **`version`** is the seed for the first publish. After that, releases bump it automatically, so
  you will not edit it again by hand (the pipeline does).
- **`appVersion`** is the image tag. Keep it in sync with `values.yaml` `image.tag`.
- **`dependencies[].version`** must point at a `common` version that is **already published**.
  If you bump `common`, bump this too in a follow-up PR.

## 3. Write `values.yaml`

The `common` library's image helper **requires** an explicit tag. Set it (and any init-container
image tag) to the same value as `appVersion`. These are defaults and are overridable at install.

```yaml
image:
  repository: "<service>"
  # defaults to appVersion; override via a values file at install time
  tag: "<image-tag>"            # same as Chart.yaml appVersion

initContainers:
  dbMigration:
    enabled: true
    image:
      repository: "<service>-db"
      tag: "<image-tag>"        # same as appVersion
    # ...
```

If you omit `image.tag`, rendering fails with `Tag is mandatory`.

## 4. Validate locally (recommended)

```bash
cd charts/<project>/<module>/<service>
helm repo add <project>-common https://helm.digit.org/<project>/common
helm repo update
helm dependency build .
helm lint .
helm template test . --set global.containerRegistry=egovio
# confirm override works:
helm template test . --set global.containerRegistry=egovio --set image.tag=override-xyz | grep image:
# clean up the vendored dependency so it is not committed:
rm -rf charts Chart.lock
```

Do **not** commit the vendored `charts/` directory or `Chart.lock`; CI rebuilds dependencies.

## 5. Open the PR and release

Follow [release-sop.md](release-sop.md). In short: branch off the latest `master`, commit only
your chart, open the PR, tick one Release option in the form, get `require-label` green, and merge.
Remember: **one module per PR**.

## 6. Verify after merge

```bash
helm repo add <project>-<module> https://helm.digit.org/<project>/<module>
helm repo update
helm search repo <project>-<module>                 # your chart + version appears
curl -sI https://helm.digit.org/<project>/<module>/<service>/<service>-<version>.tgz   # HTTP 200
```

Also check the chart appears on `https://helm.digit.org/<project>/<module>/` with a changelog entry.

---

## Adding a new module

No configuration is needed for the normal (PR-merge) flow - the module is detected from the path.

1. Create `charts/<project>/<new-module>/<service>/...` as above.
2. Open a PR (one module per PR) and release.
3. The module directory, its `index.yaml`, `changelog.yaml`, and the `<project>/modules.json`
   entry are created automatically, and it appears on the project page.

Optional: to make the new module selectable in the **manual** `workflow_dispatch` run, add it to
the `module` `options` list in `.github/workflows/release.yaml`.

## Adding a new project

1. Create `charts/<new-project>/common/` (the library) and publish it first (it is its own PR,
   module = `common`). Seed its `Chart.yaml` `version`.
2. Create `charts/<new-project>/<module>/<service>/...` with `dependencies[].repository` pointing
   at `https://helm.digit.org/<new-project>/common`.
3. Release `common` first, then the service (dependency ordering).
4. `projects.json` and the project page are generated automatically; the project appears on
   `helm.digit.org`.

Optional: add the new project to the `project` `options` list in `release.yaml` for manual dispatch.
