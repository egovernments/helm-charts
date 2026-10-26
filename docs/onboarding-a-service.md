# Onboarding a new service

This walks through adding a new service chart to an existing module (the common case: a service
under `digit3/core`). Sections at the end cover a new module and a new project.

Prerequisite: the project's `common` library is already published (for `digit3` it is, at `3.0.0`).

## 0. Where charts come from

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

Service charts are thin — the templates just include the `common` library helpers.

## 2. Write `Chart.yaml`

```yaml
apiVersion: v2
name: <service>                        # MUST match the directory and the registry name.
                                       # Strip any leftover "digit-" prefix from the modulith
                                       # Chart.yaml name, or the chart publishes mis-named.
description: <one line>
type: application

version: 3.0.0                         # digit3 seed baseline. First publish uses this as-is.

appVersion: "flyway-branch-18c74fe"    # the REAL image tag. The modulith/LTS charts ship the
                                       # helm-create default (1.16.0 / 1.0.0) — replace it. In prod
                                       # mode this drives BOTH the service image and the
                                       # <service>-db init-image tag.

dependencies:
  - name: common
    version: 3.0.0
    repository: https://helm.digit.org/digit3/common
```

Key points:

- **`version: 3.0.0`** is the seed. First publish takes it as-is; later releases bump it from the
  registry, so you will not edit it again by hand.
- **`name`** must equal the directory and the intended registry name. A `digit-<name>` left over
  from the modulith chart publishes a mis-named chart — strip the prefix.
- **`appVersion`** is the image tag. Do **not** leave it at the scaffold default `1.16.0` — set the
  real tag (e.g. `flyway-branch-18c74fe`, the same tag the `<service>-db` images use).
- **`dependencies[].version`** must be a `common` version that is **already published** (`3.0.0`).

## 3. Write `values.yaml`

### Image tag — the `global.mode` mechanism

`global.mode` (default **`prod`**, set in `common`) decides the tag:

- **prod** (default): the image tag is always `Chart.AppVersion`. You do **not** set `image.tag`; it
  is ignored. Just set `appVersion` in `Chart.yaml`.
- **dev**: uses `.Values.image.tag` if set, else falls back to `appVersion`.

```yaml
image:
  repository: "<service>"
initContainers:
  dbMigration:
    enabled: true
    image:
      repository: "<service>-db"   # tag also comes from appVersion in prod mode
```

### Ingress — the gateway

```yaml
ingress:
  enabled: true
  gateway: true          # the gateway flag (renamed from the old `zuul`)
  context: "<service>"   # NO "digit-" prefix; drives the /<context> route + gateway/route-path
```

The ingress backend resolves to the **`kong-proxy`** service (from the `kong` chart), and the
`gateway/route-path` annotation is taken from `ingress.context`.

### Context path (`SERVER_CONTEXT_PATH` / `CONTEXT_PATH`)

The app serves at its **codebase default** context path. Rule for the Helm context-path env:

1. Take the chart's context-path env value and strip any `digit-` prefix.
2. If the result **equals the codebase default**, **remove the env** — the app already defaults
   there (e.g. `filestore`, `pg-service`, `idgen`, `notification`, `otp`, `workflow`, …).
3. If it **differs**, **keep the env** with the stripped value. Known exceptions where the code
   default is not the plain name: `account` → `/accounts`, `employee` → `/employee-java`,
   `individual` → `/individuals`.
4. Services whose context-path is hardcoded in the code (no env) never need the Helm env.

The env-var name varies by service (`CONTEXT_PATH`, `SERVER_CONTEXT_PATH`,
`SERVER_SERVLET_CONTEXT_PATH`) — match whatever the source chart uses.

### Health checks (optional, per service)

Disabled by default in `common`. To enable, set `healthChecks.enabled: true` in the service values.
The probe path is **context-prefixed** so the kubelet hits the app's real served path:

```
path: "/{{ .Values.ingress.context }}{{ .Values.healthChecks.livenessProbePath }}"
```

e.g. context `localization` + `/health` → `/localization/health`.

## 4. Clean up formatting

Strip trailing whitespace, ensure a single trailing newline, and no leading blank lines — the whole
registry is kept whitespace-clean.

## 5. Validate locally

```bash
cd charts/digit3/core/<service>
helm dependency build .      # pulls common 3.0.0 from the registry
helm lint .
helm template test .         # confirm it renders; the ingress backend should be kong-proxy
rm -rf charts Chart.lock     # do NOT commit the vendored dependency; CI rebuilds it
```

## 6. Open the PR and release

Branch off the latest `master`, commit only your chart(s), open the PR, tick one Release option in
the auto-added form, get `require-label` green, and merge. **One module per PR** — several services
under the same module in one PR is fine (they all count as module `core`).

- **First publish seeds from `Chart.yaml` version** (`3.0.0`); the Release type is ignored.
- **Later releases bump the published version** per the Release type (patch/minor/major), read from
  the registry `index.yaml` (not from `Chart.yaml`).

## 7. Verify after merge

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm search repo digit3-core            # your chart + 3.0.0 appears
```

Check that `https://helm.digit.org/digit3/core/` lists it with a changelog entry.

---

## Adding a new module

No configuration is needed for the normal (PR-merge) flow — the module is detected from the path.

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
3. Release `common` first, then the service (dependency ordering).
4. `projects.json` and the project page are generated automatically; the project appears on
   `helm.digit.org`.

Optional: add the new project to the `project` `options` list in `release.yaml` for manual dispatch.
