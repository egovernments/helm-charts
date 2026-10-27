# Troubleshooting

## The release form lists charts I did not change

**Symptom:** the auto-added form shows many projects/modules (old charts), not just yours.

**Cause:** your branch was cut from a `master` that still had the old flat chart tree, so the diff
against the current `master` shows those old charts as "added".

**Fix:** rebase onto the current master and force-push.

```bash
git fetch origin master
git rebase origin/master
git push --force-with-lease
```

The form regenerates on push and should then list only your project/module.

## The release form vanished from the PR description

**Symptom:** the **Release selection** form is gone and `require-label` is red.

**Cause:** the PR description was edited (e.g. to rewrite the summary) in a way that removed the
`<!-- RELEASE-FORM:START --> … <!-- RELEASE-FORM:END -->` block. The block is managed by the
`pr-release-form` workflow, not by hand.

**Fix:** push any commit, or close and reopen the PR. The workflow re-injects the form (preserving
any ticks it finds). Do not paste a hand-written form; let the workflow generate it.

## `require-label` check is red / merge is blocked

The gate requires, for a chart PR: exactly one project changed, exactly one module changed, and
exactly one Release option ticked.

- **No Release ticked** -> tick exactly one `major`/`minor`/`patch` in the form.
- **Two Release options ticked** -> leave only one.
- **Two modules or two projects changed** -> split into separate PRs (one module per PR).
- **It is a docs/CI/UI PR with no chart changes** -> it should auto-pass; if it is red, re-run the
  check from the Checks tab.

## `helm dependency build` fails with "not found" / 404 for common

**Cause:** the `common` version pinned in `dependencies[].version` is not published yet, or the
`repository:` URL is wrong.

**Fix:**

- Confirm the version exists: `helm repo add c https://helm.digit.org/<project>/common && helm repo update && helm search repo c`.
- Point `dependencies[].version` at a version that is actually published.
- Publish/bump `common` first, then release the dependent (dependency ordering).
- Ensure `repository: https://helm.digit.org/<project>/common` (per-project path).

## The service deploys the wrong image tag (or `appVersion` is `1.16.0`)

**Cause:** `appVersion` was left at the `helm create` scaffold default (`1.16.0` / `1.0.0`). In the
default `prod` mode, `common.image` uses `Chart.AppVersion` as the image tag, so the pod pulls
`<service>:1.16.0` - which does not exist.

**Fix:** set `appVersion` in `Chart.yaml` to the **real image tag** (e.g. `flyway-branch-18c74fe`,
the same tag the `<service>-db` images use). It drives both the service image and the db-migration
init-image. Do **not** set `image.tag` in prod mode - it is ignored; only `global.mode: dev` reads
`image.tag`. See [onboarding-a-service.md](onboarding-a-service.md).

## The chart published under the wrong name (`digit-<name>`)

**Cause:** the modulith `Chart.yaml` kept `name: digit-<service>`, so the chart publishes as
`digit-<service>` even though the directory is `<service>`.

**Fix:** set `name:` in `Chart.yaml` to the plain service name (match the directory). To correct an
already-published one: fix the name, delete the `digit-<name>` entry + its `.tgz` from the module
`index.yaml` on `gh-pages`, then re-release (it first-publishes under the correct name at the seed
version).

## The published version is not what I expected

- **First publish seeds from `Chart.yaml`.** The very first release of a chart uses the `version:`
  in `Chart.yaml` as-is; the Release type does not change that number. Set the seed you want.
- **Later releases bump from the registry**, not from `Chart.yaml`. Editing `Chart.yaml` `version`
  by hand on a subsequent release does not override the computed bump.

## The chart published but the URL 404s

- **CDN cache:** GitHub Pages can lag a minute or two after a release. Retry.
- **URL vs file mismatch:** the `index.yaml` URL must match the nested file path
  `<project>/<module>/<chart>/<chart>-<version>.tgz`. If they diverge (e.g. after a workflow change),
  re-run the release or correct the one-off entry on `gh-pages`.

## helm.digit.org shows an old or empty page

- The website HTML, `projects.json`, and `<project>/modules.json` are regenerated and copied on
  every **release** run. A docs/CI/UI-only merge does **not** trigger a release, so the site updates
  on the next chart release. To refresh immediately, run `release.yaml` via `workflow_dispatch`, or
  copy the HTML and discovery files into `gh-pages` once by hand.

## Release failed: "expected one project + one module per PR (got 1/N)"

**Cause (historical):** the release job used to diff `base.sha..merge_commit_sha`. If your branch
was behind `master`, that range also captured whatever else merged in between, so unrelated modules
leaked in and tripped the one-module gate. Fixed - the job now reads the PR's own files via the API
(same source as `require-label`), so it should not recur.

**If it still happens:** re-sync your branch onto the current `master` (merge or rebase) and re-run,
so the PR genuinely changes one module.

## Release failed at "Sync chart versions to master & tag" (`protected branch hook declined`)

**Symptom:** the chart publishes to `gh-pages` fine, but the run fails writing the bumped version
back to `master` with `GH006: Protected branch update failed` / `N of N required status checks are
expected`, and no new tag is created.

**Cause:** `master`'s branch protection demands its required status checks on *every* push, including
the release bot's. The bot's `GITHUB_TOKEN` commit has no check runs, so the push is rejected. This
only affects the version **bump** write-back - a first publish (seed) does not push to master, so it
is not hit until a chart is released a second time.

**Note:** versioning does not depend on this. The next version is computed from the published
`index.yaml`, not from `master`'s `Chart.yaml`; the write-back is traceability only. To make it work,
the release must push with a **GitHub App / PAT** that is a bypass actor on a `master` ruleset - the
Actions `GITHUB_TOKEN` cannot be allowlisted past required status checks.

## Releases fail after protecting `gh-pages` (`protected branch hook declined`)

**Cause:** a "restrict who can push" allowlist on `gh-pages` (even listing the `github-actions` app)
does **not** cover the release bot's `GITHUB_TOKEN` push - GitHub does not treat the Actions token as
that app for push restrictions. Every release then fails pushing to `gh-pages`.

**Fix:** do not put a push-who restriction on `gh-pages`. Protect it with **block force-push + block
deletion** only - those are the real tampering risks and do not block normal bot pushes. A strict
per-identity push restriction needs a GitHub App/PAT bypass via a ruleset, not the default token.

## I cannot merge even though I am an admin

Branch protection on `master` has `enforce_admins` on. Satisfy the `require-label` check (tick a
Release option) like everyone else. Do not disable protection to merge.

## `helm template`/`lint` fails: `template "common.deployment" not defined`

**Cause:** you ran it on a service chart without its `common` dependency present.

**Fix:** either `helm dependency build .` (pulls `common` from the registry), or vendor the in-repo
copy just for the check and delete it after:

```bash
mkdir -p charts/digit3/core/<service>/charts
cp -r charts/digit3/common charts/digit3/core/<service>/charts/common
helm template test charts/digit3/core/<service> --set global.namespace=core
rm -rf charts/digit3/core/<service>/charts   # throwaway; never commit it
```

## Health probe is `/<context>/<context>/health` or the pod is never Ready

**Cause:** the context was baked into `livenessProbePath`/`readinessProbePath` (e.g.
`/mdms-v2/health` or `/<context>/actuator/health/liveness`). `common` already prepends
`ingress.context`, so the value double-prefixes, and the kubelet probes a path the app does not serve.

**Fix:** set both probe paths to just `/health`. `common` adds the context, and because it sets the
actuator base-path to `/`, `/health` is the real endpoint - the rendered probe is `/<context>/health`.

## PostgreSQL (CloudNativePG): app database / user not created

The `backbone/postgresql` chart does **not** create the application database or role through CNPG's
`initdb`. CNPG keeps its own managed superuser and the reserved `postgres` database; a **reconciler**
(a post-install/post-upgrade hook Job plus a periodic CronJob, both in the `backbone` namespace)
connects as the superuser and idempotently creates the role + database from live config:
`db-name` from `egov-config`, username/password from the `db` Secret.

- **It reads config from the app namespace** (default `egov`, override with
  `custom_config.enabled: true` + `custom_config.namespace: <ns>`), via a `Role`+`RoleBinding` the
  chart renders there. If the role/db are not created, check that Role/RoleBinding exists in the app
  namespace and that `egov-config`/`db` are present there.
- **`postgres` is reserved** - it cannot be the application database in CNPG; services connect to the
  reconciler-created database named by `egov-config/db-name` (e.g. `coredb`).
- The CronJob reconciles every few minutes, so a later change to `db-name` or the `db` Secret is
  picked up without a redeploy (`reconciler.cron.*`).

## I need to delete tags or branches in bulk

Local automation may flag bulk `git push --delete ... (loop)` as unsafe. An explicit, single
`git push origin --delete <name> <name> ...` works. Back up first:

```bash
git for-each-ref refs/tags --format='%(refname:short) %(objectname)' > tags-backup.txt
git branch -r --format='%(refname:short) %(objectname)' > branches-backup.txt
```
