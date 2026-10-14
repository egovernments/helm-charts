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

## Template render fails with "Tag is mandatory"

**Cause:** the `common.image` helper requires an image tag and does not default to `appVersion`.
Your `values.yaml` is missing `image.tag` (or an init-container `image.tag`).

**Fix:** add `image.tag` (and any init-container image tag) in `values.yaml`, set to the same value
as the chart's `appVersion`. See [onboarding-a-service.md](onboarding-a-service.md).

> If you want tags to default from `appVersion` automatically (so you can omit them from
> `values.yaml`), that requires a change to the `common` library (`common.image` and the
> `_deployment.yaml` call sites to use `default $.Chart.AppVersion`). It is a deliberate design
> choice, not currently enabled.

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

## I cannot merge even though I am an admin

Branch protection on `master` has `enforce_admins` on. Satisfy the `require-label` check (tick a
Release option) like everyone else. Do not disable protection to merge.

## I need to delete tags or branches in bulk

Local automation may flag bulk `git push --delete ... (loop)` as unsafe. An explicit, single
`git push origin --delete <name> <name> ...` works. Back up first:

```bash
git for-each-ref refs/tags --format='%(refname:short) %(objectname)' > tags-backup.txt
git branch -r --format='%(refname:short) %(objectname)' > branches-backup.txt
```
