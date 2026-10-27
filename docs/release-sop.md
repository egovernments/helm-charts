# Release SOP (standard operating procedure)

Follow this every time you release a chart. It is deliberately short. Background is in
[packaging-and-releasing.md](packaging-and-releasing.md).

## Golden rules

- **One project + one module per PR.** Multiple service charts inside that one module are fine.
- **Branch off the latest `master`.** A stale branch makes the release form list the wrong charts.
- **Publish dependencies first.** Publish/bump `common` (or any dependency) before the charts that
  depend on it, and point the dependent's `dependencies[].version` at a version that already exists.
- **Everything goes through a PR.** No direct pushes to `master`; no manual packaging or tagging.
- **Leave the release form alone.** The `<!-- RELEASE-FORM -->` block in the PR description is
  managed by a workflow. Do not delete it when editing the description - if you do, push any commit
  (or reopen the PR) to re-inject it; your ticks are preserved.

## Pre-flight checklist

- [ ] `git checkout master && git pull` - start from the latest master.
- [ ] `git checkout -b <type>/<short-name>` - a fresh branch.
- [ ] Changes are limited to **one** `charts/<project>/<module>/...` module.
- [ ] For a **new chart**: `Chart.yaml` has the seed `version`, the right `appVersion`
      (= image tag), and `dependencies[].version` points at a **published** `common` version.
- [ ] `appVersion` is the **real image tag** (prod mode uses it for the service + init images; do
      not leave the `1.16.0` scaffold default). No vendored `charts/` or `Chart.lock` committed.
- [ ] (Recommended) `helm dependency build` + `helm lint` + `helm template` pass locally.

## Release steps

1. **Push the branch and open a PR to `master`.**
2. **Wait a few seconds, refresh the PR.** The bot appends the **Release selection** form.
   Project and module should be auto-ticked (verify they are correct).
3. **Tick exactly one Release option:**
   - `major` - breaking change (or a deliberate 1.0.0-style baseline)
   - `minor` - backwards-compatible feature
   - `patch` - fix / config tweak

   *(First publish of a chart ignores this for the version number - it seeds from `Chart.yaml` -
   but still tick one; the gate requires it and it is recorded in the changelog.)*
4. **Confirm the `require-label` check is green.** If it is red, see the form rules below.
5. **Merge.** CI versions, packages, publishes, updates the changelog/site, and tags the run.
6. **Verify** (see below).

## Choosing the release type

| You changed... | Pick |
|----------------|------|
| New chart, first publish | the type that matches your seed intent (commonly `major` for a 1.0.0 baseline) |
| New feature, values, or template addition, backwards compatible | `minor` |
| Bug fix, image tag bump, config tweak | `patch` |
| Breaking change (removed/renamed values, incompatible template) | `major` |

## Post-release verification

```bash
helm repo add <project>-<module> https://helm.digit.org/<project>/<module>
helm repo update
helm search repo <project>-<module>                                  # new version listed
curl -sI https://helm.digit.org/<project>/<module>/<svc>/<svc>-<ver>.tgz   # HTTP 200
```

- [ ] New version shows in `helm search repo`.
- [ ] The `.tgz` returns HTTP 200.
- [ ] The chart and a changelog entry appear on `https://helm.digit.org/<project>/<module>/`.
- [ ] The traceability tag `"<project>/<module>/PR-<number>"` exists.

## If the gate (`require-label`) stays red

- No Release option ticked -> tick exactly one.
- More than one module or project changed -> split into separate PRs.
- Form lists charts you did not touch (old charts show as "added") -> your branch is off a stale
  `master`. Rebase: `git fetch origin master && git rebase origin/master && git push --force-with-lease`.

More in [troubleshooting.md](troubleshooting.md).

## Dependency-ordered releases (e.g. updating `common`)

1. PR 1: change `charts/<project>/common`, release it (module = `common`). Note the new version.
2. PR 2: bump `dependencies[].version` to the new `common` version in each dependent chart and
   release them. `helm dependency build` will only find the new `common` after PR 1 is published.
