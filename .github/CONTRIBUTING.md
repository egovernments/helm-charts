# Contributing

Thanks for contributing to the DIGIT Helm charts. Releases are automated: open a PR, pick a
release type, and merge. CI versions, packages, and publishes the chart to
**https://helm.digit.org**.

## Before you start

- Read the **[Release SOP](../docs/release-sop.md)** - the step-by-step checklist.
- New to the repo or adding a chart? See **[Onboarding a service](../docs/onboarding-a-service.md)**.
- Want the full picture? See the **[docs index](../docs/README.md)**.

## The rules in short

1. **Branch off the latest `master`.** A stale branch makes the release form list the wrong charts.
2. **One project + one module per PR.** Multiple service charts inside that one module are fine.
3. **Publish dependencies first.** Release/bump `common` before the charts that depend on it, and
   point `dependencies[].version` at a `common` version that is already published.
4. **Everything goes through a PR.** No direct pushes to `master`; no manual packaging or tagging.

## What happens on your PR

- A bot adds a **Release selection** form to the PR description. Tick exactly one of
  `major` / `minor` / `patch`.
- Two checks must pass before merge:
  - **`require-label`** - a valid release selection (one project, one module, one release).
  - **`lint-template`** - every changed chart passes `helm lint` and `helm template`.
- On merge, the chart is versioned, packaged, published, tagged (`<project>/<module>/PR-<n>`),
  and the registry site is updated.

## Example: releasing a change to `localization`

```bash
# 1. fresh branch off master
git checkout master && git pull
git checkout -b patch/localization-config

# 2. edit the chart (one module only)
$EDITOR charts/digit3/core/localization/values.yaml

# 3. (recommended) validate locally
cd charts/digit3/core/localization
helm repo add digit3-common https://helm.digit.org/digit3/common && helm repo update
helm dependency build . && helm lint . && helm template t . --set global.containerRegistry=egovio >/dev/null
rm -rf charts Chart.lock && cd -

# 4. push and open a PR
git add charts/digit3/core/localization
git commit -m "patch(localization): tweak config"
git push -u origin patch/localization-config
```

Then on the PR:

1. Wait a few seconds, refresh. The **Release selection** form appears with `digit3` / `core`
   auto-ticked.
2. Tick **one** Release option (here, `patch`).
3. Wait for **`require-label`** and **`lint-template`** to go green.
4. Merge. `localization` is bumped (e.g. `1.0.1`), packaged, and published to
   `https://helm.digit.org/digit3/core/localization`.

## Optional: CI render values

If your chart needs specific values to render (beyond `global.containerRegistry`), add a
`ci/test-values.yaml` inside the chart. `lint-template` applies it automatically during the check.
