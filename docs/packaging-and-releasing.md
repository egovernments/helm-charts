# Packaging and releasing

Releases are automated. You never run `helm package`, create a version tag, or upload a `.tgz`
by hand. You open a PR, pick a release type, and merge. This doc explains exactly what happens.

## The workflows

| Workflow file | Trigger | Job | What it does |
|---------------|---------|-----|--------------|
| `.github/workflows/pr-release-form.yaml` | PR opened / reopened / synchronize | `form` | Detects the project+module your branch changed and appends a **Release selection** form to the PR description. |
| `.github/workflows/require-release-label.yaml` | PR opened / reopened / synchronize / edited | `require-label` | The mandatory gate. Blocks merge until the form is valid. **Required status check** on `master`. |
| `.github/workflows/chart-ci.yaml` | PR opened / reopened / synchronize | `lint-template` | `helm lint` + `helm template` every changed chart. Blocks merge until they render cleanly. **Required status check** on `master`. |
| `.github/workflows/chart-image-check.yaml` | PR opened / reopened / synchronize | `image-check` | Renders every changed chart and verifies each image it references actually exists in its registry (and is not a deprecated Bitnami/bitnamilegacy image). **Required status check** on `master`. |
| `.github/workflows/release.yaml` | PR merged to `master`, or manual `workflow_dispatch` | `release` | Versions, packages, publishes to `gh-pages`, updates changelog/discovery files, and cuts the traceability tag. |

Three checks must be green before a chart PR can merge: **`require-label`** (valid release selection),
**`lint-template`** (charts lint and render), and **`image-check`** (every referenced image exists).
All three auto-pass on PRs with no chart changes.

## Step 1 - the Release selection form (automatic)

When you open or push to a PR, `pr-release-form` diffs your branch against `master`
(`--diff-filter=ACMR`, so deletions are ignored) and finds which `<project>/<module>` you touched.
It then appends a block to the PR description:

```
<!-- RELEASE-FORM:START -->
### Release selection
_Tick exactly one per section._

**Project**
- [x] digit3

**Module**
- [x] core

**Release**
- [ ] major
- [ ] minor
- [ ] patch
<!-- RELEASE-FORM:END -->
```

- Project and module are **auto-ticked** when only one was changed (the usual case).
- You tick exactly **one** Release option.
- Your Release pick is **preserved** if you push more commits (the form regenerates but keeps the tick).
- GitHub Markdown has no radio buttons, so these are checkboxes; "exactly one" is enforced by the gate, not the UI.
- Fork PRs cannot be edited by the bot (read-only token). Use a branch in this repo.

## Step 2 - the gate (mandatory, blocks merge)

`require-label` runs on every PR and is a **required status check** on `master`
(branch protection, admins included). It passes only when:

- the PR has **no chart changes** (docs/CI/UI only) -> auto-passes; or
- the PR changes **exactly one project and one module**, and the form has **exactly one**
  Release option ticked.

If you change two modules in one PR, or forget to tick a Release option, the check stays red and
the merge button is disabled.

> One project + one module per PR. Multiple service charts inside that one module are allowed and
> are all released together.

## Step 3 - on merge: version, package, publish

When the PR merges, `release.yaml` runs:

1. **Resolve targets.** Project and module come from the changed paths; the release type comes
   from the ticked Release option in the merged PR body. Every changed service chart under that
   module is a target.
2. **Compute the version.**
   - It reads the chart's highest **published** version from the module `index.yaml` on `gh-pages`.
   - **First publish** (no entry yet): the version is taken **as-is from `Chart.yaml`** (the
     release type is recorded but does not change the number). So seed the version you want in
     `Chart.yaml` for a brand-new chart.
   - **Subsequent publish**: the previous version is bumped by the release type
     (`major` -> X+1.0.0, `minor` -> X.Y+1.0, `patch` -> X.Y.Z+1).
3. **Build dependencies.** `helm dependency build` pulls `common` (and any other deps) from the
   registry. The dependency version must already be published.
4. **Package.** `helm package` -> `gh-pages/<project>/<module>/<chart>/<chart>-<version>.tgz`.
5. **Re-index.** Rebuilds the module `index.yaml` (merging existing entries) with correct nested URLs.
6. **Changelog + discovery.** Prepends an entry to `<project>/<module>/changelog.yaml`
   (date, PR ref, title, author, release type, each chart `from -> to`), regenerates
   `projects.json` and `<project>/modules.json`, and copies the registry HTML into `gh-pages`.
7. **Tag.** Cuts one annotated tag `"<project>/<module>/PR-<number>"` for traceability.

### Versioning examples

| Situation | `Chart.yaml` version | Release ticked | Published version |
|-----------|----------------------|----------------|-------------------|
| First publish of `common` | `1.0.0` | major | **`1.0.0`** (seeded; label ignored for the number) |
| First publish of `localization` | `1.0.0` | major | **`1.0.0`** (seeded) |
| Next `localization` release | (any) | `minor` | **`1.1.0`** (bumped from published `1.0.0`) |
| Next `localization` release | (any) | `patch` | **`1.0.1`** |

## Manual releases (workflow_dispatch)

For backfills or re-publishing without a code change, run **release.yaml** from the Actions tab.
It exposes dropdowns:

- `project` (choice), `module` (choice), `service` (free text, blank/ignored for `common`),
  `release` (choice).

The dropdown lists are hardcoded in `release.yaml`. A brand-new module or project works
automatically on the **PR-merge** path (it is path-detected), but to appear in the **manual**
dropdowns you must add it to the `workflow_dispatch` `options` in `release.yaml`.

## Branch protection and auto-delete

- `master` requires the `require-label` and `lint-template` status checks to pass; `enforce_admins` is on.
- Direct pushes and force-pushes to `master` are blocked; everything goes through a PR.
- Merged PR branches are auto-deleted (`delete_branch_on_merge`).
