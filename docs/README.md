# DIGIT Helm Charts - Documentation

This repository holds all DIGIT Helm charts and publishes them to the chart registry at
**https://helm.digit.org**. Charts are organised as **project -> module -> service**, and
every release is driven from a pull request (no manual packaging, tagging or uploading).

## Start here

| Doc | Read it when you want to... |
|-----|------------------------------|
| [architecture.md](architecture.md) | Understand the layout, the registry URLs, the branches, and how the website is generated. |
| [packaging-and-releasing.md](packaging-and-releasing.md) | Understand exactly what the CI pipeline does on a PR: the form, the gate, versioning, tagging and publishing. |
| [onboarding-a-service.md](onboarding-a-service.md) | Add a new service chart (or a new module / project) to the registry. |
| [release-sop.md](release-sop.md) | Follow the step-by-step checklist your team uses to cut a release. |
| [troubleshooting.md](troubleshooting.md) | Fix a stuck PR check, a failed dependency build, or an unexpected version number. |

## The 30-second version

1. Charts live under `charts/<project>/<module>/<service>/`.
2. You open a PR with your chart change (one project + one module per PR).
3. A bot adds a **Release selection** form to the PR description; you tick one of
   `major` / `minor` / `patch`.
4. On merge, CI versions the chart, packages it, and publishes it to
   `https://helm.digit.org/<project>/<module>/<service>`.

```bash
# consume a published chart
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install my-release digit3-core/localization -n core -f values.yaml
```

## Conventions

- One **project** + one **module** per PR. Multiple service charts inside that module in a
  single PR are fine.
- Service charts depend on the per-project **`common`** library chart, pulled from the registry.
- Versions are derived from the published registry, not from git tags. There are no
  per-service version tags; a single `"<project>/<module>/PR-<number>"` tag is cut per release
  run for traceability.
