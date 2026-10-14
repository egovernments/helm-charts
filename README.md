# helm-charts

Helm charts for all DIGIT services, published to the chart registry at
**https://helm.digit.org** (organised as project -> module -> service).

Add a module as a Helm repo and install a chart:

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install my-release digit3-core/localization -n core -f values.yaml
```

## Documentation

Full documentation lives in [`docs/`](docs/README.md):

- [Architecture](docs/architecture.md) - layout, registry URLs, branches, the website.
- [Packaging and releasing](docs/packaging-and-releasing.md) - what CI does on a PR.
- [Onboarding a service](docs/onboarding-a-service.md) - add a new chart / module / project.
- [Release SOP](docs/release-sop.md) - the step-by-step checklist for teams.
- [Troubleshooting](docs/troubleshooting.md) - fixes for common issues.

## Releasing in one line

Open a PR with your chart change (one module per PR), tick a release type in the auto-added form,
and merge. CI versions, packages, and publishes it. See the SOP for details.
