# DIGIT 3.0 one-click helmfiles

The helmfiles of the DIGIT one-click install on Azure. They deploy DIGIT core 3.0 with its backbone and monitoring from the charts in this repository (https://helm.digit.org) and community charts.

They are **not** used on their own. The one-click workflow in
[egovernments/DIGIT-DevOps](https://github.com/egovernments/DIGIT-DevOps) (`.github/workflows/digit_install.yml`, guide `one-click/README.md`) checks out this folder at a pinned ref. That ref is `infra.helmfiles.ref` in the team's environment file.

## Inputs: only the two environment files

| Variable | File (in the team's DIGIT-DevOps fork) |
|---|---|
| `DIGIT_ENV_FILE` | `one-click/<env>.yaml` (egov-demo layout: `infra`, `global`, one block per release) |
| `DIGIT_SECRETS_FILE` | `one-click/<env>-secrets.yaml` (SOPS; `cluster-configs.secrets.*`) |

Each release gets its own block as Helm values. Releases built on the DIGIT common chart also get `global`, with their namespace.

## Layout

| Path | What it is |
|---|---|
| `digit-helmfile.yaml.gotmpl` | main helmfile: the three layers in order |
| `helmfiles/_environment.yaml.gotmpl` | shared base: the two env files + Helm defaults |
| `helmfiles/backbone.yaml.gotmpl` | cluster-configs, cert-manager, ingress-nginx, redis, kafka-kraft, minio, vault, elasticsearch, kibana |
| `helmfiles/core.yaml.gotmpl` | keycloak, kong, and every env-file block with a `chart-version` that is not a platform release (the DIGIT services) |
| `helmfiles/monitoring.yaml.gotmpl` | kube-prometheus-stack, loki, grafana, opentelemetry-collector, jaeger, blackbox exporter |
| `helmfiles/argocd.yaml.gotmpl` | Argo CD itself (Argo CD mode) with the `digit-helmfile` plugin |
| `scripts/vault-bootstrap.sh` | initialises and unseals Vault, enables transit + AppRole, writes `egov/vault-approle` (postsync hook + workflow) |
| `scripts/argocd-plugin.sh` | Argo CD plugin: the root app gives one Application per release; each release is rendered with `helmfile template` |

## Run by hand

```bash
export DIGIT_ENV_FILE=/path/to/<env>.yaml DIGIT_SECRETS_FILE=/path/to/<env>-secrets.yaml
helmfile -f digit-helmfile.yaml.gotmpl template          # or apply
helmfile -f helmfiles/core.yaml.gotmpl apply -l name=workflow
```

Needs helmfile 1.x, Helm with the helm-secrets plugin, and sops able to decrypt the secrets file.

This folder is not a chart: chart CI and the release form ignore it. Changes are pinned per environment by `infra.helmfiles.ref`, so a commit here doesn't change any running environment until a team moves its ref.
