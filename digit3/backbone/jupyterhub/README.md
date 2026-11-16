# jupyterhub

Multi-user Jupyter installation

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install jupyterhub digit3-backbone/jupyterhub -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/jupyterhub`.

Vendored from upstream `jupyterhub/zero-to-jupyterhub-k8s` chart `unpublished (built from source by chartpress)` (app `4.1.4`), with DIGIT modifications - see the `digit.org/*` annotations in `Chart.yaml`.
