# cert-manager

cert-manager for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install cert-manager digit3-backbone/cert-manager -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/cert-manager`.

Vendored from upstream `jetstack/cert-manager` chart `1.13.3` (app `v1.13.3`), with DIGIT modifications - see the `digit.org/*` annotations in `Chart.yaml`.
