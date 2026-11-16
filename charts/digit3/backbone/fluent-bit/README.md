# fluent-bit

fluent-bit for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install fluent-bit digit3-backbone/fluent-bit -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/fluent-bit`.

## Required values

Expects DIGIT environment values (for example `global.*` or `cluster-configs.configmaps.egov-config.data`). `ci/test-values.yaml` shows the minimum needed to render it.
