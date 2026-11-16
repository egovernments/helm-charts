# prometheus-kafka-exporter

prometheus-kafka-exporter for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install prometheus-kafka-exporter digit3-backbone/prometheus-kafka-exporter -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/prometheus-kafka-exporter`.

Built on the `digit3/common` library. The image tag comes from `appVersion` (`latest`) unless `global.mode: dev` and `image.tag` are set.

## Required values

Expects DIGIT environment values (for example `global.*` or `cluster-configs.configmaps.egov-config.data`). `ci/test-values.yaml` shows the minimum needed to render it.
