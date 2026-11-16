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

## Configuration

Everything is set from values, so nothing needs editing in `templates/`:

| Key | Default | Purpose |
|---|---|---|
| `elasticsearch.host` / `elasticsearch.port` | `elasticsearch-data-v1.es-cluster` / `9200` | Elasticsearch output |
| `config.<file>` | the shipped fluent-bit files | Each config file (`fluent-bit.conf`, `parsers.conf`, inputs, filters, Kafka outputs), rendered with `tpl`. Override one file and the others are kept. |
| `namespace`, `image.*`, `egov-services-log-topic`, `egov-infra-log-topic` | see `values.yaml` | |
