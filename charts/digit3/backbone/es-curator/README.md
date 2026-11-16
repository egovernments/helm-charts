# es-curator

es-curator for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install es-curator digit3-backbone/es-curator -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/es-curator`.

Built on the `digit3/common` library. The image tag comes from `appVersion` (`8.0.15`) unless `global.mode: dev` and `image.tag` are set.

## Configuration

| Key | Purpose |
|---|---|
| `config."action_file.yml"` | Curator actions: index patterns, age filters, retention. Rendered with `tpl`. |
| `config."config.yml"` | Curator client config. |
| `cron.schedule`, `logs-to-retain-in-days`, `logs-cleanup-enabled`, `jaeger-cleanup-enabled`, `namespace`, `image.*`, `env` | see `values.yaml` |
