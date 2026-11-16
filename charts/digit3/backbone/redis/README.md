# redis

redis for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install redis digit3-backbone/redis -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/redis`.

Built on the `digit3/common` library. The image tag comes from `appVersion` (`7.2.4`) unless `global.mode: dev` and `image.tag` are set.
