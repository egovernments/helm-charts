# novu

Novu community deployment

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install novu digit3-backbone/novu -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/novu`.

Built on the `digit3/common` library. The image tag comes from `appVersion` (`2.3.0`) unless `global.mode: dev` and `image.tag` are set.
