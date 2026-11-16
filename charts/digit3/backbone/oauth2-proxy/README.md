# oauth2-proxy

oauth2-proxy for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install oauth2-proxy digit3-backbone/oauth2-proxy -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/oauth2-proxy`.

Built on the `digit3/common` library. The image tag comes from `appVersion` (`v5.1.0`) unless `global.mode: dev` and `image.tag` are set.
