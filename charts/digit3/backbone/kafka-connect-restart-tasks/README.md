# kafka-connect-restart-tasks

kafka-connect-restart-tasks for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install kafka-connect-restart-tasks digit3-backbone/kafka-connect-restart-tasks -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/kafka-connect-restart-tasks`.

Built on the `digit3/common` library. The image tag comes from `appVersion` (`1-master-02d88e7f`) unless `global.mode: dev` and `image.tag` are set.
