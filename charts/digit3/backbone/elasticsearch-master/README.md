# elasticsearch-master

elasticsearch-master for the DIGIT backbone.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install elasticsearch-master digit3-backbone/elasticsearch-master -n backbone -f <environment-values>.yaml
```

## Source

Migrated from `egovernments/DIGIT-DevOps` (master) `deploy-as-code/helm/charts/backbone-services/elasticsearch-master`.
