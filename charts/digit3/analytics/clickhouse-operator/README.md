# clickhouse-operator

ClickHouse Inc.'s official Kubernetes operator
([ClickHouse/clickhouse-operator](https://github.com/ClickHouse/clickhouse-operator)),
which manages `ClickHouseCluster` and `KeeperCluster` resources. The
`digit3/analytics/clickhouse` chart creates those resources.

This chart is a thin wrapper: the upstream chart
(`oci://ghcr.io/clickhouse/clickhouse-operator-helm`, version 0.0.8) is a
dependency, bundled into the package at release time. Upgrading the operator
means bumping that version, not re-copying its templates and CRDs.

## Install

Install this chart before `clickhouse`, because it registers the CRDs.

```bash
helm repo add digit3-analytics https://helm.digit.org/digit3/analytics
helm install clickhouse-operator digit3-analytics/clickhouse-operator -n clickhouse --create-namespace
```

With Argo CD, use one Application for this chart and one for `clickhouse`, and
give this one the earlier sync wave.

## Prerequisites

- **cert-manager.** The operator's webhook and metrics certificates come from a
  self-signed cert-manager `Issuer`.
- **Kubernetes >= 1.28.**

## Environment overrides

Upstream values go under `clickhouse-operator:`, which is the dependency alias.
The same top-level block in an environment file overrides them:

```yaml
clickhouse-operator:
  manager:
    resources:
      requests:
        cpu: 20m
```

## Compared with DIGIT-DevOps `backbone-services/clickhouse-operator`

unified-dev runs a vendored copy of the upstream chart at 0.0.1, with the
operator image pinned by digest. That pin was added after an unpinned `latest`
pull broke the cluster on 2026-08-18. This chart installs the same operator,
with the same settings, at release 0.0.8:

- **Image:** pinned by version (`v0.0.8`, the chart's appVersion), so nothing
  floats with `latest`.
- **RBAC:** grants `pods/status`, the permission whose absence caused that
  outage.
- **Values:** keys are `enabled:` instead of 0.0.1's `enable:`.
- **NetworkPolicy support:** present in the operator, but clusters get no
  policy unless they set `spec.networkPolicy`.
- **CRD additions:** `settings.systemLogsTTLDays`, `extraReloadableConfig`,
  `encryption`, `startupProbe` and `additionalVolumeClaimTemplates`. The
  `clickhouse` chart uses the first of these.
