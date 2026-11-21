# clickhouse

A ClickHouse cluster and its ClickHouse Keeper, declared as `ClickHouseCluster`
and `KeeperCluster` resources. Install `digit3/analytics/clickhouse-operator`
first: it owns the CRDs and creates the StatefulSets, Services, configuration
and PodDisruptionBudgets from these resources.

## Install

```bash
helm repo add digit3-analytics https://helm.digit.org/digit3/analytics
helm install clickhouse-operator digit3-analytics/clickhouse-operator -n clickhouse --create-namespace
helm install clickhouse digit3-analytics/clickhouse -n clickhouse -f <environment>.yaml
```

With Argo CD, use two Applications and give the operator the earlier sync wave.

## Connecting

The operator creates the client Service `<name>-clickhouse-headless`, which is
`clickhouse-clickhouse-headless` with the defaults. Its ports:

| Port | Use |
|---|---|
| 8123 | HTTP (Superset's `clickhousedb://` / clickhouse-connect) |
| 9000 | native protocol |
| 9363 | Prometheus metrics |

From Superset, for example:
`clickhousedb://app:<password>@clickhouse-clickhouse-headless.clickhouse.svc.cluster.local:8123/default`

## Secrets

No password is set in values. Every password comes from the Secret
`auth.secretName` (default `clickhouse-users`) in the cluster's namespace:

| Key | User |
|---|---|
| `default-password` (`auth.defaultUserPasswordKey`) | `default`, via the operator's `settings.defaultUserPassword` |
| `app-password` | `app` |
| `otelcollector-password` | `otelcollector` |
| `<users.X.passwordKey>` | any user added under `users:` |

The users under `users:` get their password through an env var on the
ClickHouse container, set from the Secret, and ClickHouse's `from_env`
attribute. The password therefore never appears in the generated config or in
the `ClickHouseCluster` object. Create the Secret yourself (for example with
cluster-configs from SOPS), or set `auth.createSecret: true` and pass
`auth.passwords`.

## Environment overrides

Every value can be overridden from the environment file under a top-level
`clickhouse:` key:

```yaml
clickhouse:
  cluster:
    spec:
      containerTemplate:
        resources:
          requests:
            cpu: "250m"
      dataVolumeClaimSpec:
        resources:
          requests:
            storage: 50Gi
  users:
    superset:
      passwordKey: superset-password
      grants:
        - GRANT SELECT ON analytics.*
```

`templates/_helpers.tpl` (`clickhouse.init`) merges that block over
`values.yaml` in every template. Empty strings, `false` and `0` override as
expected. Maps merge key by key, and lists replace the default whole.
`cluster.spec` and `keeper.spec` pass through to the operator unchanged, so any
field of the CRDs (`kubectl explain clickhousecluster.spec`) can be set.

## Compared with DIGIT-DevOps `backbone-services/clickhouse` (unified-dev)

unified-dev's chart is a renamed copy of the ClickStack observability chart
(`ClickHouse/ClickStack-helm-charts` `clickstack` 3.0.0). The only parts it uses
are ClickHouse and Keeper; HyperDX, MongoDB and the OTel collector are all
disabled. This chart keeps the part that is used and what it does:

| | unified-dev | here |
|---|---|---|
| Topology | 1 shard × 1 replica + 1 Keeper | same |
| Server | `clickhouse-server:25.7-alpine` (floating minor) | `25.7.8.71-alpine`, the same build unified-dev runs today |
| Keeper | `clickhouse-keeper:latest` (runs 26.10 today) | `25.7.8.71-alpine`, matching the server |
| Users | `default`, `app`, `otelcollector`, same grants | same users and grants |
| Passwords | plain text in `values.yaml` (in git) | from a Secret (`defaultUserPassword`, `from_env`) |
| `extraConfig` | `max_connections` 4096, `keep_alive_timeout` 64, `max_concurrent_queries` 100 | same |
| Storage | 10Gi data, 5Gi Keeper (live volume resized to 50Gi by hand) | same defaults; set the size per environment |
| Resources | server 1/2Gi requests, 4/8Gi limits; Keeper 250m/512Mi, 1/512Mi | same |
| Logging | `trace`, 50 × 1000M files | `information` with the operator's 10 × 100M |
| System log retention | unbounded | `systemLogsTTLDays: 30` |
| Unused leftovers | HyperDX, MongoDB, OTel collector, alert CronJob templates and their hard-coded credentials | dropped |

The logging and retention rows are the only behavioural differences, and both
are deliberate. On unified-dev, `trace` logging wrote every log line into
`system.text_log` with no TTL. By 2026-11-21 that table had reached 24.4 GiB,
and the 50Gi data volume was full.

### Taking over unified-dev's existing cluster

The operator identifies the cluster by the resource names, and the existing
volumes belong to them. To manage the existing cluster with this chart:

1. Keep its names: `name: clickhouse-clickstack-clickhouse` and
   `keeperName: clickhouse-clickstack-keeper`.
2. Create `clickhouse-users` with the current passwords.
3. Set `dataVolumeClaimSpec` storage to the live size (50Gi).

Changing the Keeper image from `latest` (26.10) to 25.7 would be a Keeper
downgrade. To avoid one, pin `keeper.spec.containerTemplate.image.tag` to the
version that is running.
