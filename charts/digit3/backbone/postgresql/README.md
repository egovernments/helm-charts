# postgresql

PostgreSQL 18 on CloudNativePG for the DIGIT backbone. Vendored from the official cloudnative-pg/cluster chart, with a reconciler that provisions the application database + role (from egov-config and the db secret) after the cluster is ready.

| | |
|---|---|
| **Project / Module** | `digit3` / `backbone` |
| **Chart type** | application |
| **Depends on** | none declared; the chart is vendored (templates copied in), not wired as a subchart dependency |
| **Upstream chart** | `cloudnative-pg/cluster` `0.9.0` (app version `18`) |
| **Operand image** | `ghcr.io/cloudnative-pg/postgresql:18-standard-trixie` |

This chart is a vendored copy of the upstream CloudNativePG cluster chart, so every upstream value passes through under its documented key. The parameters below are the ones you are most likely to set; the upstream chart documents the full set. Chart `version` follows this repo's digit3 3.x convention (seed `3.0.0`), not the upstream cluster chart version; the `digit.org/upstream-*` annotations in `Chart.yaml` record the provenance for re-sync.

> This chart renders a CNPG `Cluster` custom resource. It requires the CloudNativePG operator to already be installed in the cluster.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install postgresql digit3-backbone/postgresql -n backbone -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **upstream chart defaults** - the vendored cloudnative-pg/cluster defaults (lowest).
2. **this chart's `values.yaml`** - the backbone defaults listed below (for example `fullnameOverride: postgresql`, single-instance cluster, the `reconciler` block).
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
postgresql:
  cluster:
    instances: 3            # three schedulable nodes -> automated failover
    storage:
      size: 50Gi
      storageClass: "gp3"
    resources:
      requests: { cpu: "2000m", memory: "8Gi" }
      limits:   { cpu: "2000m", memory: "8Gi" }
  backups:
    enabled: true
```

The application database and role are **not** set here. They come from the `egov-config` ConfigMap (`db-name`) and the `db` secret (`username` / `password`) in the application namespace, and are applied by the reconciler (see below). Keep `fullnameOverride: postgresql` so the generated Services and the superuser secret keep stable names that the reconciler and the service alias depend on.

## Parameters

### Cluster identity and mode

| Key | Description | Default |
|---|---|---|
| `fullnameOverride` | Fixed release name; keeps the `Cluster` and its `-rw`/`-ro`/`-r` Services stable (consumed by the service alias + reconciler) | `postgresql` |
| `nameOverride` | Override the chart name | `""` |
| `namespaceOverride` | Override the target namespace | `""` |
| `type` | CNPG database type (`postgresql`, `postgis`, `timescaledb`) | `postgresql` |
| `version.postgresql` | PostgreSQL major version | `"18"` |
| `mode` | Cluster mode of operation (`standalone`, `replica`, `recovery`) | `standalone` |

### Image

| Key | Description | Default |
|---|---|---|
| `cluster.imageName` | Operand image (pinned to the "standard" variant; adds pgvector, PGAudit, failover slots) | `ghcr.io/cloudnative-pg/postgresql:18-standard-trixie` |
| `cluster.imageCatalogRef` | Reference to an `ImageCatalog`/`ClusterImageCatalog`; takes precedence over `imageName` | `{}` |
| `cluster.imagePullPolicy` | Image pull policy | `IfNotPresent` |
| `cluster.imagePullSecrets` | Pull secrets for the images | `[]` |

### Instances and storage

| Key | Description | Default |
|---|---|---|
| `cluster.instances` | Number of PostgreSQL instances (set to 3 on three schedulable nodes for automated failover) | `1` |
| `cluster.storage.size` | PGDATA volume size | `8Gi` |
| `cluster.storage.storageClass` | PGDATA storage class (empty uses the cluster default) | `""` |
| `cluster.walStorage.enabled` | Separate volume for WAL | `false` |
| `cluster.walStorage.size` | WAL volume size | `1Gi` |
| `cluster.walStorage.storageClass` | WAL storage class | `""` |

### Resources and scheduling

| Key | Description | Default |
|---|---|---|
| `cluster.resources` | Pod resource requests/limits (set equal values for Guaranteed QoS) | `{}` |
| `cluster.priorityClassName` | Priority class for the pods | `""` |
| `cluster.affinity.topologyKey` | Anti-affinity topology key | `topology.kubernetes.io/zone` |
| `cluster.primaryUpdateMethod` | Primary upgrade method (`switchover` or `restart`) | `switchover` |
| `cluster.primaryUpdateStrategy` | Primary upgrade strategy (`unsupervised` or `supervised`) | `unsupervised` |
| `cluster.enablePDB` | Create a PodDisruptionBudget | `true` |
| `cluster.logLevel` | Instance log level | `info` |

### PostgreSQL tuning

| Key | Description | Default |
|---|---|---|
| `cluster.postgresql.parameters` | `postgresql.conf` overrides (for example `max_connections`) | `{}` |
| `cluster.postgresql.pg_hba` | Extra `pg_hba.conf` rules | `[]` |
| `cluster.postgresql.shared_preload_libraries` | Extra shared preload libraries | `[]` |
| `cluster.replicationSlots` | HA / failover replication-slot management | `{}` |
| `cluster.initdb` | initdb bootstrap configuration | `{}` |

### Superuser access

| Key | Description | Default |
|---|---|---|
| `cluster.enableSuperuserAccess` | Let the operator manage the `postgres` superuser password via a secret | `true` |
| `cluster.superuserSecret` | Name of an existing superuser secret to use instead | `""` |

### Monitoring

| Key | Description | Default |
|---|---|---|
| `cluster.monitoring.enabled` | Enable monitoring | `false` |
| `cluster.monitoring.podMonitor.enabled` | Create a `PodMonitor` | `true` |
| `cluster.monitoring.prometheusRule.enabled` | Create a `PrometheusRule` with the default alerts | `true` |
| `cluster.monitoring.instrumentation.pgStatStatements` | Enable `pg_stat_statements` metrics | `false` |

### Backups (barman-cloud)

| Key | Description | Default |
|---|---|---|
| `backups.enabled` | Enable scheduled backups (manual configuration required) | `false` |
| `backups.method` | Backup method (`barmanObjectStore` or `plugin`) | `barmanObjectStore` |
| `backups.provider` | Object-store provider (`s3`, `azure`, `google`) | `s3` |
| `backups.retentionPolicy` | Retention policy | `30d` |
| `backups.scheduledBackups` | List of scheduled backup definitions | `daily-backup @ 0 0 0 * * *` |
| `backups.wal.compression` / `backups.wal.encryption` | WAL compression / encryption | `gzip` / `AES256` |
| `backups.data.compression` / `backups.data.encryption` | Data compression / encryption | `gzip` / `AES256` |

Backup credentials are read from the object-store sections (`backups.s3` / `backups.azure` / `backups.google`); on cloud platforms prefer IAM/workload-identity (`backups.s3.inheritFromIAMRole`) over static keys. See the recovery, replica and pooler sections of `values.yaml` for the remaining upstream options.

### Databases and poolers

| Key | Description | Default |
|---|---|---|
| `databases` | Declarative `Database` resources to create | `[]` |
| `poolers` | List of PgBouncer poolers | `[]` |
| `imageCatalog.create` | Provision an image catalog (ignored when `images` is empty) | `true` |

### Application-DB reconciler (DIGIT addition)

After the cluster is Ready, a `Job` (and an optional periodic `CronJob`) runs in this chart's namespace, connects as the CNPG superuser, and idempotently ensures the application role + database exist. The database name comes from the `egov-config` ConfigMap and the role credentials from the `db` secret, both in the application namespace.

| Key | Description | Default |
|---|---|---|
| `reconciler.enabled` | Enable the reconciler Job | `true` |
| `reconciler.egovConfig.name` | ConfigMap providing the database name | `egov-config` |
| `reconciler.egovConfig.dbNameKey` | Key holding the database name | `db-name` |
| `reconciler.dbSecret.name` | Secret providing the application role credentials | `db` |
| `reconciler.dbSecret.usernameKey` | Key holding the role username | `username` |
| `reconciler.dbSecret.passwordKey` | Key holding the role password | `password` |
| `reconciler.cron.enabled` | Run a periodic reconcile so edits apply without a redeploy | `true` |
| `reconciler.cron.schedule` | Reconcile schedule | `*/10 * * * *` |
| `reconciler.images.kubectl` | Image that reads egov-config + db via the Kubernetes API | `bitnami/kubectl:1.31` |
| `reconciler.images.psql` | Image that runs the reconcile SQL as superuser | `ghcr.io/cloudnative-pg/postgresql:18-standard-trixie` |
| `custom_config.enabled` | Override the application namespace (otherwise `egov`) | `false` |
| `custom_config.namespace` | Application namespace holding egov-config + db | `""` (-> `egov`) |

The reconciler credentials are fetched into an emptyDir by an init container and passed to `psql` via env and `\getenv`, so they never appear in process args. Because egov-config and the db secret live in the application namespace (not this chart's namespace), the chart also renders a `Role` + `RoleBinding` there granting the reconciler read access.

### Service alias (DIGIT addition)

The chart renders a plain `ExternalName` Service named after the release (`postgresql`) that CNAMEs to the CNPG read-write Service (`postgresql-rw`). This lets consumers use `postgresql.<namespace>` (the `egov-config` db-host default) without hard-coding the `-rw` suffix.

## Sourced from the platform (not set in this chart)

| Secret / resource | Keys | Source |
|---|---|---|
| `postgresql-superuser` | `username` / `password` | Secret auto-created by the CloudNativePG operator for the superuser; consumed by the reconciler as `PGUSER` / `PGPASSWORD` |
| `db` (application namespace) | `username` / `password` | Application role credentials read by the reconciler to create the role |
| `egov-config` (application namespace) | `db-name` | Application database name read by the reconciler |

No real secret values are stored in this chart; it only references the key names above. Backup/recovery object-store credentials, when enabled, are supplied through the `backups.*` / `recovery.*` sections or an existing secret.

## Source

- Chart: [`charts/digit3/backbone/postgresql`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/backbone/postgresql)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/backbone/postgresql/values.yaml)
- Upstream chart: [`cloudnative-pg/charts` (cluster)](https://github.com/cloudnative-pg/charts)
