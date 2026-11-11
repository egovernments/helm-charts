# matomo

Matomo web analytics for the DIGIT backbone. Vendored from the upstream Bitnami `matomo` chart, with every container image mirrored to the `egovio` registry (Bitnami deprecated its public Docker Hub images, moving them to `bitnamilegacy`).

| | |
|---|---|
| **Project / Module** | `digit3` / `backbone` |
| **Chart type** | application (vendored upstream) |
| **Upstream chart** | `bitnami/matomo` 11.0.0 (appVersion 5.3.2) |
| **Database** | MariaDB (Bitnami subchart, `mariadb.enabled: true`) |
| **Images (all on egovio)** | `matomo`, `os-shell`, `apache-exporter`, `mariadb` |

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install matomo digit3-backbone/matomo -n backbone -f values.yaml
```

## Images

Every image is pinned to the egovio mirror so the chart does not depend on the
deprecated Bitnami public images:

| Component | Image |
|---|---|
| Matomo (app) | `egovio/matomo:5.3.2-debian-12-r12` |
| Volume-permissions / archiver sidecar | `egovio/os-shell:12-debian-12-r50` |
| Metrics (Apache exporter, off by default) | `egovio/apache-exporter:1.0.10-debian-12-r55` |
| MariaDB (subchart) | `egovio/mariadb:12.0.2-debian-12-r0` |

Re-mirroring (when a tag changes) is a registry-to-registry copy, e.g.:

```bash
docker buildx imagetools create -t egovio/mariadb:<tag> bitnamilegacy/mariadb:<tag>
```

## Database

`mariadb.enabled: true` deploys a standalone MariaDB from the Bitnami subchart
(`mariadb.auth.database: bitnami_matomo`, user `bn_matomo`; set the passwords via
`mariadb.auth.*` or an existing secret). To use an external database instead, set
`mariadb.enabled: false` and configure the `externalDatabase.*` values.

## Configuration

This is a vendored upstream chart, so configuration follows the upstream Bitnami
`matomo` values (see the chart's `values.yaml`). The DIGIT changes on top are:

- all image `registry`/`repository` fields repointed to `egovio` (plus the archiver
  sidecar's inline image);
- `mariadb.image` pinned to `egovio/mariadb`;
- chart `version` set to this repo's digit3 `3.x` convention (see the
  `digit.org/upstream-*` annotations in `Chart.yaml` for the upstream mapping).

Common things to set per environment:

| Area | Values |
|---|---|
| Ingress | `ingress.enabled`, `ingress.hostname`, `ingress.ingressClassName`, TLS |
| Persistence | `persistence.enabled`, `persistence.size`, `persistence.storageClass` |
| Admin user | `matomoUsername`, `matomoPassword`, `matomoEmail`, `matomoWebsiteHost`, `matomoWebsiteName` |
| Archiving | the `matomo-archiver` / `matomo-scheduled-tasks` sidecars (run `core:archive` and `core:run-scheduled-tasks` every 5 min) |
| Metrics | `metrics.enabled` (Apache exporter sidecar) |

## Notes

- Packaging only: no ApplicationSet deploys this by default; it is published to the
  registry for environments that opt in.
- `helm dependency build` pulls the `mariadb` (22.0.0) and `common` (2.41.0)
  subcharts from `oci://registry-1.docker.io/bitnamicharts` at package time.
