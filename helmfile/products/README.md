# Product helmfiles

One helmfile per **product** and **major version**, each with its **releases**. An environment
never edits them: it asks a master helmfile ([`../digit3-saas`](../digit3-saas)) for a product
at a version, and gives its own values.

```
helmfile/products/
  <product>/                     backbone, core, cms, studio, crest, health, analytics
    v3/
      helmfile.yaml.gotmpl       how the product is deployed in major 3 (releases, order, namespaces)
      releases/
        3.0.0.yaml               a release = the exact chart version of every chart in it
        3.0.1.yaml
        3.1.0.yaml
    v2/ ...
    v1/ ...
  check.sh                       the PR check (below)
```

| Product | Charts (helm.digit.org) | Namespace |
|---|---|---|
| backbone | `digit3/cluster-configs` + `digit3/backbone` (cert-manager, ingress-nginx, redis, kafka-kraft, minio, vault, elasticsearch, kibana, pgadmin) | egov, backbone, vault |
| core | `digit3/core` (Keycloak, Kong, DIGIT core services) | egov |
| cms, studio, crest, health | `digit3/<product>` | egov, or `namespace` from the environment |
| analytics | `digit3/analytics` (ClickHouse operator, ClickHouse, Superset) | analytics, clickhouse-operator |

All products start at **3.0.0**. backbone and core 3.0.0 pin the chart versions of the DIGIT
one-click install (tested together); the others pin the latest published charts.

## Versions kept

The **last 3 majors**, with all their minor and patch releases. Adding `v4/` means removing the
oldest `v<major>/` in the same PR; the check fails with more than 3.

- **patch** (3.0.1): chart patch releases, fixes.
- **minor** (3.1.0): new chart versions, a new chart in the product, new values with defaults.
- **major** (4.0.0): anything an environment has to act on (renamed or removed charts, changed
  values, data migrations, a different order). Copy `v3/` to `v4/` and change it there; `v3`
  keeps working for the environments still on it.

## Add a release

1. Publish the charts first (a chart PR with its `release:*` label).
2. Copy the latest release file to the next version and change the chart versions, e.g.
   `studio/v3/releases/3.0.1.yaml`:
   ```yaml
   charts:
     digit-studio: 3.0.3
     public-service: 3.0.3
     ...
   ```
3. Open a PR. A published release file is **never changed or deleted**; to fix one, add the
   next patch release.

A new chart in a minor release: add it to the release file. cms, studio, crest, health and core
deploy any chart of their module; backbone and analytics only the charts their helmfile knows
(add it there first).

## The contract of a product helmfile

`.Values`, from the master helmfile:

| Key | Content |
|---|---|
| `product.version` | the release to deploy (`3.x.y`): the helmfile reads `releases/<version>.yaml` |
| `product.namespace` | where the product goes (optional; default per product, above) |
| `product.enabled` | `false` = every release gets `installed: false` (uninstalled) |
| `product.namespaces` | backbone only: the other products' namespaces. cluster-configs creates them with the `egov-config` ConfigMap and the `db` Secret |
| `global` | environment-wide values (`domain`, ...); DIGIT common-based charts get it with their namespace |
| `<chart>` | values for that chart, merged over its defaults; `<chart>.enabled: false` leaves it out |

Every release has the labels `product: <product>` and `wave` (Argo CD sync wave inside the
product). The PR check renders every release.

## Check

```bash
helmfile/products/check.sh origin/master   # from the repo root; needs helm, helmfile, curl
```

1. layout: `<product>/v<major>/helmfile.yaml.gotmpl` and `releases/<major>.<minor>.<patch>.yaml`
2. at most 3 majors per product
3. every chart version is published on helm.digit.org
4. every release renders
5. against the base commit: no published release file is changed or deleted (except a whole
   removed major)

Test a product alone:

```bash
cd helmfile/products/studio/v3
helmfile --state-values-set product.version=3.0.0,global.domain=x.digit.org template
```
