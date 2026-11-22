# Products on DIGIT one-click

The one-click install gives a team the **platform**: backbone, DIGIT core 3.0 and monitoring. A **product** is anything a team adds on top: its own helmfile that deploys Helm charts, either **packaged** (from `helm.digit.org` or any Helm repository) or **unpackaged charts kept in the team's own repo**, next to the helmfile.

Products are deployed **separately** from the platform:
- **Helmfile mode:** the workflow's **Products** job runs `helmfile -f products.yaml.gotmpl apply`.
- **Argo CD mode:** the root Application **`digit-products`** creates one Application per product release.

A broken product never blocks or rolls back the platform.

## Add a product (the team's DIGIT-DevOps fork)

In `one-click/<environment>.yaml`:

```yaml
products:
  health:                                   # product name
    # where its helmfile is (pick one):
    helmfile: products/health/helmfile.yaml.gotmpl            # in this folder of helm-charts (default path)
    # helmfile: git::https://github.com/<org>/<repo>.git@deploy/helmfile.yaml.gotmpl?ref=v1.4.0   # the team's own repo
    namespace: health                       # optional, default egov
    shared-secrets: [redis]                 # optional, more platform Secrets copied to that namespace
    enabled: true                           # optional; false = uninstall the product
    values:                                 # what the product helmfile gets as .Values
      health-service-request: {chart-version: 3.0.1, replicas: 2}
```

Commit it. Only the **Products** job runs; the platform isn't touched. You can also run it by hand: **Actions → Run workflow → products**.

Product passwords and keys: add a Secret under `cluster-configs.secrets` in the GitHub secret `DIGIT_SECRETS`, with `namespace: [<product namespace>]`. cluster-configs creates it (the next **deploy** applies it).

## Where the helmfile lives

| | Use when |
|---|---|
| `products/<product>/helmfile.yaml.gotmpl` in **egovernments/helm-charts** | The platform team reviews it. It is pinned by `helmfiles_ref` (input.yaml), like the platform layers. |
| The team's **own repo**, `git::https://…@<path>?ref=<tag or commit>` | The team releases on its own schedule. **Always pin `ref`** to a tag or commit, never a branch, or deploys aren't reproducible. The helmfile may also use **unpackaged charts from the same repo** (see below). |

Private repositories are not supported: the workflow and Argo CD fetch the helmfile without credentials.

## Charts that are not packaged

A team that hasn't published its charts can keep them in its own repo and point the helmfile at them with a relative path:

```
team-repo/
  deploy/helmfile.yaml.gotmpl       # chart: ../charts/my-service
  charts/my-service/                # Chart.yaml, values.yaml, templates/
```

This works because helmfile downloads the **whole repo** at the pinned `ref`, so relative paths resolve inside it. It also downloads the chart's dependencies (for example the DIGIT `common` chart from `helm.digit.org`) on every deploy. The `ref` is the version: a tag or commit pins the helmfile and the charts together.

This applies only to the team's own repo. A product helmfile kept **here** in helm-charts can't use `../../charts/...`: the workflow and Argo CD download only `helmfile/digit3-one-click`. Put the chart under `charts/digit3/<module>/` instead; it is packaged and published to `helm.digit.org` when merged.

Prefer packaged charts when the product is used by several teams or environments: chart versions, chart CI (lint, image checks) and a release history come with them.

## The contract (what a product helmfile can rely on, and must do)

`.Values` given to the helmfile:

| Key | Content |
|---|---|
| `global` | the environment file's `global` (`domain`, `mode`, ...), plus `namespace` = the product namespace, `product` = the product name, and `enabled` (`false` when the product is switched off) |
| everything else | the product's `values` block, unchanged (typically one block per release with `chart-version`) |

Already there in the product namespace:
- ConfigMap `egov-config`: `db-host`, `db-name`, `db-url`, Kafka, Redis, ... (the platform's).
- Secret `db`: database user and password.
- Any `shared-secrets`, and the Secrets you added through `DIGIT_SECRETS`.

Platform services, reachable by in-cluster DNS (the same values are in `egov-config`):

| Service | Address |
|---|---|
| Kong (API gateway) | `http://kong-proxy.egov:8000` |
| Keycloak | `http://keycloak.egov.svc.cluster.local:8080/keycloak` |
| Kafka | `kafka-kraft-controller-headless.backbone:9092` |
| Redis | `redis.backbone:6379` |
| MinIO (S3) | `minio-svc.backbone:9000` |
| Vault | `vault.vault:8200` |

A product helmfile **must**:
1. Use packaged charts from a Helm repository, each with a pinned `version`, or unpackaged charts from the same repo as the helmfile, pinned by `ref`.
2. Put every release in `.Values.global.namespace`.
3. Not create Namespaces, or the platform's ConfigMaps or Secrets. cluster-configs owns them.
4. Not depend on its own environment files. Everything it needs comes in through `.Values`.
5. Give each release the label `product: <name>`. In Argo CD mode, the label `wave` orders releases inside the product (the default is 10). Argo CD waits for every release in a wave to be healthy before it starts the next wave.
6. Use `helmDefaults` with `wait: true`, so helmfile mode reports failures.
7. Set every release's `installed:` to false when `.Values.global.enabled` is false. That is how a product is removed: helmfile uninstalls the releases and Argo CD prunes them. (Deleting the product's block instead leaves its releases running in helmfile mode.)

A product helmfile **should**:
- Use releases built on the DIGIT common chart (they read `global` and `namespace`), where possible.
- Expose UIs through an Ingress on `.Values.global.domain` with a path prefix (`/<product>/...`). The cluster has one host and one certificate.

Start from [`_example/helmfile.yaml.gotmpl`](_example/helmfile.yaml.gotmpl).

## Test a product helmfile locally

```bash
cd helmfile/digit3-one-click
export DIGIT_ENV_FILE=/path/to/<env>.yaml DIGIT_SECRETS_FILE=/path/to/<env>-secrets.yaml   # filled env file
helmfile -f products.yaml.gotmpl list        # the product releases
helmfile -f products.yaml.gotmpl template    # what would be applied
```
