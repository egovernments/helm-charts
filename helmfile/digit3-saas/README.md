# DIGIT SaaS master helmfile

[`helmfile.yaml.gotmpl`](helmfile.yaml.gotmpl) deploys the products an environment asks for, each
at a released version, from the product helmfiles in **egovernments/helm-charts**
([`../products`](../products)).

An environment file names a **product and a version**, never a path:

```yaml
helm-charts-ref: 9f1c2e7d...        # helm-charts tag or full commit SHA (the product helmfiles' version)
global:
  domain: saas.example.digit.org
products:
  backbone: {version: 3.0.0, values: {...}}
  core:     {version: 3.0.0, values: {kong: {replicaCount: 2}}}
  studio:   {version: 3.0.0, namespace: studio}
  health:   {version: 3.0.0, enabled: false}     # uninstalled
```

The master helmfile turns `studio: {version: 3.0.0}` into
`git::https://github.com/egovernments/helm-charts.git@helmfile/products/studio/v3/helmfile.yaml.gotmpl?ref=<helm-charts-ref>`.
The repo is fixed in the file, so product helmfiles can only come from helm-charts.

Full example: [`example-env.yaml`](example-env.yaml).

## Where the master helmfile lives

| | |
|---|---|
| **Here** (helm-charts) | Run it from this folder. `helm-charts-ref: local` uses this checkout's product helmfiles (for testing a helm-charts PR). |
| **Any other repo** (e.g. DIGIT-DevOps, next to the environment files) | Copy `helmfile.yaml.gotmpl` unchanged. Set `helm-charts-ref` to a tag or full commit SHA. |

Either way, upgrading a product is a one-line change of its `version` in the environment file.
Upgrading the product helmfiles themselves (a new release added in helm-charts) also needs
`helm-charts-ref` moved to a commit that has it.

## Run

```bash
export DIGIT_ENV_FILE=/path/to/<env>.yaml
export DIGIT_SECRETS_FILE=/path/to/<env>-secrets.yaml    # optional; SOPS, needs helm-secrets
helmfile list                       # products, their releases and chart versions
helmfile -l product=studio diff     # one product
helmfile apply
```

Order: **backbone**, **core**, then the other products by name. Values per release:

- `values` of the product (`products.<product>.values.<chart>`) are merged over the chart's defaults;
- `global` goes to every product;
- secrets: the same layout in the secrets file (`products.<product>.values.<chart>...`).

## Not covered yet

- Argo CD: the one-click Argo CD plugin (`../digit3-one-click/scripts/argocd-plugin.sh`) renders
  the one-click layers; rendering this master helmfile with it is the next step. The releases
  already carry the `wave` labels it uses.
- Monitoring (kube-prometheus-stack, Loki, Grafana, ...): upstream charts, not a product here yet.
- backbone uses the one-click Vault bootstrap script (`../digit3-one-click/scripts/vault-bootstrap.sh`).
