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

## Vault (production setup)

The backbone product installs Vault but never initialises it or keeps its keys in the cluster.
Once per environment:

1. **Auto-unseal** with the cloud's KMS, in the environment file (chart README "Auto-unseal"):
   ```yaml
   products:
     backbone:
       values:
         vault:
           server:
             seal:
               type: azurekeyvault            # or awskms / gcpckms / transit
               azurekeyvault: {tenantId: ..., vaultName: ..., keyName: ...}
   ```
   Bind the Vault service account to a cloud identity with unwrap/decrypt rights on that key.
2. **Initialise** after the first deploy. The recovery keys and root token go to the operators
   (a password manager), never to the cluster or Git:
   ```bash
   kubectl -n vault exec -it vault-0 -- vault operator init -recovery-shares=5 -recovery-threshold=3
   ```
3. **AppRole for the DIGIT services** (otp and individual use Vault transit), logged in with the
   root token:
   ```bash
   vault secrets enable transit
   vault auth enable approle
   vault policy write digit-transit - <<'EOF'
   path "transit/encrypt/*"     { capabilities = ["create","update"] }
   path "transit/decrypt/*"     { capabilities = ["update"] }
   path "transit/sign/*"        { capabilities = ["create","update"] }
   path "transit/verify/*"      { capabilities = ["create","update"] }
   path "transit/keys/*"        { capabilities = ["create","read","update","list","delete"] }
   path "auth/token/renew-self" { capabilities = ["update"] }
   EOF
   vault write auth/approle/role/digit-services token_policies=digit-transit token_ttl=720h token_max_ttl=2160h
   vault read -field=role_id auth/approle/role/digit-services/role-id
   vault write -f -field=secret_id auth/approle/role/digit-services/secret-id
   ```
   Put both into the **secrets file**; cluster-configs creates the `egov/vault-approle` Secret:
   ```yaml
   products:
     backbone:
       values:
         cluster-configs:
           secrets:
             vault-approle: {role-id: ..., secret-id: ...}
   ```
   Then revoke the root token (`vault token revoke <root-token>`).

## Not covered yet

- Argo CD: an Application per product rendering this master helmfile. The releases already carry
  the `product` and `wave` labels for it.
- Monitoring (Prometheus, Loki, Grafana, ...): upstream charts, not a product here yet.
