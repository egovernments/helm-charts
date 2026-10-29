# vault

Official HashiCorp Vault Chart. Vendored from HashiCorp vault-helm for the DIGIT backbone; runs an in-cluster Vault server for secrets management, with DIGIT services calling the HTTP API (approle) directly.

| | |
|---|---|
| **Project / Module** | `digit3` / `backbone` |
| **Chart type** | application |
| **Depends on** | none declared; the chart is vendored (templates copied in), not wired as a subchart dependency |
| **Upstream chart** | `hashicorp/vault-helm` `0.29.1` (app version `1.18.1`) |
| **Server image** | `hashicorp/vault:1.18.1` |

This chart is a vendored copy of the upstream HashiCorp vault-helm chart (via the DIGIT-DevOps modulith `digit3/vault`), so every upstream value passes through under its documented key. The parameters below are the ones you are most likely to set for the digit3 install; the upstream chart documents the full set. Chart `version` follows this repo's digit3 3.x convention (seed `3.0.0`), not the upstream vault-helm version; the `digit.org/upstream-*` annotations in `Chart.yaml` record the provenance for re-sync.

## Install

```bash
helm repo add digit3-backbone https://helm.digit.org/digit3/backbone
helm repo update
helm install vault digit3-backbone/vault -n backbone -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve from lowest to highest priority:

1. **upstream chart defaults** - the vendored vault-helm defaults (lowest).
2. **this chart's `values.yaml`** - the backbone defaults listed below (for example the injector and CSI disabled, HA Raft with a single replica, ingress off, `global.tlsDisable: true`).
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (highest).

```yaml
# environments/<env>.yaml  -- overrides win over the chart defaults
vault:
  server:
    dataStorage:
      size: 20Gi
      storageClass: "gp3"
    seal:
      type: "awskms"        # enable auto-unseal per environment
      awskms:
        region: "ap-south-1"
        kmsKeyId: "arn:aws:kms:..."
    ingress:
      enabled: true
      hosts:
        - host: vault.example.org
          paths: [/ui]
```

By default Vault is **in-cluster only**: no public `/ui` or `/v1` (ingress disabled), and services reach it through the `egov-service-host` / `egov-config` keys rather than an external route.

## Parameters

### Global

| Key | Description | Default |
|---|---|---|
| `global.enabled` | Master switch enabling all components | `true` |
| `global.namespace` | Namespace to deploy to (defaults to the install namespace) | `""` |
| `global.fullnameOverride` | Fixed release name | `vault` |
| `global.tlsDisable` | Disable end-to-end TLS transport | `true` |
| `global.externalVaultAddr` | External Vault address (set this to disable the in-cluster server) | `""` |
| `global.imagePullSecrets` | Registry pull secrets | `[]` |

### Server image

| Key | Description | Default |
|---|---|---|
| `server.image.repository` | Vault server image repository | `hashicorp/vault` |
| `server.image.tag` | Vault server image tag | `1.18.1` |
| `server.image.pullPolicy` | Image pull policy | `IfNotPresent` |

### Server deployment

| Key | Description | Default |
|---|---|---|
| `server.enabled` | Install the Vault server (`-` means follow `global.enabled`) | `"-"` |
| `server.extraArgs` | Extra Vault server arguments | `["-ui"]` |
| `server.updateStrategyType` | StatefulSet update strategy | `OnDelete` |
| `server.resources` | Server resource requests/limits | `{}` |
| `server.logLevel` | Server log level | `""` |
| `server.logFormat` | Server log format | `""` |
| `server.authDelegator.enabled` | ClusterRoleBinding for Kubernetes auth (TokenReview) | `true` |
| `server.networkPolicy.enabled` | Create a NetworkPolicy for the server pods | `false` |

### High availability (Raft)

| Key | Description | Default |
|---|---|---|
| `server.ha.enabled` | Run the server in HA mode | `true` |
| `server.ha.replicas` | Number of server replicas | `1` |
| `server.ha.raft.enabled` | Use integrated Raft storage | `true` |
| `server.ha.raft.config` | Raft server HCL config (listener, storage, service_registration, templated seal stanza) | see `values.yaml` |
| `server.ha.disruptionBudget.enabled` | Create a PodDisruptionBudget | `true` |
| `server.standalone.enabled` | Run in standalone (file backend) mode instead (`-` follows HA default) | `"-"` |
| `server.dev.enabled` | Run in dev mode (ephemeral, do not use outside experiments) | `false` |

### Storage

| Key | Description | Default |
|---|---|---|
| `server.dataStorage.enabled` | Create a PVC for Vault data | `true` |
| `server.dataStorage.size` | Data PVC size | `10Gi` |
| `server.dataStorage.mountPath` | Data mount path | `/vault/data` |
| `server.dataStorage.storageClass` | Data storage class (null uses the cluster default) | `null` |
| `server.dataStorage.accessMode` | Data PVC access mode | `ReadWriteOnce` |
| `server.auditStorage.enabled` | Create a PVC for audit logs | `false` |
| `server.auditStorage.size` | Audit PVC size | `10Gi` |
| `server.auditStorage.mountPath` | Audit mount path | `/vault/audit` |

### Auto-unseal (DIGIT note)

The default (`server.seal.type: ""`) is Shamir: the pod must be unsealed manually after init and after every restart. Set `type` plus the matching provider block per environment, and bind the server service account to a cloud identity with KMS decrypt/unwrap rights (no static credentials). The stanza is templated into `server.ha.raft.config` only when `type` is non-empty.

| Key | Description | Default |
|---|---|---|
| `server.seal.type` | Seal type (`""` Shamir, `awskms`, `azurekeyvault`, `gcpckms`, `transit`) | `""` |
| `server.seal.awskms` | AWS KMS region / `kmsKeyId` (bind SA via IRSA) | `{region, kmsKeyId}` empty |
| `server.seal.azurekeyvault` | Azure Key Vault `tenantId` / `vaultName` / `keyName` | empty |
| `server.seal.gcpckms` | GCP KMS `project` / `region` / `keyRing` / `cryptoKey` | empty |
| `server.seal.transit` | Transit auto-unseal `address` / `keyName` / `mountPath` | `mountPath: transit/` |

### Service

| Key | Description | Default |
|---|---|---|
| `server.service.enabled` | Create the Vault service | `true` |
| `server.service.active.enabled` | Create the `vault-active` service (leader pods) | `true` |
| `server.service.standby.enabled` | Create the `vault-standby` service (follower pods) | `true` |
| `server.service.port` | Service port | `8200` |
| `server.service.targetPort` | Target port | `8200` |
| `server.service.publishNotReadyAddresses` | Include not-ready pods in the service | `true` |

### Ingress

| Key | Description | Default |
|---|---|---|
| `server.ingress.enabled` | Expose Vault via Ingress (in-cluster only by default) | `false` |
| `server.ingress.ingressClassName` | Ingress class (or use the annotation) | `""` |
| `server.ingress.hosts` | Host + paths (placeholder `vault.local`; set per environment) | `[{host: vault.local, paths: [/ui]}]` |
| `server.ingress.tls` | TLS configuration | placeholder `vault-local-tls-certs` |
| `server.route.enabled` | OpenShift route instead of Ingress | `false` |

### Service account

| Key | Description | Default |
|---|---|---|
| `server.serviceAccount.create` | Create the server service account | `true` |
| `server.serviceAccount.name` | Service account name (generated if empty) | `""` |
| `server.serviceAccount.createSecret` | Create a non-expiring token secret for the SA | `false` |
| `server.serviceAccount.serviceDiscovery.enabled` | RoleBinding for Kubernetes `service_registration` | `true` |

### UI

| Key | Description | Default |
|---|---|---|
| `ui.enabled` | Create a Service for the Vault UI | `true` |
| `ui.serviceType` | UI service type (`ClusterIP`, `NodePort`, `LoadBalancer`) | `ClusterIP` |
| `ui.externalPort` | UI external port | `8200` |
| `ui.targetPort` | UI target port | `8200` |

### Agent injector (disabled)

The agent injector is disabled for the digit3 single-node install: DIGIT services call Vault's HTTP API directly (approle), nothing uses sidecar injection.

| Key | Description | Default |
|---|---|---|
| `injector.enabled` | Enable the Vault Agent sidecar injector | `false` |
| `injector.replicas` | Injector replicas | `1` |
| `injector.image.repository` / `injector.image.tag` | vault-k8s injector image | `hashicorp/vault-k8s` / `1.6.2` |
| `injector.agentImage.repository` / `injector.agentImage.tag` | Vault Agent image | `hashicorp/vault` / `1.18.1` |
| `injector.authPath` | Mount path of the Kubernetes auth method | `auth/kubernetes` |

### CSI provider (disabled)

| Key | Description | Default |
|---|---|---|
| `csi.enabled` | Install the secrets-store-csi-driver-provider-vault daemonset | `false` |
| `csi.image.repository` / `csi.image.tag` | vault-csi-provider image | `hashicorp/vault-csi-provider` / `1.5.0` |
| `csi.agent.enabled` | Run a Vault Agent alongside the CSI provider | `true` |
| `csi.agent.image.tag` | Agent image tag | `1.18.1` |

### Telemetry

| Key | Description | Default |
|---|---|---|
| `serverTelemetry.serviceMonitor.enabled` | Create a Prometheus Operator `ServiceMonitor` (operator must be installed first) | `false` |
| `serverTelemetry.serviceMonitor.interval` | Scrape interval | `30s` |
| `serverTelemetry.prometheusRules.enabled` | Create a `PrometheusRule` for AlertManager | `false` |
| `global.serverTelemetry.prometheusOperator` | Enable Prometheus Operator integration | `false` |

## Secrets referenced (not set in this chart)

Vault stores no plaintext secret values in this chart; the following are references only, created or supplied out of band.

| Secret | Keys | Source |
|---|---|---|
| `server.enterpriseLicense.secretName` | `license` (`enterpriseLicense.secretKey`) | Existing Kubernetes secret holding a Vault Enterprise license; blank for OSS | 
| `server.extraSecretEnvironmentVars` | per-entry `secretName` / `secretKey` | Existing secrets injected as env vars (for example auto-unseal credentials) |
| `injector.certs.secretName` | `tls.crt` / `tls.key` | TLS secret for the injector webhook (null uses auto-managed certs; injector disabled by default) |
| `server.serviceAccount.createSecret` | service-account token | Optional non-expiring SA token secret (default `false`) |

The Vault server's own root token and unseal keys are produced at `vault operator init` time and are **not** created or stored by this chart; keep them out of the cluster per your key-management policy.

## Source

- Chart: [`charts/digit3/backbone/vault`](https://github.com/egovernments/helm-charts/tree/master/charts/digit3/backbone/vault)
- Full defaults: [`values.yaml`](https://github.com/egovernments/helm-charts/blob/master/charts/digit3/backbone/vault/values.yaml)
- Upstream chart: [`hashicorp/vault-helm`](https://github.com/hashicorp/vault-helm)
