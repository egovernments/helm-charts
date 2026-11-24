# kubescape

Cluster security posture and image vulnerability scanning for DIGIT
environments. A thin wrapper around the upstream
[kubescape-operator](https://github.com/kubescape/helm-charts) chart that pins a
version and carries the DIGIT defaults, so every environment runs the same
profile instead of a hand-written `helm install`.

```
helm repo add digit3-security https://helm.digit.org/digit3/security
helm install kubescape digit3-security/kubescape \
  -n kubescape --create-namespace \
  --set kubescape-operator.clusterName=<env>
```

## What it deploys

Four Deployments, two CronJobs and one PVC:

| Component | Role |
|---|---|
| `operator` | watches the cluster, enqueues scans |
| `kubescape` | posture scanner (frameworks / controls) |
| `kubevuln` | image SBOM + CVE scanner |
| `storage` | aggregated API server holding the results, backed by a PVC |

Scans run daily — posture at 01:18 UTC, images at 03:18 UTC.

## What it deliberately does NOT deploy

This is a **reporting** deployment, not a runtime security or policy
enforcement one. Two upstream capability groups are off:

- **The eBPF node agent** (`runtimeObservability`, `runtimeDetection`,
  `malwareDetection`, `networkPolicyService`, `networkEventsStreaming`,
  `nodeProfileService`, `httpDetection`, `seccompProfileService`,
  `relevancy`). These put a privileged DaemonSet on every node. They are
  genuinely useful, and nothing here argues against adopting them — but that
  is a deliberate, per-environment decision, not a chart default.
- **The admission controller.** A webhook in the pod-admission path can block
  pod creation cluster-wide if it goes unhealthy, which is a poor trade for a
  reporting deployment. Preventive policy belongs in a dedicated policy engine.

The operator also keeps read-only RBAC: `remediation` and `manageWorkloads`
are disabled, so it cannot annotate workloads or write NetworkPolicies. The one
exception is the certificate init container described below, which may create
Secrets in the release namespace and patch the storage APIService.

## Certificates: why `strategy: initContainer`

The `storage` component serves an aggregated API over mTLS and needs a CA and a
leaf certificate. Upstream's default, `certificates.strategy: template`, mints
them with Sprig at render time and reuses an existing CA only through a
`lookup` of the `storage-tls-ca` Secret.

Argo CD renders with `helm template` and no cluster access, so that lookup
always returns empty. Every render mints a **new** CA, which means:

- the Application is permanently `OutOfSync` — two renders a second apart
  differ;
- with `selfHeal: true` it re-syncs in a loop, rotating the aggregated API
  server's certificate each time.

This chart therefore sets `certificates.strategy: initContainer`. Certificates
are generated in-cluster, the CA is reused from its Secret once created, and
the init container patches the APIService `caBundle` itself. No Secret appears
in the rendered output and the render is byte-identical run to run.

The cost is the narrow write RBAC it needs:

| Scope | Permission |
|---|---|
| Role `storage-certgen` | `create` on Secrets; `get` on the Secret `storage-tls-init` |
| ClusterRole `storage-certgen-kubescape` | `get`, `patch` on `apiservices` |

`template` remains the right choice for a plain `helm install`, where the
lookup works. Override it per environment if you are not deploying via Argo CD.

## Reading the results

There is no UI. Results are Kubernetes objects served by the aggregated API:

```bash
kubectl get configurationscansummaries -A              # per-namespace posture
kubectl get workloadconfigurationscansummaries -A      # per-workload posture
kubectl get vulnerabilitymanifestsummaries -A          # per-workload CVE counts
kubectl get vulnerabilitymanifests -A                  # full CVE lists
```

**The storage API strips `spec.controls` and `spec.severities` from LIST
responses.** A `kubectl get ... -o json` over a whole namespace returns zeroes
for every count. The numbers only appear on a single-object `GET`, so any
tooling that aggregates these must fetch each object individually.

The detailed `workloadconfigurationscans` objects — the ones that carry the
matched resource paths for a failed control — are not persisted, only the
summaries. To see *why* a control such as C-0012 failed on a given workload you
need a `kubescape` CLI scan.

## Values

Everything sits under the `kubescape-operator` key; this chart has no templates
of its own. To change something upstream exposes but `values.yaml` does not
mention, add it under that key in the environment's values file rather than
forking the subchart.

| Value | Default | Notes |
|---|---|---|
| `kubescape-operator.clusterName` | `CHANGEME` | **Must be overridden.** Results are filed under this name. |
| `kubescape-operator.defaultFrameworks` | `allcontrols`, `nsa`, `cis-aks-t1.2.0` | The CIS framework is cloud-specific — an EKS cluster must switch to `cis-eks-t1.2.0`. |
| `kubescape-operator.excludeNamespaces` | `kubescape` | Upstream also excludes `kube-system`, `kube-public`, `kube-node-lease`. Those are *included* here: the CIS benchmark is largely about control-plane add-ons, so excluding them makes the benchmark score meaningless. |
| `kubescape-operator.persistence.size.backingStorage` | `5Gi` | Summaries plus the SBOM cache. SBOMs dominate — raise before widening namespace scope. |

### The `/tmp` spill volume

`values.yaml` mounts an `emptyDir` at `/tmp` on the `kubescape` Deployment. The
scanner streams the whole resource graph and spills to disk; upstream's
read-only root filesystem makes that spill fail, and it falls back to holding
everything in memory with only a log warning. On a cluster with a few thousand
objects that makes the scanner the component most likely to be OOM-killed.
Do not remove the volume without also raising its memory limit.

## No SaaS backend

`account`, `accessKey` and `server` are empty: results stay in the cluster.
Supplying an ARMO account would ship posture and CVE data for every workload to
a third party — a decision for the platform owner, not a chart default.
