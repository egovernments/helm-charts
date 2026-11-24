# kyverno

Kyverno policy engine for the unified environments: Kubernetes Pod Security
Standards reported as findings, never enforced, on webhooks that cannot wedge
the cluster.

Wrapper around two upstream charts, pinned together:

| chart | version | what it is |
|---|---|---|
| `kyverno` | 3.10.0 | the engine: controllers, webhooks, CRDs. Ships **no policies** |
| `kyverno-policies` | 3.10.0 | the Pod Security Standards policy set |

The engine on its own installs cleanly and does nothing. The policies are what
produce findings.

## The two settings that make this safe

They are independent. Relaxing either one alone is enough to start rejecting
workloads, so read both before changing anything.

### `validationFailureAction: Audit`

A policy that finds a violation records it and admits the resource anyway.
Nobody's deployment fails because their pod is non-compliant.

### `failurePolicy: Ignore`

**This is the one that matters, and it is not the same thing.**

`Audit` decides what happens when a policy *finds* something. `failurePolicy`
decides what happens when the webhook itself cannot be reached — Kyverno
restarting, rescheduling, OOMing, a node draining, or simply not ready yet.

Upstream defaults it to `Fail`, which is what people mean when they say an
admission webhook took the cluster down: for as long as Kyverno is
unreachable, every matching admission request in the cluster is rejected —
including the deployments trying to fix it.

`Ignore` means the API server proceeds when Kyverno does not answer. The cost
is that a resource created during that window goes unevaluated, which for an
Audit-only install costs one report entry. That is the right trade until
policies are actually enforced.

## Where the findings go

Kyverno writes `PolicyReport` (namespaced) and `ClusterPolicyReport`
(cluster-scoped) resources. That is the whole output, and what the security
dashboard reads:

```sh
kubectl get policyreports -A
kubectl get clusterpolicyreports

# everything currently failing, by policy
kubectl get policyreports -A -o json | jq -r '.items[].results[]
  | select(.result=="fail") | .policy' | sort | uniq -c | sort -rn
```

Reports cover resources that **already exist**, not just newly admitted ones:
the policies' `evaluation.background.enabled` defaults to true and the
reports controller scans the cluster. A fresh install populates on its own
without anything being deployed.

## Going from Audit to Enforce

Not a one-line change. In order:

1. Get the baseline findings to zero, or scope the policies to exclude what
   will not be fixed. Enforcing against a dirty estate blocks deployments.
2. Raise `admissionController.replicas` — with `Ignore` an outage is
   harmless, so one replica is fine; under `Fail` it is a single point of
   failure for all admission.
3. Change `failurePolicy` to `Fail` **last**, and understand that this is the
   step that can take an environment down, not the `Enforce` flag.

## DIGIT defaults

| setting | value | why |
|---|---|---|
| `validationFailureAction` | `Audit` | report, never reject |
| `failurePolicy` | `Ignore` | an unreachable webhook cannot block admission |
| `podSecurityStandard` | `baseline` | `restricted` needs runAsNonRoot/seccomp that DIGIT images do not meet yet |
| `cleanupController` | disabled | nothing uses CleanupPolicy; one less controller with cluster-wide delete rights |
| `reportsController` | enabled | without it there are no reports and no dashboard |
| `crds.migration` | disabled | see below |
| replicas | 1 per controller | sufficient while fail-open |

### `crds.migration.enabled: false`

Upstream ships a post-upgrade Helm hook that rewrites legacy CRD data. There
is nothing to migrate on a fresh install, and under Argo CD it is actively
harmful: its `helm.sh/hook-delete-policy` is commented out upstream, so Argo
maps it to a PostSync hook that is never cleaned up and the *next* sync fails
trying to recreate an immutable Job.

Re-enable it, or run the job by hand, only when upgrading across a Kyverno
release that actually requires it.

## Notes for Argo CD

- Sync with `ServerSideApply=true`. The Kyverno CRDs are far too large for
  the client-side-apply annotation.
- Kyverno creates its `ValidatingWebhookConfiguration` /
  `MutatingWebhookConfiguration` objects **at runtime**, not from the chart,
  so they are not Argo-managed and need no AppProject entry.
- `helm.sh/hook: test` pods in the chart are ignored by Argo CD.
