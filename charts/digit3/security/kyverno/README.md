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
produce findings. This chart also carries two policies of its own -- see
[DIGIT policies](#digit-policies).

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

## DIGIT policies

Two policies DIGIT wrote, on top of the upstream Pod Security set. They are
rendered from `templates/digit-*.yaml`, and everything about them --
on/off, Audit vs Enforce, which namespaces -- is configured in the single
`digitPolicies` block of `values.yaml`. The CEL does not need editing to
change any of that.

| policy | what it reports | default severity |
|---|---|---|
| `digit-require-resources` | a container with no cpu/memory request, or no memory limit (and, by default, no cpu limit) | medium |
| `digit-workload-source` | a workload created or changed by anyone other than Argo CD, Kargo or the control plane | high |

Both ship as `Audit`. They record findings in the same `PolicyReport`s as
everything else, so they appear on the security dashboard without any change
there.

### The intended rollout

```yaml
digitPolicies:
  action: Audit                 # 1. everything audited, everywhere
  policies:
    requireResources:
      action: Enforce           # 3. one policy ...
      namespaces:
        include: [playground]   #    ... in one namespace
```

1. Audit everywhere. Read the findings.
2. Fix them, or scope out what will not be fixed.
3. Move one policy to `Enforce` in one namespace, by setting that policy's
   own `action` and `namespaces.include`.
4. Add namespaces to that list one at a time.
5. Drop the per-policy overrides once the whole estate passes.

A policy's `namespaces.include` / `.exclude` **replaces** the global list of
the same name rather than merging with it, so `include: []` on a policy
widens it back to every namespace.

`Enforce` renders as `validationActions: [Deny, Audit]` -- the request is
rejected *and* still recorded, so the dashboard keeps showing what was
blocked. This is independent of `failurePolicy`, which stays `Ignore`; see
above for why those are not the same decision.

### `digit-require-resources`

Reports one finding per object a human can actually edit. A Pod owned by a
ReplicaSet and a Job owned by a CronJob are skipped, because the fix belongs
on the controller, which the policy also matches -- otherwise the same
missing field is reported two or three times and churns every time a pod is
replaced. The test is on the *owner's kind*, not on having an owner at all,
so a Deployment created by an operator keeps its finding.

Init containers are included. A sidecar declared as a `restartPolicy: Always`
init container runs for the life of the pod, and an init container with no
memory limit can OOM a node as well as any app container.

The failure message names the containers, not just the object:

```
no cpu/memory request: istio-proxy, egov-service
```

`requireCpuLimit` is worth a deliberate decision. A CPU limit does not
protect the node the way a memory limit does -- the kernel throttles the
container rather than killing it -- so a limit set too low becomes latency
nobody can explain, and the common advice is to set CPU requests and skip CPU
limits. It defaults to `true` here because it was asked for; set it to
`false` to report only requests and the memory limit.

### `digit-workload-source`

**This policy reports what is created or changed from now on, not what is
already running.** It reads `request.userInfo`, which exists only on a live
admission request, so its `evaluation.background.enabled` is `false` and the
background scanner never evaluates it. Workloads already on the cluster
produce no findings until something touches them.

To audit what is already there, compare field managers instead:

```sh
kubectl get deploy -A --show-managed-fields -o json | \
  jq -r '.items[].metadata.managedFields[].manager' | sort | uniq -c | sort -rn
```

On unified-dev that currently reads 234 of 245 Deployments applied by
`argocd-controller`, the rest by `helm`, `kubectl-client-side-apply` and
`eno` -- which is roughly what the policy will report once those objects are
next touched.

`allowedUsers` and `allowedPrefixes` are the whole definition of "comes from
Argo CD or Kargo". The control-plane identities (`system:kube-controller-manager`,
`system:apiserver`, the `kube-system` service accounts) are allowed because
they write to these objects as part of running Kubernetes -- adopting a
resource, creating a Job from a CronJob -- and are not deployments.

Note that `UPDATE` is matched as well as `CREATE`: a `kubectl rollout
restart` is a change that did not come from git, and is reported as one.

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
| `upgrade.legacyPolicyCheck` | disabled | its hook Job races its own TTL and hangs Argo syncs; see below |
| `crds.migration` | disabled | see below |
| replicas | 1 per controller | sufficient while fail-open |

### `upgrade.legacyPolicyCheck.enabled: false`

A pre-install Helm hook Job that blocks installation when legacy `kyverno.io`
policy resources exist. It hangs Argo CD syncs.

The Job finishes in seconds, then Kubernetes deletes it via its own
`ttlSecondsAfterFinished: 300`. Argo records `hookPhase: Running` when it
applies the hook and then polls the Job to see it finish -- so if the TTL
fires first, Argo polls a Job that no longer exists and waits forever:

```
waiting for completion of hook batch/Job/kyverno-check-legacy-policies
```

It is a race, so it does not reproduce reliably: installing this chart on
unified-dev, -qa and -uat minutes apart, dev won and the other two wedged.

Recovery is awkward, which is the other reason to disable it. Terminating the
operation is not enough -- the next one resumes the same recorded hookPhase.
The cached result has to be cleared directly:

```sh
kubectl -n argocd patch application <app> --type=merge -p '{"status":{"operationState":null}}'
```

Nothing is lost here: it guards an upgrade path from legacy policy types, and
these are fresh installs. The chart's render-time check stays on.

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
