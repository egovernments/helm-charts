# dashboard

Internal security dashboard: Kubescape configuration and vulnerability
findings next to Kyverno policy violations, for one cluster or several, served
behind the same oauth2-proxy the other internal tools use.

Two pods. A CronJob reads the findings and renders a page into a ConfigMap; an
nginx serves it. No database, no PVC, no custom image.

## Why it is not on GitHub Pages

The page names every workload that fails a control, with namespaces and image
tags. GitHub Pages access control applies only to **private or internal**
repositories -- in a public repo the site is world-readable regardless of the
enterprise plan, and `DIGIT-DevOps` and `helm-charts` are both public.
`helm-charts` cannot be made private either, because Argo CD pulls charts from
`helm.digit.org` anonymously.

So the page is served in-cluster behind authentication instead.

## Required values

```yaml
dashboard:
  ingress:
    host: unified-dev.digit.org      # no default; the chart refuses to render
  collector:
    localClusterName: unified-dev    # what this cluster is called in the report
```

`ingress.host` has no default on purpose. A dashboard quietly published on an
unintended hostname is a worse outcome than one that fails to deploy.

## Authentication

`ingress.auth` is on by default and carries the same two annotations Argo CD
uses on these clusters:

```
nginx.ingress.kubernetes.io/auth-url:    http://oauth2-proxy.backbone.svc.cluster.local:4180/oauth2/auth
nginx.ingress.kubernetes.io/auth-signin: https://$host/oauth2/start?rd=$escaped_request_uri
```

That is the only thing between this page and anyone who can reach the
ingress. Setting `auth.enabled: false` is a deliberate act, not a convenience.

## Including other clusters

The collector reads its own cluster through its ServiceAccount. For others,
give it a read-only token per cluster:

```yaml
dashboard:
  collector:
    remoteClusters:
      - name: unified-qa
        server: https://unified-qa-xxxx.hcp.centralindia.azmk8s.io:443
        tokenSecret: dashboard-collector-unified-qa   # Secret with key: token
```

Create that token on the remote cluster from a ServiceAccount bound to the
same read-only ClusterRole this chart installs -- `get` and `list` on four
Kubescape resources and PolicyReports, nothing else. Never an admin kubeconfig.

A cluster whose token is missing or unusable is recorded as **unreachable**
and rendered as such. It is never shown as an environment with no findings.

## Populating it immediately

The schedule is every six hours, which matches how often the underlying scans
change. Until the first run the page says so rather than showing zeros. To
fill it now:

```sh
kubectl -n <ns> create job --from=cronjob/security-dashboard-collector dashboard-now
```

## Why the page ConfigMap is not in this chart

The collector rewrites `security-dashboard-page` on every run. If Helm owned
it, Argo CD would see drift and self-heal the page back to its placeholder on
every sync. nginx mounts it with `optional: true` and falls back to a
placeholder until the first collection.

## The compliance score, and why this chart computes it

Nothing in the cluster stores a compliance score. The Kubescape operator writes
findings; the score you recognise from `kubescape scan` and from the ARMO
platform is computed **client-side** from a scan report. No object in the
aggregated API has a score field — `ConfigurationScanSummary` carries severity
counts and nothing else.

So `files/collect.py` reimplements the upstream formula rather than inventing a
grade, following `kubescape/opa-utils/score`:

| | |
|---|---|
| Control score | `100` if the control passed, otherwise `passed / all × 100`, where `all` includes resources the control skipped |
| Cluster score | the **plain mean** of the control scores — deliberately not weighted by severity, and not weighted by how many resources each control touched |

That plain mean is worth preserving even though a weighted score looks more
sophisticated, because it is what makes a movement explainable. Every control
carries exactly `1/N` of the score, so a control going from 40% to 100% moves
the cluster score by exactly `60/N` points, and the per-control contributions
**sum to the total change**. The page shows that arithmetic directly. Weight the
controls and the attribution stops adding up.

One caveat the page repeats in its footer: the score is taken over the workloads
this scan-only profile collects, so it will not equal `kubescape scan` run from
a laptop against the same cluster, which also evaluates host- and cluster-level
controls this deployment does not gather. It is consistent with itself over
time, which is what a trend needs.

## Every number opens

The page is read by people who do not operate these clusters, so no figure is
allowed to stand on its own. Clicking any number with a dotted underline opens
a panel giving, in this order: what the figure means in plain words, the
arithmetic that produced it, and the actual resources behind it.

The drill-down never recomputes anything. It renders the same values the page
already holds, so it cannot disagree with the headline it was opened from.

Two units get confused constantly, and the panels name which one is in play:

- A **finding** is one check failing against one resource. A Deployment
  breaking four rules is four findings.
- A **grant** is how Kubescape counts the RBAC controls. One ServiceAccount
  reached by five role bindings that can read secrets is **five** findings
  naming the same account. The panel says so explicitly and shows the role and
  binding on each row, because otherwise the list reads as duplicated and the
  reader concludes the page is broken.

`collector.drillWorkloads` caps how many resources each control lists. If the
rendered page would exceed `collector.maxPageBytes` (800 KiB, against a 1 MiB
ConfigMap limit) those lists are trimmed and then dropped, in that order,
rather than letting the publish fail. The trimming is logged.

## The CIS score, and why it is split in two

The headline is the **generic** CIS Kubernetes Benchmark (`cis-v1.12.0`),
chosen over the cloud-specific variants deliberately: its controls are plain
Kubernetes API objects, so the same score means the same thing on AKS, EKS,
GKE or bare metal. `cis-aks-*`, `cis-eks-*` and `cis-gke-*` exist precisely
because the *other* half of the benchmark is not portable.

The benchmark's own section tree is downloaded with it, so the split below is
CIS's, not a classification invented from control names:

| CIS section | What it covers | Assessed here |
|---|---|---|
| 1 Control Plane Components | API server, controller manager, scheduler, node config files | 0 / 60 |
| 2 etcd | etcd flags and file permissions | 1 / 7 |
| 3 Control Plane Configuration | authn/authz, audit logging | 0 / 5 |
| 4 Worker Nodes | kubelet, kube-proxy, node file permissions | 0 / 25 |
| **5 Policies** | **Pod Security, RBAC, network policy, secrets** | **30 / 34** |

Sections 1–4 are not a gap this chart can close. On a managed cluster there is
no control-plane node whose files you can read, and the provider attests to
them separately under its own certifications. Section 5 is entirely properties
of objects we deploy, which is why it is nearly fully assessed and entirely
actionable.

**The two halves are never averaged together.** Blending a half we measure
completely with a half we cannot measure at all yields one number that
misrepresents both. The page reports them side by side, and the unassessed
half says *"Not assessed"* rather than showing a score.

For the same reason a section reports a score only once at least
`MIN_COVERAGE` (default half) of its controls were assessed. Scoring 1 of
etcd's 7 controls and publishing "100%" would be true of the one and
meaningless of the section — and a number that is only correct beside its own
caveat eventually gets quoted without it.

`collector.framework` takes any framework published by the Kubescape
regolibrary (`cis-v1.12.0`, `cis-v1.10.0`, `nsa`, `mitre`, `armobest`,
`devopsbest`, `soc2`). `collector.workloadSections` lists which top-level
sections of it are the workload half — `5` for CIS. Change one and you must
revisit the other.

## History, and why it lives in a ConfigMap

The storage API **overwrites each summary in place** on every scan — the object
keeps its name and resourceVersion simply increments, with a fresh
`kubescape.io/report-created-at` annotation. There is no history CRD and no
retained revisions. A number not recorded at the time is gone.

Each collector run therefore appends one point to the
`security-dashboard-history` ConfigMap and reads the whole series back. The
ConfigMap is not templated by this chart, for the same reason the page is not
(see above): Argo CD would self-heal over every write.

Retention is expressed in **days**, not points, because the posture scan runs
once a night — the four collections a day are four copies of the same posture.

- `collector.history.retainDays` (60) — days of trend kept.
- `collector.history.detailDays` (30) — days that also keep per-control
  scores, which are what attribute a movement to specific controls. Older
  points keep their headline figures and their place on the trend line.
- `collector.history.rawDays` (3) — days at the near end where **every**
  reading is kept rather than compacted to one a day. Beyond this window only
  the last reading of each UTC day survives. Compacting the near window would
  erase exactly the comparison this page exists to show: fix something this
  morning, re-run the collector this afternoon, and there must still be a
  before and an after.
- A hard 700 KiB ceiling drops the oldest points if the series somehow grows
  past it. Shedding the oldest end beats having the whole write rejected.

Measured: 40 days of four-hourly readings compact to 50 stored points, 327 KiB
across three clusters, with per-control detail spanning exactly the newest 30
days.

The page itself carries per-control detail for only the newest two points,
since attribution only ever compares a reading with the one before it.

If the history ConfigMap cannot be read or written, the run logs a warning and
still publishes the page — a dashboard with today's numbers and no trend is
useful; one that renders nothing is not.

## The collection detail that will trip you up

Kyverno PolicyReports are ordinary CRs in etcd and `LIST` cleanly.

Kubescape summaries are served by an **aggregated API** (the `storage` pod).
A `LIST` of those returns every spec field zeroed; only a `GET` of each
individual object carries real numbers. Summing a `LIST` yields a flawless
cluster with no findings and no error to warn you. That is why the collector
fetches thousands of objects one at a time instead of making one list call --
it is not an oversight to optimise away.

## Control names and remediation

The cluster reports only control IDs such as `C-0245`. Names, descriptions and
fixes are downloaded at collection time from Kubescape's published control
library, so nothing shown to a reader is invented here. If that download
fails, the page shows bare IDs -- the honest degradation.
