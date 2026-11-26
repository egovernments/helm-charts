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

## One cluster or several: the same chart

`collector.remoteClusters` is the only difference between the two deployments.

**Left empty (the default), the dashboard covers the cluster it runs in.** That
is the single-environment deployment — a production team installs this chart,
sets `ingress.host` and `collector.localClusterName`, and gets a dashboard for
their own cluster with no reference to any other. Nothing to switch off, no
separate chart, no second image. Verified: with `remoteClusters: []` the page
renders one environment and every drill-down works.

**Listing other clusters rolls them into one page.** Each entry needs:

```yaml
collector:
  remoteClusters:
    - name: unified-qa
      server: https://unified-qa-xxxx.hcp.centralindia.azmk8s.io:443
      tokenSecret: dashboard-collector-unified-qa
```

A cluster that is listed but cannot be read is reported as **unreachable**,
never rendered as clean. An environment with no data must not look like an
environment with no problems.

### The token Secret

The Secret needs a `token` **and** a `ca.crt`. That is exactly what a Secret of
type `kubernetes.io/service-account-token` contains, so the simplest correct
thing is to copy one whole.

The read-only ServiceAccount already exists on any cluster running this chart
(`security-dashboard` in the release namespace), with `get` and `list` on the
Kubescape aggregated API and on PolicyReports, and nothing else. **Never use an
admin credential here.**

On the remote cluster, mint a long-lived token for it:

```sh
kubectl -n kubescape create -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: security-dashboard-token
  namespace: kubescape
  annotations:
    kubernetes.io/service-account.name: security-dashboard
type: kubernetes.io/service-account-token
EOF
```

Then copy `token` and `ca.crt` out of it into a Secret of the configured
`tokenSecret` name on the cluster running the collector. Put it through the
same secrets path as everything else in that environment (SOPS, Azure Key
Vault) — it is a cluster-wide read credential and belongs nowhere near git in
plaintext.

`kubectl create token security-dashboard -n kubescape --duration=...` issues a
**bounded** token instead, which is better practice but has to be re-issued
before it expires. A Secret-backed token does not expire, so if you use one,
decide who rotates it and when.

### Why the CA is not optional

The connection crosses the public internet carrying a cluster-wide read token.
An AKS API server presents a certificate signed by its own cluster CA, not a
public one, so the system trust store cannot validate it and the CA has to
travel with the token.

A remote with no `ca.crt` is **marked unreachable and skipped**, with the
reason logged — the collector will not send a token to an endpoint it cannot
verify. `insecureSkipTlsVerify: true` on an entry overrides this and exists
only for throwaway testing.

### Networking

Nothing special is needed if the API servers are reachable from the collector's
cluster. On AKS with no authorized-IP-range restriction they are. If a cluster
restricts its API server by IP, the collector cluster's egress address has to
be allowed, or that cluster has to run its own dashboard instead.

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

## Memory, and why the manifests are fetched in batches

The per-image vulnerability manifests are by far the largest objects this
collector touches: a couple of MB of JSON each, tens of MB once parsed.

The first version mapped a thread pool over the whole list. `ThreadPoolExecutor.map`
**submits every task immediately** and holds each result until the consuming
loop reaches it, so all ten manifests were resident at peak. A single-cluster
run survived that; the first three-cluster roll-up was OOM-killed on the
second cluster.

They are now fetched in batches of `collector.cve.fetchConcurrency` (2), each
batch dropped before the next is submitted, so what is live no longer scales
with `collector.cve.images`.

Measured after the fix: a three-cluster roll-up peaks at **875 MiB** resident.
The limit is **2Gi** — roughly 2.3x that — and the request is 768Mi so the
scheduler places the pod somewhere it can actually run. Both are deliberately
generous: the job runs for about two minutes every six hours, and a collector
killed halfway leaves the page serving the previous run **with no indication
anything failed**. If more clusters or more images are added, raise the limit
rather than reducing what is sampled.

## Start-up races, and why the collector waits

These clusters are stopped overnight and started on a schedule. On start-up
the collector can beat Kubescape's `storage` pod, which serves the aggregated
API every configuration and vulnerability figure comes from. A read that fails
then is **indistinguishable, in the data, from a cluster with nothing wrong
with it**.

So the collector waits for that API (`collector.apiWaitSeconds`, default 600,
polled every `apiWaitInterval`) before concluding anything. A dashboard a few
minutes late is fine; a dashboard that records an empty cluster is not.

If it still cannot read after the wait, `kubescape.available` is `false` with a
`reason`, and the page says so everywhere the figures would otherwise appear:
the environment card reads **"not collected"** rather than `0`, its pill reads
**"data missing"**, and the headline score is omitted rather than shown as
absent-and-therefore-fine. Kyverno numbers are unaffected -- PolicyReports are
ordinary cluster resources, not an aggregated API, so they survive a storage
outage.

This was not hypothetical: 1.2.1 rendered `Config findings (high) 0` and
`Image CVEs (critical) 0` on all three environments after an overnight
restart, which reads as a clean cluster. That is the single thing this page
exists not to say.

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

## Four pages

One file, four pages, routed on the URL hash so each can be sent to someone on
its own: `#overview`, `#config`, `#policies`, `#images`.

**Overview** is written for a reader who does not operate these clusters and
has never heard of Kubescape. Four plain-language panels say what the page is,
what the score means, what it deliberately does not measure, and how to read
it; then the environments, the trend and the coverage gaps. No acronym is used
before it is explained.

**Configuration**, **Policies** and **Image CVEs** are the technical views. The
drill-down rule holds on all of them.

## Every policy, not only the ones with findings

The Policies page is built **inventory first**: the collector lists the policy
objects from the Kyverno API and merges the report results onto them, rather
than deriving the list from the reports.

That ordering is the whole point. A page built from reports alone cannot tell
these three apart:

| | what the reports say |
|---|---|
| the policy is satisfied | nothing |
| the policy matches nothing on this cluster | nothing |
| Kyverno refused to compile the policy and never ran it | nothing |

All three are an absence, and an absence rendered as `0` reads as good news.
Only the first is. So every installed policy gets a row whatever it has
produced, and each row carries a **state in words** beside the number:

| state | means |
|---|---|
| `not satisfied` | resources were checked and some failed |
| `erroring` | the policy ran and threw against some resources, so its counts are incomplete |
| `not running` | installed, but Kyverno reports it as not ready — nothing has been checked against it |
| `nothing evaluated` | installed and running, and no resource matched it |
| `satisfied` | resources were checked and all passed — the only one of these that is good news |

`not running` is the state that matters most and the one nothing else surfaces.
A policy whose CEL does not compile is accepted by the API server, appears in
`kubectl get vpol`, and reports nothing forever. `kubectl get vpol` shows it:

```sh
kubectl get vpol                       # the READY column
kubectl get vpol <name> -o yaml | yq .status.conditionStatus
```

Note that the field is `.status.conditionStatus.ready`, not `.status.ready` —
the obvious jsonpath returns empty and looks like a broken policy.

Policy kinds collected: `ValidatingPolicy`, `NamespacedValidatingPolicy`,
`ImageValidatingPolicy`, `NamespacedImageValidatingPolicy` and the legacy
`ClusterPolicy` / `Policy`. Mutating, generating and deleting policies are
deliberately **not** listed: they never produce a `PolicyReport` result, so
showing them beside findings would imply they are being measured.

### Two things the page refuses to guess

**A policy that only sees new resources.** A policy with background scanning
off — any policy whose CEL reads `request`, such as `digit-workload-source` —
evaluates admission requests only. Its zero means "nothing has been deployed
by hand *since the policy went in*", not "nothing was deployed by hand". The
row says so rather than letting the zero speak.

**A policy list that could not be read.** If the collector gets 403 on the
policy objects, the findings are still true but the policy's mode and scope
are not known. Those fields render as `not read`, never as a guessed "Audit".
The page note also says on how many environments the list was unreadable.

This needs `policies.kyverno.io` and `kyverno.io` read in the ClusterRole. It
is the same RBAC trap that shipped broken in 1.2.0 — a local harness using an
admin kubeconfig cannot see it, because the collector runs as its
ServiceAccount and nothing else does.

## Image vulnerabilities, and the two ways to count them

The same vulnerability can be counted twice over and both answers are honest:

| | what it answers | `cveDetail` field |
|---|---|---|
| **once per image** | how much *work* there is: one image to rebuild, however many places run it | `totalsUnique` |
| **once per container** | how much *exposure* there is: how many running things carry it | `totals` |

They differ a lot. On unified-dev, critical findings are **13,495** per image
and **22,642** per container, because images like `egovio/airflow` run in a
dozen containers and each copy re-counts the same CVEs.

**The page leads with the per-image figure** — it is the one that corresponds
to an action someone can take — states the unit on the tile, and gives both
when the figure is opened. A single number here, presented without its unit,
gets challenged the first time somebody notices the same image counted twice,
and they would be right.

Three levels of detail are collected, because three different questions get
asked:

- **every image**, with counts by severity — cheap, from the per-workload
  summaries.
- **every workload and container**, with the image it runs — so an owner can
  find their own service by namespace, kind, name and container rather than
  working backwards from an image.
- **the actual CVE ids** for the worst `collector.cve.images` images: id,
  severity, package, installed version and the version that fixes it.

That last one is sampled deliberately. A per-image manifest holds every match
the scanner produced — around 800 for a mid-sized image — and fetching all of
them moves roughly **700 MB** on a cluster this size, against a collector with
a few hundred MiB. Counts are complete for every image; the itemised list is
not, and the page says so rather than implying otherwise.

**Fixable CVEs are separated from the rest.** A critical with no published
patch is not work anyone can do today, and adding the two together produces a
number that cannot be driven down. `project-factory` on unified-dev reports
7,672 CVEs of which 5,777 have an upstream fix; those are the ones worth
listing first.

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
