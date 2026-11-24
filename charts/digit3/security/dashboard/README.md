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
