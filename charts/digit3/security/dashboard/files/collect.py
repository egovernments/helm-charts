#!/usr/bin/env python3
"""Render the security dashboard and publish it into a ConfigMap.

Runs as a CronJob in-cluster. No kubectl and no kubeconfig: it talks to the
API with its own ServiceAccount token, and to any remote clusters with the
read-only tokens mounted at /remote/<name>/token.

Three things here are not obvious and will look like mistakes to the next
person who reads this.

1. THE AGGREGATED API ZEROES ITS SPEC ON LIST.
   Kubescape summaries are served by the `storage` pod, not etcd. A LIST
   returns every object with its spec fields zeroed or absent; only a GET of
   the individual object carries real numbers. Summing a LIST produces a
   flawless cluster with no findings and no error to warn you. Every Kubescape
   figure below therefore comes from a per-object GET, which is why there is a
   thread pool rather than one tidy list call.

   Kyverno PolicyReports are ordinary CRs in etcd and LIST perfectly well.

2. A CLUSTER THAT CANNOT BE READ IS RECORDED AS UNREACHABLE, NOT AS EMPTY.
   An environment with no data must never render as an environment with no
   problems. Same reasoning as the unscanned-image panel on the page itself.

3. CONTROL NAMES AND REMEDIATION COME FROM UPSTREAM.
   The cluster reports only control IDs like C-0245. Names, descriptions and
   fixes are downloaded from Kubescape's published control library so that
   nothing shown to a reader is invented here. If that download fails the page
   shows bare IDs, which is the honest degradation.
"""
import json, os, ssl, sys, time, datetime, collections, urllib.request, urllib.error
import concurrent.futures as cf

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
API = "/apis/spdx.softwarecomposition.kubescape.io/v1beta1"
KYV = "/apis/wgpolicyk8s.io/v1alpha2"
CONTROLS_URL = "https://github.com/kubescape/regolibrary/releases/latest/download/controls"
SEV = ["critical", "high", "medium", "low"]
TOP_CONTROLS = int(os.environ.get("TOP_CONTROLS", "25"))
EXAMPLES = 6


def log(m):
    sys.stderr.write(m + "\n")
    sys.stderr.flush()


class Client:
    """Minimal read client for one cluster."""

    def __init__(self, base, token, cafile=None, insecure=False):
        self.base = base.rstrip("/")
        self.token = token
        if insecure or not cafile:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            ctx = ssl.create_default_context(cafile=cafile)
        self.opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))

    def get(self, path, timeout=60):
        r = urllib.request.Request(self.base + path, headers={"Authorization": "Bearer " + self.token})
        with self.opener.open(r, timeout=timeout) as resp:
            return json.load(resp)

    def try_get(self, path, timeout=60):
        try:
            return self.get(path, timeout)
        except Exception:
            return None

    def send(self, method, path, body, timeout=60, content_type="application/json"):
        data = json.dumps(body).encode()
        r = urllib.request.Request(self.base + path, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token,
            "Content-Type": content_type,
            "Accept": "application/json",
        })
        with self.opener.open(r, timeout=timeout) as resp:
            return json.load(resp)


def control_library():
    try:
        with urllib.request.urlopen(CONTROLS_URL, timeout=40) as r:
            return {c["controlID"]: c for c in json.load(r)}
    except Exception as e:
        log(f"  WARNING: control library unavailable ({e}); findings will show IDs only")
        return {}


def kubescape(c, lib):
    out = {"available": True, "posture": {}, "cve": {}, "unscanned": [], "controls": []}
    idx = c.try_get(f"{API}/configurationscansummaries")
    vdx = c.try_get(f"{API}/vulnerabilitysummaries")
    if idx is None and vdx is None:
        out["available"] = False
        return out

    def posture_one(it):
        n = it["metadata"]["name"]
        d = c.try_get(f"{API}/configurationscansummaries/{n}")
        s = ((d or {}).get("spec") or {}).get("severities") or {}
        return n, {k: s.get(k, 0) or 0 for k in SEV}

    def cve_one(it):
        n = it["metadata"]["name"]
        d = c.try_get(f"{API}/vulnerabilitysummaries/{n}")
        s = ((d or {}).get("spec") or {}).get("severities") or {}
        return n, {k: (s.get(k) or {}).get("all", 0) or 0 for k in SEV}

    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        for n, v in ex.map(posture_one, (idx or {}).get("items", [])):
            out["posture"][n] = v
        for n, v in ex.map(cve_one, (vdx or {}).get("items", [])):
            out["cve"][n] = v

    wl = c.try_get(f"{API}/workloadconfigurationscansummaries") or {"items": []}

    def wl_one(it):
        m = it["metadata"]
        return c.try_get(f"{API}/namespaces/{m['namespace']}/workloadconfigurationscansummaries/{m['name']}")

    counts, sevs, examples, nss = collections.Counter(), {}, collections.defaultdict(list), collections.defaultdict(set)
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        for o in ex.map(wl_one, wl.get("items", [])):
            if not o:
                continue
            L = o["metadata"].get("labels", {}) or {}
            ns = L.get("kubescape.io/workload-namespace", "?")
            kind = L.get("kubescape.io/workload-kind", "?")
            name = L.get("kubescape.io/workload-name", "?")
            for cid, ctl in ((o.get("spec") or {}).get("controls") or {}).items():
                if ((ctl.get("status") or {}).get("status")) != "failed":
                    continue
                counts[cid] += 1
                sevs[cid] = ((ctl.get("severity") or {}).get("severity") or "?").lower()
                nss[cid].add(ns)
                if len(examples[cid]) < EXAMPLES:
                    examples[cid].append(f"{ns}/{kind}/{name}")

    for cid, n in counts.most_common(TOP_CONTROLS):
        meta = lib.get(cid, {})
        out["controls"].append({
            "id": cid, "name": meta.get("name", ""),
            "remediation": meta.get("remediation", ""),
            "description": (meta.get("description") or "")[:320],
            "severity": sevs.get(cid, "?"), "count": n,
            "namespaces": len(nss[cid]), "examples": examples[cid],
        })

    for it in (c.try_get(f"{API}/sbomsyfts") or {"items": []})["items"]:
        a = (it.get("metadata", {}) or {}).get("annotations", {}) or {}
        if a.get("kubescape.io/status") == "too-large":
            out["unscanned"].append({
                "image": a.get("kubescape.io/image-tag", it["metadata"]["name"]),
                "reason": a.get("kubescape.io/status-reason", "unknown"),
                "size": int(a.get("kubescape.io/resource-size") or 0),
            })
    return out


def kyverno(c):
    out = {"available": True, "policies": [], "by_namespace": {}, "totals": {"pass": 0, "fail": 0}}
    d = c.try_get(f"{KYV}/policyreports")
    if d is None:
        out["available"] = False
        return out
    agg = {}
    for it in d.get("items", []):
        ns = (it.get("metadata") or {}).get("namespace", "-")
        s = it.get("summary") or {}
        out["totals"]["pass"] += s.get("pass", 0) or 0
        out["totals"]["fail"] += s.get("fail", 0) or 0
        scope = it.get("scope") or {}
        for res in it.get("results") or []:
            if res.get("result") != "fail":
                continue
            pol = res.get("policy", "?")
            a = agg.setdefault(pol, {"id": pol, "severity": res.get("severity", "?"),
                                     "category": res.get("category", ""), "message": res.get("message", ""),
                                     "count": 0, "examples": [], "namespaces": set()})
            a["count"] += 1
            a["namespaces"].add(ns)
            if len(a["examples"]) < EXAMPLES:
                a["examples"].append(f"{ns}/{scope.get('kind','?')}/{scope.get('name','?')}")
            out["by_namespace"].setdefault(ns, {"fail": 0})["fail"] += 1
    for a in agg.values():
        a["namespaces"] = len(a["namespaces"])
    out["policies"] = sorted(agg.values(), key=lambda x: -x["count"])
    return out


def clusters():
    """The local cluster, plus any remote ones whose token is mounted."""
    out = []
    local = os.environ.get("LOCAL_CLUSTER_NAME") or "this-cluster"
    with open(f"{SA}/token") as f:
        tok = f.read().strip()
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    out.append((local, Client(f"https://{host}:{port}", tok, cafile=f"{SA}/ca.crt")))

    for spec in json.loads(os.environ.get("REMOTE_CLUSTERS", "[]")):
        name, server = spec["name"], spec["server"]
        path = f"/remote/{name}/token"
        try:
            with open(path) as f:
                rtok = f.read().strip()
        except OSError as e:
            log(f"  WARNING: no token for {name} at {path} ({e}); it will be marked unreachable")
            out.append((name, None))
            continue
        out.append((name, Client(server, rtok, insecure=True)))
    return out


def publish(c, namespace, name, html):
    """Write the page into the ConfigMap nginx serves."""
    body = {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": namespace},
            "data": {"index.html": html}}
    path = f"/api/v1/namespaces/{namespace}/configmaps/{name}"
    try:
        c.send("PUT", path, body)
        log(f"  updated ConfigMap {namespace}/{name} ({len(html)} bytes)")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            c.send("POST", f"/api/v1/namespaces/{namespace}/configmaps", body)
            log(f"  created ConfigMap {namespace}/{name}")
        else:
            raise


def main():
    log("fetching control library ...")
    lib = control_library()
    log(f"  {len(lib)} controls")

    data = {"generated": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "clusters": {}}
    first = None
    for name, c in clusters():
        if first is None and c is not None:
            first = c
        if c is None:
            data["clusters"][name] = {"reachable": False, "error": "no credentials mounted"}
            continue
        t = time.time()
        log(f"collecting {name} ...")
        try:
            data["clusters"][name] = {"reachable": True, "kubescape": kubescape(c, lib), "kyverno": kyverno(c)}
            log(f"  done in {time.time() - t:.1f}s")
        except Exception as e:
            log(f"  FAILED: {e}")
            data["clusters"][name] = {"reachable": False, "error": str(e)}

    with open("/opt/dashboard/template.html") as f:
        html = f.read().replace("/*__DATA__*/null", json.dumps(data))

    out = os.environ.get("OUTPUT_PATH")
    if out:
        with open(out, "w") as f:
            f.write(html)
        log(f"wrote {out}")
    else:
        publish(first, os.environ["POD_NAMESPACE"], os.environ["PAGE_CONFIGMAP"], html)

    bad = [k for k, v in data["clusters"].items() if not v.get("reachable")]
    if bad:
        log("unreachable: " + ", ".join(bad))


if __name__ == "__main__":
    main()
