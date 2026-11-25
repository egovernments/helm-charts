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

4. THE COMPLIANCE SCORE IS COMPUTED HERE, BECAUSE NOTHING IN THE CLUSTER
   COMPUTES IT. The score Kubescape shows in its CLI and SaaS is calculated by
   the client from a scan report; the in-cluster operator stores findings only,
   and no object in the aggregated API carries a score field. So `score()`
   below reimplements the upstream formula exactly -- see its docstring.

5. HISTORY IS OURS TO KEEP. The storage API overwrites each summary in place
   on every scan (the object keeps its name and gains a new
   `kubescape.io/report-created-at`), so yesterday's numbers are gone the
   moment tonight's scan lands. Each run therefore appends one point to a
   history ConfigMap and reads the whole series back, which is the only reason
   the page can say a score moved rather than just what it is now.
"""
import json, os, ssl, sys, time, datetime, collections, urllib.request, urllib.error
import concurrent.futures as cf

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
API = "/apis/spdx.softwarecomposition.kubescape.io/v1beta1"
KYV = "/apis/wgpolicyk8s.io/v1alpha2"
REGO = "https://github.com/kubescape/regolibrary/releases/latest/download"
CONTROLS_URL = f"{REGO}/controls"

# The compliance framework the headline score is reported against.
#
# cis-v1.12.0 is the GENERIC CIS Kubernetes Benchmark, which is the point: its
# controls are plain Kubernetes API objects, so the same score means the same
# thing on AKS, EKS, GKE or bare metal. The cloud-specific variants
# (cis-aks-*, cis-eks-*, cis-gke-*) exist precisely because the control-plane
# half of the benchmark is NOT portable -- and that half is the part a managed
# service does not let you read anyway. See README.
FRAMEWORK = os.environ.get("FRAMEWORK", "cis-v1.12.0")
FRAMEWORK_URL = f"{REGO}/{FRAMEWORK}"

# Top-level section numbers whose controls are workload configuration -- the
# part these clusters own and can fix. For CIS that is section 5, "Policies";
# sections 1-4 are control plane, etcd and worker nodes. Kept as a setting
# rather than inferred, so a framework change is a deliberate edit.
WORKLOAD_SECTIONS = set(filter(None, os.environ.get("WORKLOAD_SECTIONS", "5").split(",")))

# A section reports a score only once at least this fraction of its controls
# were actually assessed. Scoring 1 of etcd's 7 controls and publishing "100%"
# would be true of the one and meaningless of the section, and a number that
# is only correct beside its own caveat eventually gets quoted without it.
MIN_COVERAGE = float(os.environ.get("MIN_COVERAGE", "0.5"))
SEV = ["critical", "high", "medium", "low"]
TOP_CONTROLS = int(os.environ.get("TOP_CONTROLS", "25"))
EXAMPLES = 6

# History retention, in days rather than points, because the thing being
# retained is days of posture.
#
# The posture scan runs once a night, so the four collections a day produce
# four identical readings. Older points are therefore compacted to one per UTC
# day, which loses nothing. The exception is a RAW_DAYS window at the near
# end: within it every reading is kept, so that fixing something and
# re-running the collector the same day still has a before and an after to
# compare. Compacting that window would erase exactly the comparison this
# page exists to show.
#
# MAX_HISTORY_BYTES is the backstop: a ConfigMap is capped at 1 MiB, and
# losing the whole series to a rejected write would be worse than losing its
# oldest end.
RETAIN_DAYS = int(os.environ.get("RETAIN_DAYS", "60"))
DETAIL_DAYS = int(os.environ.get("DETAIL_DAYS", "30"))
RAW_DAYS = int(os.environ.get("RAW_DAYS", "3"))
MAX_HISTORY_BYTES = 700 * 1024


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


def score(tally, nworkloads):
    """Kubescape's compliance score, reimplemented from the upstream formula.

    Nothing in the cluster stores a score: the operator writes findings, and
    the score a reader recognises is computed client-side by the Kubescape CLI
    and by the ARMO backend. Rather than invent a grading scheme, this
    reproduces `kubescape/opa-utils/score` exactly, so the number here means
    what the same number means anywhere else in the Kubescape ecosystem:

      GetControlComplianceScore:  a control that passed scores 100; otherwise
                                  passed / all * 100, where `all` includes
                                  resources the control skipped.
      SetPostureReportComplianceScores:
                                  the overall score is the PLAIN MEAN of the
                                  control scores -- deliberately not weighted
                                  by severity or by how many resources each
                                  control touched.

    That plain mean is what makes the trend readable rather than decorative:
    because every control carries weight 1/N, a control moving from 40 to 100
    contributes exactly (100-40)/N points to the cluster score, and the
    per-control contributions sum to the total change. The page attributes a
    movement on precisely that arithmetic.

    One honest caveat, repeated on the page: this is scored over the workloads
    the operator scanned, so it is not identical to `kubescape scan` on the
    same cluster, which also evaluates host- and cluster-level controls that
    this scan-only profile does not collect. It is consistent with itself over
    time, which is what a trend needs.
    """
    scores = {}
    for cid, t in tally.items():
        allr = t["passed"] + t["failed"] + t["other"]
        scores[cid] = 100.0 if t["failed"] == 0 else (round(t["passed"] / allr * 100.0, 4) if allr else 0.0)
    return {
        "compliance": round(sum(scores.values()) / len(scores), 2) if scores else None,
        "controlsEvaluated": len(scores),
        "workloadsScanned": nworkloads,
        "controlScores": scores,
    }


def framework():
    """The CIS benchmark's own section tree, downloaded rather than hard-coded.

    This is what makes the control-plane/workload split defensible: the
    sections are CIS's ("1 Control Plane Components" ... "5 Policies"), not a
    classification invented here from control names. Each control also carries
    its CIS number in its name (`CIS-5.2.2 Minimize the admission of
    privileged containers`), which is what an auditor will ask for.

    Returns (meta, {controlID: (sectionID, sectionName)}, {controlID: name}).
    Degrades to empty, which drops the framework panel but leaves the rest of
    the page intact.
    """
    try:
        with urllib.request.urlopen(FRAMEWORK_URL, timeout=60) as r:
            f = json.load(r)
    except Exception as e:
        log(f"  WARNING: framework {FRAMEWORK} unavailable ({e}); no framework score")
        return None, {}, {}

    sections, order = {}, []

    def walk(node, top=None, topname=None):
        for key, v in (node or {}).items():
            if not isinstance(v, dict):
                continue
            t, tn = (top, topname) if top else (key, v.get("name", key))
            if not top:
                order.append((t, tn))
            for cid in (v.get("controlsIDs") or []):
                sections[cid] = (t, tn)
            walk(v.get("subSections"), t, tn)

    walk(f.get("subSections"))
    names = {c["controlID"]: c.get("name", "") for c in (f.get("controls") or [])}
    meta = {"id": f.get("name") or FRAMEWORK,
            "description": (f.get("description") or "").split(" - ")[0],
            "order": [t for t, _ in order],
            "sectionNames": dict(order)}
    return meta, sections, names


def framework_score(scores, meta, sections, names):
    """Score the framework by section, and never average across the split.

    The two halves are reported separately on purpose. Blending a workload
    score we measure completely with a control-plane score we cannot measure
    at all produces one number that misrepresents both.
    """
    if not meta:
        return None
    per = {}
    for cid, (sid, sname) in sections.items():
        b = per.setdefault(sid, {"id": sid, "name": sname, "total": 0, "covered": 0, "sum": 0.0})
        b["total"] += 1
        if cid in scores:
            b["covered"] += 1
            b["sum"] += scores[cid]
    out = []
    for sid in meta["order"]:
        b = per.get(sid)
        if not b:
            continue
        enough = b["covered"] >= b["total"] * MIN_COVERAGE and b["covered"] > 0
        out.append({"id": sid, "name": b["name"], "total": b["total"], "covered": b["covered"],
                    "score": round(b["sum"] / b["covered"], 2) if enough else None,
                    "scope": "workload" if sid in WORKLOAD_SECTIONS else "infrastructure"})

    def roll(scope):
        rows = [r for r in out if r["scope"] == scope]
        cov = sum(r["covered"] for r in rows)
        tot = sum(r["total"] for r in rows)
        # Weighted back by control count, not a mean of the section scores: a
        # two-control section must not outweigh a thirty-control one. Summed
        # from the raw control scores so that sections suppressed above still
        # contribute what they measured.
        num = sum(scores[cid] for cid, (sid, _) in sections.items()
                  if cid in scores and (sid in WORKLOAD_SECTIONS) == (scope == "workload"))
        enough = cov >= tot * MIN_COVERAGE and cov > 0
        return {"score": round(num / cov, 2) if enough else None,
                "covered": cov, "total": tot, "assessed": enough}

    detail = sorted(
        ({"id": cid, "name": names.get(cid) or cid, "score": round(scores[cid], 1),
          "section": sections[cid][0]}
         for cid in sections if cid in scores and sections[cid][0] in WORKLOAD_SECTIONS),
        key=lambda r: r["score"])
    return {"id": meta["id"], "description": meta["description"], "sections": out,
            "workload": roll("workload"), "infrastructure": roll("infrastructure"),
            "controls": detail}


def kubescape(c, lib, fw):
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
    tally = collections.defaultdict(lambda: {"passed": 0, "failed": 0, "other": 0})
    nworkloads = 0
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        for o in ex.map(wl_one, wl.get("items", [])):
            if not o:
                continue
            nworkloads += 1
            L = o["metadata"].get("labels", {}) or {}
            ns = L.get("kubescape.io/workload-namespace", "?")
            kind = L.get("kubescape.io/workload-kind", "?")
            name = L.get("kubescape.io/workload-name", "?")
            for cid, ctl in ((o.get("spec") or {}).get("controls") or {}).items():
                st = (ctl.get("status") or {}).get("status")
                t = tally[cid]
                t["passed" if st == "passed" else "failed" if st == "failed" else "other"] += 1
                if st != "failed":
                    continue
                counts[cid] += 1
                sevs[cid] = ((ctl.get("severity") or {}).get("severity") or "?").lower()
                nss[cid].add(ns)
                if len(examples[cid]) < EXAMPLES:
                    examples[cid].append(f"{ns}/{kind}/{name}")

    out["score"] = score(tally, nworkloads)
    out["framework"] = framework_score(out["score"]["controlScores"], *fw)

    # So the page can say "top N of M" rather than leaving a reader to wonder
    # why the control counts do not add up to the headline total.
    out["controlsTotal"] = len(counts)
    out["failuresTotal"] = {sv: sum(c for cid, c in counts.items() if sevs.get(cid) == sv)
                            for sv in ("critical", "high", "medium", "low")}

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


def point(data):
    """One row of history: the headline figures, small enough to keep forever."""
    row = {"at": data["generated"], "clusters": {}}
    for name, c in data["clusters"].items():
        if not c.get("reachable"):
            row["clusters"][name] = {"reachable": False}
            continue
        ks, ky = c.get("kubescape") or {}, c.get("kyverno") or {}
        sc = ks.get("score") or {}
        row["clusters"][name] = {
            "reachable": True,
            "compliance": sc.get("compliance"),
            "controlsEvaluated": sc.get("controlsEvaluated"),
            "workloadsScanned": sc.get("workloadsScanned"),
            "failing": ks.get("controlsTotal"),
            "failures": ks.get("failuresTotal") or {},
            "cveCritical": sum((v or {}).get("critical", 0) for v in (ks.get("cve") or {}).values()),
            "cveHigh": sum((v or {}).get("high", 0) for v in (ks.get("cve") or {}).values()),
            "policyFail": (ky.get("totals") or {}).get("fail", 0),
            "framework": ({"id": fwk["id"], "workload": fwk["workload"],
                           "infrastructure": fwk["infrastructure"]}
                          if (fwk := ks.get("framework")) else None),
            "controlScores": sc.get("controlScores") or {},
        }
    return row


def _ts(iso):
    try:
        return datetime.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except Exception:
        return None


def trim(points):
    """Keep RETAIN_DAYS of posture, compacting older readings to one a day.

    Four readings a day off one nightly scan are four copies of the same
    posture, so beyond RAW_DAYS only the last reading of each UTC day is
    kept. Inside RAW_DAYS every reading survives, because that is where a fix
    applied this morning and the collector re-run this afternoon need a before
    and an after.

    Detail is shed before history is: per-control scores stay on the newest
    DETAIL_DAYS, everything older keeps its headline figures and its place on
    the trend line.
    """
    points = sorted((p for p in points if p.get("at") and _ts(p["at"])), key=lambda p: str(p["at"]))
    if not points:
        return []
    newest = _ts(points[-1]["at"])

    raw_cut = newest - datetime.timedelta(days=RAW_DAYS)
    daily, raw = {}, []
    for pt in points:
        if _ts(pt["at"]) > raw_cut:
            raw.append(pt)
        else:
            daily[str(pt["at"])[:10]] = pt      # sorted, so the last of the day wins
    points = sorted(daily.values(), key=lambda p: str(p["at"])) + raw

    keep_cut = newest - datetime.timedelta(days=RETAIN_DAYS)
    points = [pt for pt in points if _ts(pt["at"]) > keep_cut]

    detail_cut = newest - datetime.timedelta(days=DETAIL_DAYS)
    for pt in points:
        if _ts(pt["at"]) <= detail_cut:
            for cl in pt.get("clusters", {}).values():
                cl.pop("controlScores", None)

    while len(points) > 2 and len(json.dumps(points)) > MAX_HISTORY_BYTES:
        points.pop(0)
    return points


def for_page(points):
    """The series as the page needs it, which is not the series as stored.

    Attribution only ever compares the newest point with the one before it, so
    only those two need per-control scores. Everything older contributes a
    dot on a sparkline. Shipping the full detail would push the rendered page
    towards the same 1 MiB ConfigMap ceiling the history is already guarding.
    """
    out = []
    for i, pt in enumerate(points):
        keep = i >= len(points) - 2
        out.append({"at": pt.get("at"), "clusters": {
            k: ({**v} if keep else {kk: vv for kk, vv in v.items() if kk != "controlScores"})
            for k, v in (pt.get("clusters") or {}).items()}})
    return out


def history(c, namespace, name, data):
    """Append this run to the history ConfigMap and return the whole series.

    A failure here must not cost the page: a dashboard that renders today's
    numbers without a trend is useful, one that renders nothing is not. So
    every step degrades to an empty series and logs why.
    """
    path = f"/api/v1/namespaces/{namespace}/configmaps/{name}"
    existing, points = None, []
    try:
        existing = c.get(path)
        points = json.loads((existing.get("data") or {}).get("history.json") or "[]")
        if not isinstance(points, list):
            raise ValueError("history.json is not a list")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            log(f"  WARNING: cannot read history ({e}); starting a new series")
    except Exception as e:
        log(f"  WARNING: history unreadable ({e}); starting a new series")
        points = []

    points = trim(points + [point(data)])
    body = {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": namespace},
            "data": {"history.json": json.dumps(points, separators=(",", ":"))}}
    try:
        if existing is None:
            c.send("POST", f"/api/v1/namespaces/{namespace}/configmaps", body)
            log(f"  created history ConfigMap {namespace}/{name}")
        else:
            c.send("PUT", path, body)
        log(f"  history: {len(points)} points, {len(body['data']['history.json'])} bytes")
    except Exception as e:
        log(f"  WARNING: could not write history ({e}); this run is not recorded")
    return points


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
    log(f"fetching framework {FRAMEWORK} ...")
    fw = framework()
    log(f"  {len(fw[1])} controls in {len(fw[0]['order']) if fw[0] else 0} sections")

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
            data["clusters"][name] = {"reachable": True, "kubescape": kubescape(c, lib, fw), "kyverno": kyverno(c)}
            log(f"  done in {time.time() - t:.1f}s")
        except Exception as e:
            log(f"  FAILED: {e}")
            data["clusters"][name] = {"reachable": False, "error": str(e)}

    out = os.environ.get("OUTPUT_PATH")
    if out:
        # Local render: no cluster to keep history in, so the trend comes from
        # a file if one is given. Never silently from nowhere.
        hp = os.environ.get("HISTORY_PATH")
        points = []
        if hp:
            try:
                with open(hp) as f:
                    points = json.load(f)
            except Exception as e:
                log(f"  WARNING: no readable history at {hp} ({e})")
            points = trim(points + [point(data)])
            with open(hp, "w") as f:
                json.dump(points, f, separators=(",", ":"))
    else:
        points = history(first, os.environ["POD_NAMESPACE"],
                         os.environ["HISTORY_CONFIGMAP"], data)
    data["history"] = for_page(points)

    # The current scan's per-control scores are already on the newest history
    # point; a second copy only grows the page.
    for c2 in data["clusters"].values():
        (c2.get("kubescape") or {}).get("score", {}).pop("controlScores", None)

    with open(os.environ.get("TEMPLATE_PATH", "/opt/dashboard/template.html")) as f:
        html = f.read().replace("/*__DATA__*/null", json.dumps(data))

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
