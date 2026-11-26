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
import gc, json, os, socket, ssl, sys, time, datetime, collections, urllib.parse, urllib.request, urllib.error
import concurrent.futures as cf

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
API = "/apis/spdx.softwarecomposition.kubescape.io/v1beta1"
KYV = "/apis/wgpolicyk8s.io/v1alpha2"

# Every Kyverno policy kind that can put a result in a PolicyReport. Mutating,
# generating and deleting policies never appear in one, so they are not listed
# here: showing them beside findings would imply they are being measured.
#
# These are ordinary CRs in etcd, so unlike the Kubescape summaries a LIST
# returns them fully populated.
POLICY_KINDS = [
    ("/apis/policies.kyverno.io/v1", "validatingpolicies", "ValidatingPolicy"),
    ("/apis/policies.kyverno.io/v1", "namespacedvalidatingpolicies", "NamespacedValidatingPolicy"),
    ("/apis/policies.kyverno.io/v1", "imagevalidatingpolicies", "ImageValidatingPolicy"),
    ("/apis/policies.kyverno.io/v1", "namespacedimagevalidatingpolicies", "NamespacedImageValidatingPolicy"),
    ("/apis/kyverno.io/v1", "clusterpolicies", "ClusterPolicy"),
    ("/apis/kyverno.io/v1", "policies", "Policy"),
]
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
# Affected workloads listed per control when a reader drills into it. Every
# number on the page has to be answerable with "which ones?", and a count
# without the list behind it is the thing people stop believing.
DRILL_WORKLOADS = int(os.environ.get("DRILL_WORKLOADS", "25"))
# How many of the worst images to open up for their actual CVE list.
#
# Deliberately a handful. The per-image manifests hold every match Grype
# found -- roughly 800 for a mid-sized image -- and fetching all 343 of them
# on unified-dev moves about 700 MB, against a collector limited to 512Mi.
# The summaries give every image its counts cheaply; only the worst few need
# their CVEs enumerated, because those are the only ones anyone is going to
# work through.
# How long to wait for Kubescape's aggregated API to start serving.
#
# These clusters are stopped overnight and started on a schedule. On start-up
# the collector can easily win the race against the `storage` pod, and a
# Kubescape read that fails then is indistinguishable, in the data, from a
# cluster with nothing wrong with it. Waiting is the correct answer: a
# dashboard a few minutes late is fine, a dashboard that records an empty
# cluster is not.
API_WAIT_SECONDS = int(os.environ.get("API_WAIT_SECONDS", "600"))
API_WAIT_INTERVAL = int(os.environ.get("API_WAIT_INTERVAL", "20"))

# How long a cluster may be unreachable before it is reported unreachable,
# rather than waited on. Separate from API_WAIT_SECONDS because the two answer
# different questions: that one is "is Kubescape ready yet", this one is "is
# the cluster there at all". A name that does not resolve skips even this.
CLUSTER_GRACE_SECONDS = int(os.environ.get("CLUSTER_GRACE_SECONDS", "60"))

CVE_IMAGES = int(os.environ.get("CVE_IMAGES", "10"))
# How many per-image manifests may be resident at once. These are by far the
# largest objects here; see the batching comment in cve_detail().
CVE_FETCH_CONCURRENCY = int(os.environ.get("CVE_FETCH_CONCURRENCY", "2"))
CVE_PER_IMAGE = int(os.environ.get("CVE_PER_IMAGE", "30"))
# Tables are capped at what a person would ever scroll. The totals above them
# are computed over EVERYTHING, so a cap shortens the list without making any
# headline figure wrong -- and each table says how many it is showing.
CVE_TOP_IMAGES = int(os.environ.get("CVE_TOP_IMAGES", "100"))
CVE_TOP_WORKLOADS = int(os.environ.get("CVE_TOP_WORKLOADS", "150"))

# The page is served from a ConfigMap, which Kubernetes caps at 1 MiB. The
# drill-down detail is what grows, so it is what gets shed -- in order of
# least use first -- rather than letting the publish fail.
MAX_PAGE_BYTES = int(os.environ.get("MAX_PAGE_BYTES", str(800 * 1024)))

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


def cluster_down(c):
    """Why this cluster cannot be reached at all, or None if it can.

    This exists to tell two situations apart that the aggregated API cannot:

      the cluster is RUNNING and Kubescape is not ready yet  -> worth waiting
      the cluster is STOPPED                                 -> waiting is futile

    A stopped AKS cluster's API FQDN stops resolving entirely, so an NXDOMAIN
    is a definite answer and there is nothing to wait for. A name that resolves
    but refuses the connection is ambiguous -- a restarting API server looks
    the same for a few seconds -- so that one is reported and left to the
    caller's grace window rather than treated as fatal.
    """
    host = urllib.parse.urlsplit(c.base).hostname or ""
    port = urllib.parse.urlsplit(c.base).port or 443
    try:
        socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return (f"{host} does not resolve, so the cluster's API server is not there. "
                f"A stopped AKS cluster looks exactly like this; so does a deleted one.")
    except Exception:
        return None
    if c.try_get("/version", timeout=10) is None:
        return f"{host} resolves but its API server did not answer."
    return None


def kubescape(c, lib, fw):
    out = {"available": True, "posture": {}, "cve": {}, "unscanned": [], "controls": []}

    # Wait for the aggregated API rather than recording an empty cluster. See
    # API_WAIT_SECONDS. Both index reads are retried together because the
    # whole API server goes away at once -- it is one pod -- so there is no
    # case where one succeeds and the other needs waiting for.
    deadline = time.time() + API_WAIT_SECONDS
    down_deadline = time.time() + CLUSTER_GRACE_SECONDS
    waited = False
    while True:
        idx = c.try_get(f"{API}/configurationscansummaries")
        vdx = c.try_get(f"{API}/vulnerabilitysummaries")
        if idx is not None or vdx is not None:
            break

        # Do not spend the full API wait on a cluster that is not running. The
        # wait is there for the storage pod starting up; against a stopped
        # cluster it is ten minutes of sleeping before reaching the conclusion
        # DNS already gave us, and with three clusters that is most of the run.
        down = cluster_down(c)
        if down and "does not resolve" in down:
            out["available"] = False
            out["reason"] = f"Cluster unreachable: {down}"
            log(f"  SKIPPED: {down}")
            return out
        if down and time.time() >= down_deadline:
            out["available"] = False
            out["reason"] = f"Cluster unreachable: {down}"
            log(f"  SKIPPED after {CLUSTER_GRACE_SECONDS}s: {down}")
            return out
        if not down:
            # The cluster is up, so this really is Kubescape starting. Reset
            # the grace window; only the aggregated API deadline applies now.
            down_deadline = time.time() + CLUSTER_GRACE_SECONDS

        if time.time() >= deadline:
            out["available"] = False
            out["reason"] = (f"Kubescape's aggregated API did not respond within "
                             f"{API_WAIT_SECONDS}s. The storage pod serves it; check that it is "
                             f"running and that the APIService is Available.")
            log(f"  ERROR: {out['reason']}")
            return out
        if not waited:
            log(f"  Kubescape's aggregated API is not responding; waiting up to "
                f"{API_WAIT_SECONDS}s for it (the storage pod may still be starting)")
            waited = True
        time.sleep(API_WAIT_INTERVAL)
    if waited:
        log("  aggregated API is responding; continuing")

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
    affected = collections.defaultdict(list)
    subjects = collections.defaultdict(set)
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
                if len(affected[cid]) < DRILL_WORKLOADS:
                    # Some controls are evaluated against the Namespace itself,
                    # which has no workload-namespace label of its own.
                    # The summary object's own name is only worth carrying
                    # when it says more than the subject does -- for a grant
                    # finding it names the role and binding, which is the part
                    # you revoke; for a plain workload it is just the subject
                    # again, and 2500 copies of that is most of the page.
                    obj = o["metadata"].get("name", "")
                    row = {"ns": "" if ns == "?" and kind == "Namespace" else ns,
                           "kind": kind, "name": name}
                    if obj and obj != f"{kind.lower()}-{name}":
                        row["obj"] = obj[:160]
                    affected[cid].append(row)
                # RBAC controls produce one finding per GRANT, not per subject:
                # a ServiceAccount with five role bindings that reach secrets
                # is five findings naming the same ServiceAccount. Both numbers
                # are real and the drill-down has to be able to say which is
                # which, or the list reads as duplicated rows.
                subjects[cid].add((ns, kind, name))

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

    # After out["controls"], not before: the detail set is the union of the
    # framework's controls and the worklist's, and the worklist does not exist
    # until the loop above has run.
    out["detail"] = control_detail(out, tally, affected, nss, sevs, lib, fw, subjects)
    out["cveDetail"] = cve_detail(c)

    for it in (c.try_get(f"{API}/sbomsyfts") or {"items": []})["items"]:
        a = (it.get("metadata", {}) or {}).get("annotations", {}) or {}
        if a.get("kubescape.io/status") == "too-large":
            out["unscanned"].append({
                "image": a.get("kubescape.io/image-tag", it["metadata"]["name"]),
                "reason": a.get("kubescape.io/status-reason", "unknown"),
                "size": int(a.get("kubescape.io/resource-size") or 0),
            })
    return out


def control_detail(out, tally, affected, nss, sevs, lib, fw, subjects):
    """Everything needed to answer "why is this number what it is?" offline.

    The page must be able to justify every figure it prints without going back
    to the cluster: which controls make up a score, how each one was scored,
    which workloads failed it, and what the fix is. So each control carries
    its own arithmetic -- passed, failed and skipped counts -- rather than
    just the percentage those produce.

    Controls that could NOT be assessed are listed too, by name. "0 of 60
    assessed" is meaningless to a reader until they can see the 60 and read
    what they would have checked.
    """
    meta, sections, fwnames = fw
    scores = out["score"]["controlScores"]
    wanted = set(cid for cid in sections if sections[cid][0] in WORKLOAD_SECTIONS) if meta else set()
    wanted |= {c["id"] for c in out["controls"]}

    detail = {}
    for cid in wanted:
        if cid not in scores:
            continue
        t = tally[cid]
        lm = lib.get(cid, {})
        allr = t["passed"] + t["failed"] + t["other"]
        detail[cid] = {
            "id": cid,
            "name": fwnames.get(cid) or lm.get("name") or cid,
            "section": sections.get(cid, (None, None))[0],
            "score": round(scores[cid], 1),
            "passed": t["passed"], "failed": t["failed"], "skipped": t["other"], "total": allr,
            "severity": sevs.get(cid) or ((lm.get("baseScore") or 0) >= 7 and "high" or "medium"),
            "description": (lm.get("description") or "")[:600],
            "remediation": (lm.get("remediation") or "")[:800],
            "namespaces": sorted(nss.get(cid, []))[:40],
            "subjects": len(subjects.get(cid, ())),
            "workloads": affected.get(cid, []),
        }

    # The controls we could not assess, so "0 of 60" can be opened and read.
    unassessed = collections.defaultdict(list)
    for cid, (sid, _) in (sections or {}).items():
        if cid not in scores:
            unassessed[sid].append({"id": cid, "name": fwnames.get(cid) or cid})
    for v in unassessed.values():
        v.sort(key=lambda r: r["name"])
    return {"controls": detail, "unassessed": dict(unassessed)}


def cve_detail(c):
    """CVEs by image and by workload, plus the real CVE list for the worst few.

    Three levels, because three different questions get asked:

      which images are worst        -> every image, counts by severity
      which service is affected     -> every workload+container, with its image
      what exactly do I fix         -> the actual CVE ids, for the top images only

    The last one is sampled on purpose. A per-image manifest holds every match
    Grype produced -- around 800 for a mid-sized image -- and pulling all of
    them moves roughly 700 MB on a cluster this size, against a collector
    limited to 512Mi. The counts come from the cheap summaries; only the
    images anyone would actually start with get their CVEs enumerated.

    Fixable CVEs are separated from the rest. A critical with no upstream fix
    is not work anyone can do today, and mixing the two produces a number that
    cannot be driven down.
    """
    # TWO totals, because there are two honest answers and they differ a lot.
    #
    #   totals        summed over every running container. Twelve containers
    #                 on one image count that image's CVEs twelve times. This
    #                 is exposure: how many vulnerable things are running.
    #   totalsUnique  summed over distinct images. This is WORK: one image to
    #                 rebuild, however many places run it.
    #
    # The page leads with the unique figure, because it is the one that can be
    # driven to zero by doing something, and names the other beside it. One
    # number presented alone would be challenged the first time someone
    # noticed the same image counted repeatedly -- and they would be right.
    out = {"byImage": [], "byWorkload": [], "topCves": {}, "sampled": 0,
           "totals": {k: 0 for k in SEV}, "totalsUnique": {k: 0 for k in SEV},
           "images": 0, "workloads": 0}
    # Not try_get: a failure here has to be LOUD. This pass was shipped once
    # with its two resources missing from the collector's ClusterRole, and a
    # swallowed 403 is indistinguishable in the logs from a cluster that
    # simply has no images. The page degrades honestly either way -- it says
    # the data was not collected rather than showing zero -- but nobody reads
    # the page to find out why the collector is unhappy.
    try:
        idx = c.get(f"{API}/vulnerabilitymanifestsummaries")
    except Exception as e:
        code = getattr(e, "code", None)
        if code == 403:
            log(f"  ERROR: not allowed to read vulnerabilitymanifestsummaries ({e}). The "
                f"collector's ClusterRole needs `vulnerabilitymanifestsummaries` and "
                f"`vulnerabilitymanifests` in the spdx.softwarecomposition.kubescape.io group. "
                f"Image vulnerabilities will be reported as not collected.")
        else:
            log(f"  ERROR: cannot read vulnerabilitymanifestsummaries ({e}); image "
                f"vulnerabilities will be reported as not collected.")
        out["available"] = False
        return out
    out["available"] = True

    def one(it):
        m = it["metadata"]
        return m, c.try_get(f"{API}/namespaces/{m['namespace']}/vulnerabilitymanifestsummaries/{m['name']}")

    images, refs = {}, {}
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        for m, d in ex.map(one, idx.get("items", [])):
            if not d:
                continue
            L = m.get("labels", {}) or {}
            # The labels carry a SANITISED image name (dots and slashes
            # flattened to dashes) because label values are constrained. The
            # real reference is on the annotation, and the real reference is
            # what someone has to go and change.
            A_ = (d["metadata"].get("annotations") or {}) or (m.get("annotations") or {})
            sev = ((d.get("spec") or {}).get("severities") or {})
            counts = {k: (sev.get(k) or {}).get("all", 0) or 0 for k in SEV}
            img = A_.get("kubescape.io/image-tag") or L.get("kubescape.io/image-name", "?")
            ns = L.get("kubescape.io/workload-namespace", "?")
            wl = {
                "ns": ns,
                "kind": (L.get("kubescape.io/workload-kind") or "?"),
                "name": L.get("kubescape.io/workload-name", "?"),
                "container": L.get("kubescape.io/workload-container-name", "?"),
                "image": img,
                "digest": (A_.get("kubescape.io/image-id", "").split("@") + [""])[1][:19],
                **counts,
            }
            out["byWorkload"].append(wl)
            for k in SEV:
                out["totals"][k] += counts[k]

            e = images.setdefault(img, {"image": img, "workloads": 0, "namespaces": set(),
                                        **{k: 0 for k in SEV}})
            # Counts are per image, so take them once rather than summing over
            # every workload that happens to run the same image.
            for k in SEV:
                e[k] = max(e[k], counts[k])
            e["workloads"] += 1
            e["namespaces"].add(ns)
            ref = ((d.get("spec") or {}).get("vulnerabilitiesRef") or {}).get("all") or {}
            if ref.get("name"):
                refs[img] = (ref.get("namespace") or "kubescape", ref["name"])

    for e in images.values():
        e["namespaceList"] = sorted(e["namespaces"])[:20]
        e["namespaces"] = len(e["namespaces"])
    allimg = sorted(images.values(), key=lambda r: (-r["critical"], -r["high"], r["image"]))
    out["byWorkload"].sort(key=lambda r: (-r["critical"], -r["high"], r["ns"], r["name"]))
    for e in allimg:
        for k in SEV:
            out["totalsUnique"][k] += e[k]
    out["images"] = len(images)
    out["workloads"] = len(out["byWorkload"])
    out["byImage"] = allimg[:CVE_TOP_IMAGES]
    out["byWorkload"] = out["byWorkload"][:CVE_TOP_WORKLOADS]

    worst = [e["image"] for e in allimg[:CVE_IMAGES] if e["image"] in refs]
    def manifest(img):
        ns, nm = refs[img]
        return img, c.try_get(f"{API}/namespaces/{ns}/vulnerabilitymanifests/{nm}", timeout=120)

    rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}

    # Fetched in small batches, NOT with one ex.map over the whole list.
    #
    # ThreadPoolExecutor.map submits every task immediately and holds each
    # result until the consuming loop reaches it. These are the largest
    # objects this collector ever touches -- a manifest is a couple of MB of
    # JSON that parses into tens of MB of Python -- so mapping over ten images
    # keeps all ten resident at once. That OOM-killed the collector the first
    # time it ran across three clusters. Batching bounds what is live to
    # CVE_FETCH_CONCURRENCY manifests, and each batch is dropped before the
    # next is submitted.
    for start in range(0, len(worst), CVE_FETCH_CONCURRENCY):
        batch = worst[start:start + CVE_FETCH_CONCURRENCY]
        with cf.ThreadPoolExecutor(max_workers=len(batch)) as ex:
            fetched = list(ex.map(manifest, batch))
        for img, d in fetched:
            if not d:
                continue
            out["sampled"] += 1
            seen, rows = set(), []
            for mt in ((d.get("spec") or {}).get("payload") or {}).get("matches") or []:
                v = mt.get("vulnerability") or {}
                sv = (v.get("severity") or "").lower()
                if sv not in rank:
                    continue
                art = mt.get("artifact") or {}
                fix = v.get("fix") or {}
                key = (v.get("id"), art.get("name"), art.get("version"))
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "id": v.get("id", "?"), "severity": sv,
                    "pkg": art.get("name", "?"), "version": art.get("version", "?"),
                    "fixedIn": ", ".join((fix.get("versions") or [])[:2]),
                    "fixable": fix.get("state") == "fixed",
                })
            rows.sort(key=lambda r: (not r["fixable"], rank[r["severity"]], r["id"]))
            out["topCves"][img] = {
                "fixable": sum(1 for r in rows if r["fixable"]),
                "total": len(rows),
                "rows": rows[:CVE_PER_IMAGE],
            }
        del fetched
        gc.collect()
    return out


def policy_ready(status):
    """(ready, reason) for a Kyverno policy, or (None, "") when it says nothing.

    Two shapes. The CEL policy types carry it under `conditionStatus`; the
    legacy kyverno.io types put `ready` at the top of status. A policy that
    reports neither is left as None rather than guessed at -- an unknown state
    must not be rendered as a failed one any more than as a healthy one.
    """
    st = status or {}
    cs = st.get("conditionStatus")
    block = cs if isinstance(cs, dict) else st
    conds = block.get("conditions") or []
    bad = [c for c in conds if str(c.get("status")) != "True"]
    reason = "; ".join(f"{c.get('type')}: {c.get('message') or c.get('reason') or ''}".strip()
                       for c in bad)[:300]
    if "ready" in block:
        return bool(block["ready"]), reason
    if conds:
        return not bad, reason
    return None, ""


def policy_inventory(c):
    """Every policy installed on the cluster, whether or not it has findings.

    The reports alone cannot answer "is this policy there?". A policy that
    matches nothing, or that Kyverno refused to compile, produces no result at
    all -- and a dashboard built only from results would show it as absent, or
    worse, silently as clean. So the policies are enumerated from the API and
    the findings merged onto them, not the other way round.
    """
    out, kinds = {}, []
    for base, plural, kind in POLICY_KINDS:
        d = c.try_get(f"{base}/{plural}")
        if d is None:
            continue              # kind not installed on this cluster
        kinds.append(kind)
        for it in d.get("items", []):
            m, spec = it.get("metadata") or {}, it.get("spec") or {}
            ann = m.get("annotations") or {}
            ready, reason = policy_ready(it.get("status"))
            actions = spec.get("validationActions")
            if actions is None and spec.get("validationFailureAction"):
                actions = [spec["validationFailureAction"]]
            bg = ((spec.get("evaluation") or {}).get("background") or {}).get("enabled")
            if bg is None:
                bg = spec.get("background")
            # A namespaced policy of the same name in two namespaces collapses
            # to one row, because a report result names only the policy. The
            # namespaces are kept so the row can still say where it lives.
            row = out.setdefault(m.get("name", "?"), {
                "kind": kind, "ready": ready, "readyReason": reason,
                "title": ann.get("policies.kyverno.io/title", ""),
                "category": ann.get("policies.kyverno.io/category", ""),
                "severity": (ann.get("policies.kyverno.io/severity") or "").lower(),
                "enforced": bool(actions) and any(str(a).lower() in ("deny", "enforce") for a in actions),
                # False means the policy only ever sees live admission requests:
                # it reports what is created or changed from now on and says
                # nothing about what is already running. On the page that has to
                # be stated, or its zero reads as "nothing is wrong".
                "scansExisting": True if bg is None else bool(bg),
                "namespaces": [],
            })
            if m.get("namespace"):
                row["namespaces"].append(m["namespace"])
    return out, kinds


def kyverno(c):
    """Every installed policy, with its findings merged on.

    Built inventory-first on purpose. A dashboard assembled only from report
    results cannot distinguish "this policy found nothing" from "this policy
    was never installed" from "Kyverno refused to compile it" -- and all three
    render as the same reassuring absence. Here every installed policy gets a
    row whatever it has produced, and the row carries the state that explains
    its numbers.
    """
    out = {"available": True, "policies": [], "by_namespace": {},
           "totals": {"pass": 0, "fail": 0}}

    inventory, kinds = policy_inventory(c)
    out["policyKinds"] = kinds
    out["inventoryAvailable"] = bool(kinds)

    d = c.try_get(f"{KYV}/policyreports")
    if d is None:
        # No reports at all. If the policies themselves were readable, say so
        # with the inventory rather than claiming Kyverno is absent.
        out["available"] = False
        out["reportsAvailable"] = False
        if inventory:
            out["policies"] = [blank_policy(n, p) for n, p in sorted(inventory.items())]
            # The policies are known even though their findings are not. Keep
            # the tallies populated so the page says "installed, no results"
            # rather than showing Kyverno as absent entirely.
            out["totals"]["policies"] = len(out["policies"])
            for k, st in (("failing", "failing"), ("satisfied", "clean"), ("notReady", "not-ready"),
                          ("noData", "no-data"), ("errored", "error")):
                out["totals"][k] = sum(1 for p in out["policies"] if p["state"] == st)
            out["totals"]["enforced"] = sum(1 for p in out["policies"] if p.get("enforced"))
        return out
    out["reportsAvailable"] = True

    agg = {}
    passes, errors, skips = collections.Counter(), collections.Counter(), collections.Counter()
    err_msg = {}
    for it in d.get("items", []):
        ns = (it.get("metadata") or {}).get("namespace", "-")
        s_ = it.get("summary") or {}
        out["totals"]["pass"] += s_.get("pass", 0) or 0
        out["totals"]["fail"] += s_.get("fail", 0) or 0
        scope = it.get("scope") or {}
        for res in it.get("results") or []:
            pol = res.get("policy", "?")
            kind = res.get("result")
            if kind == "pass":
                passes[pol] += 1
                continue
            if kind == "error":
                # The policy compiled but blew up against a real object. It is
                # producing nothing for those resources, which without this
                # would look exactly like finding nothing wrong.
                errors[pol] += 1
                err_msg.setdefault(pol, (res.get("message") or "")[:300])
                continue
            if kind == "skip":
                skips[pol] += 1
                continue
            if kind != "fail":
                continue
            a = agg.setdefault(pol, {"id": pol, "severity": res.get("severity", "?"),
                                     "category": res.get("category", ""), "message": res.get("message", ""),
                                     "count": 0, "examples": [], "namespaces": set(),
                                     "workloads": []})
            a["count"] += 1
            a["namespaces"].add(ns)
            if len(a["examples"]) < EXAMPLES:
                a["examples"].append(f"{ns}/{scope.get('kind','?')}/{scope.get('name','?')}")
            if len(a["workloads"]) < DRILL_WORKLOADS:
                a["workloads"].append({"ns": ns, "kind": scope.get("kind", "?"), "name": scope.get("name", "?")})
            out["by_namespace"].setdefault(ns, {"fail": 0})["fail"] += 1

    # Every policy that produced any result, even if only passes or errors.
    for pol in list(passes) + list(errors) + list(skips):
        agg.setdefault(pol, {"id": pol, "severity": "", "category": "", "message": "",
                             "count": 0, "examples": [], "namespaces": set(), "workloads": []})
    # ... and every policy that is installed and produced nothing at all.
    for pol in inventory:
        agg.setdefault(pol, {"id": pol, "severity": "", "category": "", "message": "",
                             "count": 0, "examples": [], "namespaces": set(), "workloads": []})

    for pol, a in agg.items():
        a["namespaceCount"] = len(a["namespaces"])
        a["namespaces"] = sorted(a["namespaces"])[:40]
        a["passed"] = passes.get(pol, 0)
        a["errors"] = errors.get(pol, 0)
        a["skipped"] = skips.get(pol, 0)
        a["evaluated"] = a["passed"] + a["count"]
        if a["errors"]:
            a["errorMessage"] = err_msg.get(pol, "")
        apply_inventory(a, inventory.get(pol), bool(kinds))

    # Failing first, then the states that need attention before the clean ones:
    # a policy that is installed and reporting nothing is more interesting than
    # one that is passing, and must not be buried under a hundred clean rows.
    rank = {"failing": 0, "error": 1, "not-ready": 2, "no-data": 3, "clean": 4, "unknown": 5}
    out["policies"] = sorted(agg.values(),
                             key=lambda x: (rank.get(x["state"], 9), -x["count"], x["id"]))
    out["totals"]["policies"] = len(agg)
    out["totals"]["satisfied"] = sum(1 for a in agg.values() if a["state"] == "clean")
    out["totals"]["failing"] = sum(1 for a in agg.values() if a["state"] == "failing")
    out["totals"]["notReady"] = sum(1 for a in agg.values() if a["state"] == "not-ready")
    out["totals"]["noData"] = sum(1 for a in agg.values() if a["state"] == "no-data")
    out["totals"]["errored"] = sum(1 for a in agg.values() if a["state"] == "error")
    out["totals"]["enforced"] = sum(1 for a in agg.values() if a.get("enforced"))
    return out


def blank_policy(name, inv):
    """An installed policy with no reports to merge onto it."""
    a = {"id": name, "severity": inv.get("severity", ""), "category": inv.get("category", ""),
         "message": "", "count": 0, "examples": [], "namespaces": [], "namespaceCount": 0,
         "workloads": [], "passed": 0, "errors": 0, "skipped": 0, "evaluated": 0}
    apply_inventory(a, inv, True)
    return a


def apply_inventory(a, inv, inventory_read):
    """Attach what the policy object itself says, and name the row's state.

    The state is the whole point of this file for policies: it is what stops a
    row of zeros being read as a clean result. Only `clean` means that.
    """
    if inv:
        a["installed"] = True
        a["kind"] = inv["kind"]
        a["ready"] = inv["ready"]
        a["enforced"] = inv["enforced"]
        a["scansExisting"] = inv["scansExisting"]
        if inv.get("readyReason"):
            a["readyReason"] = inv["readyReason"]
        if inv.get("title") and not a.get("name"):
            a["name"] = inv["title"]
        if inv.get("category") and not a.get("category"):
            a["category"] = inv["category"]
        if inv.get("severity") and a.get("severity") in ("", "?", None):
            a["severity"] = inv["severity"]
        if inv.get("namespaces"):
            a["definedIn"] = sorted(set(inv["namespaces"]))[:10]
    else:
        # Reports name a policy the API does not list. Either it was deleted
        # since the last scan, or -- more often -- the collector could not read
        # that policy kind at all.
        a["installed"] = False if inventory_read else None

    if a["errors"]:
        a["state"] = "error"
    elif a.get("installed") and a.get("ready") is False:
        a["state"] = "not-ready"
    elif a["evaluated"] == 0:
        a["state"] = "no-data"
    elif a["count"]:
        a["state"] = "failing"
    else:
        a["state"] = "clean"


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
        # VERIFY THE REMOTE API SERVER'S CERTIFICATE.
        #
        # This connection crosses the public internet carrying a bearer token
        # for a read-only-but-cluster-wide ServiceAccount. Skipping
        # verification would hand that token to anything that can answer for
        # the hostname. An AKS API server presents a certificate signed by the
        # cluster's OWN CA, not a public one, so the system trust store cannot
        # validate it -- the CA has to come with the token.
        #
        # A Secret of type kubernetes.io/service-account-token carries exactly
        # what is needed: `token`, `ca.crt` and `namespace`. Mount the whole
        # Secret and both files are there.
        ca = f"/remote/{name}/ca.crt"
        if not os.path.exists(ca):
            if not spec.get("insecureSkipTlsVerify"):
                log(f"  ERROR: no CA at {ca} for {name}. Mount the ServiceAccount token Secret "
                    f"(it contains ca.crt), or set insecureSkipTlsVerify on this entry to accept "
                    f"an unverified connection. Marking {name} unreachable rather than sending its "
                    f"token to an unverified endpoint.")
                out.append((name, None))
                continue
            log(f"  WARNING: {name} is configured with insecureSkipTlsVerify; its token is being "
                f"sent over an unverified TLS connection.")
            out.append((name, Client(server, rtok, insecure=True)))
            continue
        out.append((name, Client(server, rtok, cafile=ca)))
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


def render(tpl, data):
    return tpl.replace("/*__DATA__*/null", json.dumps(data, separators=(",", ":")))


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
            data["clusters"][name] = {"reachable": False,
                                      "error": "no usable credentials mounted (see collector logs)"}
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
        tpl = f.read()

    html = render(tpl, data)
    for shrink in (10, 0):
        if len(html.encode()) <= MAX_PAGE_BYTES:
            break
        log(f"  page is {len(html.encode())} bytes; trimming drill-down lists to {shrink}")
        for cl in data["clusters"].values():
            for d in (((cl.get("kubescape") or {}).get("detail") or {}).get("controls") or {}).values():
                d["workloads"] = d["workloads"][:shrink]
            for pol in ((cl.get("kyverno") or {}).get("policies") or []):
                pol["workloads"] = pol["workloads"][:shrink]
        html = render(tpl, data)

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
