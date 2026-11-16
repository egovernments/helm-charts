#!/usr/bin/env python3
# Gateway-sync reconciler. Keeps Kong routes/services/plugins in sync with the
# per-Service gateway annotations (the single source of truth) and prunes them when
# a Service or annotation is removed. Idempotent full pass; only ever touches Kong
# entities carrying the managed tag. Pure stdlib (no pip install needed).
import json, os, re, ssl, sys, urllib.error, urllib.request

CFG = json.load(open(os.environ.get("CONFIG_PATH", "/etc/gateway-sync/config.json")))
TAG = CFG["tag"]
DOMAIN = CFG["domain"]
ADMIN = CFG["kongAdminUrl"].rstrip("/")
NAMESPACES = CFG.get("namespaces") or []
DEF_PORT = int(CFG.get("servicePort", 8080))
KEYCLOAK = CFG["keycloakBaseUrl"]
CLIENT_ID = CFG.get("clientId", "auth-server")
REDIS = CFG.get("redis", {})
PC = CFG.get("pluginConfigs", {})
MIXED_ANON = CFG.get("mixedAnonymousFlag", {})
PROTECTED_PLUGINS = CFG.get("protectedPlugins", [])
MIXED_PLUGINS = CFG.get("mixedPlugins", [])
PREFIX = CFG.get("annotationPrefix", "gateway")
REGEX_PRIORITY = int(CFG.get("regexPriority", 1000))
A_ROUTE = "%s/route-path" % PREFIX
A_ROUTEAUTH = "%s/route-auth" % PREFIX
A_OPEN = "%s/open-paths" % PREFIX
A_MIXED = "%s/mixed-paths" % PREFIX
A_PROTECTED = "%s/protected-paths" % PREFIX
A_METHODS = "%s/methods" % PREFIX
# Main-route plugin stack by route-auth tier.
AUTH_PLUGINS = {"open": [], "mixed": MIXED_PLUGINS, "protected": PROTECTED_PLUGINS}
DRY = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

K8S = "https://kubernetes.default.svc"
_SA = "/var/run/secrets/kubernetes.io/serviceaccount"
TOKEN = open(_SA + "/token").read().strip()
_K8S_CTX = ssl.create_default_context(cafile=_SA + "/ca.crt")


def _req(url, method="GET", body=None, headers=None, ctx=None):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Accept": "application/json"}
    if data is not None:
        h["Content-Type"] = "application/json"
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(req, context=ctx) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


def k8s_get(path):
    return _req(K8S + path, headers={"Authorization": "Bearer " + TOKEN}, ctx=_K8S_CTX)


def list_services():
    # Discover Services across the target namespaces (or cluster-wide if none set).
    items = []
    if NAMESPACES:
        for ns in NAMESPACES:
            items += k8s_get("/api/v1/namespaces/%s/services" % ns).get("items", [])
    else:
        items += k8s_get("/api/v1/services").get("items", [])
    return items


def kong(method, path, body=None):
    try:
        return _req(ADMIN + path, method=method, body=body)
    except urllib.error.HTTPError as e:
        if method == "GET" and e.code == 404:
            return None
        sys.stderr.write("KONG %s %s -> %s %s\n" % (method, path, e.code, e.read().decode()[:400]))
        raise


def kong_list_tagged(entity):
    out, nxt = [], "/%s?tags=%s&size=1000" % (entity, TAG)
    while nxt:
        r = kong("GET", nxt) or {}
        out += r.get("data", [])
        nxt = r.get("next")
    return out


def slug(path):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", path.lower())).strip("-") or "root"


def _paths(value):
    return [p.strip() for p in (value or "").split(",") if p.strip()]


def _entries(value):
    # Parse a comma-separated path annotation into (path, methods) pairs. Splits on
    # top-level commas only, so a "(GET,POST)" method group stays with its path.
    # Each token is "PATH" or "PATH (M1,M2)". A path starting with "~" is a Kong regex.
    out = []
    for tok in re.split(r",(?![^(]*\))", value or ""):
        tok = tok.strip()
        if not tok:
            continue
        m = re.match(r"^(.*?)\s*\(([^)]*)\)\s*$", tok)
        if m:
            path = m.group(1).strip()
            methods = [x.strip() for x in m.group(2).split(",") if x.strip()] or None
        else:
            path, methods = tok, None
        out.append((path, methods))
    return out


def _withslash(p):
    if p.startswith("~") or p.startswith("/"):
        return p
    return "/" + p


def plugin_config(name, mixed=False):
    cfg = dict(PC.get(name, {}))
    if name in ("dynamic-jwt", "keycloak-rbac"):
        cfg["keycloak_base_url"] = KEYCLOAK
    if name == "keycloak-rbac":
        cfg["client_id"] = CLIENT_ID
    if name == "token-revocation":
        if REDIS.get("host"):
            cfg["redis_host"] = REDIS["host"]
        if REDIS.get("port"):
            cfg["redis_port"] = int(REDIS["port"])
    if mixed and name == "dynamic-jwt":
        cfg.update(MIXED_ANON)
    return cfg


def _route(name, svc, path, methods):
    regex = path.startswith("~")
    r = {
        "name": name, "service": {"name": svc}, "paths": [_withslash(path)],
        "hosts": [DOMAIN], "methods": methods, "strip_path": False,
        "path_handling": "v0", "protocols": ["http", "https"], "tags": [TAG],
    }
    if regex:
        # A regex route needs a high priority to out-match the protected main route
        # (plain exact paths alone did not win in the traditional router).
        r["regex_priority"] = REGEX_PRIORITY
    return r


def desired():
    # Build desired Kong entities from Services annotated with <prefix>/route-path.
    # Upstream host/port come from the Service itself (fixes the hostname/plural/hyphen
    # class of bugs). The main route's tier is <prefix>/route-auth (open|mixed|protected,
    # default protected); open/mixed/protected-paths are per-path exceptions layered on.
    # Returns (services, routes, route_plugins, route_mixed).
    svcs, routes, rplugins, rmixed = {}, {}, {}, {}
    for s in list_services():
        meta = s.get("metadata", {})
        ann = meta.get("annotations") or {}
        if A_ROUTE not in ann:
            continue
        name = meta["name"]
        ns = meta["namespace"]
        port = DEF_PORT
        ports = s.get("spec", {}).get("ports") or []
        if ports:
            port = int(ports[0].get("port", DEF_PORT))
        host = "%s.%s.svc.cluster.local" % (name, ns)
        svcs[name] = {
            "name": name, "host": host, "port": port, "protocol": "http",
            "path": None, "retries": 5, "connect_timeout": 60000, "tags": [TAG],
        }
        methods = _paths(ann.get(A_METHODS)) or None
        rp = ann.get(A_ROUTE, "").strip()
        if rp:
            auth = (ann.get(A_ROUTEAUTH) or "protected").strip().lower()
            rn = "%s-route" % name
            routes[rn] = _route(rn, name, rp, methods)
            rplugins[rn] = list(AUTH_PLUGINS.get(auth, PROTECTED_PLUGINS))
            rmixed[rn] = auth == "mixed"
        for p, m in _entries(ann.get(A_OPEN)):
            rn = "%s-%s-public-route" % (name, slug(p))
            routes[rn] = _route(rn, name, p, m or methods)
            rplugins[rn] = []
            rmixed[rn] = False
        for p, m in _entries(ann.get(A_MIXED)):
            rn = "%s-%s-mixed-route" % (name, slug(p))
            routes[rn] = _route(rn, name, p, m or methods)
            rplugins[rn] = list(MIXED_PLUGINS)
            rmixed[rn] = True
        for p, m in _entries(ann.get(A_PROTECTED)):
            rn = "%s-%s-protected-route" % (name, slug(p))
            routes[rn] = _route(rn, name, p, m or methods)
            rplugins[rn] = list(PROTECTED_PLUGINS)
            rmixed[rn] = False
    return svcs, routes, rplugins, rmixed


def reconcile_plugins(route_name, want_names, mixed):
    existing = (kong("GET", "/routes/%s/plugins" % route_name) or {}).get("data", [])
    existing = [p for p in existing if TAG in (p.get("tags") or [])]
    by_name = {p["name"]: p for p in existing}
    for pn in want_names:
        body = {"name": pn, "config": plugin_config(pn, mixed), "tags": [TAG]}
        if pn in by_name:
            if not DRY:
                kong("PATCH", "/plugins/%s" % by_name[pn]["id"], body)
        else:
            if not DRY:
                kong("POST", "/routes/%s/plugins" % route_name, body)
    for pn, p in by_name.items():
        if pn not in want_names and not DRY:
            kong("DELETE", "/plugins/%s" % p["id"])


def main():
    svcs, routes, rplugins, rmixed = desired()

    for name, body in svcs.items():
        print("service upsert %s -> %s:%s" % (name, body["host"], body["port"]))
        if not DRY:
            kong("PUT", "/services/%s" % name, body)
    for rn, body in routes.items():
        print("route upsert %s %s" % (rn, body["paths"]))
        if not DRY:
            kong("PUT", "/routes/%s" % rn, body)
        reconcile_plugins(rn, rplugins.get(rn, []), rmixed.get(rn, False))

    # Prune tagged entities no longer desired (routes before services).
    for r in kong_list_tagged("routes"):
        if r.get("name") not in routes:
            print("route prune %s" % r.get("name"))
            if not DRY:
                kong("DELETE", "/routes/%s" % r["id"])
    for s in kong_list_tagged("services"):
        if s.get("name") not in svcs:
            print("service prune %s" % s.get("name"))
            if not DRY:
                kong("DELETE", "/services/%s" % s["id"])
    print("reconcile done: %d services, %d routes%s" % (len(svcs), len(routes), " (dry-run)" if DRY else ""))


if __name__ == "__main__":
    main()
