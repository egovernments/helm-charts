#!/usr/bin/env python3
# Gateway-sync reconciler. Keeps Kong routes/services/plugins in sync with the
# declarative whitelist in config.json. Idempotent full pass; only ever touches
# Kong entities carrying the managed tag. Pure stdlib (no pip install needed).
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
SERVICES = CFG.get("services", [])
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


def find_service(name):
    # Look the Service up in the configured namespaces so host/port come from the
    # real object (fixes the hostname/plural/hyphen class of bugs).
    nss = NAMESPACES or [None]
    for ns in nss:
        path = ("/api/v1/namespaces/%s/services/%s" % (ns, name)) if ns else None
        if path is None:
            continue
        try:
            svc = k8s_get(path)
            port = DEF_PORT
            ports = svc.get("spec", {}).get("ports") or []
            if ports:
                port = int(ports[0].get("port", DEF_PORT))
            return ns, port
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            raise
    return None, None


def kong(method, path, body=None):
    url = ADMIN + path
    try:
        return _req(url, method=method, body=body)
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


def desired():
    # Returns (services{name:body}, routes{name:body}, route_plugins{route:[names]})
    svcs, routes, rplugins = {}, {}, {}
    for s in SERVICES:
        name = s["name"]
        ns, port = find_service(name)
        if ns is None:
            sys.stderr.write("skip %s: no k8s Service in %s\n" % (name, NAMESPACES))
            continue
        host = "%s.%s.svc.cluster.local" % (name, ns)
        svcs[name] = {
            "name": name, "host": host, "port": port, "protocol": "http",
            "path": None, "retries": 5, "connect_timeout": 60000, "tags": [TAG],
        }
        methods = s.get("methods")
        rp = s.get("routePath")
        if rp:
            rn = "%s-route" % name
            routes[rn] = {
                "name": rn, "service": {"name": name}, "paths": [rp], "hosts": [DOMAIN],
                "methods": methods, "strip_path": False, "path_handling": "v0",
                "protocols": ["http", "https"], "tags": [TAG],
            }
            rplugins[rn] = list(PROTECTED_PLUGINS)
        for p in s.get("openPaths", []) or []:
            rn = "%s-%s-public-route" % (name, slug(p))
            routes[rn] = {
                "name": rn, "service": {"name": name}, "paths": [p], "hosts": [DOMAIN],
                "methods": s.get("openMethods") or methods, "strip_path": False,
                "path_handling": "v0", "protocols": ["http", "https"], "tags": [TAG],
            }
            rplugins[rn] = []
        for p in s.get("mixedPaths", []) or []:
            rn = "%s-%s-mixed-route" % (name, slug(p))
            routes[rn] = {
                "name": rn, "service": {"name": name}, "paths": [p], "hosts": [DOMAIN],
                "methods": methods, "strip_path": False, "path_handling": "v0",
                "protocols": ["http", "https"], "tags": [TAG],
            }
            rplugins[rn] = list(MIXED_PLUGINS)
    return svcs, routes, rplugins


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
    svcs, routes, rplugins = desired()
    mixed_routes = {n for n in routes if n.endswith("-mixed-route")}

    for name, body in svcs.items():
        print("service upsert %s -> %s:%s" % (name, body["host"], body["port"]))
        if not DRY:
            kong("PUT", "/services/%s" % name, body)
    for rn, body in routes.items():
        print("route upsert %s %s" % (rn, body["paths"]))
        if not DRY:
            kong("PUT", "/routes/%s" % rn, body)
        reconcile_plugins(rn, rplugins.get(rn, []), rn in mixed_routes)

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
