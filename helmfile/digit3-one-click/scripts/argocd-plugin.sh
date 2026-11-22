#!/usr/bin/env bash
# Argo CD config-management plugin "digit-helmfile" (Argo CD mode of the one-click install).
# Runs in the argocd-repo-server sidecar (helmfile image). The current directory is the
# Application path in the team's fork, one-click/ (input.yaml + the env files).
# The plugin command (helmfiles/argocd.yaml.gotmpl) first fetches this helmfile directory
# from helm-charts at helmfiles_repo/helmfiles_ref of input.yaml, then runs this script.
# Prints Kubernetes manifests on stdout only.
#
#   MODE=apps     root Application "digit": one Argo CD Application per installed helmfile
#                 release, ordered by the release label "wave" (argocd.argoproj.io/sync-wave).
#                 Release label "createonly" (Secret names, joined with +): Secrets the chart
#                 generates once via lookup, which helm template cannot do. Argo CD creates them,
#                 then ignores their data (ignoreDifferences + RespectIgnoreDifferences).
#                 ROOT_HELMFILE: digit-helmfile.yaml.gotmpl (platform, root app "digit", default)
#                 or products.yaml.gotmpl (team products, root app "digit-products"). A product
#                 release is rendered through products.yaml.gotmpl and labelled layer=products.
#   MODE=release  one release (RELEASE + NAMESPACE + HELMFILE = its layer helmfile):
#                 `helmfile template` of just that release
#
# Inputs: input.yaml (environment, domain, email) from the fork, and Application
# spec.source.plugin.env -> ARGOCD_ENV_*: MODE, DB_HOST, DB_NAME, RELEASE, NAMESPACE, HELMFILE.
# Argo CD also sets ARGOCD_APP_SOURCE_REPO_URL / _TARGET_REVISION / _PATH, ARGOCD_APP_NAMESPACE.
# Values are exactly the helmfile mode's: <env>.yaml filled by render-env.sh, and
# <env>-secrets.yaml (helm-secrets; sops decrypts with the cluster's managed identity).
set -euo pipefail
# Several renders run at once (reposerver.parallelism.limit). `helm repo add` locks
# repositories.yaml for at most 30s, so each render gets its own HELM_CONFIG_HOME; the
# downloaded chart-repo indexes (HELM_CACHE_HOME) stay shared.
export HOME=/tmp HELM_CACHE_HOME=/tmp/helm/cache
HELM_CONFIG_HOME=$(mktemp -d /tmp/helm-config.XXXXXX); export HELM_CONFIG_HOME
work=$(mktemp -d /tmp/plugin.XXXXXX)
trap 'rm -rf "$HELM_CONFIG_HOME" "$work"' EXIT
env=$(yq -r '.environment // ""' input.yaml)
[[ $env =~ ^[a-z][a-z0-9-]{1,18}[a-z0-9]$ ]] || { echo "input.yaml: bad environment '$env'" >&2; exit 1; }
here=$(cd "$(dirname "$0")/.." && pwd)    # the helmfile directory fetched from helm-charts
DB_HOST=${ARGOCD_ENV_DB_HOST:?DB_HOST missing} DB_NAME=${ARGOCD_ENV_DB_NAME:?DB_NAME missing} \
  bash "$here/scripts/render-env.sh" "$PWD/$env.yaml" "$PWD/input.yaml" "$work/$env.yaml"
export DIGIT_ENV_FILE="$work/$env.yaml" DIGIT_SECRETS_FILE="$PWD/$env-secrets.yaml"
cd "$here"
log="$work/log"
run() { "$@" 2>"$log" || { cat "$log" >&2; exit 1; }; }

case "${ARGOCD_ENV_MODE:?MODE missing}" in
  apps)
    root=${ARGOCD_ENV_ROOT_HELMFILE:-digit-helmfile.yaml.gotmpl}
    case "$root" in digit-helmfile.yaml.gotmpl|products.yaml.gotmpl) ;; *) echo "bad ROOT_HELMFILE '$root'" >&2; exit 1 ;; esac
    run helmfile -f "$root" list --output json > "$work/releases.json"
    jq -r --arg repo "$ARGOCD_APP_SOURCE_REPO_URL" --arg rev "$ARGOCD_APP_SOURCE_TARGET_REVISION" --arg root "$root" \
          --arg path "$ARGOCD_APP_SOURCE_PATH" --arg argons "${ARGOCD_APP_NAMESPACE:-argocd}" \
          --arg dbhost "$ARGOCD_ENV_DB_HOST" --arg dbname "$ARGOCD_ENV_DB_NAME" '
      .[] | select(.installed)
      | (.labels | split(",") | map(split(":") | {(.[0]): (.[1:] | join(":"))}) | add // {}) as $l
      | ($root == "products.yaml.gotmpl") as $product
      | (($l.createonly // "") | split("+") | map(select(. != ""))) as $createonly
      | {
          apiVersion: "argoproj.io/v1alpha1", kind: "Application",
          metadata: {
            name: .name, namespace: $argons,
            labels: ({"digit.org/layer": (if $product then "products" else ($l.layer // "none") end)}
              + (if $l.product then {"digit.org/product": $l.product} else {} end)),
            annotations: {"argocd.argoproj.io/sync-wave": ($l.wave // "10")},
            finalizers: ["resources-finalizer.argocd.argoproj.io"]
          },
          spec: ({
            project: "default",
            source: {repoURL: $repo, targetRevision: $rev, path: $path,
              plugin: {name: "digit-helmfile", env: [
                {name: "MODE", value: "release"},
                {name: "DB_HOST", value: $dbhost}, {name: "DB_NAME", value: $dbname},
                {name: "RELEASE", value: .name},
                {name: "NAMESPACE", value: .namespace},
                {name: "HELMFILE", value: (if $product then $root else ("helmfiles/" + $l.layer + ".yaml.gotmpl") end)}]}},
            destination: {server: "https://kubernetes.default.svc", namespace: .namespace},
            syncPolicy: {
              automated: {prune: true, selfHeal: true},
              syncOptions: (["CreateNamespace=true", "ServerSideApply=true", "SkipDryRunOnMissingResource=true"]
                + (if ($createonly | length) > 0 then ["RespectIgnoreDifferences=true"] else [] end)),
              retry: {limit: 20, backoff: {duration: "30s", factor: 2, maxDuration: "10m"}}
            }
          } + (if ($createonly | length) > 0 then
              {ignoreDifferences: [$createonly[] | {group: "", kind: "Secret", name: ., jsonPointers: ["/data"]}]}
            else {} end))
        }
      | "---", (. | tojson)' "$work/releases.json"
    ;;
  release)
    : "${ARGOCD_ENV_RELEASE:?RELEASE missing}" "${ARGOCD_ENV_NAMESPACE:?NAMESPACE missing}"
    hf=${ARGOCD_ENV_HELMFILE:-digit-helmfile.yaml.gotmpl}
    case "$hf" in helmfiles/*.yaml.gotmpl|digit-helmfile.yaml.gotmpl|products.yaml.gotmpl) ;; *) echo "bad HELMFILE '$hf'" >&2; exit 1 ;; esac
    out="$work/out.yaml"
    # --api-versions: CRD groups this install provides, so charts render the same objects
    # (ServiceMonitors, Certificates) as helmfile mode does against the live cluster.
    run helmfile -f "$hf" template --include-crds --skip-tests \
      --args "--api-versions=monitoring.coreos.com/v1 --api-versions=cert-manager.io/v1" \
      -l "name=${ARGOCD_ENV_RELEASE},namespace=${ARGOCD_ENV_NAMESPACE}" > "$out"
    cat "$out"
    ;;
  *) echo "unknown MODE '$ARGOCD_ENV_MODE'" >&2; exit 1 ;;
esac
