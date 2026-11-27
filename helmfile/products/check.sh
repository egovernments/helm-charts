#!/usr/bin/env bash
# Checks the product helmfiles (helmfile/products). Run by .github/workflows/products-check.yaml
# on every PR that touches them; also runs locally:
#
#   helmfile/products/check.sh [<base-commit>]     (from the repo root)
#
#   1. layout: products/<product>/v<major>/helmfile.yaml.gotmpl + releases/<major>.<minor>.<patch>.yaml
#   2. at most 3 majors per product (the last 3 are kept: drop the oldest v<major>/ with a new one)
#   3. every chart version of every release is published on helm.digit.org
#   4. every release renders (helmfile list with that version)
#   5. with <base-commit>: a published release file is never changed or deleted (a whole old major
#      may be removed). Fix a release by adding the next patch release.
set -uo pipefail
cd "$(dirname "$0")"
MAX_MAJORS=3
errors=0
err() { echo "::error::$*"; errors=$((errors + 1)); }

# chart -> helm.digit.org module index (cluster-configs is its own module)
index_of() { # <product> <chart>
  if [ "$2" = cluster-configs ]; then echo cluster-configs; else echo "$1"; fi
}
cache=$(mktemp -d)
trap 'rm -rf "$cache"' EXIT
published() { # <module> <chart> <version>
  local f="$cache/$1.yaml"
  [ -f "$f" ] || curl -fsSL "https://helm.digit.org/digit3/$1/index.yaml" -o "$f" || : > "$f"
  # entries:\n  <chart>:\n    - ...\n      version: <v>
  awk -v c="$2" -v v="$3" '
    /^entries:/ {e = 1; next}  /^[a-z]/ {e = 0}
    e && /^  [^ ].*:$/ {n = $1; sub(":$", "", n)}
    e && n == c && /^      version:/ {x = $2; gsub(/"/, "", x); if (x == v) f = 1}
    END {exit !f}' "$f"
}
# charts of a release file: "<chart> <version>" lines (flat "charts:" map, see any release file)
charts_of() {
  awk '/^charts:/ {c = 1; next} /^[^ #]/ {c = 0}
       c && /^  [a-z0-9-]+: *[^ ]+/ {n = $1; sub(":$", "", n); v = $2; gsub(/"/, "", v); print n, v}' "$1"
}

for pdir in */; do
  p=${pdir%/}
  [[ $p =~ ^[a-z][a-z0-9-]*$ ]] || { err "products/$p: bad product name"; continue; }
  majors=()
  for mdir in "$p"/*/; do
    m=$(basename "$mdir")
    [[ $m =~ ^v([0-9]+)$ ]] || { err "products/$p/$m: only v<major>/ folders belong here"; continue; }
    majors+=("${BASH_REMATCH[1]}")
    major=${BASH_REMATCH[1]}
    hf="$p/$m/helmfile.yaml.gotmpl"
    [ -f "$hf" ] || err "products/$hf is missing"
    n=0
    for rf in "$p/$m"/releases/*; do
      [ -e "$rf" ] || continue
      ver=$(basename "$rf" .yaml)
      if ! [[ $(basename "$rf") =~ ^$major\.[0-9]+\.[0-9]+\.yaml$ ]]; then
        err "products/$rf: release files of $m are named $major.<minor>.<patch>.yaml"; continue
      fi
      n=$((n + 1))
      lines=$(charts_of "$rf")
      [ -n "$lines" ] || { err "products/$rf: no charts (expected a charts: map of <chart>: <version>)"; continue; }
      while read -r chart cv; do
        published "$(index_of "$p" "$chart")" "$chart" "$cv" ||
          err "products/$rf: $chart $cv is not published on https://helm.digit.org/digit3/$(index_of "$p" "$chart")"
      done <<< "$lines"
      [ -f "$hf" ] && ! out=$(cd "$p/$m" && helmfile --state-values-set "product.version=$ver" list 2>&1) &&
        err "products/$rf: does not render: $(echo "$out" | tail -1)"
    done
    [ "$n" -gt 0 ] || err "products/$p/$m: no releases"
  done
  [ "${#majors[@]}" -le "$MAX_MAJORS" ] ||
    err "products/$p: ${#majors[@]} majors (${majors[*]}); keep the last $MAX_MAJORS - remove the oldest v<major>/"
  echo "products/$p: majors ${majors[*]}"
done

if [ -n "${1:-}" ]; then
  while read -r st f; do
    [ -n "$st" ] || continue
    case $st in
      A) ;;
      D) d=$(dirname "$(dirname "$f")")   # helmfile/products/<p>/v<major>
         [ -d "../../$d" ] && err "$f: a published release cannot be deleted (only a whole old major)";;
      *) err "$f: a published release cannot be changed - add the next patch release instead";;
    esac
  done < <(cd ../.. && git diff --name-status --no-renames "$1" -- 'helmfile/products/*/v*/releases/*.yaml')
fi

[ "$errors" -eq 0 ] && echo "product helmfiles OK" || { echo "$errors error(s)"; exit 1; }
