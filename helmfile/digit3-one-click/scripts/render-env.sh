#!/usr/bin/env bash
# Fill the placeholders of a one-click environment file at deploy time.
#
#   DB_HOST=... DB_NAME=... render-env.sh <env-file> <input.yaml> <output-file>
#
# <env-file> (one-click/<environment>.yaml, made from digit-template.yaml) keeps its
# placeholders in git; this writes a filled copy for helmfile:
#   <domain> <email> <env>   from input.yaml (domain, email, environment)
#   <db_host> <db_name>      from DB_HOST / DB_NAME (the database Terraform created)
# Used by the DIGIT-DevOps workflow (helmfile mode) and by the Argo CD plugin, so both modes
# deploy exactly the same values. Fails if any other <placeholder> is left in a value.
set -euo pipefail
envf=${1:?environment file}; input=${2:?input.yaml}; out=${3:?output file}
[ -f "$envf" ] || { echo "$envf not found" >&2; exit 1; }
[ -f "$input" ] || { echo "$input not found" >&2; exit 1; }
get() { yq -r ".$1 // \"\"" "$input"; }
domain=$(get domain); email=$(get email); env=$(get environment)
[[ $domain =~ ^([a-z0-9]([a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,}$ ]] || { echo "input.yaml: domain '$domain' is not a web address" >&2; exit 1; }
email_re='^[^@[:space:]|&\\]+@[^@[:space:]|&\\]+\.[^@[:space:]|&\\]+$'   # no | & \ (used by sed below)
[[ $email =~ $email_re ]] || { echo "input.yaml: email '$email' is not an e-mail address" >&2; exit 1; }
[[ $env =~ ^[a-z][a-z0-9-]{1,18}[a-z0-9]$ ]] || { echo "input.yaml: environment '$env' is not allowed" >&2; exit 1; }
db_host=${DB_HOST:?DB_HOST missing}; db_name=${DB_NAME:?DB_NAME missing}
[[ $db_host =~ ^[a-z0-9.-]+$ && $db_name =~ ^[A-Za-z0-9_-]+$ ]] || { echo "bad DB_HOST/DB_NAME" >&2; exit 1; }

sed -e "s|<domain>|$domain|g" -e "s|<email>|$email|g" -e "s|<env>|$env|g" \
    -e "s|<db_host>|$db_host|g" -e "s|<db_name>|$db_name|g" "$envf" > "$out"

left=$(yq -r '.. | select(tag == "!!str") | select(test("<[a-z_]+>")) | path | join(".")' "$out")
[ -z "$left" ] || { echo "$envf: unfilled placeholders: $(echo "$left" | paste -sd' ' -)" >&2; exit 1; }
