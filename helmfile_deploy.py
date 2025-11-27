#!/usr/bin/env python3
"""
Dynamic Helmfile Deployer using GitHub API (no local clone required)

Behavior:
- Replaces only {{ env "ENV_FILE" }} and {{ env "SECRET_FILE" }} with provided paths (handles whitespace variations).
- Preserves all other Go template expressions exactly.
- Deduplicates repositories, templates, values, secrets, releases.
- Removes empty sections from the final dynamic helmfile.
- Keeps indentation/anchors/merge keys as well as possible using ruamel.yaml.
"""

import argparse
import subprocess
import shutil
import re
from io import StringIO
from pathlib import Path
from datetime import datetime
import sys
import traceback

def error_exit(step: str, error: Exception, debug: bool = False):
    print(f"\n❌ Error during step: {step}")
    print(f"   Reason: {error}")
    if debug:
        print("\n--- Debug Traceback ---")
        traceback.print_exc()
        print("-----------------------")
    sys.exit(1)

# --- minimal dependency self-install helper (optional) ---
def ensure_package(package_name):
    try:
        __import__(package_name)
    except ModuleNotFoundError:
        print(f"Package '{package_name}' not found. Installing...")
        subprocess.check_call([__import__("sys").executable, "-m", "pip", "install", package_name])

ensure_package("ruamel.yaml")
ensure_package("requests")

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
import requests

# YAML config
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.preserve_quotes = True
yaml.width = 4096

GITHUB_API_BASE = "https://api.github.com/repos/egovernments/helm-charts/contents/helmfiles"

# ---------------------------
# Mask / unmask helpers
# ---------------------------
# We'll mask all {{ ... }} occurrences EXCEPT the env placeholders.
# Masking replaces the braces inside each matched expression with marker tokens
# so the YAML parser won't interpret/quote them. After dump we unmask.

_START = "__GT_START__"
_END = "__GT_END__"

_env_pattern = re.compile(r'{{\s*env\s*"(?P<name>ENV_FILE|SECRET_FILE)"\s*}}', re.IGNORECASE)

def mask_go_templates_keep_env(content: str) -> str:
    """
    Replace braces inside every {{ ... }} with markers, except when the expression
    is an env placeholder (ENV_FILE or SECRET_FILE) — those stay as-is so we can
    replace them by text before parsing.
    """
    def replacer(m):
        text = m.group(0)
        # If it's an env placeholder, leave it unchanged
        if _env_pattern.fullmatch(text.strip()):
            return text
        # Otherwise replace braces only (keep inner content)
        inner = text[2:-2]
        return f"{_START}{inner}{_END}"
    # Use non-greedy to match each {{ ... }} instance
    return re.sub(r'{{.*?}}', replacer, content, flags=re.DOTALL)

def unmask_go_templates(content: str) -> str:
    """Restore markers back to {{ ... }}"""
    # Replace marker pair back to braces
    return content.replace(_START, "{{").replace(_END, "}}")

# ---------------------------
# Placeholder replacement (text-level)
# ---------------------------
def replace_env_placeholders_in_text(content: str, env_file: str, secrets_file: str) -> str:
    # Replace various spacing variants of env placeholders.
    # Only intended to operate on text where env placeholders remain unmasked.
    content = re.sub(r'{{\s*env\s*"ENV_FILE"\s*}}', env_file, content)
    content = re.sub(r'{{\s*env\s*"SECRET_FILE"\s*}}', secrets_file, content)
    return content

# ---------------------------
# GitHub helper
# ---------------------------
def get_github_json(url, token=None, branch=None):
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    params = {}
    if branch:
        params["ref"] = branch
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

# list modules and helmfiles
def list_modules(token=None, branch=None):
    try:
        data = get_github_json(GITHUB_API_BASE, token, branch)
        return [item["name"] for item in data if item["type"] == "dir"]
    except Exception as e:
        error_exit("Fetching module list from GitHub", e, debug=DEBUG_MODE)

def list_helmfiles(module_name, token=None, branch=None):
    try:
        url = f"{GITHUB_API_BASE}/{module_name}"
        data = get_github_json(url, token, branch)
        return [item for item in data if item["type"] == "file" and item["name"].endswith(".yaml")]
    except Exception as e:
        error_exit(f"Fetching helmfiles for module '{module_name}'", e, debug=DEBUG_MODE)

# ---------------------------
# Selection helpers (unchanged)
# ---------------------------
def parse_selection_input(user_input, max_index):
    user_input = user_input.strip().lower()
    if not user_input:
        return []
    if user_input == "all":
        return list(range(1, max_index + 1))
    parts = [p.strip() for p in user_input.split(",") if p.strip()]
    selected = set()
    for part in parts:
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                a_i = int(a); b_i = int(b)
            except ValueError:
                continue
            if a_i > b_i:
                a_i, b_i = b_i, a_i
            for i in range(a_i, min(b_i, max_index) + 1):
                if 1 <= i <= max_index:
                    selected.add(i)
        else:
            try:
                v = int(part)
            except ValueError:
                continue
            if 1 <= v <= max_index:
                selected.add(v)
    return sorted(selected)

def prompt_choose(prompt, items, allow_multiple=True, single_selection=False):
    if not items:
        return []
    while True:
        print()
        print(prompt)
        for idx, it in enumerate(items, start=1):
            print(f"  {idx}) {it}")
        raw = input("Select number(s): ").strip()
        if not raw:
            print("No selection provided. Please try again.")
            continue
        if allow_multiple:
            sel = parse_selection_input(raw, len(items))
        else:
            try:
                v = int(raw)
                sel = [v] if 1 <= v <= len(items) else []
            except ValueError:
                sel = []
        if not sel:
            print("Invalid selection. Please try again.")
            continue
        if single_selection and len(sel) != 1:
            print("Please select exactly one item.")
            continue
        return sel

# ---------------------------
# Merge logic
# ---------------------------
def merge_helmfiles_from_texts(file_contents_list, env_file, secrets_file, custom_helmfile_text=None):
    """
    High-level plan:
    - For each raw content: mask non-env templates, replace env placeholders in text,
      parse YAML via ruamel, collect sections deduping by sensible keys.
    - After merging produce a CommentedMap, dump to string, unmask templates, return final string.
    """
    if custom_helmfile_text:
        file_contents_list = list(file_contents_list)  # ensure mutable copy
        file_contents_list.append(custom_helmfile_text)

    merged = CommentedMap()
    merged["repositories"] = CommentedSeq()
    merged["templates"] = CommentedMap()
    merged["values"] = CommentedSeq()
    merged["secrets"] = CommentedSeq()
    merged["releases"] = CommentedSeq()

    seen_repos = set()
    seen_templates = set()
    seen_values = set()
    seen_secrets = set()
    seen_releases = set()

    for raw in file_contents_list:
        # 1) Mask all non-env go templates so YAML parser won't break or quote them
        masked = mask_go_templates_keep_env(raw)
        # 2) Replace env placeholders (they were left unmasked by mask function)
        replaced = replace_env_placeholders_in_text(masked, env_file, secrets_file)
        # 3) Parse YAML safely (contains masked templates and replaced env paths)
        data = yaml.load(StringIO(replaced)) or {}

        # Merge repositories (dedupe by 'name')
        for repo in data.get("repositories", []):
            if not isinstance(repo, dict):
                continue
            name = repo.get("name")
            if name and name not in seen_repos:
                merged["repositories"].append(repo)
                seen_repos.add(name)

        # Merge templates (dedupe by template key)
        for tname, tval in data.get("templates", {}).items():
            if tname not in seen_templates:
                merged["templates"][tname] = tval
                seen_templates.add(tname)

        # Merge values (dedupe by string representation)
        for val in data.get("values", []):
            sval = _seq_item_key(val)
            if sval not in seen_values:
                merged["values"].append(val)
                seen_values.add(sval)

        # Merge secrets
        for sec in data.get("secrets", []):
            ssec = _seq_item_key(sec)
            if ssec not in seen_secrets:
                merged["secrets"].append(sec)
                seen_secrets.add(ssec)

        # Merge releases (dedupe by release name)
        for rel in data.get("releases", []):
            if not isinstance(rel, dict):
                continue

            # Generate a uniqueness key based on full structure (namespace, template, chart, set, etc.)
            # Remove volatile fields that shouldn't participate in dedupe (e.g., installed: true)
            rel_copy = dict(rel)

            # Convert to YAML string to compute stable fingerprint
            tmp = StringIO()
            yaml.dump(rel_copy, tmp)
            rel_key = tmp.getvalue()

            if rel_key not in seen_releases:
                merged["releases"].append(rel)
                seen_releases.add(rel_key)

        # Merge releases (dedupe by release name)
        # for rel in data.get("releases", []):
        #     if not isinstance(rel, dict):
        #         continue
        #     rname = rel.get("name")
        #     if rname and rname not in seen_releases:
        #         merged["releases"].append(rel)
        #         seen_releases.add(rname)

    # Build final map removing empty sections
    final = CommentedMap()
    for k, v in merged.items():
        if v:  # keep only non-empty sequences/maps
            final[k] = v

    # Dump to string then unmask go templates markers
    buf = StringIO()
    yaml.dump(final, buf)
    output = buf.getvalue()
    output = unmask_go_templates(output)
    return output

def _seq_item_key(item):
    """
    Return a stable key for deduping sequence items.
    If item is scalar, return str(item). If mapping/list, dump to YAML string.
    """
    if isinstance(item, (str, int, float, bool)):
        return str(item)
    tmp = StringIO()
    yaml.dump(item, tmp)
    return tmp.getvalue()

# ---------------------------
# Build dynamic helmfile (fetch from GitHub -> merge -> write)
# ---------------------------
def build_dynamic_helmfile(selected_files_map, out_filename, env_file_path, secrets_file_path, token=None, branch=None, custom_helmfile_content=None):
    try:
        file_contents_list = []
        for module, files in selected_files_map.items():
            for file_item in files:
                try:
                    headers = {"Authorization": f"token {token}"} if token else {}
                    url = f"https://api.github.com/repos/egovernments/helm-charts/contents/helmfiles/{module}/{file_item['name']}"
                    params = {}
                    if branch:
                        params["ref"] = branch
                    resp = requests.get(url, headers=headers, params=params)
                    resp.raise_for_status()
                    content_json = resp.json()
                    # download_url points to the file content at the chosen ref
                    content = requests.get(content_json["download_url"], headers=headers).text
                    file_contents_list.append(content)
                except Exception as e:
                    error_exit(f"Downloading helmfile '{file_item['name']}' for module '{module}'", e, debug=DEBUG_MODE)
        try:
            if custom_helmfile_content and not selected_files_map:
                print("⚠️ No GitHub helmfiles selected. Using only custom helmfile...")
                final_text = custom_helmfile_content
            elif custom_helmfile_content:
                final_text = merge_helmfiles_from_texts(file_contents_list, env_file_path, secrets_file_path, custom_helmfile_content)
            else:
                final_text = merge_helmfiles_from_texts(file_contents_list, env_file_path, secrets_file_path) 
        except Exception as e:
            error_exit("Merging helmfiles", e, debug=DEBUG_MODE)

        with open(out_filename, "w", encoding="utf-8") as out:
            out.write(final_text)

        return out_filename
    except Exception as e:
        error_exit("Building final dynamic helmfile", e, debug=DEBUG_MODE)

# ---------------------------
# Run helmfile
# ---------------------------
def run_helmfile(helmfile_path, dry_run=False):
    try:
        helmfile_bin = shutil.which("helmfile")
        if not helmfile_bin:
            raise FileNotFoundError("helmfile binary not found in PATH. Install helmfile or skip running.")
        cmd = [helmfile_bin, "-f", str(helmfile_path), "diff" if dry_run else "apply"]
        print(f"Running: {' '.join(cmd)}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            print(line, end="")
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"helmfile exited with status {proc.returncode}")
        return proc.returncode
    except Exception as e:
        error_exit("Running helmfile command", e, debug=DEBUG_MODE)

# ---------------------------
# CLI main
# ---------------------------
def main():
    global DEBUG_MODE
    parser = argparse.ArgumentParser(description="Dynamic Helmfile Deployer using GitHub API")
    parser.add_argument("--branch", default=None, help="Branch/tag/commit to use (default repository default branch)")
    parser.add_argument("--env-file", required=True, help="Path to environment values file to inject")
    parser.add_argument("--secrets-file", required=True, help="Path to secrets file to inject")
    parser.add_argument("--no-apply", action="store_true", help="Don't run helmfile apply")
    parser.add_argument("--out", default=None, help="Output filename for dynamic helmfile")
    parser.add_argument("--github-token", default=None, help="GitHub token (for private repos)")
    parser.add_argument("--dry-run", action="store_true", help="Run helmfile diff instead of apply")
    parser.add_argument("--debug", action="store_true", help="Enable verbose error traceback output")
    parser.add_argument("--modules", nargs="+", help="List of module names to deploy (e.g. backbone core)")
    parser.add_argument("--versions", nargs="+", help="Helmfile versions corresponding to selected modules")
    parser.add_argument("--custom-helmfile", default=None, help="Path to a local custom helmfile that can be deployed alone or merged with fetched helmfiles")

    args = parser.parse_args()
    DEBUG_MODE = args.debug

    modules = list_modules(args.github_token, branch=args.branch)
    if not modules:
        print("No modules found.")
        return

    # If modules passed via CLI, skip prompts
    if args.modules:
        selected_modules = args.modules
        print("✅ Using preselected modules:", selected_modules)
    else:
        selected_module_indices = prompt_choose("Select modules:", modules, allow_multiple=True)
        if not selected_module_indices:
            print("No modules selected. Exiting.")
            return
        selected_modules = [modules[i - 1] for i in selected_module_indices]

    selected_map = {}
    for idx, module in enumerate(selected_modules):
        helmfiles = list_helmfiles(module, args.github_token, branch=args.branch)
        if not helmfiles:
            print(f"No helmfiles found for module '{module}', skipping.")
            continue
        
        if args.versions and len(args.versions) > idx:
            version_name = args.versions[idx]
            sel_file = next((f for f in helmfiles if f["name"] == version_name), None)
            if not sel_file:
                print(f"⚠️ Version '{version_name}' not found for module '{module}', skipping.")
                continue
            selected_map[module] = [sel_file]
            print(f"✅ Using version '{version_name}' for module '{module}'")
        else:
            sel_idx = prompt_choose(f"Select helmfile version for module '{module}':",
                                    [f["name"] for f in helmfiles],
                                    allow_multiple=False,
                                    single_selection=True)
            if not sel_idx:
                continue
            sel_file = helmfiles[sel_idx[0] - 1]
            selected_map[module] = [sel_file]

    custom_helmfile_content = None
    if args.custom_helmfile:
        if not Path(args.custom_helmfile).exists():
            print(f"❌ Custom helmfile '{args.custom_helmfile}' not found.")
            return
        with open(args.custom_helmfile, "r", encoding="utf-8") as f:
            custom_helmfile_content = f.read()
        print(f"✅ Loaded custom helmfile: {args.custom_helmfile}")


    if not selected_map:
        print("No helmfiles selected. Exiting.")
        return

    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_fname = args.out or f"dynamic-helmfile-{ts}.yaml"
    out_path = Path.cwd() / out_fname

    build_dynamic_helmfile(selected_map, out_path, args.env_file, args.secrets_file, token=args.github_token, branch=args.branch, custom_helmfile_content=custom_helmfile_content)
    print(f"\nDynamic helmfile created: {out_path}")

    if args.no_apply:
        print("Skipping helmfile apply (--no-apply set).")
        return

    yn = input(f"Run `helmfile {'diff' if args.dry_run else 'apply'} -f {out_path}` now? (y/N): ").strip().lower()
    if yn == "y":
        try:
            rc = run_helmfile(out_path, dry_run=args.dry_run)
            action = "diff" if args.dry_run else "apply"
            print(f"helmfile {action} completed." if rc == 0 else f"helmfile {action} failed with code {rc}")
        except Exception as e:
            action = "diff" if args.dry_run else "apply"
            print(f"Error running helmfile {action}:", e)
            print(f"You can run manually: helmfile -f {out_path} {action}")
    else:
        action = "diff" if args.dry_run else "apply"
        print(f"You can run manually: helmfile -f {out_path} {action}")

if __name__ == "__main__":
    main()
