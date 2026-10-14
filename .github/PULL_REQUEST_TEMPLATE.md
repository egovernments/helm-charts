<!--
  Helm chart release PR. One project + one module per PR.
  Service charts are detected automatically from the paths you changed
  under charts/<project>/<module>/<service>/ .
-->

## What changed
<!-- brief summary of the chart change(s) -->

## Required labels (ALL THREE)
Add **exactly one of each** from the label dropdown:

- `project:<name>`  - which project (e.g. `project:digit3`)
- `module:<name>`   - which module (`common` | `core` | `health` | `urban` | `frontend` | `backbone`)
- `release:<type>`  - `release:major` (breaking) | `release:minor` (feature) | `release:patch` (fix)

> The **Require release labels** check stays red (merge blocked for everyone) until exactly one
> `project:*`, one `module:*`, and one `release:*` label are all present.
> On merge, each changed chart under the labelled project/module is versioned (bumped from the
> published `index.yaml`), packaged, and published to `https://helm.digit.org/<project>/<module>/<service>`.

## Charts touched
<!-- list the charts/<project>/<module>/<service> dirs you changed -->
- 
