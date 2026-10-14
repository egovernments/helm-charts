<!--
  Helm chart release PR.
  Project / module / service are detected automatically from the paths you changed
  under charts/<project>/<module>/<service>/ . You only choose the version bump.
-->

## What changed
<!-- brief summary of the chart change(s) -->

## Release type (REQUIRED)
Add **exactly one** label to this PR from the label dropdown:

- `release:major`  - breaking change
- `release:minor`  - backwards-compatible feature
- `release:patch`  - fix / config tweak

> The **Require release label** check stays red until exactly one is set, and merge is blocked without it.
> On merge, every changed chart is versioned (bumped from the published `index.yaml`),
> packaged, and published to `https://helm.digit.org/<project>/<module>/<service>`.

## Charts touched
<!-- list the charts/<project>/<module>/<service> dirs you changed -->
- 
