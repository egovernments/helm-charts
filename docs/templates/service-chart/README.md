# __SERVICE__

__One short sentence describing the service.__

| | |
|---|---|
| **Project / Module** | `digit3` / `core` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `__SERVICE__` |

## Install

```bash
helm repo add digit3-core https://helm.digit.org/digit3/core
helm repo update
helm install __SERVICE__ digit3-core/__SERVICE__ -n digit3-core -f values.yaml
```

## How configuration works

Every parameter below is overridable. Values resolve lowest to highest priority:

1. **common chart defaults** - shared platform defaults.
2. **this chart's `values.yaml`** - the service defaults listed below.
3. **the environment file** - a block named after the chart in `environments/<env>.yaml` (wins).

```yaml
# environments/<env>.yaml
__SERVICE__:
  context-path: "/__SERVICE__"
```

Shared infrastructure (database, Kafka, Redis, OpenTelemetry) is injected from the `egov-config`
ConfigMap and secrets managed by `cluster-configs`, not set here.

## Parameters

### Routing

| Key | Env var | Description | Default |
|---|---|---|---|
| `ingress.context` | - | Gateway route segment | `__SERVICE__` |
| `context-path` | `SERVER_CONTEXT_PATH` | Blank derives `/<ingress.context>` | `""` -> `/__SERVICE__` |

<!-- Add a row per override key you added to values.yaml: Key | Env var | Description | Default -->

## Sourced from the platform (not set in this chart)

| Env var | Source |
|---|---|
| `DB_URL` / `FLYWAY_LOCATIONS` | `egov-config` (init container) |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | secret `db` (init container) |

## Source

- Chart: `charts/digit3/core/__SERVICE__`
- Full defaults: `values.yaml`
