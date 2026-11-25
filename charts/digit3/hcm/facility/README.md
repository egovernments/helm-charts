# facility

Facility master data and facility-project mapping for health campaigns.

| | |
|---|---|
| **Project / Module** | `digit3` / `hcm` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `facility` (+ `facility-db` flyway init image) |
| **Context path** | `/facility` (hardcoded in the jar; `ingress.context` kept in sync) |

## Install

```bash
helm repo add digit3-hcm https://helm.digit.org/digit3/hcm
helm repo update
helm install facility digit3-hcm/facility -n hcm-3-0 -f values.yaml
```

## How configuration works

Every parameter is overridable. Values resolve lowest to highest priority:

1. **common chart defaults** - shared platform defaults.
2. **this chart's `values.yaml`** - the service defaults below.
3. **the environment file** - a `facility` block in `environments/digit-3.0/hcm-3.0.yaml` (wins).

`appType: java`, so the datasource, Hikari pool, Kafka bootstrap, flyway, timezone and
Tomcat tuning are injected by `common` from the `egov-config` ConfigMap and the `db` Secret.
The flyway history table is `public_facility` and missing migrations are ignored. The image
tag is the chart `appVersion` in `prod` mode.

## Parameters

| Key | Env var | Default |
|---|---|---|
| `ingress.context` | gateway route-path | `facility` |
| `idgen-host` | `EGOV_IDGEN_HOST` | `http://idgen.core-3-0:8080` |
| `user-host` | `EGOV_USER_HOST` | `http://user.core-3-0:8080` |
| `mdms-host` | `EGOV_MDMS_HOST` | `http://mdms-v2.core-3-0:8080` |
| `boundary-host` | `EGOV_BOUNDARY_HOST` | `http://boundary-management.core-3-0:8080` |
| `boundary-hierarchy` | `EGOV_BOUNDARY_HIERARCHY` | `HCM-Moz-Hierarchy` |
| `validate-outside-household-sites` | `FACILITY_VALIDATE_OUTSIDE_HOUSEHOLD_SITES` | `true` |
| `redis-host` | `SPRING_REDIS_HOST` | `redis.backbone` |

The context path is **not** injected as an env var: the app hardcodes
`server.servlet.context-path=/facility`.

## Verify in the environment

`idgen` and `boundary` hosts are `localhost` in the app source, so they MUST be set for the
target environment (the defaults above point at in-cluster `core-3-0`). `boundary-hierarchy`
is country-specific. Set these in `environments/digit-3.0/hcm-3.0.yaml`. The probe hits
`/facility/health`; confirm the app serves it.
