# product

Product and product-variant master data for health campaigns.

| | |
|---|---|
| **Project / Module** | `digit3` / `hcm` |
| **Chart type** | application |
| **Depends on** | `common` (library chart) |
| **Backend image** | `product` (+ `product-db` flyway init image) |
| **Context path** | `/product` (hardcoded in the jar; `ingress.context` kept in sync) |

## Install

```bash
helm repo add digit3-hcm https://helm.digit.org/digit3/hcm
helm repo update
helm install product digit3-hcm/product -n hcm-3-0 -f values.yaml
```

## How configuration works

Every parameter is overridable. Values resolve lowest to highest priority:

1. **common chart defaults** - shared platform defaults.
2. **this chart's `values.yaml`** - the service defaults below.
3. **the environment file** - a `product` block in `environments/digit-3.0/hcm-3.0.yaml` (wins).

`appType: java`, so the datasource, Hikari pool, Kafka bootstrap, flyway, timezone and
Tomcat tuning are injected by `common` from the `egov-config` ConfigMap and the `db` Secret.
Do not redefine them here. The image tag is the chart `appVersion` in `prod` mode.

## Parameters

| Key | Env var | Default |
|---|---|---|
| `ingress.context` | gateway route-path | `product` |
| `idgen-host` | `EGOV_IDGEN_HOST` | `http://idgen.core-3-0:8080` |
| `user-host` | `EGOV_USER_HOST` | `http://user.core-3-0:8080` |
| `mdms-host` | `EGOV_MDMS_HOST` | `http://mdms-v2.core-3-0:8080` |
| `mdms-master-name` | `EGOV_MDMS_MASTER_NAME` | `product_master` |
| `redis-host` | `SPRING_REDIS_HOST` | `redis.backbone` |
| `cache-ttl` | `SPRING_CACHE_REDIS_TIME_TO_LIVE` | `60` |

The context path is **not** injected as an env var: the app hardcodes
`server.servlet.context-path=/product`, so an env would be dead config. Keep `ingress.context`
equal to the hardcoded path.

## Verify in the environment

The external-service hosts above point at in-cluster `core-3-0` services; set the real hosts
for the target environment in `environments/digit-3.0/hcm-3.0.yaml`. The liveness/readiness
probe hits `/product/health`; confirm the app serves it (adjust `healthChecks` if the service
exposes health under `/actuator`).
