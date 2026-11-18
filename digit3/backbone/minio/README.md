# minio (digit3/backbone)

MinIO object storage for DIGIT, based on the DIGIT-DevOps `digit-lts` chart.

- **Mode:** `replicas: 1` = standalone (`minio server /export`); `replicas > 1` = distributed over the headless Service `minio-svc`.
- **Login:** `auth.existingSecret` + `auth.existingSecretKeys`. Example: share DIGIT filestore's `minio` Secret with `{rootUser: accesskey, rootPassword: secretkey}`.
- **Buckets:** `provisioning.buckets` (`name`, `policy`). A sidecar creates them once MinIO is up. It is not a Helm hook, so an install never fails on it.
- **Objects:** `minio` (StatefulSet + S3 Service), `minio-console`, `minio-svc` (headless). Names don't depend on the release name.
- **Console / S3 ingresses:** `ingress.*` / `apiIngress.*`, off by default.
