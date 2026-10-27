{{/*
DIGIT application-DB reconciler helpers.

The reconciler runs in THIS chart's namespace (backbone) as the CNPG superuser,
and provisions the application role + database from live cluster-configs that
live in the APP namespace (egov-config ConfigMap + db Secret).
*/}}

{{/* App namespace holding egov-config + the db secret. Defaults to "egov"; overridden
     by custom_config.namespace only when custom_config.enabled is true. */}}
{{- define "postgresql.appNamespace" -}}
{{- if and .Values.custom_config .Values.custom_config.enabled .Values.custom_config.namespace -}}
{{- .Values.custom_config.namespace -}}
{{- else -}}
egov
{{- end -}}
{{- end -}}

{{- define "postgresql.reconciler.name" -}}
{{- printf "%s-reconciler" (include "cluster.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Shared pod spec for the reconciler Job and CronJob.
  - initContainer (kubectl): reads db-name from egov-config and username/password
    from the db secret, in the app namespace, into an emptyDir.
  - container (psql): waits for the cluster, then idempotently CREATE ROLE /
    CREATE DATABASE / GRANT as the CNPG superuser. Credentials are passed via env
    and pulled into psql with \getenv, so they never appear in process args.
*/}}
{{- define "postgresql.reconciler.podSpec" -}}
serviceAccountName: {{ include "postgresql.reconciler.name" . }}
restartPolicy: Never
initContainers:
  - name: fetch-config
    image: {{ .Values.reconciler.images.kubectl | quote }}
    command: ["/bin/sh", "-ec"]
    args:
      - |
        ns="{{ include "postgresql.appNamespace" . }}"
        cm="{{ .Values.reconciler.egovConfig.name }}"
        sec="{{ .Values.reconciler.dbSecret.name }}"
        kubectl get configmap "$cm" -n "$ns" -o "jsonpath={.data.{{ .Values.reconciler.egovConfig.dbNameKey }}}" > /work/db-name
        kubectl get secret "$sec" -n "$ns" -o "jsonpath={.data.{{ .Values.reconciler.dbSecret.usernameKey }}}" | base64 -d > /work/app-user
        kubectl get secret "$sec" -n "$ns" -o "jsonpath={.data.{{ .Values.reconciler.dbSecret.passwordKey }}}" | base64 -d > /work/app-pass
        test -s /work/db-name && test -s /work/app-user && test -s /work/app-pass
        echo "fetched db-name + credentials from ${ns}/${cm},${sec}"
    volumeMounts:
      - name: work
        mountPath: /work
containers:
  - name: reconcile
    image: {{ .Values.reconciler.images.psql | quote }}
    command: ["/bin/sh", "-ec"]
    env:
      - name: PGHOST
        value: {{ include "cluster.fullname" . }}-rw
      - name: PGUSER
        valueFrom:
          secretKeyRef:
            name: {{ include "cluster.fullname" . }}-superuser
            key: username
      - name: PGPASSWORD
        valueFrom:
          secretKeyRef:
            name: {{ include "cluster.fullname" . }}-superuser
            key: password
    args:
      - |
        export APP_USER="$(cat /work/app-user)"
        export APP_PASS="$(cat /work/app-pass)"
        export APP_DB="$(cat /work/db-name)"
        echo "reconcile target: host=${PGHOST} db=${APP_DB} user=${APP_USER}"
        until pg_isready -q -d postgres; do echo "waiting for ${PGHOST}"; sleep 3; done
        psql -X -q -v ON_ERROR_STOP=1 -d postgres <<'SQL'
        \getenv app_user APP_USER
        \getenv app_pass APP_PASS
        \getenv app_db APP_DB
        SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_pass')
          WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'app_user') \gexec
        SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'app_user', :'app_pass') \gexec
        SELECT format('CREATE DATABASE %I OWNER %I', :'app_db', :'app_user')
          WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'app_db') \gexec
        SELECT format('ALTER DATABASE %I OWNER TO %I', :'app_db', :'app_user') \gexec
        SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'app_db', :'app_user') \gexec
        SQL
        echo "reconcile: done (db=${APP_DB} owner=${APP_USER})"
    volumeMounts:
      - name: work
        mountPath: /work
volumes:
  - name: work
    emptyDir: {}
{{- end -}}
