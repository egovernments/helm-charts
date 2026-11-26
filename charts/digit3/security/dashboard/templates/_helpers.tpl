{{- define "dashboard.name" -}}security-dashboard{{- end -}}
{{- define "dashboard.pageConfigMap" -}}security-dashboard-page{{- end -}}
{{- define "dashboard.historyConfigMap" -}}security-dashboard-history{{- end -}}
{{- define "dashboard.labels" -}}
app.kubernetes.io/name: {{ include "dashboard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: security
{{- end -}}
{{/*
The collector pod, defined once.

Two things run it: the CronJob on a schedule, and the PostSync hook Job that
fires on every Argo CD sync, so a change to this chart or its values shows on
the page within a minute instead of at the next scheduled run. The two must
stay identical, so neither of them owns the spec.
*/}}
{{- define "dashboard.collectorPod" -}}
metadata:
  labels: {{- include "dashboard.labels" . | nindent 4 }}
spec:
  restartPolicy: Never
  serviceAccountName: {{ include "dashboard.name" . }}
  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: collector
      image: "{{ .Values.collector.image.repository }}:{{ .Values.collector.image.tag }}"
      imagePullPolicy: {{ .Values.collector.image.pullPolicy }}
      command: ["python3", "-I", "/opt/dashboard/collect.py"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
      env:
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: PAGE_CONFIGMAP
          value: {{ include "dashboard.pageConfigMap" . | quote }}
        - name: HISTORY_CONFIGMAP
          value: {{ include "dashboard.historyConfigMap" . | quote }}
        - name: DRILL_WORKLOADS
          value: {{ .Values.collector.drillWorkloads | quote }}
        - name: MAX_PAGE_BYTES
          value: {{ .Values.collector.maxPageBytes | quote }}
        - name: API_WAIT_SECONDS
          value: {{ .Values.collector.apiWaitSeconds | quote }}
        - name: API_WAIT_INTERVAL
          value: {{ .Values.collector.apiWaitInterval | quote }}
        - name: CLUSTER_GRACE_SECONDS
          value: {{ .Values.collector.clusterGraceSeconds | quote }}
        - name: CVE_IMAGES
          value: {{ .Values.collector.cve.images | quote }}
        - name: CVE_FETCH_CONCURRENCY
          value: {{ .Values.collector.cve.fetchConcurrency | quote }}
        - name: CVE_PER_IMAGE
          value: {{ .Values.collector.cve.perImage | quote }}
        - name: CVE_TOP_IMAGES
          value: {{ .Values.collector.cve.topImages | quote }}
        - name: CVE_TOP_WORKLOADS
          value: {{ .Values.collector.cve.topWorkloads | quote }}
        - name: FRAMEWORK
          value: {{ .Values.collector.framework | quote }}
        - name: WORKLOAD_SECTIONS
          value: {{ .Values.collector.workloadSections | quote }}
        - name: RETAIN_DAYS
          value: {{ .Values.collector.history.retainDays | quote }}
        - name: DETAIL_DAYS
          value: {{ .Values.collector.history.detailDays | quote }}
        - name: RAW_DAYS
          value: {{ .Values.collector.history.rawDays | quote }}
        - name: LOCAL_CLUSTER_NAME
          value: {{ .Values.collector.localClusterName | quote }}
        - name: REMOTE_CLUSTERS
          value: {{ toJson (.Values.collector.remoteClusters | default list) | quote }}
      resources: {{- toYaml .Values.collector.resources | nindent 8 }}
      volumeMounts:
        - name: code
          mountPath: /opt/dashboard
        - name: tmp
          mountPath: /tmp
        {{- range .Values.collector.remoteClusters }}
        - name: remote-{{ .name }}
          mountPath: /remote/{{ .name }}
          readOnly: true
        {{- end }}
  volumes:
    - name: code
      configMap:
        name: {{ include "dashboard.name" . }}-collector
    - name: tmp
      emptyDir: {}
    {{- range .Values.collector.remoteClusters }}
    - name: remote-{{ .name }}
      secret:
        secretName: {{ .tokenSecret }}
    {{- end }}
{{- end -}}
