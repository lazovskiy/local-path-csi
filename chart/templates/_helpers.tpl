{{- define "local-path-csi.image" -}}
{{- required "image.repository is required" .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/* Namespace of the objects of the chart: the one of the release unless another is named. */}}
{{- define "local-path-csi.namespace" -}}
{{- .Values.namespaceOverride | default .Release.Namespace }}
{{- end }}

{{- define "local-path-csi.labels" -}}
app.kubernetes.io/name: local-path-csi
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Security context of a container that needs nothing from the node. */}}
{{- define "local-path-csi.restricted" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}

{{/* The configuration of the agent: checked here so that a typo fails the install, not the agent. */}}
{{- define "local-path-csi.config" -}}
{{- $pools := list }}
{{- range .Values.pools }}
{{- $pool := dict "name" (required "pools[].name is required" .name) "path" (required "pools[].path is required" .path) }}
{{- if hasKey . "quota" }}{{ $_ := set $pool "quota" .quota }}{{ end }}
{{- if hasKey . "requireMountpoint" }}{{ $_ := set $pool "requireMountpoint" .requireMountpoint }}{{ end }}
{{- if hasKey . "overcommit" }}{{ $_ := set $pool "overcommit" (.overcommit | float64) }}{{ end }}
{{- $pools = append $pools $pool }}
{{- end }}
{{- $transfer := dict "port" (.Values.transfer.port | int64) "concurrency" (.Values.transfer.concurrency | int64) "bandwidthLimitKiB" (.Values.transfer.bandwidthLimitKiB | int64) "keepSource" (.Values.transfer.keepSource | toString) "compression" (.Values.transfer.compression | toString) "compressionLevel" (.Values.transfer.compressionLevel | int64) }}
{{- dict "pools" $pools "projectIdBase" (.Values.projectIdBase | int64) "transfer" $transfer | toJson }}
{{- end }}

{{/* Scrape annotations for a Prometheus that discovers pods by them. */}}
{{- define "local-path-csi.scrape" -}}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .Values.metrics.port | quote }}
{{- end }}
