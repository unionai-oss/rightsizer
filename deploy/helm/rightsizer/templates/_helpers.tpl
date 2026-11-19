{{- define "rightsizer.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}{{ .Release.Name | trunc 50 | trimSuffix "-" }}{{- else -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" }}{{- end -}}
{{- end -}}

{{- define "rightsizer.labels" -}}
app.kubernetes.io/name: rightsizer
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "rightsizer.selector" -}}
app.kubernetes.io/name: rightsizer
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rightsizer.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "rightsizer.serviceName" -}}{{ include "rightsizer.fullname" . }}-webhook{{- end -}}
{{- define "rightsizer.tlsSecret" -}}{{ include "rightsizer.fullname" . }}-tls{{- end -}}
{{- define "rightsizer.keySecret" -}}{{ .Values.signingKey.existingSecret | default (printf "%s-signing-key" (include "rightsizer.fullname" .)) }}{{- end -}}
{{- define "rightsizer.historySecret" -}}{{ .Values.history.postgres.existingSecret | default (printf "%s-history" (include "rightsizer.fullname" .)) }}{{- end -}}
{{- define "rightsizer.nodeAgentTokenSecret" -}}{{ .Values.nodeAgent.token.existingSecret | default (printf "%s-node-agent-token" (include "rightsizer.fullname" .)) }}{{- end -}}
