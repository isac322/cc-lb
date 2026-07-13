{{/* Chart name. */}}
{{- define "cc-lb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name. */}}
{{- define "cc-lb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "cc-lb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cc-lb.labels" -}}
helm.sh/chart: {{ include "cc-lb.chart" . }}
{{ include "cc-lb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: cc-lb
{{- end }}

{{- define "cc-lb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cc-lb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "cc-lb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cc-lb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Fully qualified image reference; tag falls back to the chart appVersion. */}}
{{- define "cc-lb.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/* Name of the Secret holding CC_LB_MASTER_KEY and optional tokens. */}}
{{- define "cc-lb.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- include "cc-lb.fullname" . }}
{{- end }}
{{- end }}

{{/* Name of the PVC holding the sqlite DB / data dir. */}}
{{- define "cc-lb.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- printf "%s-data" (include "cc-lb.fullname" .) }}
{{- end }}
{{- end }}
