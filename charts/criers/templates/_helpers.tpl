{{- /*
Base name for every object. Truncated to 58 characters so derived names such as
"<name>-ops" and "<name>-data" stay within the 63-character DNS label limit.
*/ -}}
{{- define "criers.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 58 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 58 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "criers.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "criers.labels" -}}
{{ include "criers.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "criers.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{- define "criers.claimName" -}}
{{- .Values.persistence.existingClaim | default (printf "%s-data" (include "criers.fullname" .)) -}}
{{- end -}}


{{- /*
Rejects pod metadata that would override chart-owned keys: selector labels must
match the Deployment's immutable selector, and checksum/config rolls the pod on
configuration changes.
*/ -}}
{{- define "criers.validatePodMetadata" -}}
{{- range $key, $_ := fromYaml (include "criers.selectorLabels" .) }}
{{- if hasKey $.Values.podLabels $key }}
{{- fail (printf "podLabels must not set %s; the chart owns its selector labels" $key) }}
{{- end }}
{{- end }}
{{- if hasKey .Values.podAnnotations "checksum/config" }}
{{- fail "podAnnotations must not set checksum/config; the chart uses it to roll the pod on configuration changes" }}
{{- end }}
{{- end -}}