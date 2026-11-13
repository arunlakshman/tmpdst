{{- define "mdscontrolplane.name" -}}
{{- .Release.Name }}-mdscontrolplane
{{- end }}

{{- define "mdscontrolplane.labels" -}}
app.kubernetes.io/name: mdscontrolplane
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
