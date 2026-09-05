{{/*
Is the bundled portal proxy (portal-proxy.yaml) on?

`portal.proxy.enabled` is unset by default and then means "follow the Ingress":
an Ingress needs a single backend that knows the viewer/management path split,
and the proxy is that backend. An explicit true runs the proxy without an
Ingress (the dev port-forward). An explicit false alongside an Ingress is the
one combination that cannot be honoured, so it is refused instead — see
whistler.portal.validate.

Returns "true" or "" so callers can use it in `if`.
*/}}
{{- define "whistler.portal.proxyEnabled" -}}
{{- $p := .Values.portal.proxy | default dict -}}
{{- if kindIs "invalid" $p.enabled -}}
{{- if .Values.portal.ingress.enabled }}true{{ end -}}
{{- else -}}
{{- if $p.enabled }}true{{ end -}}
{{- end -}}
{{- end -}}

{{/*
Refuse value shapes that would otherwise fail quietly.

The portal's value keys were reorganised on 2026-09-05: `portal.ingress` used
to name the bundled Traefik *pod* and the real Ingress object was buried at
`portal.ingress.resource`, which is the inverse of what every other chart
means by `ingress`. Helm merges unknown keys silently, so a values file
written against the old names would keep parsing and simply stop taking
effect — a portal that quietly comes up with no Ingress, or no basic auth.
Each legacy key therefore names its replacement and stops the render.
*/}}
{{- define "whistler.portal.validate" -}}
{{- $ing := .Values.portal.ingress | default dict -}}
{{- if hasKey $ing "resource" -}}
{{- fail "portal.ingress.resource.* has been renamed: the Kubernetes Ingress is now portal.ingress.* itself (enabled/className/host/path/pathType/annotations/tls), and the bundled Traefik pod it used to share a key with moved to portal.proxy.*. Drop the `resource:` level." -}}
{{- end -}}
{{- if or (hasKey $ing "image") (hasKey $ing "service") (hasKey $ing "resources") (hasKey $ing "basicAuth") -}}
{{- fail "portal.ingress.{image,service,resources,basicAuth} configured the bundled Traefik pod, which is now portal.proxy.{image,service,resources,basicAuth}. portal.ingress.* is the Kubernetes Ingress object, and it turns the proxy on by itself." -}}
{{- end -}}
{{- if hasKey .Values.portal "managementService" -}}
{{- fail "portal.managementService.{port,nodePort} is now portal.service.{managementPort,managementNodePort} — both ports have always been on the one portal Service, and the old name implied a second one." -}}
{{- end -}}
{{- if and $ing.enabled (not (include "whistler.portal.proxyEnabled" .)) -}}
{{- fail "portal.ingress.enabled with portal.proxy.enabled: false — an Ingress needs one backend that knows the viewer/management path split, and the bundled proxy is that backend. Leave portal.proxy.enabled unset (it follows the Ingress) or set it true." -}}
{{- end -}}
{{- end -}}
