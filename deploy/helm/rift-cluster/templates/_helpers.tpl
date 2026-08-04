{{/*
Names. `fullname` is what every resource is prefixed with; the headless Service's name is also the
StatefulSet's `serviceName` and appears inside the seed address, so all three have to agree.

`fullname` is truncated to 56, not the customary 63, and the six characters are not spare. Two
things get built on top of it and both have a hard 63-character DNS-label ceiling:

  - `<fullname>-peers`, the headless Service (+6);
  - `<fullname>-<ordinal>`, every pod's hostname (+2 and up).

Truncating each derived name to 63 *after* appending — the usual chart idiom — silently discards
the suffix once the base is already at the limit, so `peerService` collapses onto `fullname` and
the two Services collide. The one that survives is the readiness-gating client Service, while
`serviceName` and the seed address still point at that name: no pod is Ready during a cold start,
so the Service publishes no addresses, seeds never resolve, and the cluster deadlocks forming.
Reserving the room up front is what makes that unreachable.
*/}}
{{- define "rift-cluster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 56 | trimSuffix "-" -}}
{{- end -}}

{{- define "rift-cluster.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 56 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 56 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 56 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* The headless Service peers resolve. Named separately because seeds are built from it. */}}
{{- define "rift-cluster.peerService" -}}
{{- printf "%s-peers" (include "rift-cluster.fullname" .) -}}
{{- end -}}

{{- define "rift-cluster.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "rift-cluster.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
`app: rift-cluster-server` is carried deliberately alongside the app.kubernetes.io set: it is the
label the raw manifest uses and the one the default `topologySpreadConstraints` in values.yaml
selects on, so dropping it would silently un-spread every install that took the default.
*/}}
{{- define "rift-cluster.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rift-cluster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: rift-cluster-server
{{- end -}}

{{/*
The container image, and the guard that the flavor can actually run this chart.

The `-static` flavor is `FROM scratch`, and every pod's container command in this chart is
`/bin/sh -c` (statefulset.yaml) — the ordinal-0 bootstrap branch is a shell script. That command
sits on the pod template, so it is uniform across ordinals: a `-static` image does not degrade the
founding node, it fails EVERY pod with a container exec error, and only at rollout. Refusing at
render time turns that into a sentence at `helm install`, the same trade `rift-cluster.secretName`
below makes.

The effective tag is computed here, so an explicit `image.tag` is what gets checked; the default
(`.Chart.AppVersion`) is a published chart version and can never carry the suffix.
*/}}
{{- define "rift-cluster.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if not (kindIs "string" $tag) -}}
{{- fail "image.tag must be quoted. An unquoted YAML number (`tag: 1.2`) parses as a float, not a string — and quoting is the fix, not something the chart can do for you: coercing 1.10 back to a string would yield the tag `1.1`, which resolves to a different image than the one you asked for." -}}
{{- end -}}
{{- if hasSuffix "-static" $tag -}}
{{- fail "image.tag: the `-static` flavor is FROM scratch — no shell — and this chart runs a shell script as EVERY pod's container command (the ordinal-0 bootstrap branch), so all pods would fail at exec. A -static fleet also serves no git+ imposter sources. Use the default flavor; see the image-flavors table in deploy/README.md." -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
The ServiceAccount name, or empty when there is nothing to name.

`create: false` with a `name` set is not a no-op — it is the IRSA / Workload Identity shape, where
the account is created out of band carrying a role annotation and the chart only has to reference
it. Empty means "say nothing and let Kubernetes use `default`".
*/}}
{{- define "rift-cluster.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "rift-cluster.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The Secret the cluster secret is read from, and the guard that there is one.

Refusing here rather than rendering a broken StatefulSet is the point: a pod that starts without a
cluster secret either refuses to boot or — worse, if someone reaches for the insecure switch — joins
a fleet with peer authentication off. `helm install` failing with a sentence is the cheaper outcome.
*/}}
{{- define "rift-cluster.secretName" -}}
{{- if .Values.clusterSecret.existingSecret -}}
{{- .Values.clusterSecret.existingSecret -}}
{{- else if .Values.clusterSecret.create -}}
{{- printf "%s-secret" (include "rift-cluster.fullname" .) -}}
{{- else -}}
{{- fail "clusterSecret: set clusterSecret.existingSecret to a Secret you manage, or clusterSecret.create=true with clusterSecret.value. The cluster port authenticates every peer RPC with it." -}}
{{- end -}}
{{- end -}}

{{- define "rift-cluster.secretKey" -}}
{{- if .Values.clusterSecret.existingSecret -}}
{{- .Values.clusterSecret.existingSecretKey -}}
{{- else -}}
cluster-secret
{{- end -}}
{{- end -}}

{{/*
terminationGracePeriodSeconds, DERIVED — see `leaveTimeoutSeconds` in values.yaml.

`2 * leaveTimeout + 10`. The rule the raw manifest states in a comment ("raise them together, never
one alone") becomes arithmetic here, so the two cannot drift.
*/}}
{{- define "rift-cluster.terminationGracePeriod" -}}
{{- add (mul 2 (int .Values.leaveTimeoutSeconds)) 10 -}}
{{- end -}}
