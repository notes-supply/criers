#!/usr/bin/env bash
# Chart contract checks for `mise run chart`: lint, schema validation of the
# rendered manifests, behavioral assertions and rejected configurations.
set -euo pipefail
chart="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:?output directory}"
mkdir -p "$out"
k8s_version=1.36.0

render() { helm template criers "$chart" "${@:2}" > "$out/$1.yaml"; }
check() { # description, document selector, predicate, rendered file
  # At least one document must be selected and the predicate must hold for each.
  if ! yq ea -e "[$2 | ($3)] | ((length > 0) and all)" "$out/$4.yaml" > /dev/null; then
    echo "FAIL [$4]: $1" >&2
    exit 1
  fi
  echo "ok   [$4]: $1"
}
reject() { # description, expected message, helm template arguments...
  local description="$1" message="$2"
  shift 2
  if output=$(helm template criers "$chart" "$@" 2>&1); then
    echo "FAIL: accepted $description" >&2
    exit 1
  fi
  grep -qF -- "$message" <<<"$output" || { echo "FAIL: $description lacked '$message': $output" >&2; exit 1; }
  echo "ok   [rejected]: $description"
}

helm lint --strict "$chart"
helm lint --strict "$chart" -f "$chart/ci/existing-claim-values.yaml"
render default
render existing-claim -f "$chart/ci/existing-claim-values.yaml"
render storage-class -f "$chart/ci/storage-class-values.yaml"
# Helm allows release names up to 53 characters.
long_release=$(printf 'r%.0s' $(seq 1 53))
helm template "$long_release" "$chart" > "$out/max-name.yaml"
for variant in default existing-claim storage-class max-name; do
  kubeconform -strict -summary -kubernetes-version "$k8s_version" "$out/$variant.yaml"
done

deployment='select(.kind == "Deployment")'
pod="$deployment | .spec.template.spec"
container="$pod | .containers[0]"
claim='select(.kind == "PersistentVolumeClaim")'
services='select(.kind == "Service")'
ingest='select(.kind == "Service" and .metadata.name == "criers")'
ops='select(.kind == "Service" and .metadata.name == "criers-ops")'
config='select(.kind == "ConfigMap") | .data["criers.toml"]'

check "exactly one replica, replaced with Recreate" "$deployment" '.spec.replicas == 1 and .spec.strategy.type == "Recreate"' default
check "selector matches pod labels" "$deployment" '((.spec.selector.matchLabels | to_entries) - (.spec.template.metadata.labels | to_entries) | length) == 0' default
check "Services select the pod" "$services" '(.spec.selector | length) == 2 and .spec.selector["app.kubernetes.io/name"] == "criers" and .spec.selector["app.kubernetes.io/instance"] == "criers"' default
check "UID/GID 65532 and fsGroup" "$pod" '.securityContext.runAsUser == 65532 and .securityContext.runAsGroup == 65532 and .securityContext.fsGroup == 65532 and .securityContext.runAsNonRoot == true' default
check "read-only root, no privilege escalation, capabilities dropped" "$container" '.securityContext.readOnlyRootFilesystem == true and .securityContext.allowPrivilegeEscalation == false and .securityContext.capabilities.drop[0] == "ALL" and (.securityContext.capabilities.drop | length) == 1' default
check "no service account token or service links" "$pod" '.automountServiceAccountToken == false and .enableServiceLinks == false' default
check "Secrets are a projected directory" "$pod | .volumes[] | select(.name == \"secrets\")" '.projected.sources[0].secret.name == "criers-secrets"' default
check "no mount uses subPath" "$container | .volumeMounts[]" 'has("subPath") | not' default
check "secret directory mounted where the config expects it" "$container | .volumeMounts[] | select(.name == \"secrets\")" '.mountPath == "/run/secrets/criers" and .readOnly == true' default
check "probes use the ops port" "$container" '.startupProbe.httpGet.path == "/health/live" and .livenessProbe.httpGet.path == "/health/live" and .readinessProbe.httpGet.path == "/health/ready" and .startupProbe.httpGet.port == "ops" and .livenessProbe.httpGet.port == "ops" and .readinessProbe.httpGet.port == "ops"' default
check "grace exceeds both 12s shutdown phases" "$pod" '.terminationGracePeriodSeconds >= 30' default
check "image defaults to the chart appVersion" "$container" '.image == "ghcr.io/notes-supply/criers:0.1.0"' default
check "created claim fences with ReadWriteOncePod and is kept" "$claim" '.spec.accessModes[0] == "ReadWriteOncePod" and (.spec.accessModes | length) == 1 and .metadata.annotations["helm.sh/resource-policy"] == "keep"' default
check "data volume uses the created claim" "$pod | .volumes[] | select(.name == \"data\")" '.persistentVolumeClaim.claimName == "criers-data"' default
check "ingest Service exposes the ingest port" "$ingest" '.spec.ports[0].targetPort == "ingest" and .spec.ports[0].port == 8080' default
check "ops Service is ClusterIP and scrapeable" "$ops" '.spec.type == "ClusterIP" and .spec.ports[0].targetPort == "ops" and .metadata.annotations["prometheus.io/scrape"] == "true" and .metadata.annotations["prometheus.io/port"] == "9090" and .metadata.annotations["prometheus.io/path"] == "/metrics"' default
check "default config is shadow mode" "$config" 'test("(?m)^mode = \"shadow\"$")' default
check "default config consumes the Watchdog heartbeat" "$config" 'test("\\[sources\\.alertmanager\\.heartbeat\\]\\nalertname = \"Watchdog\"")' default
check "existing claim mounted" "$pod | .volumes[] | select(.name == \"data\")" '.persistentVolumeClaim.claimName == "criers-history"' existing-claim
check "digest pins the image" "$container" '.image == "ghcr.io/notes-supply/criers@sha256:0000000000000000000000000000000000000000000000000000000000000000"' existing-claim
check "mapped Secret keys and extra Secrets projected" "$pod | .volumes[] | select(.name == \"secrets\")" '.projected.sources[0].secret.items[0].path == "alertmanager-token" and .projected.sources[1].secret.name == "discord-webhooks"' existing-claim
check "extra pod metadata merges with chart-owned keys" "$deployment | .spec.template.metadata" '.labels.team == "alerting" and .labels["app.kubernetes.io/instance"] == "criers" and .annotations["example.com/owner"] == "alerting" and (.annotations["checksum/config"] | length) == 64' existing-claim
check "image pull secrets" "$pod" '(.imagePullSecrets | length) == 1 and .imagePullSecrets[0].name == "registry-credentials"' existing-claim
check "scrape annotations can be disabled" "$ops" '.metadata.annotations == null' existing-claim
check "ReadWriteOnce on a named class, not retained" "$claim" '.spec.accessModes[0] == "ReadWriteOnce" and (.spec.accessModes | length) == 1 and .spec.storageClassName == "example-replicated" and .spec.resources.requests.storage == "5Gi" and .metadata.annotations == null' storage-class
if [ "$(yq ea '[select(.kind == "PersistentVolumeClaim")] | length' "$out/existing-claim.yaml")" != 0 ]; then
  echo "FAIL [existing-claim]: a claim was created" >&2
  exit 1
fi
echo "ok   [existing-claim]: no claim created"

check "53-character release keeps every name within 63 characters" 'select(.kind == "Service" or .kind == "Deployment" or .kind == "ConfigMap" or .kind == "PersistentVolumeClaim")' '(.metadata.name | length) <= 63' max-name
check "53-character release still has a distinct ops Service" 'select(.kind == "Service")' '(.metadata.name | test("-ops$")) or ((.metadata.name + "-ops") | length) <= 63' max-name

reject "podLabels overriding a selector label" "podLabels must not set app.kubernetes.io/instance" --set podLabels.app\\.kubernetes\\.io/instance=other
reject "podAnnotations overriding the config checksum" "podAnnotations must not set checksum/config" --set podAnnotations.checksum/config=fixed
reject "unknown values such as replicas" "additional properties 'replicas' not allowed" --set replicas=2
reject "no Secrets" "at '/secrets': minItems" --set-json 'secrets=[]'
reject "a shared access mode" "at '/persistence/accessMode'" --set persistence.accessMode=ReadWriteMany
reject "a mutable tag in the digest field" "at '/image/digest'" --set image.digest=latest
reject "equal container ports" "ports.ingest and ports.ops must differ" --set ports.ops=8080

# The generic Flux example: valid Flux resources whose values the chart accepts.
flux="$chart/../../deploy/flux/criers.yaml"
kubeconform -strict -summary -kubernetes-version "$k8s_version" -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  "$flux"
# Flux merges valuesFrom first and inline values over it, like these -f files.
yq 'select(.kind == "ConfigMap") | .data["values.yaml"]' "$flux" > "$out/flux-configmap-values.yaml"
yq 'select(.kind == "HelmRelease") | .spec.values' "$flux" > "$out/flux-values.yaml"
render flux-example -f "$out/flux-configmap-values.yaml" -f "$out/flux-values.yaml"
check "Flux example values render a claim on the example class" "$claim" '.spec.storageClassName == "example-replicated"' flux-example
yq '.config' "$out/flux-configmap-values.yaml" > "$out/flux-source.toml"
yq 'select(.kind == "ConfigMap") | .data["criers.toml"]' "$out/flux-example.yaml" > "$out/flux-rendered.toml"
if ! cmp -s "$out/flux-source.toml" "$out/flux-rendered.toml"; then
  echo "FAIL [flux-example]: rendered criers.toml differs from the values ConfigMap" >&2
  exit 1
fi
echo "ok   [flux-example]: values ConfigMap TOML reaches criers.toml byte for byte"

helm package "$chart" --destination "$out" > /dev/null
echo "packaged $out/criers-$(yq '.version' "$chart/Chart.yaml").tgz"
