# Criers Helm chart

Runs one Criers replica with persistent SQLite storage. The default
configuration is shadow mode: decisions are recorded and nothing is sent.

## Prerequisites

- Kubernetes 1.29 or newer.
- Linux/amd64 nodes for the current release image.
- A StorageClass whose CSI driver supports `ReadWriteOncePod`, which keeps a
  second pod off the volume. Without it, set `persistence.accessMode` to
  `ReadWriteOnce`; that only limits mounting to one node, so rely on the
  `Recreate` strategy and Criers' lock file to avoid a second writer.
- Existing Secrets holding the files your configuration references. The chart
  never creates Secrets. The default configuration expects one Secret named
  `criers-secrets` with keys `alertmanager-token` and, for live mode,
  `discord-ops-url`:

  ```sh
  kubectl create namespace criers
  kubectl -n criers create secret generic criers-secrets \
    --from-literal=alertmanager-token="$(head -c 32 /dev/urandom | base64)"
  ```

## Install and upgrade

The chart and image are published to GHCR by the release workflow. Before a
release exists, install from a checkout with `./charts/criers` instead of the
OCI reference and set `image.repository` to an image you built.

```sh
helm install criers oci://ghcr.io/notes-supply/charts/criers --version 0.1.0 \
  --namespace criers -f my-values.yaml
helm upgrade criers oci://ghcr.io/notes-supply/charts/criers --version 0.1.0 \
  --namespace criers -f my-values.yaml
```

Put the complete Criers TOML in `config` (see `docs/operations.md`). Paths must
use the mounts: Secrets under `/run/secrets/criers/` and the database under
`/var/lib/criers/`. The listeners must use `ports.ingest` and `ports.ops`
(8080 and 9090 by default). A configuration change rolls the pod. Pin
`image.digest` for an immutable image. `persistence.existingClaim` reuses a
claim you manage.

Upgrades replace the pod with `Recreate`: the old pod stops before the new one
starts, so ingestion is briefly unavailable and Alertmanager retries. Keep
`terminationGracePeriodSeconds` above twice `server.shutdown_grace`.

Uninstalling keeps the created claim and its history
(`persistence.retainOnUninstall`). Delete it explicitly if you no longer need it.

## Endpoints and metrics

With the release name `criers` used above:

- `criers:8080` receives Alertmanager webhooks at
  `/v1/sources/<id>/alertmanager`.
- `criers-ops:9090` serves probes and `/metrics`. It is always
  ClusterIP and unauthenticated; restrict who can reach it with NetworkPolicy
  where your cluster enforces it.

For another release name, `helm get notes <release>` prints the Service URLs.

For plain Prometheus, scrape the ops Service as a static target, or use
`kubernetes_sd_configs` with the `prometheus.io/scrape`, `port` and `path`
annotations the chart adds (`metrics.scrapeAnnotations`).

## Publication

GHCR creates a new package as private, even for a public repository. After the
first release, a maintainer must set both `criers` and `charts/criers` to public
in each package's settings before anonymous pulls work. Until then, clusters
need registry credentials (`imagePullSecrets` and an authenticated chart source).

`mise run chart` lints, renders, validates and packages the chart.
