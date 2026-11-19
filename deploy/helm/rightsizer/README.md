# rightsizer Helm chart

Webhook + leader-elected watcher (one Deployment, 2 replicas, PDB), optional
node agent DaemonSet, `MutatingWebhookConfiguration` (`jobs.rightsizer.io`,
`pods.rightsizer.io`; `failurePolicy: Ignore`, `timeoutSeconds: 2`,
`reinvocationPolicy: IfNeeded`, `sideEffects: NoneOnDryRun`), PrometheusRule
and a Grafana dashboard. See `docs/spec/kubernetes.md`.

```bash
helm install rightsizer deploy/helm/rightsizer -n rightsizer --create-namespace \
  --set cluster=prod-us-east-1 \
  --set history.postgres.url="$PG_URL" \
  --set watcher.prometheusUrl=http://prometheus-server.monitoring.svc
kubectl label ns ml rightsizer.io/enabled=true      # default mode: recommend
```

## TLS

- **Self-signed bootstrap (default):** Helm generates a CA and a serving
  certificate at install (`genCA`/`genSignedCert`), stores them in
  `<release>-tls` and puts the CA in `caBundle`. Upgrades keep them
  (`lookup`). `helm template` (no cluster) generates fresh ones each time.
- **cert-manager:** `--set tls.certManager.enabled=true` creates a
  self-signed `Issuer` (or uses `tls.certManager.issuerRef`) and a
  `Certificate`; the CA injector fills `caBundle`
  (`cert-manager.io/inject-ca-from`). The webhook reloads the mounted
  certificate within 10 s of a rotation.

An expired certificate fails open silently (`failurePolicy: Ignore`): the
`RightsizerWebhookFailOpen` alert watches the apiserver's
`apiserver_admission_webhook_fail_open_count`.

## Signing key

`<release>-signing-key` (generated once, kept on uninstall) holds the HMAC
key of the signed `rightsizer.io/prior` annotation. Rotate with two active
key ids:

```bash
rsz webhook rotate-key --secret rightsizer/rightsizer-signing-key
```

The webhook reloads the mounted key file on change; `rsz k8s restore` needs
the same key file (`[k8s] signing_key_file`).

## History

`history.postgres.url` (or `existingSecret` with key `url`) for postgres;
`history.uri` for an object store (`s3://bucket/prefix?region=…`), with
credentials through `history.env`. The schema is migrated by an operator
command once (`rsz control mode-ceiling enforce` with operator credentials).
Enforce needs a durable, shared store.

## RBAC (least privilege, kubernetes.md §7)

get/list/watch on pods, namespaces, nodes, LimitRanges, ResourceQuotas,
Jobs/CronJobs and the listed CRDs, Kueue queues/flavors, RuntimeClasses;
Leases in the release namespace. No write access to user objects.

## GitOps

Job-level mutation makes the live object differ from Git. Either keep the
webhook in `recommend` for GitOps-managed namespaces and run `rsz apply` in
CI, or ignore the fields rightsizer owns:

```yaml
# Argo CD Application
spec:
  ignoreDifferences:
    - group: batch
      kind: Job
      jqPathExpressions:
        - .spec.template.spec.containers[].resources
        - .spec.template.spec.initContainers[]?.resources
        - .metadata.annotations["rightsizer.io/prior"]
        - .metadata.annotations["rightsizer.io/decision-id"]
        - .metadata.annotations["rightsizer.io/recommendation"]
    - group: batch
      kind: CronJob
      jqPathExpressions:
        - .spec.jobTemplate.spec.template.spec.containers[].resources
    - group: jobset.x-k8s.io
      kind: JobSet
      jqPathExpressions:
        - .spec.replicatedJobs[].template.spec.template.spec.containers[].resources
    - group: ray.io
      kind: RayCluster
      jqPathExpressions:
        - .spec.headGroupSpec.template.spec.containers[].resources
        - .spec.workerGroupSpecs[].template.spec.containers[].resources
        - .spec.workerGroupSpecs[].rayStartParams
```

## kubectl plugin

`kubectl rsz …` runs `kubectl-rsz` from `PATH`. Symlink (or copy) the `rsz`
binary: `ln -s "$(command -v rsz)" ~/.local/bin/kubectl-rsz`, then
`kubectl rsz shadow -n ml --since 14d`, `kubectl rsz k8s restore cronjob/x -n ml`.

## Tests

`tests/integration/k8s/helm_test.sh` (lint + template assertions, no
cluster); `RSZ_IT_K8S=1 tests/integration/k8s/run.sh d` installs this chart
on the local `rsz-it` cluster.
