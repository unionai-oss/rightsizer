#!/usr/bin/env bash
# k3d (K3s) integration cluster "rsz-it" with optional add-ons.
# See tests/infra/README.md. Never touches ~/.kube/config or other clusters.
set -euo pipefail
RSZ_COMPONENT=k3s
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

CLUSTER="${RSZ_IT_K3S_CLUSTER:-rsz-it}"
K3S_IMAGE="${RSZ_IT_K3S_IMAGE:-rancher/k3s:v1.34.3-k3s1}"
API_PORT="${RSZ_IT_K3S_API_PORT:-16443}"
PROM_PORT="${RSZ_IT_PROM_PORT:-19090}"   # host port -> NodePort 30909 (Prometheus)
PROM_NODEPORT=30909
export KUBECONFIG="${STATE_DIR}/${CLUSTER}.kubeconfig"

# Pinned add-on versions (each published >= 2 weeks before pinning).
PROM_CHART_VERSION="29.40.3"      # prometheus-community/prometheus
KUBERAY_VERSION="1.7.1"           # ray-project/kuberay-helm kuberay-operator
KUEUE_VERSION="v0.20.4"
JOBSET_VERSION="v0.13.0"
TRAINER_V2_VERSION="v2.3.0"       # kubeflow/trainer: TrainJob CRDs (trainer.kubeflow.org)
TRAINING_OPERATOR_VERSION="v1.9.4" # kubeflow/trainer v1 line: PyTorchJob CRDs (kubeflow.org)
VOLCANO_VERSION="v1.16.0"

# Helm must not read or write the user's repo config/cache.
export HELM_CACHE_HOME="${STATE_DIR}/helm/cache"
export HELM_CONFIG_HOME="${STATE_DIR}/helm/config"
export HELM_DATA_HOME="${STATE_DIR}/helm/data"

assert_ours "${CLUSTER}"

cluster_exists() { k3d cluster list -o json 2>/dev/null | jq -e --arg n "$CLUSTER" 'any(.[]; .name == $n)' >/dev/null; }
kc() { kubectl --kubeconfig "${KUBECONFIG}" "$@"; }
hm() { helm --kubeconfig "${KUBECONFIG}" "$@"; }

require_cluster() {
  cluster_exists || die "cluster '${CLUSTER}' does not exist; run: tests/infra/k3s.sh up"
  [[ -s "${KUBECONFIG}" ]] || write_kubeconfig
}

write_kubeconfig() {
  k3d kubeconfig get "${CLUSTER}" >"${KUBECONFIG}.tmp"
  mv "${KUBECONFIG}.tmp" "${KUBECONFIG}"
  chmod 600 "${KUBECONFIG}"
}

# ---------------------------------------------------------------- base cluster
cmd_up() {
  need k3d kubectl jq; need_docker
  if cluster_exists; then
    log "cluster ${CLUSTER} already exists"
    if ! k3d cluster list -o json | jq -e --arg n "$CLUSTER" '.[] | select(.name==$n) | .serversRunning > 0' >/dev/null; then
      log "starting stopped cluster ${CLUSTER}"
      k3d cluster start "${CLUSTER}"
    fi
  else
    require_ports_free "${API_PORT}" "${PROM_PORT}"
    docker image inspect "${K3S_IMAGE}" >/dev/null 2>&1 || warn "${K3S_IMAGE} not present locally; k3d will pull it (~310 MB)"
    log "creating k3d cluster ${CLUSTER} (1 server + 1 agent, API 127.0.0.1:${API_PORT})"
    k3d cluster create "${CLUSTER}" \
      --image "${K3S_IMAGE}" \
      --servers 1 --agents 1 \
      --api-port "127.0.0.1:${API_PORT}" \
      --no-lb \
      --kubeconfig-update-default=false \
      --kubeconfig-switch-context=false \
      --k3s-arg "--disable=traefik@server:0" \
      --k3s-arg "--disable=metrics-server@server:0" \
      --k3s-arg "--disable=servicelb@server:0" \
      -p "127.0.0.1:${PROM_PORT}:${PROM_NODEPORT}@server:0:direct" \
      --wait --timeout 180s
  fi
  write_kubeconfig
  wait_until 180 "nodes Ready" kc wait --for=condition=Ready nodes --all --timeout=5s
  wait_until 180 "coredns Ready" kc -n kube-system rollout status deploy/coredns --timeout=5s
  wait_until 60 "host.k3d.internal in CoreDNS NodeHosts" \
    sh -c "kubectl --kubeconfig '${KUBECONFIG}' -n kube-system get cm coredns -o jsonpath='{.data.NodeHosts}' | grep -q host.k3d.internal"
  wait_until 60 "default ServiceAccount" kc -n default get serviceaccount default
  ok "cluster ${CLUSTER} up; export KUBECONFIG=${KUBECONFIG}"
}

cmd_down() {
  need k3d
  if cluster_exists; then
    log "deleting k3d cluster ${CLUSTER}"
    k3d cluster delete "${CLUSTER}"
  else
    log "cluster ${CLUSTER} does not exist"
  fi
  rm -f "${KUBECONFIG}"
  ok "cluster ${CLUSTER} down"
}

cmd_status() {
  need k3d jq
  if ! cluster_exists; then echo "k3s: cluster ${CLUSTER}: DOWN"; return 0; fi
  local running
  running="$(k3d cluster list -o json | jq -r --arg n "$CLUSTER" '.[] | select(.name==$n) | "\(.serversRunning)/\(.serversCount) servers, \(.agentsRunning)/\(.agentsCount) agents"')"
  echo "k3s: cluster ${CLUSTER}: UP (${running}); KUBECONFIG=${KUBECONFIG}"
  [[ -s "${KUBECONFIG}" ]] || write_kubeconfig
  kc get nodes --no-headers 2>/dev/null | awk '{print "  node " $1 " " $2}' || true
  local addons=()
  hm status rsz-it-prometheus -n rsz-it-monitoring >/dev/null 2>&1 && addons+=("prometheus(http://127.0.0.1:${PROM_PORT})")
  hm status kuberay-operator -n rsz-it-kuberay >/dev/null 2>&1 && addons+=("kuberay")
  kc get ns kueue-system >/dev/null 2>&1 && addons+=("kueue")
  kc get crd jobsets.jobset.x-k8s.io >/dev/null 2>&1 && addons+=("jobset")
  kc get crd trainjobs.trainer.kubeflow.org >/dev/null 2>&1 && addons+=("trainer-v2-crds")
  kc get crd pytorchjobs.kubeflow.org >/dev/null 2>&1 && addons+=("pytorchjob-crds")
  kc get crd jobs.batch.volcano.sh >/dev/null 2>&1 && addons+=("volcano-crds")
  echo "  add-ons: ${addons[*]:-none}"
}

# -------------------------------------------------- host-reachability check
# Proves both pods and the API server can reach an HTTPS server on the host at
# host.k3d.internal, which is how a dev build of the admission webhook is served.
cmd_webhook_check() {
  require_cluster; need openssl python3
  local port="${RSZ_IT_WEBHOOK_PORT:-$(free_port 18443)}"
  require_ports_free "$port"
  # Globals (not locals): the EXIT trap runs after this function's scope is gone.
  WHC_DIR="${STATE_DIR}/webhook-check"; WHC_NS=rsz-it-webhook-check; WHC_VWC=rsz-it-webhook-check; WHC_PID=""
  local dir="$WHC_DIR" ns="$WHC_NS" vwc="$WHC_VWC"
  rm -rf "$dir"; mkdir -p "$dir"
  trap webhook_check_cleanup EXIT

  log "generating throwaway CA + serving cert for host.k3d.internal"
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=rsz-it-ca" \
    -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=host.k3d.internal" \
    -keyout "$dir/tls.key" -out "$dir/tls.csr" 2>/dev/null
  printf 'subjectAltName=DNS:host.k3d.internal,DNS:localhost,IP:127.0.0.1\n' >"$dir/ext.cnf"
  openssl x509 -req -in "$dir/tls.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -days 1 -extfile "$dir/ext.cnf" -out "$dir/tls.crt" 2>/dev/null

  log "starting host HTTPS server on 127.0.0.1:${port}"
  python3 "${INFRA_DIR}/common/webhook_echo.py" --port "$port" --cert "$dir/tls.crt" --key "$dir/tls.key" \
    >/dev/null 2>"$dir/server.log" &
  WHC_PID=$!
  wait_until 15 "host webhook server" curl -sf --cacert "$dir/ca.crt" "https://localhost:${port}/healthz"

  local host_ip
  host_ip="$(docker exec "k3d-${CLUSTER}-server-0" sh -c 'grep host.k3d.internal /etc/hosts' | awk '{print $1}' | head -1 || true)"
  log "host.k3d.internal in server node /etc/hosts -> ${host_ip:-<missing>}"

  kc delete ns "$ns" --ignore-not-found --wait=true --timeout=90s >/dev/null  # leftover from a previous run
  kc create ns "$ns" >/dev/null
  kc label ns "$ns" rsz-it/webhook-check=true >/dev/null
  # 1) pod -> host
  ensure_image curlimages/curl:8.5.0
  log "pod -> host: curl https://host.k3d.internal:${port}/healthz"
  kc -n "$ns" run curl --image=curlimages/curl:8.5.0 --image-pull-policy=IfNotPresent --restart=Never \
    --command -- curl -sS -k --max-time 10 --retry 30 --retry-delay 2 --retry-all-errors \
    "https://host.k3d.internal:${port}/healthz" >/dev/null
  kc -n "$ns" wait --for=jsonpath='{.status.phase}'=Succeeded pod/curl --timeout=90s >/dev/null \
    || { kc -n "$ns" logs curl || true; die "pod could not reach host.k3d.internal:${port}"; }
  ok "pod reached host: $(kc -n "$ns" logs curl | tr -d '\n')"

  # 2) apiserver -> host (a real ValidatingWebhookConfiguration with caBundle)
  log "apiserver -> host: registering ValidatingWebhookConfiguration ${vwc}"
  kc apply -f - >/dev/null <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: ${vwc}
webhooks:
  - name: check.rsz-it.dev
    admissionReviewVersions: ["v1"]
    sideEffects: None
    failurePolicy: Fail
    timeoutSeconds: 5
    namespaceSelector:
      matchLabels: {rsz-it/webhook-check: "true"}
    rules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["configmaps"]
    clientConfig:
      url: https://host.k3d.internal:${port}/validate
      caBundle: $(base64 <"$dir/ca.crt" | tr -d '\n')
EOF
  sleep 2
  kc -n "$ns" create configmap rsz-it-allow-me --from-literal=a=b >/dev/null \
    || die "allowed configmap was rejected (apiserver could not call host webhook?)"
  local out
  out="$(kc -n "$ns" create configmap rsz-it-deny-me --from-literal=a=b 2>&1 || true)"
  if grep -q denied-by-rsz-it-host-webhook <<<"$out"; then
    ok "apiserver called the host webhook: ${out}"
  else
    die "expected denial from host webhook, got: ${out}"
  fi
  grep AdmissionReview "$dir/server.log" | sed 's/^/  host log: /' >&2
  ok "host reachability verified (pods and apiserver); cleaning up"
}

webhook_check_cleanup() {
  [[ -n "${WHC_PID:-}" ]] && kill "${WHC_PID}" 2>/dev/null || true
  kc delete validatingwebhookconfiguration "${WHC_VWC}" --ignore-not-found >/dev/null 2>&1 || true
  kc delete ns "${WHC_NS}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  rm -rf "${WHC_DIR}"
}

ensure_image() {
  local img="$1"
  if docker image inspect "$img" >/dev/null 2>&1; then
    k3d image import -c "${CLUSTER}" "$img" >/dev/null 2>&1 || warn "k3d image import $img failed; cluster will pull it"
  fi
}

# -------------------------------------------------------------- Prometheus
cmd_prometheus() {
  require_cluster; need helm
  case "${1:-up}" in
    up)
      log "installing prometheus-community/prometheus ${PROM_CHART_VERSION} (server + kube-state-metrics, no alertmanager/pushgateway/node-exporter)"
      hm upgrade --install rsz-it-prometheus prometheus \
        --repo https://prometheus-community.github.io/helm-charts --version "${PROM_CHART_VERSION}" \
        --namespace rsz-it-monitoring --create-namespace \
        -f "${INFRA_DIR}/k8s/prometheus-values.yaml" \
        --set server.service.nodePort="${PROM_NODEPORT}" \
        --wait --timeout 10m
      wait_until 120 "Prometheus /-/ready on 127.0.0.1:${PROM_PORT}" curl -sf "http://127.0.0.1:${PROM_PORT}/-/ready"
      ok "Prometheus at http://127.0.0.1:${PROM_PORT}"
      ;;
    down) hm uninstall rsz-it-prometheus -n rsz-it-monitoring --ignore-not-found --wait
          kc delete ns rsz-it-monitoring --ignore-not-found ;;
    status) hm status rsz-it-prometheus -n rsz-it-monitoring 2>/dev/null | grep -E '^(STATUS|CHART)' || echo "prometheus: not installed" ;;
    *) die "usage: k3s.sh prometheus up|down|status" ;;
  esac
}

# ------------------------------------------------------------------ KubeRay
cmd_kuberay() {
  require_cluster; need helm
  case "${1:-up}" in
    up)
      log "installing kuberay-operator ${KUBERAY_VERSION}"
      hm upgrade --install kuberay-operator kuberay-operator \
        --repo https://ray-project.github.io/kuberay-helm/ --version "${KUBERAY_VERSION}" \
        --namespace rsz-it-kuberay --create-namespace --wait --timeout 10m
      kc get crd rayclusters.ray.io rayjobs.ray.io rayservices.ray.io >/dev/null
      ok "kuberay-operator ${KUBERAY_VERSION} ready (CRDs: rayclusters, rayjobs, rayservices)"
      ;;
    down) hm uninstall kuberay-operator -n rsz-it-kuberay --ignore-not-found --wait
          kc delete crd rayclusters.ray.io rayjobs.ray.io rayservices.ray.io rayclusterreplicasets.ray.io --ignore-not-found
          kc delete ns rsz-it-kuberay --ignore-not-found ;;
    *) die "usage: k3s.sh kuberay up|down" ;;
  esac
}

# -------------------------------------------------------------------- Kueue
cmd_kueue() {
  require_cluster
  local url="https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
  case "${1:-up}" in
    up)
      log "applying Kueue ${KUEUE_VERSION}"
      kc apply --server-side -f "$url" >/dev/null
      kc -n kueue-system rollout status deploy/kueue-controller-manager --timeout=300s
      ok "Kueue ${KUEUE_VERSION} ready"
      ;;
    down) kc delete -f "$url" --ignore-not-found >/dev/null; ok "Kueue removed" ;;
    *) die "usage: k3s.sh kueue up|down" ;;
  esac
}

# -------------------------------------------------------------------- CRDs
# For admission tests the webhook only needs the CRD schemas to exist, so
# training-operator / trainer / volcano are CRD-only. JobSet gets its (small)
# controller so a JobSet actually produces Jobs and Pods.
crd_urls() {
  local gh=https://raw.githubusercontent.com
  case "$1" in
    jobset) echo "https://github.com/kubernetes-sigs/jobset/releases/download/${JOBSET_VERSION}/manifests.yaml" ;;
    trainer)
      local b="${gh}/kubeflow/trainer/${TRAINER_V2_VERSION}/manifests/base/crds"
      echo "${b}/trainer.kubeflow.org_trainjobs.yaml ${b}/trainer.kubeflow.org_trainingruntimes.yaml ${b}/trainer.kubeflow.org_clustertrainingruntimes.yaml" ;;
    pytorchjob) echo "${gh}/kubeflow/trainer/${TRAINING_OPERATOR_VERSION}/manifests/base/crds/kubeflow.org_pytorchjobs.yaml" ;;
    volcano)
      local b="${gh}/volcano-sh/volcano/${VOLCANO_VERSION}/installer/helm/chart/volcano/crd/bases"
      echo "${b}/batch.volcano.sh_jobs.yaml ${b}/scheduling.volcano.sh_podgroups.yaml ${b}/scheduling.volcano.sh_queues.yaml" ;;
    *) die "unknown CRD set '$1' (jobset|trainer|pytorchjob|volcano|kuberay|all)" ;;
  esac
}

cmd_crds() {
  require_cluster
  local action="${1:-up}"; shift || true
  local sets=("$@"); ((${#sets[@]})) || sets=(all)
  [[ "${sets[0]}" == all ]] && sets=(jobset trainer pytorchjob volcano kuberay)
  local s u
  for s in "${sets[@]}"; do
    if [[ "$s" == kuberay ]]; then
      if [[ "$action" == up ]]; then cmd_kuberay up; else cmd_kuberay down; fi
      continue
    fi
    for u in $(crd_urls "$s"); do
      case "$action" in
        up) kc apply --server-side -f "$u" >/dev/null ;;
        down) kc delete -f "$u" --ignore-not-found >/dev/null ;;
        *) die "usage: k3s.sh crds up|down [jobset|trainer|pytorchjob|volcano|kuberay|all]..." ;;
      esac
    done
    if [[ "$s" == jobset && "$action" == up ]]; then
      kc -n jobset-system rollout status deploy/jobset-controller-manager --timeout=300s
    fi
    ok "crds ${action}: ${s}"
  done
}

# -------------------------------------------------------- image import
cmd_import() {
  require_cluster
  (($#)) || die "usage: k3s.sh import IMAGE..."
  k3d image import -c "${CLUSTER}" "$@"
  ok "imported into ${CLUSTER}: $*"
}

usage() {
  cat >&2 <<EOF
usage: tests/infra/k3s.sh <command>
  up | down | status              base cluster ${CLUSTER} (kubeconfig: ${KUBECONFIG})
  webhook-check                   prove pods + apiserver reach an HTTPS server on the host
  prometheus up|down|status       Prometheus + kube-state-metrics + cAdvisor scraping
  kuberay up|down                 KubeRay operator ${KUBERAY_VERSION}
  kueue up|down                   Kueue ${KUEUE_VERSION}
  crds up|down [set...]           jobset|trainer|pytorchjob|volcano|kuberay|all
  import IMAGE...                 k3d image import into ${CLUSTER}
  kubeconfig                      print the kubeconfig path
EOF
  exit 2
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  webhook-check) cmd_webhook_check ;;
  prometheus) shift; cmd_prometheus "$@" ;;
  kuberay) shift; cmd_kuberay "$@" ;;
  kueue) shift; cmd_kueue "$@" ;;
  crds) shift; cmd_crds "$@" ;;
  import) shift; cmd_import "$@" ;;
  kubeconfig) echo "${KUBECONFIG}" ;;
  *) usage ;;
esac
