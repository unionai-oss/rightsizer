#!/usr/bin/env bash
# K8s integration suite on the k3d cluster `rsz-it` (tests/infra/k3s.sh).
#
#   RSZ_IT_K8S=1 tests/integration/k8s/run.sh [setup|a|b|c|d|e|f|all|teardown]...
#
# Prerequisites (idempotent; `setup` runs them):
#   tests/infra/k3s.sh up && k3s.sh prometheus up && k3s.sh kueue up &&
#   k3s.sh crds up jobset pytorchjob && k3s.sh kuberay up
#   cargo build -p rightsizer-cli                (host binary for the dev loop)
#   tests/infra/build-linux.sh && build-linux.sh image   (scenario d)
#
# Scenarios (docs/IMPLEMENTATION-NOTES.md, rightsizer-k8s, has the latest results):
#   a  dev loop: webhook served from the host via host.k3d.internal
#   b  watcher: OOM and success RunRecords, join ratio
#   c  rsz apply --watch ladder
#   d  in-cluster Helm deploy (self-signed certs, 2 replicas) + subset of a/b
#   e  latency: 500+ admissions, p50/p99 from the webhook histogram
#   f  rsz k8s restore round trip
# Results: tests/infra/.state/k8s-it/results.tsv (PASS/FAIL, name, evidence).
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "${RSZ_IT_K8S:-}" != 1 ]]; then
  echo "skipped: set RSZ_IT_K8S=1 to run the K8s integration suite" >&2
  exit 0
fi

M="${IT_DIR}/manifests"

recreate_ns() { # recreate_ns NS MODE|-
  kc delete ns "$1" --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
  if [[ "$2" == - ]]; then kc create ns "$1" >/dev/null; else ns_enable "$1" "$2"; fi
}

res_of() { # res_of KIND/NAME NS JSONPATH-TO-CONTAINER
  kc -n "$2" get "$1" -o "jsonpath={$3.resources}"
}

# ================================================================ setup
helm_it() {
  HELM_CACHE_HOME="${INFRA}/.state/helm/cache" HELM_CONFIG_HOME="${INFRA}/.state/helm/config" HELM_DATA_HOME="${INFRA}/.state/helm/data" \
    helm --kubeconfig "${KUBECONFIG}" "$@"
}

do_setup() {
  : >"${RESULTS}"
  # A previous scenario d leaves the in-cluster release; the dev loop needs it gone.
  helm_it uninstall rightsizer -n rightsizer --wait >/dev/null 2>&1 || true
  kc delete mutatingwebhookconfiguration rightsizer --ignore-not-found >/dev/null 2>&1 || true
  "${INFRA}/k3s.sh" up >/dev/null
  "${INFRA}/k3s.sh" prometheus up >/dev/null
  "${INFRA}/k3s.sh" kueue up >/dev/null
  "${INFRA}/k3s.sh" crds up jobset pytorchjob >/dev/null
  "${INFRA}/k3s.sh" kuberay up >/dev/null
  "${INFRA}/k3s.sh" import busybox:latest python:3.12-slim >/dev/null
  setup_pg
  reset_pg
  setup_certs_and_keys
  write_config
  rsz control mode-ceiling enforce >/dev/null # operator role: migrates the schema
}

start_dev_loop() {
  start_host_webhook
  register_webhook "https://host.k3d.internal:${WEBHOOK_PORT}" "${STATE}/certs/ca.crt"
}

# ================================================================ a: dev loop
scenario_a() {
  # History is seeded before the webhook starts: an import by another
  # process is not seen by a running webhook's history cache and memo until
  # their TTLs (10 min) pass; outcomes written by the watcher invalidate both.
  stop_host_webhook
  recreate_ns rsz-it-a recommend
  recreate_ns rsz-it-enf enforce
  recreate_ns rsz-it-lr enforce
  recreate_ns rsz-it-rq enforce
  recreate_ns rsz-it-kq enforce
  recreate_ns rsz-it-crd enforce
  recreate_ns rsz-it-ray enforce
  seed_history k8s://rsz-it/rsz-it-enf/job/train/main 6 300
  seed_history k8s://rsz-it/rsz-it-enf/job/grow/main 6 900
  seed_history k8s://rsz-it/rsz-it-lr/job/grow/main 6 900
  seed_history k8s://rsz-it/rsz-it-rq/job/grow/main 6 900
  recreate_ns rsz-it-rql enforce
  seed_history k8s://rsz-it/rsz-it-rql/job/growlim/main 6 900
  kc -n rsz-it-rql create quota tight --hard=limits.memory=600Mi >/dev/null
  seed_history k8s://rsz-it/rsz-it-kq/job/kq/main 6 300
  seed_history k8s://rsz-it/rsz-it-crd/jobset/js/workers/main 6 300
  seed_history k8s://rsz-it/rsz-it-crd/pytorchjob/pt/Master/pytorch 6 300
  seed_history k8s://rsz-it/rsz-it-ray/ray/rc/cpu 6 300
  seed_history k8s://rsz-it/rsz-it-enf/cronjob/nightly/main 6 300
  kc -n rsz-it-lr apply -f "$M/limitrange.yaml" >/dev/null
  kc -n rsz-it-rq create quota tight --hard=requests.memory=400Mi,limits.memory=4Gi >/dev/null
  kc apply -f "$M/kueue.yaml" >/dev/null
  start_dev_loop

  # a1 recommend → annotation-only
  kc -n rsz-it-a create -f "$M/job-small.yaml" >/dev/null
  local rec res
  rec="$(ann job/small rsz-it-a rightsizer.io/recommendation)"
  res="$(res_of job/small rsz-it-a .spec.template.spec.containers[0])"
  check a1.recommend-annotation-only "res=${res} rec=${rec:0:80}" \
    test -n "$rec" -a -z "$(ann job/small rsz-it-a rightsizer.io/prior)" -a "$res" = '{"limits":{"memory":"1Gi"},"requests":{"cpu":"500m","memory":"1Gi"}}'

  # a2 enforce with history → reduced (signed prior), and increased
  kc -n rsz-it-enf create -f "$M/job-train.yaml" >/dev/null
  kc -n rsz-it-enf create -f "$M/job-grow.yaml" >/dev/null
  local mtrain mgrow prior
  mtrain="$(jp job/train rsz-it-enf '{.spec.template.spec.containers[0].resources.requests.memory}')"
  mgrow="$(jp job/grow rsz-it-enf '{.spec.template.spec.containers[0].resources.requests.memory}')"
  prior="$(ann job/train rsz-it-enf rightsizer.io/prior)"
  check a2.enforce-reduce "train memory 2Gi -> ${mtrain}, prior=${prior:0:24}…" test "$mtrain" != 2Gi -a -n "$prior"
  check a2.enforce-increase "grow memory 256Mi -> ${mgrow}" python3 -I -c 'import sys; u={"Mi":2**20,"Gi":2**30}; v=sys.argv[1]; sys.exit(0 if int(v[:-2])*u[v[-2:]] > 256*2**20 else 1)' "$mgrow"
  kc -n rsz-it-enf get job train -o json >"${STATE}/train-enforced.json"

  # a3 LimitRange rejection → annotation-only
  kc -n rsz-it-lr create -f "$M/job-grow.yaml" >/dev/null
  res="$(res_of job/grow rsz-it-lr .spec.template.spec.containers[0])"
  rec="$(ann job/grow rsz-it-lr rightsizer.io/recommendation)"
  check a3.limitrange-annotation-only "res=${res} rejected=$(metric 'rightsizer_webhook_requests_total.*result="rejected"') rec=${rec:0:70}" \
    test "$res" = '{"requests":{"cpu":"250m","memory":"256Mi"}}' -a -n "$rec" -a -z "$(ann job/grow rsz-it-lr rightsizer.io/prior)"

  # a3b ResourceQuota rejection → annotation-only
  kc -n rsz-it-rq create -f "$M/job-grow.yaml" >/dev/null
  res="$(res_of job/grow rsz-it-rq .spec.template.spec.containers[0])"
  check a3.quota-not-exceeded "res=${res}" python3 -I -c 'import json,sys; r=json.loads(sys.argv[1])["requests"]["memory"]; u={"Mi":2**20,"Gi":2**30}; sys.exit(0 if int(r[:-2])*u[r[-2:]] <= 400*2**20 else 1)' "$res"

  # a3c ResourceQuota on limits: the increase raises the limit past the quota → rejected, annotation-only
  kc -n rsz-it-rql create -f "$M/job-growlim.yaml" >/dev/null
  res="$(res_of job/growlim rsz-it-rql .spec.template.spec.containers[0])"
  check a3.quota-rejection-annotation-only "res=${res} rejected=$(metric 'rightsizer_webhook_requests_total.*result="rejected"') rec=$(ann job/growlim rsz-it-rql rightsizer.io/recommendation | cut -c1-90)" \
    test "$res" = '{"limits":{"memory":"512Mi"},"requests":{"cpu":"250m","memory":"256Mi"}}' -a -n "$(ann job/growlim rsz-it-rql rightsizer.io/recommendation)" -a -z "$(ann job/growlim rsz-it-rql rightsizer.io/prior)"

  # a4 Kueue: sized at Job CREATE, pods untouched, no GPU type change
  kc -n rsz-it-kq create -f "$M/job-kueue.yaml" >/dev/null
  local jmem
  jmem="$(jp job/kq rsz-it-kq '{.spec.template.spec.containers[0].resources.requests.memory}')"
  wait_for 90 "kueue pod" sh -c "kubectl --kubeconfig '${KUBECONFIG}' -n rsz-it-kq get pods -l job-name=kq -o name | grep -q pod"
  local pod pmem pprior wl
  pod="$(kc -n rsz-it-kq get pods -l job-name=kq -o name | head -1)"
  pmem="$(jp "$pod" rsz-it-kq '{.spec.containers[0].resources.requests.memory}')"
  pprior="$(ann "$pod" rsz-it-kq rightsizer.io/prior)"
  wl="$(kc -n rsz-it-kq get workloads.kueue.x-k8s.io -o jsonpath='{.items[0].spec.podSets[0].template.spec.containers[0].resources.requests.memory}' 2>/dev/null)"
  check a4.kueue-sized-at-job-create "job ${jmem}, workload ${wl}, pod ${pmem}, pod prior='${pprior}', nodeSelector='$(jp "$pod" rsz-it-kq '{.spec.nodeSelector}')'" \
    test "$jmem" != 2Gi -a "$pmem" = "$jmem" -a "$wl" = "$jmem" -a -z "$pprior"

  # a5 JobSet and PyTorchJob CRDs: patches the apiserver accepts
  local jsout ptout
  jsout="$(kc -n rsz-it-crd create -f "$M/jobset.yaml" 2>&1)"
  ptout="$(kc -n rsz-it-crd create -f "$M/pytorchjob.yaml" 2>&1)"
  local jsmem ptmem
  jsmem="$(jp jobset/js rsz-it-crd '{.spec.replicatedJobs[0].template.spec.template.spec.containers[0].resources.requests.memory}')"
  ptmem="$(jp pytorchjob/pt rsz-it-crd '{.spec.pytorchReplicaSpecs.Master.template.spec.containers[0].resources.requests.memory}')"
  check a5.jobset-accepted "${jsout}; memory 2Gi -> ${jsmem}; decision=$(ann jobset/js rsz-it-crd rightsizer.io/decision-id)" test -n "$(ann jobset/js rsz-it-crd rightsizer.io/decision-id)"
  check a5.pytorchjob-accepted "${ptout}; memory 2Gi -> ${ptmem}; decision=$(ann pytorchjob/pt rsz-it-crd rightsizer.io/decision-id)" test -n "$(ann pytorchjob/pt rsz-it-crd rightsizer.io/decision-id)"
  # The JobSet's child Job is not sized again.
  wait_for 60 "jobset child job" sh -c "kubectl --kubeconfig '${KUBECONFIG}' -n rsz-it-crd get jobs -o name | grep -q job"
  local cj
  cj="$(kc -n rsz-it-crd get jobs -o name | head -1)"
  check a5.jobset-child-not-resized "child ${cj} memory $(jp "$cj" rsz-it-crd '{.spec.template.spec.containers[0].resources.requests.memory}'), prior='$(ann "$cj" rsz-it-crd rightsizer.io/prior)'" \
    test -z "$(ann "$cj" rsz-it-crd rightsizer.io/prior)" -a "$(jp "$cj" rsz-it-crd '{.spec.template.spec.containers[0].resources.requests.memory}')" = "$jsmem"

  # a6 KubeRay RayCluster: container resources and rayStartParams together
  kc -n rsz-it-ray create -f "$M/raycluster.yaml" >/dev/null
  local wres wparams
  wres="$(jp raycluster/rc rsz-it-ray '{.spec.workerGroupSpecs[0].template.spec.containers[0].resources}')"
  wparams="$(jp raycluster/rc rsz-it-ray '{.spec.workerGroupSpecs[0].rayStartParams}')"
  check a6.raycluster-consistent "worker resources ${wres}; rayStartParams ${wparams}; head num-cpus $(jp raycluster/rc rsz-it-ray '{.spec.headGroupSpec.rayStartParams.num-cpus}')" \
    python3 -I "${IT_DIR}/check_ray.py" "$wres" "$wparams"

  # a7 Flyte-labelled pods: flyte:// key only in the namespace the template names
  local fp1 fp2 d1 d2 k1 k2
  fp1="$(kc -n flytesnacks-development create -f "$M/pod-flyte.yaml" -o name)"
  fp2="$(kc -n rsz-it-a create -f "$M/pod-flyte.yaml" -o name)"
  d1="$(ann "$fp1" flytesnacks-development rightsizer.io/decision-id)"
  d2="$(ann "$fp2" rsz-it-a rightsizer.io/decision-id)"
  sleep 2
  k1="$(rsz explain --decision "$d1" -o json 2>/dev/null | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["key"])')"
  k2="$(rsz explain --decision "$d2" -o json 2>/dev/null | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["key"])')"
  check a7.flyte-matching-namespace "key ${k1}" test "$k1" = "flyte://testorg/flytesnacks/development/wf.t1"
  check a7.flyte-non-matching-namespace "key ${k2}" sh -c "case '$k2' in k8s://rsz-it/rsz-it-a/pod/*) exit 0;; *) exit 1;; esac"

  # a8 double sizing: a pod with RIGHTSIZER_DECISION is untouched
  kc -n rsz-it-enf create -f "$M/pod-presized.yaml" >/dev/null
  check a8.presized-pod-untouched "annotations='$(jp pod/presized rsz-it-enf '{.metadata.annotations}')' memory=$(jp pod/presized rsz-it-enf '{.spec.containers[0].resources.requests.memory}')" \
    test -z "$(jp pod/presized rsz-it-enf '{.metadata.annotations}')" -a "$(jp pod/presized rsz-it-enf '{.spec.containers[0].resources.requests.memory}')" = 2Gi

  # a9 webhook down → pods still admitted (failurePolicy Ignore), fail-open metric
  stop_host_webhook
  local before after p
  before="$(kc get --raw /metrics | awk '/^apiserver_admission_webhook_fail_open_count/ && /pods.rightsizer.io/ {s+=$NF} END {print s+0}')"
  p="$(kc -n rsz-it-a create -f "$M/pod-plain.yaml" -o name 2>&1)"
  after="$(kc get --raw /metrics | awk '/^apiserver_admission_webhook_fail_open_count/ && /pods.rightsizer.io/ {s+=$NF} END {print s+0}')"
  check a9.webhook-down-fails-open "created ${p}; apiserver_admission_webhook_fail_open_count{name=pods.rightsizer.io} ${before} -> ${after}" \
    sh -c "echo '$p' | grep -q '^pod/' && [ '${after%.*}' -gt '${before%.*}' ]"
  start_host_webhook
}

# ================================================================ b: watcher
scenario_b() {
  [[ -f "${STATE}/webhook.pid" ]] || start_dev_loop
  recreate_ns rsz-it-w recommend
  kc -n rsz-it-w create -f "$M/job-oom.yaml" >/dev/null
  kc -n rsz-it-w create -f "$M/job-steady.yaml" >/dev/null
  local uoom ust
  uoom="$(jp job/oom rsz-it-w '{.metadata.uid}')"
  ust="$(jp job/steady rsz-it-w '{.metadata.uid}')"
  log "waiting for the watcher's records (jobs ~60 s + peak delay)"
  local oom
  oom="$(wait_rec 240 k8s://rsz-it/rsz-it-w/job/oom/main "$uoom")"
  check b1.oom-record "$(echo "$oom" | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print({k: r.get(k) for k in ("status","memory_peak","trust","decision_id","source","invocation_id")})' 2>/dev/null)" \
    python3 -I -c 'import json,sys; r=json.loads(sys.argv[1]); m=r["memory_peak"]; sys.exit(0 if r["status"]=="oom_killed" and m["censor"]=="at_least" and m["value"]>=128*2**20 and r["trust"]=="verified" and r.get("decision_id") else 1)' "$oom"
  local st
  st="$(wait_rec 300 k8s://rsz-it/rsz-it-w/job/steady/main "$ust")"
  check b2.success-peak-from-prometheus "$(echo "$st" | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print({k: r.get(k) for k in ("status","memory_peak","cpu_mean","cpu_peak_window","trust","decision_id")})' 2>/dev/null)" \
    python3 -I -c 'import json,sys; r=json.loads(sys.argv[1]); m=r.get("memory_peak") or {}; sys.exit(0 if r["status"]=="succeeded" and m.get("censor")=="exact" and 95*2**20 <= m.get("value",0) <= 200*2**20 and m.get("resolution_s")==15 and r.get("decision_id") else 1)' "$st"
  local jr
  jr="$(metric 'rightsizer_outcome_join_ratio')"
  check b3.join-ratio-reported "rightsizer_outcome_join_ratio{platform=k8s}=${jr} joined=$(metric 'rightsizer_outcomes_joined_total') recorded=$(metric 'rightsizer_decisions_recorded_total') watcher_lag_s=$(metric '^rightsizer_watcher_lag_seconds') records=$(metric '^rightsizer_watcher_records_total')" \
    python3 -I -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "$jr"
}

scenario_b_import() {
  # The Prometheus importer over the same namespace (kubernetes.md §6.3).
  local out
  out="$(rsz history import k8s --namespace rsz-it-w --since 2h --dry-run 2>"${STATE}/import.log")"
  echo "$out" >"${STATE}/import.jsonl"
  check b4.prometheus-import "$(echo "$out" | python3 -I -c '
import json,sys
for l in sys.stdin:
    r=json.loads(l); m=r.get("memory_peak") or {}
    print(r["key"].rsplit("/",3)[1:], r["status"], r["attempt"], m.get("censor"), round(m.get("value",0)/2**20), "MiB;")' | tr '\n' ' ')" \
    python3 -I -c '
import json,sys
rs=[json.loads(l) for l in sys.argv[1].splitlines() if l.strip()]
by={r["key"].split("/job/")[-1]: r for r in rs if "/job/" in r["key"]}
oom=by.get("oom/main"); st=by.get("steady/main")
ok = oom and oom["status"]=="oom_killed" and oom["memory_peak"]["censor"]=="at_least" and st and st["status"]=="succeeded" and st["memory_peak"]["censor"]=="exact" and 95*2**20 <= st["memory_peak"]["value"] <= 200*2**20 and st["source"]["kind"]=="prometheus" and st["trust"]=="verified"
sys.exit(0 if ok else 1)' "$out"
}

# ================================================================ c: apply --watch
scenario_c() {
  recreate_ns rsz-it-lad - # not enabled: only rsz apply sizes it
  seed_history k8s://rsz-it/rsz-it-lad/job/ladder/main 6 60
  local out rc
  out="$(rsz apply --watch -o json "$M/job-ladder.yaml" 2>"${STATE}/apply-watch.log")"
  rc=$?
  echo "$out" >"${STATE}/apply-watch.json"
  check c1.ladder-oom-then-success "rc=${rc} $(echo "$out" | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print([(a["attempt"],a["job"],a["memory"][0][1],a["status"]) for a in r["attempts"]], r["succeeded"])' 2>/dev/null)" \
    python3 -I -c 'import json,sys; r=json.loads(sys.argv[1]); a=r["attempts"]; sys.exit(0 if r["succeeded"] and len(a)>=2 and a[0]["status"]=="oom_killed" and a[-1]["status"]=="succeeded" and a[1]["job"].endswith("-rsz1") else 1)' "$out"
  local att
  att="$(docker exec "${PG_CONTAINER}" psql -U rsz -d rsz -Atc "select count(*) from rightsizer.attempts where key like '%rsz-it-lad%'" 2>/dev/null)"
  check c2.attempt-records "attempt records in store: ${att}; runs: $(rsz history show k8s://rsz-it/rsz-it-lad/job/ladder/main -o json 2>/dev/null | python3 -I -c 'import json,sys; print([(r["attempt"],r["status"]) for r in json.load(sys.stdin) if not r["invocation_id"].startswith("seed")])')" \
    test "${att:-0}" -ge 1
  local br
  br="$(rsz breaker status 2>&1)"
  check c3.breaker-not-tripped "${br}" sh -c "echo '$br' | grep -q 'no breakers tripped'"
}

# ================================================================ d: in-cluster (Helm)
scenario_d() {
  stop_host_webhook
  kc delete mutatingwebhookconfiguration rightsizer-it --ignore-not-found >/dev/null
  # Seed before the release starts (see scenario a: running webhooks cache history).
  recreate_ns rsz-it-d recommend
  recreate_ns rsz-it-de enforce
  seed_history k8s://rsz-it/rsz-it-de/job/train/main 6 300
  "${INFRA}/build-linux.sh" image >/dev/null
  "${INFRA}/k3s.sh" import rsz-it/rsz:dev >/dev/null
  local H=(helm --kubeconfig "${KUBECONFIG}")
  export HELM_CACHE_HOME="${INFRA}/.state/helm/cache" HELM_CONFIG_HOME="${INFRA}/.state/helm/config" HELM_DATA_HOME="${INFRA}/.state/helm/data"
  "${H[@]}" upgrade --install rightsizer "${ROOT}/deploy/helm/rightsizer" -n rightsizer --create-namespace \
    -f "${IT_DIR}/values-it.yaml" --wait --timeout 180s >"${STATE}/helm.log" 2>&1
  local hrc=$?
  check d0.helm-install "rc=${hrc}; $(kc -n rightsizer get deploy,ds,pdb,svc -o name 2>/dev/null | tr '\n' ' ')" test "$hrc" = 0
  wait_for 120 "2 ready replicas" sh -c "[ \"\$(kubectl --kubeconfig '${KUBECONFIG}' -n rightsizer get deploy rightsizer -o jsonpath='{.status.readyReplicas}')\" = 2 ]"
  local leader
  leader="$(kc -n rightsizer get lease rightsizer-watcher -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)"
  check d1.two-replicas-one-leader "ready=$(kc -n rightsizer get deploy rightsizer -o jsonpath='{.status.readyReplicas}') leader=${leader}" test -n "$leader"
  # subset of a: recommend annotation-only and enforce with history
  kc -n rsz-it-d create -f "$M/job-small.yaml" >/dev/null
  check d2.recommend-in-cluster "rec=$(ann job/small rsz-it-d rightsizer.io/recommendation | cut -c1-80)" test -n "$(ann job/small rsz-it-d rightsizer.io/recommendation)"
  kc -n rsz-it-de create -f "$M/job-train.yaml" >/dev/null
  local m
  m="$(jp job/train rsz-it-de '{.spec.template.spec.containers[0].resources.requests.memory}')"
  check d3.enforce-in-cluster "train memory 2Gi -> ${m}; prior=$(ann job/train rsz-it-de rightsizer.io/prior | cut -c1-24)…" test "$m" != 2Gi
  # subset of b: OOM record from the in-cluster watcher (node agent peaks when present)
  kc -n rsz-it-d create -f "$M/job-oom.yaml" >/dev/null
  kc -n rsz-it-d create -f "$M/job-steady.yaml" >/dev/null
  local uoom ust oom st
  uoom="$(jp job/oom rsz-it-d '{.metadata.uid}')"
  ust="$(jp job/steady rsz-it-d '{.metadata.uid}')"
  oom="$(wait_rec 240 k8s://rsz-it/rsz-it-d/job/oom/main "$uoom")"
  check d4.watcher-in-cluster-oom "$(echo "$oom" | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print({k: r.get(k) for k in ("status","memory_peak","trust","decision_id")})' 2>/dev/null)" \
    python3 -I -c 'import json,sys; r=json.loads(sys.argv[1]); sys.exit(0 if r["status"]=="oom_killed" and r["memory_peak"]["censor"]=="at_least" else 1)' "$oom"
  st="$(wait_rec 300 k8s://rsz-it/rsz-it-d/job/steady/main "$ust")"
  local srcs
  srcs="$(kc get --raw /api/v1/namespaces/rightsizer/services/rightsizer-webhook:metrics/proxy/metrics 2>/dev/null | grep '^rightsizer_watcher_records_total' | tr '\n' ' ')"
  check d5.watcher-in-cluster-success-peak "$(echo "$st" | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print({k: r.get(k) for k in ("status","memory_peak","cpu_mean","cpu_peak_window")})' 2>/dev/null) | replica metrics: ${srcs}" \
    python3 -I -c 'import json,sys; r=json.loads(sys.argv[1]); m=r.get("memory_peak") or {}; sys.exit(0 if r["status"]=="succeeded" and 95*2**20 <= m.get("value",0) <= 200*2**20 else 1)' "$st"
}

# ================================================================ e: latency
scenario_e() {
  # Against the host webhook (dev loop): captured AdmissionReviews posted
  # directly, plus real apiserver round trips (server-side dry run).
  stop_host_webhook
  recreate_ns rsz-it-lat enforce
  seed_history k8s://rsz-it/rsz-it-lat/job/train/main 6 300
  start_dev_loop
  local c0
  c0="$(metric 'rightsizer_webhook_admission_duration_seconds_count')"
  python3 -I "${IT_DIR}/load.py" --url "https://127.0.0.1:${WEBHOOK_PORT}/mutate/jobs" --ca "${STATE}/certs/ca.crt" \
    --namespace rsz-it-lat --manifest "$M/job-train.yaml" -n "${RSZ_IT_LOAD_N:-1000}" -c 8 >"${STATE}/load.json"
  # 100 through the apiserver (dryRun: decided, not recorded).
  local i pids=()
  for i in $(seq 1 100); do
    sed "s/name: train/name: train-$i/" "$M/job-train.yaml" | kc -n rsz-it-lat create --dry-run=server -f - >/dev/null &
    pids+=($!)
    if ((i % 8 == 0)); then wait "${pids[@]}"; pids=(); fi
  done
  ((${#pids[@]})) && wait "${pids[@]}"
  curl -sf "http://127.0.0.1:${HTTP_PORT}/metrics" >"${STATE}/metrics-after-load.txt"
  local q
  q="$(python3 -I "${IT_DIR}/histogram.py" "${STATE}/metrics-after-load.txt" rightsizer_webhook_admission_duration_seconds)"
  echo "$q" >"${STATE}/latency.json"
  check e1.webhook-p99-under-100ms "requests=$(( $(metric 'rightsizer_webhook_admission_duration_seconds_count' | cut -d. -f1) - ${c0%.*} )) ${q} client=$(cat "${STATE}/load.json")" \
    python3 -I -c 'import json,sys; q=json.loads(sys.argv[1]); sys.exit(0 if q["count"]>=500 and q["p99"]<0.1 else 1)' "$q"
}

# ================================================================ f: restore
scenario_f() {
  [[ -s "${STATE}/train-enforced.json" ]] || { fail f1.restore-file "no enforced Job from scenario a"; return; }
  local restored
  restored="$(rsz k8s restore -f "${STATE}/train-enforced.json" -o json 2>"${STATE}/restore.log")"
  check f1.restore-file-round-trip "$(cat "${STATE}/restore.log" | tr '\n' ' ') resources=$(echo "$restored" | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["spec"]["template"]["spec"]["containers"][0]["resources"])')" \
    python3 -I -c '
import json,sys
o=json.loads(sys.argv[1]); r=o["spec"]["template"]["spec"]["containers"][0]["resources"]
a=o["metadata"].get("annotations",{})
ok = r=={"requests":{"cpu":"1","memory":"2Gi"},"limits":{"memory":"2Gi"}} and not any(k.startswith("rightsizer.io/") for k in a)
sys.exit(0 if ok else 1)' "$restored"
  # Live restore on a CronJob (its template is mutable; a Job's is not).
  [[ -f "${STATE}/webhook.pid" ]] || start_dev_loop
  kc -n rsz-it-enf delete cronjob nightly --ignore-not-found >/dev/null
  kc -n rsz-it-enf create -f "$M/cronjob.yaml" >/dev/null
  local before after
  before="$(jp cronjob/nightly rsz-it-enf '{.spec.jobTemplate.spec.template.spec.containers[0].resources}')"
  rsz k8s restore cronjob/nightly -n rsz-it-enf >/dev/null 2>"${STATE}/restore-live.log"
  after="$(jp cronjob/nightly rsz-it-enf '{.spec.jobTemplate.spec.template.spec.containers[0].resources}')"
  check f2.restore-live-cronjob "enforced ${before} -> restored ${after}; prior annotation='$(ann cronjob/nightly rsz-it-enf rightsizer.io/prior | cut -c1-10)'" \
    test "$after" = '{"limits":{"memory":"2Gi"},"requests":{"cpu":"1","memory":"2Gi"}}' -a "$before" != "$after"
  # A Job's template is immutable: live restore refuses with a clear message.
  rsz k8s restore job/train -n rsz-it-enf >/dev/null 2>"${STATE}/restore-job.log"
  check f3.restore-live-job-refused "$(head -c 200 "${STATE}/restore-job.log")" grep -q immutable "${STATE}/restore-job.log"
}

do_teardown() {
  stop_host_webhook
  kc delete mutatingwebhookconfiguration rightsizer-it rightsizer --ignore-not-found >/dev/null 2>&1 || true
  docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
  "${INFRA}/k3s.sh" down
}

for step in "${@:-all}"; do
  case "$step" in
    setup) do_setup ;;
    a) scenario_a ;;
    b) scenario_b; scenario_b_import ;;
    c) scenario_c ;;
    d) scenario_d ;;
    e) scenario_e ;;
    f) scenario_f ;;
    all) do_setup; scenario_a; scenario_b; scenario_b_import; scenario_c; scenario_e; scenario_f; scenario_d ;;
    teardown) do_teardown ;;
    *) echo "unknown step $step" >&2; exit 2 ;;
  esac
done
echo "results: ${RESULTS}" >&2
column -t -s $'\t' "${RESULTS}" >&2 || cat "${RESULTS}" >&2
! grep -q '^FAIL' "${RESULTS}"
