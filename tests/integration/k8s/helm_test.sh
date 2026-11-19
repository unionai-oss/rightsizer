#!/usr/bin/env bash
# helm lint + helm template checks for deploy/helm/rightsizer (no cluster needed).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="${ROOT}/deploy/helm/rightsizer"
STATE="${ROOT}/tests/infra/.state"
export HELM_CACHE_HOME="${STATE}/helm/cache" HELM_CONFIG_HOME="${STATE}/helm/config" HELM_DATA_HOME="${STATE}/helm/data"
PY="${ROOT}/.venv/bin/python"; [[ -x "$PY" ]] || PY=python3
RSZ="${RSZ_BIN:-${ROOT}/target/debug/rsz}"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
fails=0
ok() { echo "PASS $*"; }
ko() { echo "FAIL $*"; fails=$((fails + 1)); }

helm lint "$CHART" --set cluster=c >/dev/null && ok "helm lint" || ko "helm lint"
helm template rs "$CHART" -n rightsizer >/dev/null 2>&1 && ko "cluster is required" || ok "cluster is required"
for variant in default certmanager full; do
  case "$variant" in
    default) args=() ;;
    certmanager) args=(--set tls.certManager.enabled=true) ;;
    full) args=(--set nodeAgent.enabled=true --set prometheusRule.enabled=true --set grafanaDashboard.enabled=true --set serviceMonitor.enabled=true --set history.postgres.url=postgres://u:p@db/rsz) ;;
  esac
  helm template rs "$CHART" -n rightsizer --set cluster=c "${args[@]}" >"$TMP/$variant.yaml" || { ko "template $variant"; continue; }
  if "$PY" -I - "$TMP/$variant.yaml" "$variant" "$TMP" <<'PYEOF'
import json, sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
variant, tmp = sys.argv[2], sys.argv[3]
kinds = {d["kind"] for d in docs}
by = lambda k: [d for d in docs if d["kind"] == k]
mwc = by("MutatingWebhookConfiguration")[0]
names = [w["name"] for w in mwc["webhooks"]]
assert names == ["jobs.rightsizer.io", "pods.rightsizer.io"], names
for w in mwc["webhooks"]:
    assert w["failurePolicy"] == "Ignore" and w["timeoutSeconds"] == 2, w
    assert w["reinvocationPolicy"] == "IfNeeded" and w["sideEffects"] == "NoneOnDryRun", w
    assert w["admissionReviewVersions"] == ["v1"]
    assert w["namespaceSelector"] == {"matchLabels": {"rightsizer.io/enabled": "true"}}
    assert w["objectSelector"]["matchExpressions"][0]["values"] == ["off"]
    if variant == "certmanager":
        assert "caBundle" not in w["clientConfig"]
    else:
        assert w["clientConfig"]["caBundle"]
if variant == "certmanager":
    assert {"Certificate", "Issuer"} <= kinds and "cert-manager.io/inject-ca-from" in mwc["metadata"]["annotations"]
dep = by("Deployment")[0]
assert dep["spec"]["replicas"] == 2
assert by("PodDisruptionBudget")[0]["spec"]["minAvailable"] == 1
role = by("Role")[0]
assert any("leases" in r["resources"] for r in role["rules"])
cr = [c for c in by("ClusterRole") if not c["metadata"]["name"].endswith("node-agent")][0]
verbs = {v for r in cr["rules"] for v in r["verbs"]}
assert verbs <= {"get", "list", "watch"}, f"least privilege: {verbs}"
if variant == "full":
    rules = {r["alert"]: r["expr"] for g in by("PrometheusRule")[0]["spec"]["groups"] for r in g["rules"]}
    assert 'apiserver_admission_webhook_fail_open_count{name=~"jobs.rightsizer.io|pods.rightsizer.io"} > 0' in rules["RightsizerWebhookFailOpen"]
    assert "rightsizer_outcome_join_ratio < 0.9" in rules["RightsizerOutcomeJoinRatioLow"]
    assert "21600" in rules["RightsizerCompactionLag"]
    assert "rightsizer_estimator_circuit_open" in rules["RightsizerEstimatorCircuitStuckOpen"]
    assert "rightsizer_breaker_open" in rules["RightsizerFastBurnBreakerOpen"]
    ds = by("DaemonSet")[0]["spec"]["template"]["spec"]
    assert ds["containers"][0]["resources"]["requests"] == {"cpu": "10m", "memory": "40Mi"}
    assert ds["hostPID"] is False and ds["volumes"][0]["hostPath"]["path"] == "/sys/fs/cgroup"
    json.loads([c for c in by("ConfigMap") if "rightsizer.json" in c["data"]][0]["data"]["rightsizer.json"])
cfg = [c for c in by("ConfigMap") if "rightsizer.toml" in c["data"]][0]["data"]["rightsizer.toml"]
open(f"{tmp}/{variant}.toml", "w").write(cfg)
print(f"{variant}: {len(docs)} objects")
PYEOF
  then ok "template $variant"; else ko "template $variant"; fi
  if [[ -x "$RSZ" ]]; then
    RIGHTSIZER_CONFIG="$TMP/$variant.toml" "$RSZ" config validate >/dev/null 2>&1 && ok "rsz config validate ($variant)" || ko "rsz config validate ($variant)"
  fi
done
exit $((fails > 0))
