//! Cluster components (kubernetes.md §3.4, §4.3, §5, §6), feature `cluster`:
//!
//! - [`KubeView`]: the cached [`ClusterView`] (namespace labels, LimitRanges,
//!   ResourceQuotas, Kueue objects, Nodes, RuntimeClasses), 30 s TTL,
//!   stale-while-revalidate so the admission path rarely waits on the API;
//! - [`leader`]: Lease-based leader election;
//! - [`watcher`]: outcomes on container termination (RunRecords);
//! - [`prefetch`]: input metadata after admission, under `[k8s.inputs.allow]`;
//! - [`prom`]: Prometheus client and `rsz history import k8s`;
//! - [`live`]: `rsz k8s restore` against live objects, `rsz apply --watch`;
//! - [`node_agent`]: the optional DaemonSet (cgroup v2 peaks), with the
//!   webhook feature (its HTTP endpoint).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kube::api::{Api, ApiResource, DynamicObject, ListParams};
use serde_json::Value;

use crate::view::{ClusterInputs, ClusterView};

pub mod leader;
pub mod live;
#[cfg(feature = "webhook")]
pub mod node_agent;
pub mod prefetch;
pub mod prom;
pub mod watcher;

/// The Kubernetes client.
pub type Client = kube::Client;

/// Pods.
pub type Pod = k8s_openapi::api::core::v1::Pod;

/// An `ApiResource` from apiVersion, kind and plural.
pub fn api_resource(api_version: &str, kind: &str, plural: &str) -> ApiResource {
    let (group, version) = api_version.rsplit_once('/').map(|(g, v)| (g.to_string(), v.to_string())).unwrap_or((String::new(), api_version.to_string()));
    ApiResource { group, version, api_version: api_version.to_string(), kind: kind.to_string(), plural: plural.to_string() }
}

/// The plural of a kind (the kinds this adapter knows; else lowercase + `s`).
pub fn plural(kind: &str) -> String {
    match kind {
        "Pod" => "pods".into(),
        k => format!("{}s", k.to_lowercase()),
    }
}

/// A dynamic API for `(apiVersion, kind)` in `ns` (or cluster-scoped).
pub fn dynamic(client: &Client, api_version: &str, kind: &str, ns: Option<&str>) -> Api<DynamicObject> {
    let ar = api_resource(api_version, kind, &plural(kind));
    match ns {
        Some(n) => Api::namespaced_with(client.clone(), n, &ar),
        None => Api::all_with(client.clone(), &ar),
    }
}

/// JSON of a dynamic object, with `apiVersion` and `kind` filled.
pub fn to_json(o: &DynamicObject, api_version: &str, kind: &str) -> Value {
    let mut v = serde_json::to_value(o).unwrap_or(Value::Null);
    if let Some(m) = v.as_object_mut() {
        m.entry("apiVersion").or_insert_with(|| Value::String(api_version.into()));
        m.entry("kind").or_insert_with(|| Value::String(kind.into()));
    }
    v
}

/// List objects as JSON; a missing resource type (CRD not installed) is empty.
pub async fn list_json(client: &Client, api_version: &str, kind: &str, ns: Option<&str>, lp: &ListParams) -> Result<Vec<Value>, kube::Error> {
    match dynamic(client, api_version, kind, ns).list(lp).await {
        Ok(l) => Ok(l.items.iter().map(|o| to_json(o, api_version, kind)).collect()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(vec![]),
        Err(e) => Err(e),
    }
}

/// Get one object as JSON (`None` when absent).
pub async fn get_json(client: &Client, api_version: &str, kind: &str, ns: Option<&str>, name: &str) -> Result<Option<Value>, kube::Error> {
    match dynamic(client, api_version, kind, ns).get_opt(name).await {
        Ok(o) => Ok(o.map(|o| to_json(&o, api_version, kind))),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(e),
    }
}

#[derive(Clone)]
struct Entry<T> {
    value: T,
    at: Instant,
}

#[derive(Clone, Default)]
struct NsInputs {
    found: bool,
    labels: BTreeMap<String, String>,
    limit_ranges: Vec<Value>,
    resource_quotas: Vec<Value>,
    local_queues: Vec<Value>,
}

#[derive(Clone, Default)]
struct GlobalInputs {
    nodes: Vec<Value>,
    cluster_queues: Vec<Value>,
    resource_flavors: Vec<Value>,
    runtime_classes: Vec<Value>,
}

/// The kube-backed, cached [`ClusterView`].
#[derive(Clone)]
pub struct KubeView {
    client: Client,
    ttl: Duration,
    ns: Arc<Mutex<HashMap<String, Entry<NsInputs>>>>,
    global: Arc<Mutex<Option<Entry<GlobalInputs>>>>,
    refreshing: Arc<Mutex<std::collections::HashSet<String>>>,
}

const KUEUE_VERSIONS: [&str; 2] = ["kueue.x-k8s.io/v1beta2", "kueue.x-k8s.io/v1beta1"];

async fn kueue_list(client: &Client, kind: &str, ns: Option<&str>) -> Vec<Value> {
    for v in KUEUE_VERSIONS {
        if let Ok(xs) = list_json(client, v, kind, ns, &ListParams::default()).await
            && !xs.is_empty()
        {
            return xs;
        }
    }
    vec![]
}

impl KubeView {
    /// A view with the 30 s TTL of kubernetes.md §5.
    pub fn new(client: Client) -> Self {
        KubeView { client, ttl: Duration::from_secs(30), ns: Default::default(), global: Default::default(), refreshing: Default::default() }
    }

    /// With a cache TTL.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// The client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    async fn fetch_ns(client: &Client, ns: &str) -> Result<NsInputs, String> {
        let e = |e: kube::Error| e.to_string();
        let nso = get_json(client, "v1", "Namespace", None, ns).await.map_err(e)?;
        let Some(nso) = nso else { return Ok(NsInputs::default()) };
        let labels = crate::objects::string_map(&nso, "/metadata/labels");
        let lp = ListParams::default();
        let (lr, rq, lq) = futures::join!(
            list_json(client, "v1", "LimitRange", Some(ns), &lp),
            list_json(client, "v1", "ResourceQuota", Some(ns), &lp),
            kueue_list(client, "LocalQueue", Some(ns)),
        );
        Ok(NsInputs { found: true, labels, limit_ranges: lr.map_err(e)?, resource_quotas: rq.map_err(e)?, local_queues: lq })
    }

    async fn fetch_global(client: &Client) -> Result<GlobalInputs, String> {
        let lp = ListParams::default();
        let (nodes, cq, rf, rc) = futures::join!(
            list_json(client, "v1", "Node", None, &lp),
            kueue_list(client, "ClusterQueue", None),
            kueue_list(client, "ResourceFlavor", None),
            list_json(client, "node.k8s.io/v1", "RuntimeClass", None, &lp),
        );
        Ok(GlobalInputs { nodes: nodes.map_err(|e| e.to_string())?, cluster_queues: cq, resource_flavors: rf, runtime_classes: rc.unwrap_or_default() })
    }

    fn spawn_refresh(&self, key: &str) {
        {
            let Ok(mut r) = self.refreshing.lock() else { return };
            if !r.insert(key.to_string()) {
                return;
            }
        }
        let me = self.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            if key == "\0global" {
                if let Ok(g) = Self::fetch_global(&me.client).await
                    && let Ok(mut s) = me.global.lock()
                {
                    *s = Some(Entry { value: g, at: Instant::now() });
                }
            } else if let Ok(n) = Self::fetch_ns(&me.client, &key).await
                && let Ok(mut s) = me.ns.lock()
            {
                s.insert(key.clone(), Entry { value: n, at: Instant::now() });
            }
            if let Ok(mut r) = me.refreshing.lock() {
                r.remove(&key);
            }
        });
    }

    /// Forget everything (tests: after changing LimitRanges/quotas).
    pub fn invalidate(&self) {
        if let Ok(mut s) = self.ns.lock() {
            s.clear();
        }
        if let Ok(mut g) = self.global.lock() {
            *g = None;
        }
    }
}

#[async_trait]
impl ClusterView for KubeView {
    async fn inputs(&self, namespace: &str) -> Result<ClusterInputs, String> {
        let cached_ns = self.ns.lock().ok().and_then(|s| s.get(namespace).cloned());
        let nsi = match cached_ns {
            Some(e) if e.at.elapsed() < self.ttl => e.value,
            Some(e) => {
                self.spawn_refresh(namespace);
                e.value
            }
            None => {
                let v = Self::fetch_ns(&self.client, namespace).await?;
                if let Ok(mut s) = self.ns.lock() {
                    s.insert(namespace.to_string(), Entry { value: v.clone(), at: Instant::now() });
                }
                v
            }
        };
        let cached_g = self.global.lock().ok().and_then(|g| g.clone());
        let g = match cached_g {
            Some(e) if e.at.elapsed() < self.ttl => e.value,
            Some(e) => {
                self.spawn_refresh("\0global");
                e.value
            }
            None => {
                let v = Self::fetch_global(&self.client).await?;
                if let Ok(mut s) = self.global.lock() {
                    *s = Some(Entry { value: v.clone(), at: Instant::now() });
                }
                v
            }
        };
        Ok(ClusterInputs {
            namespace_found: nsi.found,
            namespace_labels: nsi.labels,
            limit_ranges: nsi.limit_ranges,
            resource_quotas: nsi.resource_quotas,
            local_queues: nsi.local_queues,
            nodes: g.nodes,
            cluster_queues: g.cluster_queues,
            resource_flavors: g.resource_flavors,
            runtime_classes: g.runtime_classes,
        })
    }
}

/// Select rustls' process-wide crypto provider (ring). Several crates in
/// the dependency graph enable both ring and aws-lc-rs, so rustls cannot
/// pick one by itself. Idempotent.
pub fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A client from `KUBECONFIG` / in-cluster config (kube's inference).
pub async fn client() -> Result<Client, String> {
    install_crypto();
    Client::try_default().await.map_err(|e| format!("kubernetes client: {e}"))
}
