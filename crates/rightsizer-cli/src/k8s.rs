//! Kubernetes subcommands (feature `k8s`; kubernetes.md):
//!
//! - `rsz webhook serve | manifests | certs | rotate-key`
//! - `rsz node-agent`
//! - `rsz k8s restore -f FILE|- | KIND/NAME [-n NS]`
//! - `rsz apply FILE|- [-f OUT] [--diff] [--watch]` (`-p k8s` documents)
//! - `rsz history import k8s --namespace NS --since 30d`
//!
//! `kubectl rsz …` works through a `kubectl-rsz` symlink (or copy) of the
//! binary on `PATH`: kubectl execs it with the remaining arguments, and
//! clap ignores `argv[0]`.

use std::io::Read as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use clap::{Parser, Subcommand};
use rightsizer_core::Mode;
use rightsizer_engine::store::Role;
use rightsizer_engine::{Config, Engine, Policy};
use rightsizer_k8s::admit::{AdmitOptions, Admitter, InputCache};
use rightsizer_k8s::cluster::{self, KubeView, leader, live, prefetch, prom, watcher};
use rightsizer_k8s::manifests::{self, Endpoint, ManifestOptions};
use rightsizer_k8s::render::{Output, RenderRequest};
use rightsizer_k8s::view::ClusterView;
use rightsizer_k8s::{K8sAdapter, runtime, webhook};
use rightsizer_plugin::{AdapterEnv, Native, Patch, PlatformModel};
use serde_json::Value;

use crate::{Fail, Format, Res};

/// Whether `rsz <name>` is a Kubernetes subcommand.
pub fn handles(args: &[String]) -> bool {
    matches!(args.first().map(String::as_str), Some("webhook" | "k8s" | "kubernetes" | "node-agent"))
}

#[derive(Parser, Debug)]
#[command(name = "rsz", no_binary_name = true)]
struct K8sCli {
    /// Output format (also accepted after the subcommand).
    #[arg(long, short = 'o', global = true, value_enum)]
    output: Option<Format>,
    #[command(subcommand)]
    cmd: Top,
}

#[derive(Subcommand, Debug)]
enum Top {
    /// The admission webhook (kubernetes.md §3).
    Webhook {
        #[command(subcommand)]
        action: WebhookCmd,
    },
    /// Kubernetes helpers.
    #[command(alias = "kubernetes")]
    K8s {
        #[command(subcommand)]
        action: K8sCmd,
    },
    /// The node agent DaemonSet process (kubernetes.md §6.2).
    NodeAgent(NodeAgentArgs),
}

#[derive(Subcommand, Debug)]
enum WebhookCmd {
    /// Serve the webhook (and, unless --no-watcher, the leader-elected watcher).
    Serve(ServeArgs),
    /// Print the MutatingWebhookConfiguration (kubernetes.md §3.2).
    Manifests {
        /// Base URL the apiserver calls (`https://host:port`); default: the Service.
        #[arg(long, conflicts_with = "service")]
        url: Option<String>,
        /// Service `NAMESPACE/NAME[:PORT]`.
        #[arg(long)]
        service: Option<String>,
        /// PEM CA bundle file for `caBundle`.
        #[arg(long)]
        ca_file: Option<PathBuf>,
        /// cert-manager Certificate `NAMESPACE/NAME` to inject the CA from.
        #[arg(long)]
        cert_manager: Option<String>,
        /// Object name.
        #[arg(long, default_value = "rightsizer")]
        name: String,
        /// Only the M2 kinds (no TrainJob, Ray, Volcano).
        #[arg(long)]
        m2_only: bool,
    },
    /// Write a self-signed CA and serving certificate (bootstrap without cert-manager).
    Certs {
        /// DNS names (SANs) of the serving certificate.
        #[arg(long = "dns", required = true)]
        dns: Vec<String>,
        /// Output directory (`ca.crt`, `tls.crt`, `tls.key`).
        #[arg(long)]
        out: PathBuf,
    },
    /// Rotate the prior-signing key: a new active key, the previous one kept for verification.
    RotateKey {
        /// Key file (`kid:base64` lines; created if missing).
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Also (or instead) update this Secret `NAMESPACE/NAME` (data key `signing.key`).
        #[arg(long)]
        secret: Option<String>,
        /// Keys kept (the new one included).
        #[arg(long, default_value_t = 2)]
        keep: usize,
    },
}

#[derive(clap::Args, Debug)]
struct ServeArgs {
    /// HTTPS listen address (default `[k8s] webhook_listen`).
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Plain HTTP listener for /metrics, /healthz, /readyz (optional).
    #[arg(long)]
    http_listen: Option<SocketAddr>,
    /// Certificate (default `[k8s] tls_cert`).
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    /// Key (default `[k8s] tls_key`).
    #[arg(long)]
    tls_key: Option<PathBuf>,
    /// Do not run the watcher.
    #[arg(long)]
    no_watcher: bool,
    /// Run only the watcher (no admission server; metrics on --http-listen).
    #[arg(long)]
    watcher_only: bool,
    /// Prometheus for peaks (default `[k8s] prometheus_url`).
    #[arg(long)]
    prometheus: Option<String>,
    /// cAdvisor scrape interval, s.
    #[arg(long, default_value_t = 15)]
    scrape_interval: u32,
    /// Wait after termination before reading Prometheus peaks, s (default 2 × scrape).
    #[arg(long)]
    peak_delay: Option<u64>,
    /// Namespace of the node agent pods (enables the node agent as peak source).
    #[arg(long)]
    node_agent_namespace: Option<String>,
    /// Label selector of the node agent pods.
    #[arg(long, default_value = "app.kubernetes.io/component=node-agent")]
    node_agent_selector: String,
    /// Node agent port.
    #[arg(long, default_value_t = 9102)]
    node_agent_port: u16,
    /// Node agent URL template (`http://{node}:9102`) instead of pod discovery.
    #[arg(long)]
    node_agent_url: Option<String>,
    /// Namespace of the watcher Lease (default `[k8s] lease_namespace`, `POD_NAMESPACE`, `default`).
    #[arg(long)]
    lease_namespace: Option<String>,
    /// No leader election (single replica / dev).
    #[arg(long)]
    no_leader_election: bool,
    /// Cluster-input cache TTL, s.
    #[arg(long, default_value_t = 30)]
    view_ttl: u64,
}

#[derive(clap::Args, Debug)]
struct NodeAgentArgs {
    /// Listen address.
    #[arg(long, default_value = "0.0.0.0:9102")]
    listen: SocketAddr,
    /// This node (default `NODE_NAME`).
    #[arg(long, env = "NODE_NAME")]
    node_name: String,
    /// cgroup v2 root.
    #[arg(long, default_value = "/sys/fs/cgroup")]
    cgroup_root: PathBuf,
    /// Poll interval, ms.
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
}

#[derive(Subcommand, Debug)]
enum K8sCmd {
    /// Reverse webhook mutations from the signed `rightsizer.io/prior`.
    Restore {
        /// Manifest file or `-` (prints the restored objects).
        #[arg(short = 'f', long = "filename")]
        file: Option<String>,
        /// A live object `KIND/NAME` (restored in place with your credentials).
        target: Option<String>,
        /// Namespace of the live object.
        #[arg(short = 'n', long, default_value = "default")]
        namespace: String,
        /// Print what would change; do not write.
        #[arg(long)]
        dry_run: bool,
    },
}

fn cfg_k8s(cfg: &Config) -> &rightsizer_engine::config::K8sConfig {
    &cfg.k8s
}

async fn kube_client() -> Result<cluster::Client, Fail> {
    cluster::client().await.map_err(Fail::other)
}

fn adapter_for(cfg: &Config, need_keys: bool) -> Result<K8sAdapter, Fail> {
    let a = runtime::adapter(cfg_k8s(cfg)).map_err(Fail::config)?;
    if need_keys && cfg.k8s.signing_key_file.is_none() {
        return Err(Fail::config("[k8s] signing_key_file is required (RIGHTSIZER__K8S__SIGNING_KEY_FILE)"));
    }
    Ok(a)
}

/// `rsz webhook …`, `rsz k8s …`, `rsz node-agent`.
pub async fn platform(cfg: &Config, args: Vec<String>, out: Format) -> Res {
    let cli = K8sCli::try_parse_from(&args).map_err(|e| {
        let _ = e.print();
        Fail { code: if e.use_stderr() { 3 } else { 0 }, msg: String::new() }
    })?;
    let out = cli.output.unwrap_or(out);
    match cli.cmd {
        Top::Webhook { action: WebhookCmd::Serve(a) } => serve(cfg, a).await,
        Top::Webhook { action: WebhookCmd::Manifests { url, service, ca_file, cert_manager, name, m2_only } } => {
            let endpoint = match (url, service) {
                (Some(u), _) => Endpoint::Url(u),
                (None, Some(s)) => {
                    let (ns, rest) = s.split_once('/').ok_or_else(|| Fail::parse("--service NAMESPACE/NAME[:PORT]"))?;
                    let (n, port) = rest.split_once(':').map(|(n, p)| (n, p.parse().unwrap_or(443))).unwrap_or((rest, 443));
                    Endpoint::Service { namespace: ns.into(), name: n.into(), port }
                }
                (None, None) => ManifestOptions::default().endpoint,
            };
            let ca = ca_file.map(|p| std::fs::read_to_string(&p).map_err(|e| Fail::parse(format!("{}: {e}", p.display())))).transpose()?;
            let o = ManifestOptions { name, endpoint, ca_bundle_pem: ca, inject_ca_from: cert_manager, m3_kinds: !m2_only, ..Default::default() };
            let v = manifests::mutating_webhook_configuration(&o);
            match out {
                Format::Json => println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default()),
                _ => print!("{}", yaml_safe(&serde_norway::to_string(&v).unwrap_or_default())),
            }
            Ok(())
        }
        Top::Webhook { action: WebhookCmd::Certs { dns, out: dir } } => {
            let (ca, crt, key) = webhook::self_signed(&dns).map_err(Fail::other)?;
            std::fs::create_dir_all(&dir).map_err(Fail::other)?;
            for (f, c) in [("ca.crt", &ca), ("tls.crt", &crt), ("tls.key", &key)] {
                std::fs::write(dir.join(f), c).map_err(Fail::other)?;
            }
            eprintln!("wrote ca.crt, tls.crt, tls.key to {} (SANs: {})", dir.display(), dns.join(", "));
            Ok(())
        }
        Top::Webhook { action: WebhookCmd::RotateKey { key_file, secret, keep } } => rotate_key(cfg, key_file, secret, keep).await,
        Top::NodeAgent(a) => {
            let client = kube_client().await?;
            let o = cluster::node_agent::AgentOptions {
                node: a.node_name,
                cgroup_root: a.cgroup_root,
                interval: Duration::from_millis(a.interval_ms.max(100)),
                retain: Duration::from_secs(900),
                listen: a.listen,
            };
            cluster::node_agent::run(client, o).await.map_err(Fail::other)
        }
        Top::K8s { action: K8sCmd::Restore { file, target, namespace, dry_run } } => restore(cfg, file, target, &namespace, dry_run, out).await,
    }
}

/// Quote YAML 1.1 booleans-in-disguise (`off`, `on`, `yes`, `no`) that a
/// YAML 1.2 writer leaves plain: kubectl's YAML 1.1 reader would make them bools.
fn yaml_safe(s: &str) -> String {
    let words = ["off", "on", "yes", "no", "y", "n", "Off", "On", "Yes", "No"];
    s.lines()
        .map(|l| {
            let t = l.trim_start();
            let ind = &l[..l.len() - t.len()];
            match t.strip_prefix("- ") {
                Some(w) if words.contains(&w) => format!("{ind}- '{w}'"),
                _ => match t.split_once(": ") {
                    Some((k, w)) if words.contains(&w) => format!("{ind}{k}: '{w}'"),
                    _ => l.to_string(),
                },
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn random_key() -> Result<Vec<u8>, Fail> {
    let mut b = vec![0u8; 32];
    std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b)).map_err(|e| Fail::other(format!("/dev/urandom: {e}")))?;
    Ok(b)
}

fn rotated(existing: &str, keep: usize) -> Result<String, Fail> {
    let kid = format!("k{}", chrono::Utc::now().format("%Y%m%d%H%M%S"));
    let b64 = base64::engine::general_purpose::STANDARD.encode(random_key()?);
    let mut lines = vec![format!("{kid}:{b64}")];
    for l in existing.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
        let l = if l.contains(':') { l.to_string() } else { format!("k0:{l}") };
        if lines.len() < keep.max(1) {
            lines.push(l);
        }
    }
    let text = lines.join("\n") + "\n";
    rightsizer_k8s::SigningKeys::parse(&text).map_err(Fail::other)?;
    Ok(text)
}

async fn rotate_key(cfg: &Config, key_file: Option<PathBuf>, secret: Option<String>, keep: usize) -> Res {
    let key_file = key_file.or(cfg.k8s.signing_key_file.clone());
    if key_file.is_none() && secret.is_none() {
        return Err(Fail::parse("rotate-key needs --key-file or --secret NAMESPACE/NAME"));
    }
    if let Some(p) = &key_file {
        let old = std::fs::read_to_string(p).unwrap_or_default();
        let text = rotated(&old, keep)?;
        std::fs::write(p, &text).map_err(|e| Fail::other(format!("{}: {e}", p.display())))?;
        let k = rightsizer_k8s::SigningKeys::parse(&text).map_err(Fail::other)?;
        eprintln!("{}: active key {}, {} key(s) verify", p.display(), k.active, k.keys.len());
    }
    if let Some(s) = secret {
        use k8s_openapi::api::core::v1::Secret;
        let (ns, name) = s.split_once('/').ok_or_else(|| Fail::parse("--secret NAMESPACE/NAME"))?;
        let client = kube_client().await?;
        let api: kube::Api<Secret> = kube::Api::namespaced(client, ns);
        let mut sec = api.get(name).await.map_err(|e| Fail::other(format!("secret {s}: {e}")))?;
        let old = sec.data.as_ref().and_then(|d| d.get("signing.key")).map(|b| String::from_utf8_lossy(&b.0).to_string()).unwrap_or_default();
        let text = rotated(&old, keep)?;
        sec.data.get_or_insert_with(Default::default).insert("signing.key".into(), k8s_openapi::ByteString(text.into_bytes()));
        api.replace(name, &Default::default(), &sec).await.map_err(|e| Fail::other(format!("secret {s}: {e}")))?;
        eprintln!("secret {s}: rotated (the webhook reloads the mounted file within a minute)");
    }
    Ok(())
}

async fn serve(cfg: &Config, a: ServeArgs) -> Res {
    let k = cfg_k8s(cfg);
    let engine = Engine::builder(cfg.clone()).role(Role::Client).build().await.map_err(Fail::config)?;
    if let Some(e) = engine.config_error() {
        eprintln!("rsz: warning: {e}; every admission keeps the prior (S1)");
    }
    let shared = rightsizer_k8s::ledger_shared::SharedLedger::new();
    let adapter = runtime::adapter(k).map_err(Fail::config)?.with_ledger(shared.clone());
    if k.signing_key_file.is_none() {
        eprintln!("rsz: warning: no [k8s] signing_key_file: enforce falls back to annotation-only patches");
    }
    shared.spawn_refresh(engine.clone(), Duration::from_secs(3));
    let client = kube_client().await?;
    let view = KubeView::new(client.clone()).with_ttl(Duration::from_secs(a.view_ttl));
    let base = runtime::settings_map(k);
    let opts = AdmitOptions {
        deadline: Duration::from_millis(k.webhook_deadline_ms.clamp(50, 10_000) as u64),
        exclude_namespaces: k.exclude_namespaces.clone(),
        namespaces: k.namespaces.clone(),
        require_enabled_label: true,
    };
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let cache = Arc::new(InputCache::default());
    let admitter = Arc::new(
        Admitter::new(engine.clone(), adapter, base.clone(), Arc::new(view.clone()) as Arc<dyn ClusterView>, opts).with_prefetch(tx, cache.clone()).with_shared_ledger(shared),
    );
    tokio::spawn(prefetch::run(rx, cache, engine.clone(), k.inputs.allow.clone(), cfg.features.allow_hosts.clone()));
    if let Some(p) = &k.signing_key_file {
        webhook::spawn_key_reload(admitter.clone(), p.clone(), Duration::from_secs(10));
    }
    if !a.no_watcher {
        let prom = a.prometheus.clone().or(k.prometheus_url.clone()).map(|u| prom::Prom::new(&u)).transpose().map_err(Fail::config)?;
        let node_agent = match (&a.node_agent_namespace, &a.node_agent_url) {
            (None, None) => None,
            (ns, url) => Some(
                watcher::NodeAgentClient::new(client.clone(), ns.as_deref().unwrap_or("default"), &a.node_agent_selector, a.node_agent_port, url.clone()).map_err(Fail::config)?,
            ),
        };
        let wc = watcher::WatcherConfig {
            prom,
            scrape_s: a.scrape_interval.max(1),
            node_agent,
            delay: Duration::from_secs(a.peak_delay.unwrap_or(2 * a.scrape_interval as u64)),
            base,
        };
        let ad = admitter.clone();
        let w = watcher::Watcher::new(client.clone(), engine.clone(), Arc::new(move || ad.adapter()), view.clone(), wc);
        if a.no_leader_election {
            tokio::spawn(w.watch_pods());
        } else {
            let ns = a.lease_namespace.clone().or(k.lease_namespace.clone()).or(std::env::var("POD_NAMESPACE").ok()).unwrap_or_else(|| "default".into());
            let rx = leader::spawn(client.clone(), leader::LeaseConfig::new(ns, "rightsizer-watcher"));
            tokio::spawn(w.run(rx));
        }
    }
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(a.watcher_only));
    let app = webhook::router(webhook::AppState { admitter: admitter.clone(), ready: ready.clone() });
    if let Some(h) = a.http_listen {
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = webhook::serve_http(h, app).await {
                tracing::error!(error = %e, "http listener failed");
            }
        });
    }
    if a.watcher_only {
        std::future::pending::<()>().await;
        return Ok(());
    }
    let cert = a.tls_cert.or(k.tls_cert.clone()).ok_or_else(|| Fail::config("--tls-cert (or [k8s] tls_cert) is required"))?;
    let key = a.tls_key.or(k.tls_key.clone()).ok_or_else(|| Fail::config("--tls-key (or [k8s] tls_key) is required"))?;
    let rc = webhook::ReloadingCert::new(webhook::TlsFiles { cert, key }).map_err(Fail::config)?;
    rc.spawn_watch(Duration::from_secs(10));
    let tls = webhook::tls_config(rc).map_err(Fail::config)?;
    ready.store(true, std::sync::atomic::Ordering::Relaxed);
    let listen = match a.listen {
        Some(l) => l,
        None => k.webhook_listen.parse().map_err(|e| Fail::config(format!("[k8s] webhook_listen: {e}")))?,
    };
    webhook::serve_tls(listen, app, tls).await.map_err(Fail::other)
}

fn read_input(file: &str) -> Result<Vec<u8>, Fail> {
    if file == "-" {
        let mut b = Vec::new();
        std::io::stdin().read_to_end(&mut b).map_err(|e| Fail::parse(format!("stdin: {e}")))?;
        Ok(b)
    } else {
        std::fs::read(file).map_err(|e| Fail::parse(format!("{file}: {e}")))
    }
}

/// Objects of a YAML/JSON stream (`kubectl get -o json` lists are flattened).
fn objects_of(bytes: &[u8]) -> Result<Vec<Value>, Fail> {
    let text = std::str::from_utf8(bytes).map_err(|e| Fail::parse(e.to_string()))?;
    let docs = rightsizer_k8s::yaml::parse(text).map_err(|e| Fail::parse(e.to_string()))?;
    let mut out = Vec::new();
    for d in docs {
        match d.value.get("items").and_then(Value::as_array) {
            Some(items) if d.value.get("kind").and_then(Value::as_str).is_some_and(|k| k.ends_with("List")) => out.extend(items.iter().cloned()),
            _ => out.push(d.value),
        }
    }
    Ok(out)
}

async fn restore(cfg: &Config, file: Option<String>, target: Option<String>, ns: &str, dry_run: bool, out: Format) -> Res {
    let adapter = adapter_for(cfg, true)?;
    match (file, target) {
        (Some(f), None) => {
            let objs = objects_of(&read_input(&f)?)?;
            let mut restored = Vec::new();
            for o in &objs {
                match live::restored(&adapter, o) {
                    Ok((v, r)) => {
                        eprintln!("restored {} unit(s) of {}/{}", r.units.len(), o.get("kind").and_then(Value::as_str).unwrap_or("?"), o.pointer("/metadata/name").and_then(Value::as_str).unwrap_or("?"));
                        restored.push(v);
                    }
                    Err(e) => {
                        eprintln!("unchanged {}: {e}", o.pointer("/metadata/name").and_then(Value::as_str).unwrap_or("?"));
                        restored.push(o.clone());
                    }
                }
            }
            print_objects(&restored, out);
            Ok(())
        }
        (None, Some(t)) => {
            let (kind, name) = t.split_once('/').ok_or_else(|| Fail::parse("KIND/NAME"))?;
            let (av, k) = live::resolve_kind(kind).ok_or_else(|| Fail::parse(format!("unknown kind {kind}")))?;
            let client = kube_client().await?;
            if dry_run {
                let o = cluster::get_json(&client, av, k, Some(ns), name).await.map_err(Fail::other)?.ok_or_else(|| Fail::other("not found"))?;
                let (v, _) = live::restored(&adapter, &o).map_err(Fail::other)?;
                print_objects(&[v], out);
                return Ok(());
            }
            let v = live::restore_live(&client, &adapter, av, k, ns, name).await.map_err(Fail::other)?;
            eprintln!("restored {k}/{name} in {ns}");
            print_objects(&[v], out);
            Ok(())
        }
        _ => Err(Fail::parse("k8s restore needs -f FILE|- or KIND/NAME")),
    }
}

fn print_objects(objs: &[Value], out: Format) {
    match out {
        Format::Json => {
            let v = if objs.len() == 1 { objs[0].clone() } else { serde_json::json!({"apiVersion": "v1", "kind": "List", "items": objs}) };
            println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
        }
        _ => {
            let docs: Vec<String> = objs.iter().map(|o| serde_norway::to_string(o).unwrap_or_default()).collect();
            print!("{}", docs.join("---\n"));
        }
    }
}

fn unified_diff(a: &str, b: &str, name: &str) -> String {
    let (x, y): (Vec<&str>, Vec<&str>) = (a.lines().collect(), b.lines().collect());
    // LCS table (manifests are small).
    let (n, m) = (x.len(), y.len());
    let mut l = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            l[i][j] = if x[i] == y[j] { l[i + 1][j + 1] + 1 } else { l[i + 1][j].max(l[i][j + 1]) };
        }
    }
    let mut out = format!("--- a/{name}\n+++ b/{name}\n");
    let (mut i, mut j) = (0, 0);
    while i < n || j < m {
        if i < n && j < m && x[i] == y[j] {
            out.push_str(&format!(" {}\n", x[i]));
            i += 1;
            j += 1;
        } else if j < m && (i == n || l[i][j + 1] >= l[i + 1][j]) {
            out.push_str(&format!("+{}\n", y[j]));
            j += 1;
        } else {
            out.push_str(&format!("-{}\n", x[i]));
            i += 1;
        }
    }
    out
}

/// `rsz apply FILE|- [-f OUT] [--diff] [--watch]`.
pub async fn apply(cfg: &Config, file: &str, out_path: Option<PathBuf>, diff: bool, watch: bool, out: Format) -> Res {
    let bytes = read_input(file)?;
    let text = String::from_utf8(bytes.clone()).map_err(|e| Fail::parse(e.to_string()))?;
    adapter_for(cfg, true)?;
    let native = Native::Document { media_type: if file.ends_with(".json") { "application/json".into() } else { "application/yaml".into() }, bytes };
    let mut settings = runtime::settings_map(&cfg.k8s);
    settings.insert("apply_watch".into(), Value::Bool(watch));
    let objs = objects_of(text.as_bytes())?;
    let ns = objs.first().and_then(|o| o.pointer("/metadata/namespace")).and_then(Value::as_str).unwrap_or("default").to_string();
    let client = if watch { Some(kube_client().await?) } else { None };
    if let Some(c) = &client {
        // Validation against the live namespace (LimitRanges, quotas, nodes).
        let inputs = KubeView::new(c.clone()).inputs(&ns).await.map_err(Fail::other)?;
        inputs.apply_to(&mut settings);
    }
    let env = AdapterEnv { cluster: cfg.k8s.cluster.clone(), settings: settings.clone(), ..Default::default() };
    let adapter = K8sAdapter::new(rightsizer_k8s::K8sSettings::from_env(&env).map_err(Fail::config)?).with_keys(
        runtime::load_keys(cfg.k8s.signing_key_file.as_ref().expect("checked")).map_err(Fail::config)?,
    );
    let engine = Engine::builder(cfg.clone()).role(Role::Client).build().await.map_err(Fail::config)?;
    let exs = adapter.extract_documents(&native, &env).map_err(|e| Fail::parse(e.to_string()))?;
    if watch {
        if objs.len() != 1 || objs[0].get("kind").and_then(Value::as_str) != Some("Job") {
            return Err(Fail::parse("apply --watch takes one batch/v1 Job"));
        }
        let ex = exs.into_iter().next().ok_or_else(|| Fail::parse("no object"))?;
        if let Some(s) = &ex.skip {
            return Err(Fail::other(format!("not sized: {}", serde_json::to_string(s).unwrap_or_default())));
        }
        let o = live::WatchOptions { namespace: ns, timeout: Duration::from_secs(1800), poll: Duration::from_secs(2) };
        let rep = live::apply_watch(client.as_ref().expect("client"), &engine, &adapter, &env, &objs[0], &ex.units, &ex.tenant, &o).await.map_err(Fail::other)?;
        engine.flush().await;
        let v = serde_json::to_value(&rep).unwrap_or_default();
        crate::output::value(&v, out, || {
            let mut s = String::new();
            for a in &rep.attempts {
                s.push_str(&format!("attempt {}: Job/{} memory {:?} → {:?}\n", a.attempt, a.job, a.memory, a.status));
            }
            s.push_str(&format!("{} ({})\n", if rep.succeeded { "succeeded" } else { "failed" }, rep.reason));
            s
        });
        return if rep.succeeded { Ok(()) } else { Err(Fail { code: 4, msg: format!("the Job did not succeed: {}", rep.reason) }) };
    }
    let mut all = Vec::new();
    let policy = Policy { mode: Some(Mode::Enforce), can_classify: Some(false), ..Default::default() };
    for ex in &exs {
        if let Some(s) = &ex.skip {
            eprintln!("rsz: skipped: {}", serde_json::to_string(s).unwrap_or_default());
            continue;
        }
        let state = adapter.state(&ex.tenant, &env).await.unwrap_or_default();
        let groups = runtime::requests(&adapter, ex, &state, &policy, None, false).map_err(Fail::parse)?;
        all.extend(runtime::decide_all(&engine, groups).await);
    }
    engine.flush().await;
    let mode = runtime::render_mode(&all);
    let rendered = adapter
        .render_with(&native, &RenderRequest { mode: mode.max(Mode::Recommend), decision_id: all.first().map(|(_, d)| d.id.to_string()), units: runtime::render_units(&all, mode), annotate: true, output: Output::TextEdits })
        .map_err(|e| Fail::other(e.to_string()))?;
    let new_text = match &rendered.patch {
        Patch::TextEdits { edits } => rightsizer_plugin::apply_text_edits(&text, edits).map_err(|e| Fail::other(e.to_string()))?,
        _ => text.clone(),
    };
    for (u, d) in &all {
        eprintln!("rsz: {} {}: {} ({})", d.key, u.id, if runtime::changes(u, d) && d.mode == Mode::Enforce { "resized" } else { "kept" }, d.mode);
    }
    if diff {
        print!("{}", unified_diff(&text, &new_text, file));
    } else if let Some(p) = out_path {
        std::fs::write(&p, &new_text).map_err(|e| Fail::other(format!("{}: {e}", p.display())))?;
    } else {
        print!("{new_text}");
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "rsz history import k8s", no_binary_name = true)]
struct ImportArgs {
    /// Namespace.
    #[arg(long, short = 'n')]
    namespace: String,
    /// Window.
    #[arg(long, default_value = "30d")]
    since: String,
    /// Prometheus (default `[k8s] prometheus_url`).
    #[arg(long)]
    prometheus: Option<String>,
    /// Scrape interval of the cAdvisor job, s.
    #[arg(long, default_value_t = 15)]
    scrape_interval: u32,
    /// Print the records; write nothing.
    #[arg(long)]
    dry_run: bool,
}

/// `rsz history import k8s …`.
pub async fn import(cfg: &Config, args: Vec<String>, out: Format) -> Res {
    let a = ImportArgs::try_parse_from(&args).map_err(|e| Fail::parse(e.to_string()))?;
    let url = a.prometheus.or(cfg.k8s.prometheus_url.clone()).ok_or_else(|| Fail::config("--prometheus (or [k8s] prometheus_url) is required"))?;
    let since = humantime::parse_duration(&a.since).map_err(|e| Fail::parse(format!("--since: {e}")))?;
    let cluster = cfg.k8s.cluster.clone().ok_or_else(|| Fail::config("[k8s] cluster is required (tenant prefix)"))?;
    let p = prom::Prom::new(&url).map_err(Fail::config)?;
    let recs = prom::import(&p, &prom::ImportOptions { cluster, namespace: a.namespace.clone(), since, scrape_s: a.scrape_interval.max(1), at: None }).await.map_err(Fail::other)?;
    if a.dry_run {
        for r in &recs {
            println!("{}", serde_json::to_string(r).unwrap_or_default());
        }
        return Ok(());
    }
    let e = Engine::builder(cfg.clone()).role(Role::Client).build().await.map_err(Fail::config)?;
    let (mut ins, mut dup) = (0, 0);
    for r in &recs {
        match e.store().put_run(r).await.map_err(Fail::other)? {
            rightsizer_history::PutOutcome::Inserted => ins += 1,
            rightsizer_history::PutOutcome::Duplicate => dup += 1,
        }
    }
    crate::output::value(&serde_json::json!({"read": recs.len(), "inserted": ins, "duplicates": dup, "namespace": a.namespace}), out, || {
        format!("imported {ins} run records ({dup} duplicates) from Prometheus for namespace {}\n", a.namespace)
    });
    Ok(())
}
