//! Prometheus (kubernetes.md §6.3): a small HTTP API client, the watcher's
//! per-container peak queries, and the history importer
//! (`rsz history import k8s --namespace NS --since 30d`).
//!
//! Measurement semantics:
//! - memory peak = `max_over_time(container_memory_working_set_bytes)` over
//!   the container's life: instantaneous samples, `resolution_s` = scrape
//!   interval; within 5 % of the memory limit (or OOMKilled) → `AtLeast`;
//! - CPU busiest window = `max_over_time(rate(cpu[w])[life:w])`, an
//!   *average* over `w` (≥ 2 × scrape): `resolution_s = w`;
//! - CPU mean = `increase(cpu[life]) / life`;
//! - throttling = throttled periods / periods;
//! - GPU memory = `DCGM_FI_DEV_FB_USED` (MiB) per device joined by pod
//!   labels; a device seen with more than one pod in the window (time
//!   slicing / MPS) → `AtMost`.
//!
//! Owner joins come from metric history (`kube_pod_owner` →
//! `kube_job_owner` → CronJob), so pods deleted by TTL still join.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rightsizer_core::{Censor, Confidence, Measurement, OutcomeSource, Requests, ResourceSpec, RunRecord, RunStatus, Trust, WorkloadKey};
use serde_json::Value;

use crate::keys;

/// A Prometheus HTTP API client.
#[derive(Clone, Debug)]
pub struct Prom {
    http: reqwest::Client,
    base: String,
}

/// One instant-vector sample.
pub type Sample = (BTreeMap<String, String>, f64);

impl Prom {
    /// A client for `base` (`http://prometheus:9090`).
    pub fn new(base: &str) -> Result<Self, String> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build().map_err(|e| e.to_string())?;
        Ok(Prom { http, base: base.trim_end_matches('/').to_string() })
    }

    /// An instant query (at `at`, default now).
    pub async fn query(&self, q: &str, at: Option<DateTime<Utc>>) -> Result<Vec<Sample>, String> {
        let mut params = vec![("query", q.to_string())];
        if let Some(t) = at {
            params.push(("time", format!("{:.3}", t.timestamp_millis() as f64 / 1000.0)));
        }
        let resp = self.http.get(format!("{}/api/v1/query", self.base)).query(&params).send().await.map_err(|e| format!("prometheus: {e}"))?;
        let v: Value = resp.json().await.map_err(|e| format!("prometheus: {e}"))?;
        if v.get("status").and_then(Value::as_str) != Some("success") {
            return Err(format!("prometheus: {q}: {}", v.get("error").and_then(Value::as_str).unwrap_or("error")));
        }
        let mut out = Vec::new();
        for r in v.pointer("/data/result").and_then(Value::as_array).into_iter().flatten() {
            let m: BTreeMap<String, String> = r.get("metric").and_then(|m| serde_json::from_value(m.clone()).ok()).unwrap_or_default();
            let val = r.pointer("/value/1").and_then(Value::as_str).and_then(|s| s.parse::<f64>().ok());
            if let Some(x) = val.filter(|x| x.is_finite()) {
                out.push((m, x));
            }
        }
        Ok(out)
    }

    /// A range query: per series, its labels and `(t, value)` points.
    pub async fn query_range(&self, q: &str, start: DateTime<Utc>, end: DateTime<Utc>, step_s: u64) -> Result<Vec<(BTreeMap<String, String>, Vec<(f64, f64)>)>, String> {
        let params = [
            ("query", q.to_string()),
            ("start", format!("{}", start.timestamp())),
            ("end", format!("{}", end.timestamp())),
            ("step", format!("{}s", step_s.max(1))),
        ];
        let resp = self.http.get(format!("{}/api/v1/query_range", self.base)).query(&params).send().await.map_err(|e| format!("prometheus: {e}"))?;
        let v: Value = resp.json().await.map_err(|e| format!("prometheus: {e}"))?;
        if v.get("status").and_then(Value::as_str) != Some("success") {
            return Err(format!("prometheus: {q}: {}", v.get("error").and_then(Value::as_str).unwrap_or("error")));
        }
        let mut out = Vec::new();
        for r in v.pointer("/data/result").and_then(Value::as_array).into_iter().flatten() {
            let m: BTreeMap<String, String> = r.get("metric").and_then(|m| serde_json::from_value(m.clone()).ok()).unwrap_or_default();
            let pts = r
                .get("values")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|p| Some((p.get(0)?.as_f64()?, p.get(1)?.as_str()?.parse::<f64>().ok()?)))
                .collect();
            out.push((m, pts));
        }
        Ok(out)
    }

    /// The first value of a query, if any.
    pub async fn scalar(&self, q: &str, at: Option<DateTime<Utc>>) -> Result<Option<f64>, String> {
        Ok(self.query(q, at).await?.into_iter().map(|(_, v)| v).reduce(f64::max))
    }
}

/// Escape a label value for PromQL.
pub fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Peaks of one container over `[start, end]`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Peaks {
    /// Memory working-set peak, bytes.
    pub memory: Option<f64>,
    /// CPU mean, cores (the wire unit of `RunRecord.cpu_mean`).
    pub cpu_mean: Option<f64>,
    /// CPU busiest window, cores.
    pub cpu_peak_window: Option<f64>,
    /// Throttled fraction.
    pub throttled: Option<f32>,
    /// Window of the CPU peak, s.
    pub cpu_window_s: f32,
}

/// Peaks of one container from cAdvisor metrics, queried at `end + slack`.
pub async fn container_peaks(p: &Prom, ns: &str, pod: &str, container: &str, start: DateTime<Utc>, end: DateTime<Utc>, scrape_s: u32) -> Result<Peaks, String> {
    let sel = format!("namespace=\"{}\",pod=\"{}\",container=\"{}\"", esc(ns), esc(pod), esc(container));
    let life = ((end - start).num_seconds().max(1) as u64 + 2 * scrape_s as u64).max(scrape_s as u64 * 2);
    let w = (2 * scrape_s).max(30);
    let at = Some(end + chrono::Duration::seconds(scrape_s as i64 * 2));
    let mem = p.scalar(&format!("max_over_time(container_memory_working_set_bytes{{{sel}}}[{life}s])"), at).await?;
    let mean = p.scalar(&format!("increase(container_cpu_usage_seconds_total{{{sel}}}[{life}s]) / {}", (end - start).num_seconds().max(1)), at).await?;
    let peak = p.scalar(&format!("max_over_time(rate(container_cpu_usage_seconds_total{{{sel}}}[{w}s])[{life}s:{scrape_s}s])"), at).await?;
    let thr = p
        .scalar(&format!("increase(container_cpu_cfs_throttled_periods_total{{{sel}}}[{life}s]) / increase(container_cpu_cfs_periods_total{{{sel}}}[{life}s])"), at)
        .await
        .ok()
        .flatten();
    Ok(Peaks { memory: mem, cpu_mean: mean, cpu_peak_window: peak, throttled: thr.map(|t| t as f32), cpu_window_s: w as f32 })
}

/// Censoring of a memory peak against the limit (kubernetes.md §6.3).
pub fn memory_measurement(peak: f64, limit: Option<f64>, oom: bool, resolution_s: f32) -> Measurement {
    let at_least = oom || limit.is_some_and(|l| l > 0.0 && peak >= 0.95 * l);
    let value = if oom { limit.map(|l| l.max(peak)).unwrap_or(peak) } else { peak };
    Measurement { value, censor: if at_least { Censor::AtLeast } else { Censor::Exact }, resolution_s: Some(resolution_s) }
}

/// Import options.
#[derive(Clone, Debug)]
pub struct ImportOptions {
    /// Cluster name (tenant prefix).
    pub cluster: String,
    /// Namespace.
    pub namespace: String,
    /// Window.
    pub since: Duration,
    /// Scrape interval (`resolution_s` of instantaneous samples).
    pub scrape_s: u32,
    /// Evaluation time (default now).
    pub at: Option<DateTime<Utc>>,
}

#[derive(Default, Debug, Clone)]
struct Row {
    pod: String,
    uid: Option<String>,
    container: String,
    owner_kind: Option<String>,
    owner_name: Option<String>,
    first: Option<f64>,
    last: Option<f64>,
    mem: Option<f64>,
    mem_limit: Option<f64>,
    req_cpu: Option<f64>,
    req_mem: Option<f64>,
    cpu_inc: Option<f64>,
    cpu_peak: Option<f64>,
    throttled: Option<f64>,
    periods: Option<f64>,
    reason: Option<String>,
    gpu_mem: Option<f64>,
    gpu_shared: bool,
}

fn idx<'a>(m: &'a mut HashMap<(String, String), Row>, s: &BTreeMap<String, String>) -> Option<&'a mut Row> {
    let pod = s.get("pod")?.clone();
    let c = s.get("container")?.clone();
    if c.is_empty() || c == "POD" {
        return None;
    }
    Some(m.entry((pod.clone(), c.clone())).or_insert_with(|| Row { pod, container: c, ..Default::default() }))
}

/// Build RunRecords for every container of `namespace` seen in the window.
pub async fn import(p: &Prom, o: &ImportOptions) -> Result<Vec<RunRecord>, String> {
    let ns = esc(&o.namespace);
    let win = format!("{}s", o.since.as_secs().max(60));
    let at = o.at;
    let step = o.scrape_s.max(1);
    let mut rows: HashMap<(String, String), Row> = HashMap::new();
    // Containers (cAdvisor): first/last sample, memory peak, CPU.
    let cad = format!("namespace=\"{ns}\",container!=\"\",container!=\"POD\"");
    for (m, v) in p.query(&format!("max_over_time(container_memory_working_set_bytes{{{cad}}}[{win}])"), at).await? {
        if let Some(r) = idx(&mut rows, &m) {
            r.mem = Some(r.mem.unwrap_or(0.0).max(v));
        }
    }
    for (m, v) in p.query(&format!("max_over_time(timestamp(container_memory_working_set_bytes{{{cad}}})[{win}:{step}s])"), at).await? {
        if let Some(r) = idx(&mut rows, &m) {
            r.last = Some(r.last.unwrap_or(0.0).max(v));
        }
    }
    for (m, v) in p.query(&format!("min_over_time(timestamp(container_memory_working_set_bytes{{{cad}}})[{win}:{step}s])"), at).await? {
        if let Some(r) = idx(&mut rows, &m) {
            r.first = Some(r.first.map(|f| f.min(v)).unwrap_or(v));
        }
    }
    for (m, v) in p.query(&format!("increase(container_cpu_usage_seconds_total{{{cad}}}[{win}])"), at).await? {
        if let Some(r) = idx(&mut rows, &m) {
            r.cpu_inc = Some(v);
        }
    }
    let w = (2 * o.scrape_s).max(30);
    for (m, v) in p.query(&format!("max_over_time(rate(container_cpu_usage_seconds_total{{{cad}}}[{w}s])[{win}:{step}s])"), at).await? {
        if let Some(r) = idx(&mut rows, &m) {
            r.cpu_peak = Some(v);
        }
    }
    for (m, v) in p.query(&format!("increase(container_cpu_cfs_throttled_periods_total{{{cad}}}[{win}])"), at).await.unwrap_or_default() {
        if let Some(r) = idx(&mut rows, &m) {
            r.throttled = Some(v);
        }
    }
    for (m, v) in p.query(&format!("increase(container_cpu_cfs_periods_total{{{cad}}}[{win}])"), at).await.unwrap_or_default() {
        if let Some(r) = idx(&mut rows, &m) {
            r.periods = Some(v);
        }
    }
    // kube-state-metrics: limits, requests, termination reasons, owners.
    let ksm = format!("namespace=\"{ns}\"");
    for (res, unit) in [("memory", "byte"), ("cpu", "core")] {
        for (m, v) in p.query(&format!("max_over_time(kube_pod_container_resource_limits{{{ksm},resource=\"{res}\",unit=\"{unit}\"}}[{win}])"), at).await.unwrap_or_default() {
            if res == "memory"
                && let Some(r) = idx(&mut rows, &m)
            {
                r.mem_limit = Some(v);
            }
        }
        for (m, v) in p.query(&format!("max_over_time(kube_pod_container_resource_requests{{{ksm},resource=\"{res}\",unit=\"{unit}\"}}[{win}])"), at).await.unwrap_or_default() {
            if let Some(r) = idx(&mut rows, &m) {
                if res == "memory" {
                    r.req_mem = Some(v);
                } else {
                    r.req_cpu = Some(v);
                }
            }
        }
    }
    for metric in ["kube_pod_container_status_terminated_reason", "kube_pod_container_status_last_terminated_reason"] {
        for (m, v) in p.query(&format!("max_over_time({metric}{{{ksm}}}[{win}])"), at).await.unwrap_or_default() {
            if v < 1.0 {
                continue;
            }
            let reason = m.get("reason").cloned();
            if let Some(r) = idx(&mut rows, &m)
                && (r.reason.is_none() || reason.as_deref() == Some("OOMKilled"))
            {
                r.reason = reason;
            }
        }
    }
    for (m, v) in p.query(&format!("max_over_time(kube_pod_container_state_started{{{ksm}}}[{win}])"), at).await.unwrap_or_default() {
        if let Some(r) = idx(&mut rows, &m) {
            // Short containers may never be scraped by cAdvisor: KSM still has their start.
            r.first = Some(r.first.map(|f| f.min(v)).unwrap_or(v));
            r.last = Some(r.last.unwrap_or(v).max(v));
        }
    }
    // Job creations (a recreated Job of the same name is another invocation).
    let end = at.unwrap_or_else(Utc::now);
    let start = end - chrono::Duration::from_std(o.since).unwrap_or_default();
    let mut job_created: HashMap<String, Vec<f64>> = HashMap::new();
    for (m, pts) in p.query_range(&format!("kube_job_created{{{ksm}}}"), start, end, (o.since.as_secs() / 600).max(step as u64)).await.unwrap_or_default() {
        if let Some(j) = m.get("job_name") {
            let e = job_created.entry(j.clone()).or_default();
            e.extend(pts.into_iter().map(|(_, v)| v));
            e.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            e.dedup();
        }
    }
    let mut owners: HashMap<String, (String, String, Option<String>)> = HashMap::new();
    for (m, _) in p.query(&format!("max_over_time(kube_pod_owner{{{ksm}}}[{win}])"), at).await.unwrap_or_default() {
        if let (Some(pod), Some(k), Some(n)) = (m.get("pod"), m.get("owner_kind"), m.get("owner_name"))
            && k != "<none>"
        {
            owners.insert(pod.clone(), (k.clone(), n.clone(), m.get("uid").cloned()));
        }
    }
    let mut job_owner: HashMap<String, (String, String)> = HashMap::new();
    for (m, _) in p.query(&format!("max_over_time(kube_job_owner{{{ksm}}}[{win}])"), at).await.unwrap_or_default() {
        if let (Some(j), Some(k), Some(n)) = (m.get("job_name"), m.get("owner_kind"), m.get("owner_name"))
            && k != "<none>"
        {
            job_owner.insert(j.clone(), (k.clone(), n.clone()));
        }
    }
    // GPU memory (dcgm-exporter in Kubernetes mode labels pod/container).
    let mut devices: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
    for (m, v) in p.query(&format!("max_over_time(DCGM_FI_DEV_FB_USED{{{ksm}}}[{win}])"), at).await.unwrap_or_default() {
        let dev = format!("{}/{}", m.get("Hostname").cloned().unwrap_or_default(), m.get("UUID").or(m.get("gpu")).cloned().unwrap_or_default());
        if let Some(pod) = m.get("pod") {
            devices.entry(dev).or_default().insert(pod.clone());
        }
        if let Some(r) = idx(&mut rows, &m) {
            r.gpu_mem = Some(r.gpu_mem.unwrap_or(0.0) + v * 1024.0 * 1024.0);
        }
    }
    let shared_pods: std::collections::HashSet<String> = devices.values().filter(|s| s.len() > 1).flatten().cloned().collect();
    for r in rows.values_mut() {
        r.gpu_shared = shared_pods.contains(&r.pod);
        if let Some((k, n, uid)) = owners.get(&r.pod) {
            r.owner_kind = Some(k.clone());
            r.owner_name = Some(n.clone());
            r.uid = uid.clone();
        }
    }
    let tenant = keys::tenant(&o.cluster, &o.namespace).ok_or("invalid tenant")?;
    // Attempts: pods of one Job in start order.
    let mut by_inv: BTreeMap<String, Vec<(f64, String)>> = BTreeMap::new();
    let inv_of = |r: &Row| match (r.owner_kind.as_deref(), &r.owner_name) {
        (Some("Job"), Some(j)) => {
            // The latest creation of that Job name at or before the pod's first sample.
            let created = job_created.get(j).and_then(|cs| cs.iter().rev().find(|c| **c <= r.first.unwrap_or(f64::MAX) + 1.0).copied());
            match created {
                Some(c) => format!("{}/job/{j}@{}", o.namespace, c as i64),
                None => format!("{}/job/{j}", o.namespace),
            }
        }
        _ => r.uid.clone().unwrap_or_else(|| format!("{}/pod/{}", o.namespace, r.pod)),
    };
    for r in rows.values() {
        by_inv.entry(inv_of(r)).or_default().push((r.first.unwrap_or(0.0), r.pod.clone()));
    }
    for v in by_inv.values_mut() {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        v.dedup_by(|a, b| a.1 == b.1);
    }
    let mut out = Vec::new();
    for r in rows.values() {
        let (tag, name) = match (r.owner_kind.as_deref(), &r.owner_name) {
            (Some("Job"), Some(j)) => match job_owner.get(j) {
                Some((k, cron)) if k == "CronJob" => ("cronjob", cron.clone()),
                _ => ("job", keys::base_name(j, None)),
            },
            (Some(k), _) if crate::settings::DEFAULT_SKIP_OWNERS.contains(&k) => continue, // long-running controllers / CRD-managed: not imported here
            _ => ("pod", keys::base_name(&r.pod, None)),
        };
        let suffix = format!("{tag}/{name}/{}", r.container);
        let Ok(key) = WorkloadKey::new("k8s", tenant.clone(), suffix) else { continue };
        let (Some(first), Some(last)) = (r.first, r.last) else { continue };
        let started = Utc.timestamp_millis_opt((first * 1000.0) as i64).single().unwrap_or_else(Utc::now);
        let life = (last - first).max(o.scrape_s as f64);
        let status = match r.reason.as_deref() {
            Some("OOMKilled") => RunStatus::OomKilled,
            Some("Completed") => RunStatus::Succeeded,
            Some("DeadlineExceeded") => RunStatus::Timeout,
            Some("Evicted") => RunStatus::Evicted,
            Some(_) => RunStatus::Failed,
            None => RunStatus::Unknown,
        };
        let oom = status == RunStatus::OomKilled;
        let inv = inv_of(r);
        let attempt = by_inv.get(&inv).and_then(|v| v.iter().position(|(_, p)| p == &r.pod)).map(|i| i as u32 + 1).unwrap_or(1);
        let res = o.scrape_s as f32;
        out.push(RunRecord {
            v: rightsizer_core::WIRE_VERSION,
            key,
            cluster: Some(o.cluster.clone()),
            invocation_id: inv,
            attempt,
            decision_id: None,
            started_at: started,
            inputs: vec![],
            code_digest: None,
            requested: Some(ResourceSpec {
                requests: Requests {
                    cpu: r.req_cpu.map(|c| rightsizer_core::MilliCpu((c * 1000.0).round() as u32)),
                    memory: r.req_mem.map(|b| rightsizer_core::Bytes(b as u64)),
                    ..Default::default()
                },
                limits: rightsizer_core::Limits { memory: r.mem_limit.map(|b| rightsizer_core::Bytes(b as u64)), ..Default::default() },
            }),
            origin_prior: None,
            status,
            status_confidence: if r.reason.is_some() { Confidence::Observed } else { Confidence::Inferred },
            memory_peak: match (r.mem, oom) {
                (Some(m), _) => Some(memory_measurement(m, r.mem_limit, oom, res)),
                (None, true) => r.mem_limit.map(|l| Measurement { value: l, censor: Censor::AtLeast, resolution_s: None }),
                (None, false) => None,
            },
            cpu_mean: r.cpu_inc.map(|c| Measurement { value: c / life, censor: Censor::Exact, resolution_s: Some(res) }),
            cpu_peak_window: r.cpu_peak.map(|c| Measurement { value: c, censor: Censor::Exact, resolution_s: Some(w as f32) }),
            cpu_throttled_frac: match (r.throttled, r.periods) {
                (Some(t), Some(p)) if p > 0.0 => Some((t / p) as f32),
                _ => None,
            },
            gpu_memory_peak: r.gpu_mem.map(|g| Measurement { value: g, censor: if r.gpu_shared { Censor::AtMost } else { Censor::Exact }, resolution_s: Some(res) }),
            gpu_util_mean: None,
            t_peak_frac: None,
            duration_s: Some(life),
            staged_bytes: None,
            source: OutcomeSource::Prometheus,
            trust: Trust::Verified,
        });
    }
    out.sort_by(|a, b| a.started_at.cmp(&b.started_at).then(a.key.to_string().cmp(&b.key.to_string())));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn censoring() {
        let m = memory_measurement(980.0, Some(1000.0), false, 15.0);
        assert_eq!(m.censor, Censor::AtLeast);
        let m = memory_measurement(500.0, Some(1000.0), false, 15.0);
        assert_eq!(m.censor, Censor::Exact);
        assert_eq!(m.resolution_s, Some(15.0));
        let m = memory_measurement(500.0, Some(1000.0), true, 15.0);
        assert_eq!((m.censor, m.value), (Censor::AtLeast, 1000.0));
        assert_eq!(esc("a\"b"), "a\\\"b");
    }
}
