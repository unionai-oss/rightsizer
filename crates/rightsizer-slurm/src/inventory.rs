//! Cluster inventory: parsers for `scontrol show node|partition|config`
//! (text and `--json`), `sinfo --json` / `sinfo -N -o '%N|%P|%c|%m|%G|%f'`,
//! and `sacctmgr -P show assoc|qos` (slurm.md §7). The daemon runs these
//! (cached, 5 min) and hands the result to the adapter through
//! [`crate::SlurmSettings`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use rightsizer_core::units::parse_slurm_mem;

use crate::sbatch::{parse_gres, parse_time};

/// A partition.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionInfo {
    /// Name.
    pub name: String,
    /// Node names (hostlist expanded).
    pub nodes: Vec<String>,
    /// Whether this is the default partition.
    pub is_default: bool,
    /// `DefMemPerCPU`, MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub def_mem_per_cpu: Option<u64>,
    /// `DefMemPerNode`, MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub def_mem_per_node: Option<u64>,
    /// `MaxMemPerCPU`, MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_mem_per_cpu: Option<u64>,
    /// `MaxMemPerNode`, MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_mem_per_node: Option<u64>,
    /// `DefaultTime`, seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_time_s: Option<u64>,
    /// `MaxTime`, seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_time_s: Option<u64>,
    /// `MaxCPUsPerNode`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpus_per_node: Option<u32>,
    /// Partition QOS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<String>,
}

/// A generic resource on a node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGres {
    /// Name (`gpu`).
    pub name: String,
    /// Type (`a100`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_type: Option<String>,
    /// Count.
    pub count: u32,
}

/// A node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeInfo {
    /// Name.
    pub name: String,
    /// `CPUTot` (or `CPUEfctv` when smaller).
    pub cpus: u32,
    /// `RealMemory`, MiB.
    pub real_memory_mib: u64,
    /// `MemSpecLimit`, MiB (reserved for the system).
    pub mem_spec_limit_mib: u64,
    /// Generic resources.
    pub gres: Vec<NodeGres>,
    /// Partitions.
    pub partitions: Vec<String>,
    /// Active features.
    pub features: Vec<String>,
    /// States (`IDLE`, `DOWN`, `DRAIN`, …).
    pub state: Vec<String>,
}

impl NodeInfo {
    /// Memory a job can get, MiB.
    pub fn usable_memory_mib(&self) -> u64 {
        self.real_memory_mib.saturating_sub(self.mem_spec_limit_mib)
    }
    /// GPUs of a type (`None` = any type).
    pub fn gpus(&self, ty: Option<&str>) -> u32 {
        self.gres
            .iter()
            .filter(|g| g.name == "gpu" && ty.is_none_or(|t| g.gpu_type.as_deref().is_some_and(|x| x.eq_ignore_ascii_case(t))))
            .map(|g| g.count)
            .sum()
    }
    /// Whether the node can run jobs at all (not DOWN, DRAIN, FUTURE…).
    pub fn usable(&self) -> bool {
        !self.state.iter().any(|s| matches!(s.to_ascii_uppercase().as_str(), "DOWN" | "DRAIN" | "DRAINED" | "FUTURE" | "NOT_RESPONDING" | "INVALID" | "FAIL"))
    }
}

/// TRES limits (`cpu=4,mem=4G,gres/gpu=2,node=2`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TresLimits {
    /// CPUs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<u64>,
    /// Memory, MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_mib: Option<u64>,
    /// GPUs (`gres/gpu`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus: Option<u64>,
    /// Nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<u64>,
}

impl TresLimits {
    /// Parse `cpu=4,mem=4G,gres/gpu=2` (memory in Slurm units, default MiB).
    pub fn parse(s: &str) -> Self {
        let mut t = TresLimits::default();
        for e in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let Some((k, v)) = e.split_once('=') else { continue };
            match k {
                "cpu" => t.cpu = v.parse().ok(),
                "mem" => t.mem_mib = parse_slurm_mem(v).ok().map(|b| b.0.div_ceil(1 << 20)),
                "gres/gpu" => t.gpus = v.parse().ok(),
                "node" => t.node = v.parse().ok(),
                _ => {}
            }
        }
        t
    }
    /// The tighter of two limits, per field.
    pub fn min(&self, o: &TresLimits) -> TresLimits {
        let m = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        TresLimits { cpu: m(self.cpu, o.cpu), mem_mib: m(self.mem_mib, o.mem_mib), gpus: m(self.gpus, o.gpus), node: m(self.node, o.node) }
    }
    /// Whether nothing is limited.
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.mem_mib.is_none() && self.gpus.is_none() && self.node.is_none()
    }
}

/// An association (`sacctmgr show assoc`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AssocLimit {
    /// Cluster.
    pub cluster: String,
    /// Account.
    pub account: String,
    /// User (`None` for the account's own association).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Partition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// `GrpTRES`.
    pub grp_tres: TresLimits,
    /// `MaxTRESPerJob` (`MaxTRES`).
    pub max_tres_per_job: TresLimits,
    /// `MaxTRESPerNode`.
    pub max_tres_per_node: TresLimits,
    /// QOS names.
    pub qos: Vec<String>,
    /// Default QOS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_qos: Option<String>,
}

/// A QOS (`sacctmgr show qos`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QosLimit {
    /// Name.
    pub name: String,
    /// `GrpTRES`.
    pub grp_tres: TresLimits,
    /// `MaxTRESPerJob`.
    pub max_tres_per_job: TresLimits,
    /// `MaxTRESPerNode`.
    pub max_tres_per_node: TresLimits,
}

/// Cluster-wide settings from `scontrol show config`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// `ClusterName`.
    pub cluster_name: Option<String>,
    /// `SLURM_VERSION`.
    pub version: Option<String>,
    /// `JobAcctGatherFrequency` task sampling interval, seconds.
    pub job_acct_gather_frequency: Option<u32>,
    /// `JobAcctGatherType`.
    pub job_acct_gather_type: Option<String>,
    /// `ConstrainRAMSpace` (cgroup.conf, shown by `scontrol show config`).
    pub constrain_ram_space: Option<bool>,
    /// Cluster `DefMemPerCPU`, MiB.
    pub def_mem_per_cpu: Option<u64>,
    /// Cluster `DefMemPerNode`, MiB.
    pub def_mem_per_node: Option<u64>,
    /// Cluster `MaxMemPerCPU`, MiB.
    pub max_mem_per_cpu: Option<u64>,
    /// Cluster `MaxMemPerNode`, MiB.
    pub max_mem_per_node: Option<u64>,
    /// `AccountingStoreFlags` includes `job_script`.
    pub stores_job_script: bool,
}

/// Expand a Slurm hostlist: `c[1-3,7],gpu[01-02]` → `c1 c2 c3 c7 gpu01 gpu02`.
pub fn expand_hostlist(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0;
    let mut cur = String::new();
    let mut parts = Vec::new();
    for c in s.chars() {
        match c {
            '[' => {
                depth += 1;
                cur.push(c);
            }
            ']' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    for p in parts {
        let p = p.trim();
        if p.is_empty() || p == "(null)" {
            continue;
        }
        match (p.find('['), p.rfind(']')) {
            (Some(a), Some(b)) if b > a => {
                let (prefix, suffix) = (&p[..a], &p[b + 1..]);
                for r in p[a + 1..b].split(',') {
                    if let Some((lo, hi)) = r.split_once('-') {
                        let width = lo.len();
                        if let (Ok(l), Ok(h)) = (lo.parse::<u64>(), hi.parse::<u64>()) {
                            for n in l..=h.min(l + 100_000) {
                                out.push(format!("{prefix}{n:0width$}{suffix}"));
                            }
                        }
                    } else {
                        out.push(format!("{prefix}{r}{suffix}"));
                    }
                }
            }
            _ => out.push(p.to_string()),
        }
    }
    out
}

/// Split `scontrol show` text into records of `Key=Value` pairs. Values
/// may contain spaces (`OS=Linux 6.12 #1 SMP …`): a word without `=`
/// continues the previous value.
pub fn parse_scontrol_records(text: &str) -> Vec<BTreeMap<String, String>> {
    let mut recs = Vec::new();
    let mut cur: BTreeMap<String, String> = BTreeMap::new();
    let mut last: Option<String> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            if !cur.is_empty() {
                recs.push(std::mem::take(&mut cur));
            }
            last = None;
            continue;
        }
        for w in line.split_whitespace() {
            match w.split_once('=') {
                Some((k, v)) if !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '/') => {
                    cur.insert(k.to_string(), v.to_string());
                    last = Some(k.to_string());
                }
                _ => {
                    if let Some(k) = &last
                        && let Some(v) = cur.get_mut(k)
                    {
                        v.push(' ');
                        v.push_str(w);
                    }
                }
            }
        }
    }
    if !cur.is_empty() {
        recs.push(cur);
    }
    recs
}

fn limit_mib(v: Option<&String>) -> Option<u64> {
    let v = v?.trim();
    if v.is_empty() || v.eq_ignore_ascii_case("unlimited") || v == "0" || v == "(null)" {
        return None;
    }
    parse_slurm_mem(v).ok().map(|b| b.0.div_ceil(1 << 20))
}

fn time_s(v: Option<&String>) -> Option<u64> {
    let v = v?.trim();
    if v.eq_ignore_ascii_case("none") || v.is_empty() {
        return None;
    }
    parse_time(v).ok().flatten()
}

fn gres_list(s: &str) -> Vec<NodeGres> {
    if s.trim().is_empty() || s.trim() == "(null)" {
        return vec![];
    }
    parse_gres(s).unwrap_or_default().into_iter().map(|e| NodeGres { name: e.name, gpu_type: e.gpu_type, count: e.count }).collect()
}

fn list(s: Option<&String>) -> Vec<String> {
    s.map(|v| v.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty() && x != "(null)").collect()).unwrap_or_default()
}

/// `scontrol show node` (text).
pub fn parse_show_node(text: &str) -> Vec<NodeInfo> {
    parse_scontrol_records(text)
        .into_iter()
        .filter_map(|r| {
            let name = r.get("NodeName")?.clone();
            let tot = r.get("CPUTot").and_then(|v| v.parse::<u32>().ok()).unwrap_or(0);
            let eff = r.get("CPUEfctv").and_then(|v| v.parse::<u32>().ok()).unwrap_or(tot);
            Some(NodeInfo {
                name,
                cpus: if eff > 0 { eff.min(tot.max(eff)) } else { tot },
                real_memory_mib: r.get("RealMemory").and_then(|v| v.parse().ok()).unwrap_or(0),
                mem_spec_limit_mib: r.get("MemSpecLimit").and_then(|v| v.parse().ok()).unwrap_or(0),
                gres: gres_list(r.get("Gres").map(String::as_str).unwrap_or("")),
                partitions: list(r.get("Partitions")),
                features: list(r.get("ActiveFeatures")),
                state: r.get("State").map(|s| s.split('+').map(str::to_string).collect()).unwrap_or_default(),
            })
        })
        .collect()
}

/// `scontrol show partition` (text).
pub fn parse_show_partition(text: &str) -> Vec<PartitionInfo> {
    parse_scontrol_records(text)
        .into_iter()
        .filter_map(|r| {
            Some(PartitionInfo {
                name: r.get("PartitionName")?.clone(),
                nodes: r.get("Nodes").map(|n| expand_hostlist(n)).unwrap_or_default(),
                is_default: r.get("Default").is_some_and(|d| d == "YES"),
                def_mem_per_cpu: limit_mib(r.get("DefMemPerCPU")),
                def_mem_per_node: limit_mib(r.get("DefMemPerNode")),
                max_mem_per_cpu: limit_mib(r.get("MaxMemPerCPU")),
                max_mem_per_node: limit_mib(r.get("MaxMemPerNode")),
                default_time_s: time_s(r.get("DefaultTime")),
                max_time_s: time_s(r.get("MaxTime")),
                max_cpus_per_node: r.get("MaxCPUsPerNode").and_then(|v| v.parse().ok()),
                qos: r.get("QoS").filter(|q| *q != "N/A").cloned(),
            })
        })
        .collect()
}

/// `scontrol show config` (text: `Key = Value`).
pub fn parse_show_config(text: &str) -> ClusterConfig {
    let mut c = ClusterConfig::default();
    for line in text.lines() {
        let Some((k, v)) = line.split_once('=') else { continue };
        let (k, v) = (k.trim(), v.trim());
        match k {
            "ClusterName" => c.cluster_name = Some(v.to_string()),
            "SLURM_VERSION" => c.version = Some(v.to_string()),
            "JobAcctGatherFrequency" => c.job_acct_gather_frequency = parse_acct_freq(v),
            "JobAcctGatherType" => c.job_acct_gather_type = Some(v.to_string()),
            "ConstrainRAMSpace" => c.constrain_ram_space = Some(v.eq_ignore_ascii_case("yes")),
            "DefMemPerCPU" => c.def_mem_per_cpu = limit_mib(Some(&v.to_string())),
            "DefMemPerNode" => c.def_mem_per_node = limit_mib(Some(&v.to_string())),
            "MaxMemPerCPU" => c.max_mem_per_cpu = limit_mib(Some(&v.to_string())),
            "MaxMemPerNode" => c.max_mem_per_node = limit_mib(Some(&v.to_string())),
            "AccountingStoreFlags" => c.stores_job_script = v.split(',').any(|f| f.trim() == "job_script"),
            _ => {}
        }
    }
    c
}

/// `JobAcctGatherFrequency`: `30`, `task=5`, `task=5,energy=30`.
pub fn parse_acct_freq(v: &str) -> Option<u32> {
    for p in v.split(',') {
        let p = p.trim();
        if let Some(t) = p.strip_prefix("task=") {
            return t.parse().ok();
        }
        if let Ok(n) = p.parse::<u32>() {
            return Some(n);
        }
    }
    None
}

fn jnum(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::Object(o) => {
            if o.get("infinite").and_then(Value::as_bool) == Some(true) || o.get("set").and_then(Value::as_bool) == Some(false) {
                None
            } else {
                o.get("number").and_then(Value::as_u64)
            }
        }
        _ => None,
    }
}

fn jstrs(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(a)) => a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect(),
        Some(Value::String(s)) => s.split(',').map(str::to_string).filter(|x| !x.is_empty()).collect(),
        _ => vec![],
    }
}

/// `scontrol --json show node` or `sinfo --json` (nodes).
pub fn parse_nodes_json(json: &str) -> Result<Vec<NodeInfo>, String> {
    let v: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    if let Some(nodes) = v.get("nodes").and_then(Value::as_array) {
        return Ok(nodes
            .iter()
            .filter_map(|n| {
                Some(NodeInfo {
                    name: n.get("name")?.as_str()?.to_string(),
                    cpus: n.get("effective_cpus").and_then(jnum).or_else(|| n.get("cpus").and_then(jnum)).unwrap_or(0) as u32,
                    real_memory_mib: n.get("real_memory").and_then(jnum).unwrap_or(0),
                    mem_spec_limit_mib: n.get("specialized_memory").and_then(jnum).unwrap_or(0),
                    gres: gres_list(n.get("gres").and_then(Value::as_str).unwrap_or("")),
                    partitions: jstrs(n.get("partitions")),
                    features: jstrs(n.get("active_features")),
                    state: jstrs(n.get("state")),
                })
            })
            .collect());
    }
    // sinfo --json: one entry per node group.
    let Some(groups) = v.get("sinfo").and_then(Value::as_array) else { return Err("no nodes or sinfo array".into()) };
    let mut out: Vec<NodeInfo> = Vec::new();
    for g in groups {
        let names = g.pointer("/nodes/nodes").map(|x| jstrs(Some(x))).unwrap_or_default();
        let part = g.pointer("/partition/name").and_then(Value::as_str).map(str::to_string);
        for name in names.iter().flat_map(|n| expand_hostlist(n)) {
            if let Some(n) = out.iter_mut().find(|n| n.name == name) {
                if let Some(p) = &part
                    && !n.partitions.contains(p)
                {
                    n.partitions.push(p.clone());
                }
                continue;
            }
            out.push(NodeInfo {
                name,
                cpus: g.pointer("/cpus/total").and_then(jnum).or_else(|| g.pointer("/cpus/maximum").and_then(jnum)).unwrap_or(0) as u32,
                real_memory_mib: g.pointer("/memory/maximum").and_then(jnum).unwrap_or(0),
                mem_spec_limit_mib: 0,
                gres: gres_list(g.pointer("/gres/total").and_then(Value::as_str).unwrap_or("")),
                partitions: part.clone().into_iter().collect(),
                features: jstrs(g.pointer("/features/active")),
                state: jstrs(g.pointer("/node/state")),
            });
        }
    }
    Ok(out)
}

/// `sinfo -N -h -o '%N|%P|%c|%m|%G|%f'`.
pub fn parse_sinfo_nodes(text: &str) -> Vec<NodeInfo> {
    let mut out: Vec<NodeInfo> = Vec::new();
    for line in text.lines() {
        let f: Vec<&str> = line.split('|').collect();
        if f.len() < 4 {
            continue;
        }
        let part = f[1].trim_end_matches('*').to_string();
        if let Some(n) = out.iter_mut().find(|n| n.name == f[0]) {
            n.partitions.push(part);
            continue;
        }
        out.push(NodeInfo {
            name: f[0].to_string(),
            cpus: f[2].parse().unwrap_or(0),
            real_memory_mib: f[3].parse().unwrap_or(0),
            gres: gres_list(f.get(4).copied().unwrap_or("")),
            partitions: vec![part],
            features: f.get(5).map(|x| x.split(',').map(str::to_string).filter(|x| !x.is_empty() && x != "(null)").collect()).unwrap_or_default(),
            ..Default::default()
        });
    }
    out
}

/// `scontrol --json show partition`.
pub fn parse_partitions_json(json: &str) -> Result<Vec<PartitionInfo>, String> {
    let v: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let parts = v.get("partitions").and_then(Value::as_array).ok_or("no partitions array")?;
    let mib = |x: Option<&Value>| x.and_then(jnum).filter(|n| *n > 0);
    Ok(parts
        .iter()
        .filter_map(|p| {
            let flags = jstrs(p.pointer("/partition/state"));
            let _ = flags;
            Some(PartitionInfo {
                name: p.get("name")?.as_str()?.to_string(),
                nodes: p.pointer("/nodes/configured").and_then(Value::as_str).map(expand_hostlist).unwrap_or_default(),
                is_default: jstrs(p.pointer("/flags")).iter().any(|f| f == "DEFAULT"),
                def_mem_per_cpu: mib(p.pointer("/defaults/partition_memory_per_cpu")),
                def_mem_per_node: mib(p.pointer("/defaults/partition_memory_per_node")),
                max_mem_per_cpu: mib(p.pointer("/maximums/partition_memory_per_cpu")),
                max_mem_per_node: mib(p.pointer("/maximums/partition_memory_per_node")),
                default_time_s: p.pointer("/defaults/time").and_then(jnum).map(|m| m * 60),
                max_time_s: p.pointer("/maximums/time").and_then(jnum).map(|m| m * 60),
                max_cpus_per_node: p.pointer("/maximums/cpus_per_node").and_then(jnum).map(|n| n as u32),
                qos: p.pointer("/qos/assigned").and_then(Value::as_str).filter(|q| !q.is_empty()).map(str::to_string),
            })
        })
        .collect())
}

/// `sacctmgr -P show assoc format=Cluster,Account,User,Partition,GrpTRES,MaxTRESPerJob,MaxTRESPerNode,QOS,DefaultQOS`
/// (header names as sacctmgr prints them: `MaxTRES` for MaxTRESPerJob).
pub fn parse_sacctmgr_assoc(text: &str) -> Vec<AssocLimit> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let Some(header) = lines.next() else { return vec![] };
    let cols: Vec<&str> = header.split('|').collect();
    let idx = |names: &[&str]| cols.iter().position(|c| names.iter().any(|n| c.eq_ignore_ascii_case(n)));
    let (ic, ia, iu, ip) = (idx(&["Cluster"]), idx(&["Account"]), idx(&["User"]), idx(&["Partition"]));
    let (ig, ij, inode, iq, idq) = (idx(&["GrpTRES"]), idx(&["MaxTRES", "MaxTRESPerJob"]), idx(&["MaxTRESPerNode", "MaxTRESPN"]), idx(&["QOS"]), idx(&["Def QOS", "DefaultQOS"]));
    lines
        .map(|l| {
            let f: Vec<&str> = l.split('|').collect();
            let g = |i: Option<usize>| i.and_then(|i| f.get(i)).map(|s| s.trim()).filter(|s| !s.is_empty());
            AssocLimit {
                cluster: g(ic).unwrap_or("").to_string(),
                account: g(ia).unwrap_or("").to_string(),
                user: g(iu).map(str::to_string),
                partition: g(ip).map(str::to_string),
                grp_tres: g(ig).map(TresLimits::parse).unwrap_or_default(),
                max_tres_per_job: g(ij).map(TresLimits::parse).unwrap_or_default(),
                max_tres_per_node: g(inode).map(TresLimits::parse).unwrap_or_default(),
                qos: g(iq).map(|q| q.split(',').map(str::to_string).collect()).unwrap_or_default(),
                default_qos: g(idq).map(str::to_string),
            }
        })
        .collect()
}

/// `sacctmgr -P show qos format=Name,GrpTRES,MaxTRESPerJob,MaxTRESPerNode`.
pub fn parse_sacctmgr_qos(text: &str) -> Vec<QosLimit> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let Some(header) = lines.next() else { return vec![] };
    let cols: Vec<&str> = header.split('|').collect();
    let idx = |names: &[&str]| cols.iter().position(|c| names.iter().any(|n| c.eq_ignore_ascii_case(n)));
    let (iname, ig, ij, inode) = (idx(&["Name"]), idx(&["GrpTRES"]), idx(&["MaxTRES", "MaxTRESPerJob"]), idx(&["MaxTRESPerNode", "MaxTRESPN"]));
    lines
        .filter_map(|l| {
            let f: Vec<&str> = l.split('|').collect();
            let g = |i: Option<usize>| i.and_then(|i| f.get(i)).map(|s| s.trim()).filter(|s| !s.is_empty());
            Some(QosLimit {
                name: g(iname)?.to_string(),
                grp_tres: g(ig).map(TresLimits::parse).unwrap_or_default(),
                max_tres_per_job: g(ij).map(TresLimits::parse).unwrap_or_default(),
                max_tres_per_node: g(inode).map(TresLimits::parse).unwrap_or_default(),
            })
        })
        .collect()
}

/// Everything the adapter knows about the cluster.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Inventory {
    /// Cluster config.
    pub config: ClusterConfig,
    /// Partitions.
    pub partitions: Vec<PartitionInfo>,
    /// Nodes.
    pub nodes: Vec<NodeInfo>,
    /// Associations.
    pub assocs: Vec<AssocLimit>,
    /// QOS.
    pub qos: Vec<QosLimit>,
}

impl Inventory {
    /// The partition a job lands in (`--partition`, else the default).
    pub fn partition(&self, name: Option<&str>) -> Option<&PartitionInfo> {
        match name {
            Some(n) => self.partitions.iter().find(|p| p.name == n),
            None => self.partitions.iter().find(|p| p.is_default).or(self.partitions.first()),
        }
    }

    /// Nodes of a partition (all nodes when the partition is unknown).
    pub fn nodes_of<'a>(&'a self, p: Option<&'a PartitionInfo>) -> Vec<&'a NodeInfo> {
        match p {
            Some(p) => self.nodes.iter().filter(|n| p.nodes.contains(&n.name) || n.partitions.contains(&p.name)).collect(),
            None => self.nodes.iter().collect(),
        }
    }

    /// The effective per-job and per-node limits for `(account, user,
    /// partition, qos)`: the user association if any, else the account's,
    /// tightened by the QOS.
    pub fn limits(&self, account: &str, user: Option<&str>, partition: Option<&str>, qos: Option<&str>) -> (TresLimits, TresLimits, TresLimits) {
        let acct = account.to_ascii_lowercase();
        let pick = |u: Option<&str>| {
            self.assocs.iter().find(|a| {
                a.account == acct && a.user.as_deref() == u && (a.partition.is_none() || a.partition.as_deref() == partition)
            })
        };
        let a = user.and_then(|u| pick(Some(u))).or_else(|| pick(None));
        let (mut job, mut node, mut grp) = a.map(|a| (a.max_tres_per_job.clone(), a.max_tres_per_node.clone(), a.grp_tres.clone())).unwrap_or_default();
        let qname = qos.map(str::to_string).or_else(|| a.and_then(|a| a.default_qos.clone()));
        if let Some(q) = qname.and_then(|q| self.qos.iter().find(|x| x.name == q)) {
            job = job.min(&q.max_tres_per_job);
            node = node.min(&q.max_tres_per_node);
            grp = grp.min(&q.grp_tres);
        }
        (job, node, grp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostlists() {
        assert_eq!(expand_hostlist("c[1-2]"), vec!["c1", "c2"]);
        assert_eq!(expand_hostlist("gpu[01-03,07],login1"), vec!["gpu01", "gpu02", "gpu03", "gpu07", "login1"]);
        assert_eq!(expand_hostlist("(null)"), Vec::<String>::new());
    }

    #[test]
    fn tres() {
        let t = TresLimits::parse("cpu=2,gres/gpu=2,mem=2000M");
        assert_eq!(t, TresLimits { cpu: Some(2), mem_mib: Some(2000), gpus: Some(2), node: None });
        assert_eq!(TresLimits::parse("mem=4G").mem_mib, Some(4096));
        assert_eq!(parse_acct_freq("task=5,energy=30"), Some(5));
        assert_eq!(parse_acct_freq("30"), Some(30));
    }

    #[test]
    fn records_with_spaces() {
        let r = parse_scontrol_records("NodeName=c1 OS=Linux 6.1 #1 SMP RealMemory=2048\n   Gres=gpu:a100:4(S:0-1)\n");
        assert_eq!(r[0]["OS"], "Linux 6.1 #1 SMP");
        assert_eq!(r[0]["RealMemory"], "2048");
        let n = parse_show_node("NodeName=g1 CPUTot=64 RealMemory=512000 MemSpecLimit=2048 Gres=gpu:a100:4(S:0-1),gpu:h100:2 State=MIXED+DRAIN Partitions=gpu,debug\n");
        assert_eq!(n[0].gpus(Some("a100")), 4);
        assert_eq!(n[0].gpus(None), 6);
        assert_eq!(n[0].usable_memory_mib(), 509952);
        assert!(!n[0].usable());
        assert_eq!(n[0].partitions, vec!["gpu", "debug"]);
    }
}
