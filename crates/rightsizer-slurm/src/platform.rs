//! The Slurm [`PlatformModel`] (slurm.md §7): `same_size = 0`; a Caller
//! ladder only under `rsz slurm sbatch --watch`; `memory_enforced` =
//! `ConstrainRAMSpace`; walltime from `--time` (or the partition default);
//! CPU window = `JobAcctGatherFrequency`; node shapes per partition.

use async_trait::async_trait;

use rightsizer_core::{
    Bytes, GpuShape, Ladder, LadderDriver, MilliCpu, NodeShape, PlatformRef, PlatformRetry, PlatformState, PriceModel, Quota, Requests,
    ScalarValue, Tenant,
};
use rightsizer_plugin::{AcceleratorMap, AdapterEnv, PlatformModel, Result, Unit};

use crate::keys::unescape_segment;
use crate::settings::SlurmSettings;

/// The Slurm platform model.
#[derive(Clone, Debug, Default)]
pub struct SlurmPlatform {
    /// Settings (inventory included) used when the env carries none.
    pub settings: SlurmSettings,
    accel: AcceleratorMap,
}

impl SlurmPlatform {
    /// A model over these settings.
    pub fn new(settings: SlurmSettings) -> Self {
        let accel = settings.accelerators();
        SlurmPlatform { settings, accel }
    }

    /// The static reference for a unit under `settings`.
    pub fn reference_with(settings: &SlurmSettings, unit: &Unit) -> PlatformRef {
        let walltime_s = match unit.seed.manifest.extra.get("walltime_s") {
            Some(ScalarValue::Int(w)) if *w > 0 => Some(*w as u64),
            _ => None,
        };
        let ladder = settings.watch.then(|| Ladder {
            memory_factor: settings.ladder_memory_factor.unwrap_or(2.0),
            max_steps: settings.ladder_max_steps.unwrap_or(2),
            final_floor: Requests::default(),
            driver: LadderDriver::Caller,
        });
        PlatformRef {
            kind: "slurm".into(),
            version: settings.version.clone().or_else(|| settings.inventory.config.version.clone()).unwrap_or_default(),
            overhead: Default::default(),
            retry: PlatformRetry { same_size: 0, ladder },
            walltime_s,
            cpu_window_s: settings.acct_frequency_s(),
            memory_enforced: settings.memory_enforced(),
        }
    }

    /// Live state under `settings` for an account.
    pub fn state_with(settings: &SlurmSettings, account: &str, user: Option<&str>) -> PlatformState {
        let accel = settings.accelerators();
        let inv = &settings.inventory;
        let mut shapes: Vec<NodeShape> = Vec::new();
        for n in inv.nodes.iter().filter(|n| n.usable()) {
            let gpu = n.gres.iter().find(|g| g.name == "gpu").map(|g| GpuShape {
                accel: g.gpu_type.as_deref().map(|t| accel.canonical(&t.to_ascii_lowercase())).unwrap_or(rightsizer_core::AcceleratorId::UnknownGpu),
                count: n.gpus(None),
            });
            let part = n.partitions.first().cloned().unwrap_or_default();
            let price = settings.prices.get(&part).or_else(|| settings.prices.get("default")).copied().unwrap_or(0.0);
            let shape = NodeShape {
                name: Some(if part.is_empty() { n.name.clone() } else { part.clone() }),
                cpu: MilliCpu::cores(n.cpus),
                memory: Bytes::mib(n.usable_memory_mib()),
                gpu,
                price_per_hour: price,
                host_per_gpu: None,
            };
            if !shapes.iter().any(|s| s.name == shape.name && s.cpu == shape.cpu && s.memory == shape.memory && s.gpu == shape.gpu) {
                shapes.push(shape);
            }
        }
        let (job, _node, grp) = inv.limits(account, user, None, None);
        let lim = job.min(&grp);
        let quota = (!lim.is_empty()).then(|| Quota {
            cpu: lim.cpu.map(|c| MilliCpu::cores(c.min(u32::MAX as u64 / 1000) as u32)),
            memory: lim.mem_mib.map(Bytes::mib),
            gpu_count: lim.gpus.map(|g| g as u32),
            ephemeral_storage: None,
        });
        PlatformState { shapes, request_model: Default::default(), price: PriceModel::Packed, quota, floors: Requests::default() }
    }
}

#[async_trait]
impl PlatformModel for SlurmPlatform {
    fn reference(&self, unit: &Unit) -> PlatformRef {
        Self::reference_with(&self.settings, unit)
    }

    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> Result<PlatformState> {
        let s = if env.settings.is_empty() { self.settings.clone() } else { SlurmSettings::from_env(env) };
        let account = tenant.as_str().split('/').nth(1).map(unescape_segment).unwrap_or_default();
        Ok(Self::state_with(&s, &account, env.principal.as_deref()))
    }

    fn accelerators(&self) -> &AcceleratorMap {
        &self.accel
    }
}
