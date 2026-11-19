//! Grids: the finite set of requests per axis (SPEC §5.3).
//!
//! A cell `j` of an axis with values `v` covers the demand interval
//! `(v[j-1], v[j]]` (cell 0 covers `(0, v[0]]`); a demand's cell is the
//! smallest cell covering it. On the GPU-memory axis index 0 is "no GPU"
//! (value 0).

use serde::{Deserialize, Serialize};

use crate::types::{Bytes, MilliCpu};

/// Id of the default grid.
pub const RS_GRID_V1: &str = "rs-grid-v1";

/// The tuner's memory ladder, MiB (`tuner:bench/grid.py:18`).
pub const TUNER_MEMORY_MIB: [u64; 17] =
    [256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768, 49152, 65536];
/// The tuner's CPU steps, cores (`tuner:bench_cpu/grid.py:11`).
pub const TUNER_CPU_CORES: [f64; 16] = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 7.0, 8.0];
/// The memory bench's coarse CPU grid (`tuner:bench/grid.py:23`). A subset,
/// not a prefix, of [`TUNER_CPU_CORES`]; rebinning handles it.
pub const TUNER_MEM_BENCH_CPU_CORES: [f64; 7] = [0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0];
/// The tuner's GPU memory cells, GiB (`tuner:bench_gpu/grid.py:29`).
pub const TUNER_GPU_GIB: [f64; 17] =
    [1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 12.0, 15.0, 22.0, 32.0, 44.0, 64.0, 88.0, 120.0, 176.0];

const EXTRA_MEMORY_GIB: [u64; 10] = [96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048];
const EXTRA_CPU_CORES: [f64; 9] = [10.0, 12.0, 16.0, 24.0, 32.0, 48.0, 64.0, 96.0, 128.0];
const EXTRA_GPU_GIB: [f64; 8] = [256.0, 352.0, 512.0, 640.0, 768.0, 1024.0, 1152.0, 1536.0];

/// A versioned grid (SPEC §5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Grid {
    /// Grid id, e.g. `"rs-grid-v1"`.
    pub id: String,
    /// Memory cells, ascending.
    pub memory: Vec<Bytes>,
    /// CPU cells, ascending.
    pub cpu: Vec<MilliCpu>,
    /// Total GPU memory cells, ascending; index 0 means "no GPU".
    pub gpu_memory: Vec<Bytes>,
}

/// One axis of a grid.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GridAxis {
    /// Memory, bytes.
    Memory,
    /// CPU, cores.
    Cpu,
    /// Total GPU memory, bytes (index 0 = no GPU).
    GpuMemory,
}

impl Grid {
    /// `rs-grid-v1` (SPEC §5.3): memory 27 cells (256Mi … 2Ti), CPU 25 cells
    /// (0.25 … 128), GPU memory 25 cells (1 … 1536 GiB) plus "none".
    pub fn rs_grid_v1() -> Grid {
        let memory = TUNER_MEMORY_MIB.iter().map(|&m| Bytes::mib(m)).chain(EXTRA_MEMORY_GIB.iter().map(|&g| Bytes::gib(g))).collect();
        let cpu = TUNER_CPU_CORES.iter().chain(EXTRA_CPU_CORES.iter()).map(|&c| MilliCpu::from_cores_f64(c)).collect();
        let gpu_memory = std::iter::once(Bytes(0))
            .chain(TUNER_GPU_GIB.iter().chain(EXTRA_GPU_GIB.iter()).map(|&g| Bytes::from_gib_f64(g)))
            .collect();
        Grid { id: RS_GRID_V1.into(), memory, cpu, gpu_memory }
    }

    /// The tuner's joint grid (`tuner:bench_all/grid.py`): memory 17, CPU 16,
    /// GPU none + 17. Id `"tuner-v1"`.
    pub fn tuner_v1() -> Grid {
        Grid {
            id: "tuner-v1".into(),
            memory: TUNER_MEMORY_MIB.iter().map(|&m| Bytes::mib(m)).collect(),
            cpu: TUNER_CPU_CORES.iter().map(|&c| MilliCpu::from_cores_f64(c)).collect(),
            gpu_memory: std::iter::once(Bytes(0)).chain(TUNER_GPU_GIB.iter().map(|&g| Bytes::from_gib_f64(g))).collect(),
        }
    }

    /// The tuner's memory bench grid (`tuner:bench/grid.py`): memory 17, CPU 7.
    /// Id `"tuner-mem-v1"`.
    pub fn tuner_mem_v1() -> Grid {
        Grid {
            id: "tuner-mem-v1".into(),
            cpu: TUNER_MEM_BENCH_CPU_CORES.iter().map(|&c| MilliCpu::from_cores_f64(c)).collect(),
            ..Grid::tuner_v1()
        }
    }

    /// The tuner's GPU bench grid (`tuner:bench_gpu/grid.py`): GPU memory
    /// without a "none" cell (index 0 = 1 GiB). Id `"tuner-gpu-v1"`.
    pub fn tuner_gpu_v1() -> Grid {
        Grid {
            id: "tuner-gpu-v1".into(),
            gpu_memory: TUNER_GPU_GIB.iter().map(|&g| Bytes::from_gib_f64(g)).collect(),
            ..Grid::tuner_v1()
        }
    }

    /// A known grid by id.
    pub fn by_id(id: &str) -> Option<Grid> {
        match id {
            RS_GRID_V1 => Some(Grid::rs_grid_v1()),
            "tuner-v1" => Some(Grid::tuner_v1()),
            "tuner-mem-v1" => Some(Grid::tuner_mem_v1()),
            "tuner-gpu-v1" => Some(Grid::tuner_gpu_v1()),
            _ => None,
        }
    }

    /// Axis values as floats in natural units (bytes, cores, bytes).
    pub fn values(&self, axis: GridAxis) -> Vec<f64> {
        match axis {
            GridAxis::Memory => self.memory.iter().map(|b| b.0 as f64).collect(),
            GridAxis::Cpu => self.cpu.iter().map(|c| c.as_cores()).collect(),
            GridAxis::GpuMemory => self.gpu_memory.iter().map(|b| b.0 as f64).collect(),
        }
    }

    /// Whether this grid's GPU axis has a "none" cell at index 0.
    pub fn has_gpu_none(&self) -> bool {
        self.gpu_memory.first().is_some_and(|b| b.0 == 0)
    }

    /// The smallest memory cell covering `b` (clamped to the top cell).
    pub fn memory_cell(&self, b: Bytes) -> usize {
        covering(&self.values(GridAxis::Memory), b.0 as f64).0
    }

    /// The smallest CPU cell covering `c` (clamped to the top cell).
    pub fn cpu_cell(&self, c: MilliCpu) -> usize {
        covering(&self.values(GridAxis::Cpu), c.as_cores()).0
    }

    /// The smallest GPU-memory cell covering `b` (0 = none when `b` is 0).
    pub fn gpu_cell(&self, b: Bytes) -> usize {
        covering(&self.values(GridAxis::GpuMemory), b.0 as f64).0
    }

    /// Whether `prefix` is a per-axis prefix of `self`.
    pub fn has_prefix(&self, prefix: &Grid) -> bool {
        self.memory.starts_with(&prefix.memory)
            && self.cpu.starts_with(&prefix.cpu)
            && self.gpu_memory.starts_with(&prefix.gpu_memory)
    }
}

/// Index of the smallest value `≥ v` (with a 1e-9 relative tolerance),
/// clamped to the last index; the flag is true when `v` exceeds every value.
pub fn covering(values: &[f64], v: f64) -> (usize, bool) {
    for (i, &x) in values.iter().enumerate() {
        if v <= x * (1.0 + 1e-9) + 1e-12 {
            return (i, false);
        }
    }
    (values.len().saturating_sub(1), true)
}

/// Index of the largest value `≤ v`, or `None` when `v` is below every value.
pub fn at_or_below(values: &[f64], v: f64) -> Option<usize> {
    values.iter().rposition(|&x| x <= v * (1.0 + 1e-9) + 1e-12)
}

/// Conservative rebinning of a foreign `Cells` distribution (SPEC §5.3): each
/// source cell's mass moves to the smallest target cell covering the source
/// cell's upper edge (its value). Never rejects; mass above the target's top
/// lands on the top cell.
pub fn rebin(src_values: &[f64], probs: &[f64], dst_values: &[f64]) -> Vec<f64> {
    let mut out = vec![0.0; dst_values.len()];
    if dst_values.is_empty() {
        return out;
    }
    for (i, &p) in probs.iter().enumerate() {
        if p.is_nan() || p <= 0.0 {
            continue;
        }
        let edge = src_values.get(i).copied().unwrap_or(f64::INFINITY);
        out[covering(dst_values, edge).0] += p;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rs_grid_v1_shape() {
        let g = Grid::rs_grid_v1();
        assert_eq!(g.memory.len(), 27);
        assert_eq!(g.cpu.len(), 25);
        assert_eq!(g.gpu_memory.len(), 26, "25 cells + none");
        assert_eq!(g.memory[0], Bytes::mib(256));
        assert_eq!(g.memory[16], Bytes::gib(64));
        assert_eq!(*g.memory.last().unwrap(), Bytes::gib(2048));
        assert_eq!(g.cpu[0], MilliCpu(250));
        assert_eq!(*g.cpu.last().unwrap(), MilliCpu::cores(128));
        assert_eq!(g.gpu_memory[0], Bytes(0));
        assert_eq!(g.gpu_memory[1], Bytes::gib(1));
        assert_eq!(*g.gpu_memory.last().unwrap(), Bytes::gib(1536));
        for axis in [GridAxis::Memory, GridAxis::Cpu, GridAxis::GpuMemory] {
            let v = g.values(axis);
            assert!(v.windows(2).all(|w| w[0] < w[1]), "{axis:?} ascending");
        }
    }

    #[test]
    fn tuner_ladders_are_prefixes() {
        let g = Grid::rs_grid_v1();
        assert!(g.has_prefix(&Grid::tuner_v1()));
        // The memory bench's 7-step CPU grid is a subset, not a prefix.
        let m = Grid::tuner_mem_v1();
        assert!(g.memory.starts_with(&m.memory));
        assert!(!g.cpu.starts_with(&m.cpu));
        assert!(m.cpu.iter().all(|c| g.cpu.contains(c)));
        // The GPU bench grid has no "none" cell: it is a prefix after index 0.
        let gp = Grid::tuner_gpu_v1();
        assert!(g.gpu_memory[1..].starts_with(&gp.gpu_memory));
    }

    #[test]
    fn covering_lookup() {
        let g = Grid::rs_grid_v1();
        assert_eq!(g.memory_cell(Bytes::mib(1)), 0);
        assert_eq!(g.memory_cell(Bytes::mib(256)), 0);
        assert_eq!(g.memory_cell(Bytes::mib(257)), 1);
        assert_eq!(g.memory_cell(Bytes::gib(3)), 7);
        assert_eq!(g.memory_cell(Bytes::gib(5000)), 26, "clamped");
        assert_eq!(g.cpu_cell(MilliCpu(1100)), 4);
        assert_eq!(g.cpu_cell(MilliCpu(9000)), 16);
        assert_eq!(g.gpu_cell(Bytes(0)), 0);
        assert_eq!(g.gpu_cell(Bytes::gib(20)), 11);
        assert_eq!(covering(&[1.0, 2.0], 3.0), (1, true));
        assert_eq!(at_or_below(&[1.0, 2.0], 0.5), None);
        assert_eq!(at_or_below(&[1.0, 2.0], 1.5), Some(0));
    }

    #[test]
    fn rebin_is_conservative() {
        let g = Grid::rs_grid_v1();
        let src = Grid::tuner_mem_v1().values(GridAxis::Cpu);
        let dst = g.values(GridAxis::Cpu);
        let probs = vec![0.1, 0.2, 0.3, 0.1, 0.1, 0.1, 0.1];
        let out = rebin(&src, &probs, &dst);
        assert!((out.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        assert_eq!(out[1], 0.1); // 0.5 → 0.5
        assert_eq!(out[3], 0.2); // 1 → 1
        assert_eq!(out[7], 0.3); // 2 → 2
        // A coarse foreign grid: 1.4 GiB → 1.5Gi cell (never down).
        let foreign = [1.4 * Bytes::GIB as f64, 70.0 * Bytes::GIB as f64, 9e15];
        let out = rebin(&foreign, &[0.5, 0.3, 0.2], &g.values(GridAxis::Memory));
        assert_eq!(out[5], 0.5);
        assert_eq!(out[17], 0.3);
        assert_eq!(out[26], 0.2);
    }
}
