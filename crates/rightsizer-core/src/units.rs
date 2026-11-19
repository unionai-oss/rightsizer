//! Platform quantity strings ⇄ integer quantities (SPEC §5.1).
//!
//! - **K8s** (and **Flyte**, which uses K8s strings): `1.5Gi`, `500m`, `1e9`,
//!   `128974848`, `129M`. Parsing is exact (integer arithmetic) and rounds up
//!   to the target unit (bytes or millicores).
//! - **Slurm** `--mem`: default unit MiB; suffixes K, M, G, T are powers of 1024.
//! - **Ray**: bytes (a JSON number).
//!
//! Human-readable strings exist only at platform boundaries; the wire carries
//! integers (ARCHITECTURE.md §4).

use crate::types::{Bytes, MilliCpu};

/// Errors parsing a quantity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnitsError {
    /// Empty input.
    #[error("empty quantity")]
    Empty,
    /// Malformed input.
    #[error("invalid quantity {0:?}")]
    Invalid(String),
    /// Negative where a resource quantity is required.
    #[error("negative quantity {0:?}")]
    Negative(String),
    /// Too large for the target integer.
    #[error("quantity {0:?} overflows")]
    Overflow(String),
}

/// An exactly parsed K8s quantity: `±mantissa × 10^exp10 × 2^pow2`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Quantity {
    negative: bool,
    mantissa: u128,
    exp10: i32,
    pow2: u32,
}

impl Quantity {
    /// Whether the value is negative.
    pub fn is_negative(&self) -> bool {
        self.negative && self.mantissa != 0
    }

    /// The value × `10^extra10`, rounded up to an integer (magnitude only).
    fn scaled_ceil(&self, extra10: i32) -> Option<u128> {
        let num = self.mantissa.checked_mul(1u128.checked_shl(self.pow2)?)?;
        let e = self.exp10 + extra10;
        if e >= 0 {
            num.checked_mul(10u128.checked_pow(e as u32)?)
        } else {
            let d = -e as u32;
            if d > 38 {
                return Some(if num == 0 { 0 } else { 1 });
            }
            Some(num.div_ceil(10u128.pow(d)))
        }
    }

    /// Bytes, rounded up. Errors on negative or overflowing values.
    pub fn to_bytes(&self, src: &str) -> Result<Bytes, UnitsError> {
        if self.is_negative() {
            return Err(UnitsError::Negative(src.into()));
        }
        let v = self.scaled_ceil(0).ok_or_else(|| UnitsError::Overflow(src.into()))?;
        u64::try_from(v).map(Bytes).map_err(|_| UnitsError::Overflow(src.into()))
    }

    /// Millicores (value × 1000), rounded up.
    pub fn to_millicpu(&self, src: &str) -> Result<MilliCpu, UnitsError> {
        if self.is_negative() {
            return Err(UnitsError::Negative(src.into()));
        }
        let v = self.scaled_ceil(3).ok_or_else(|| UnitsError::Overflow(src.into()))?;
        u32::try_from(v).map(MilliCpu).map_err(|_| UnitsError::Overflow(src.into()))
    }

    /// Approximate value as `f64`.
    pub fn as_f64(&self) -> f64 {
        let v = self.mantissa as f64 * 10f64.powi(self.exp10) * 2f64.powi(self.pow2 as i32);
        if self.negative { -v } else { v }
    }
}

/// Parse a decimal number `[+-]digits[.digits]` into (negative, mantissa, exp10).
fn parse_decimal(num: &str, src: &str) -> Result<(bool, u128, i32), UnitsError> {
    let bad = || UnitsError::Invalid(src.to_string());
    let (neg, body) = match num.as_bytes().first() {
        Some(b'-') => (true, &num[1..]),
        Some(b'+') => (false, &num[1..]),
        _ => (false, num),
    };
    if body.is_empty() || body == "." {
        return Err(bad());
    }
    let (int, frac) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let mut m: u128 = 0;
    for b in int.bytes().chain(frac.bytes()) {
        m = m.checked_mul(10).and_then(|m| m.checked_add((b - b'0') as u128)).ok_or_else(|| UnitsError::Overflow(src.into()))?;
    }
    Ok((neg, m, -(frac.len() as i32)))
}

/// Parse a Kubernetes quantity (`resource.Quantity` syntax): a decimal number
/// followed by a binary suffix (`Ki Mi Gi Ti Pi Ei`), a decimal suffix
/// (`n u m k M G T P E`), or a decimal exponent (`e3`, `E-2`).
pub fn parse_k8s_quantity(s: &str) -> Result<Quantity, UnitsError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(UnitsError::Empty);
    }
    let bad = || UnitsError::Invalid(s.to_string());
    // Split number from suffix: the number is [+-]?[0-9.]*.
    let split = t
        .char_indices()
        .find(|&(i, c)| !(c.is_ascii_digit() || c == '.' || (i == 0 && (c == '+' || c == '-'))))
        .map(|(i, _)| i)
        .unwrap_or(t.len());
    let (num, suffix) = t.split_at(split);
    let (negative, mantissa, mut exp10) = parse_decimal(num, s)?;
    let mut pow2 = 0u32;
    match suffix {
        "" => {}
        "Ki" => pow2 = 10,
        "Mi" => pow2 = 20,
        "Gi" => pow2 = 30,
        "Ti" => pow2 = 40,
        "Pi" => pow2 = 50,
        "Ei" => pow2 = 60,
        "n" => exp10 -= 9,
        "u" => exp10 -= 6,
        "m" => exp10 -= 3,
        "k" => exp10 += 3,
        "M" => exp10 += 6,
        "G" => exp10 += 9,
        "T" => exp10 += 12,
        "P" => exp10 += 15,
        "E" => exp10 += 18,
        other if other.starts_with(['e', 'E']) => {
            let e: i32 = other[1..].parse().map_err(|_| bad())?;
            if !other[1..].bytes().all(|b| b.is_ascii_digit() || b == b'-' || b == b'+') || e.abs() > 60 {
                return Err(bad());
            }
            exp10 += e;
        }
        _ => return Err(bad()),
    }
    Ok(Quantity { negative, mantissa, exp10, pow2 })
}

/// Parse a K8s memory / storage quantity to bytes (rounded up).
pub fn parse_k8s_bytes(s: &str) -> Result<Bytes, UnitsError> {
    parse_k8s_quantity(s)?.to_bytes(s)
}

/// Parse a K8s CPU quantity to millicores (rounded up).
pub fn parse_k8s_cpu(s: &str) -> Result<MilliCpu, UnitsError> {
    parse_k8s_quantity(s)?.to_millicpu(s)
}

/// Format bytes as a K8s quantity that parses back exactly. Prefers the largest
/// binary suffix that divides exactly (`64Gi`), then a half step of a binary
/// suffix (`1.5Gi`), then a decimal suffix (`129M`), then plain bytes.
pub fn format_k8s_bytes(b: u64) -> String {
    if b == 0 {
        return "0".into();
    }
    const BIN: [(&str, u32); 6] = [("Ei", 60), ("Pi", 50), ("Ti", 40), ("Gi", 30), ("Mi", 20), ("Ki", 10)];
    for (suf, p) in BIN {
        let unit = 1u128 << p;
        let v = b as u128;
        if v.is_multiple_of(unit) {
            return format!("{}{}", v / unit, suf);
        }
        if v >= unit && (v * 2).is_multiple_of(unit) {
            return format!("{}.5{}", v / unit, suf);
        }
    }
    const DEC: [(&str, u32); 6] = [("E", 18), ("P", 15), ("T", 12), ("G", 9), ("M", 6), ("k", 3)];
    for (suf, e) in DEC {
        let unit = 10u64.pow(e);
        if b.is_multiple_of(unit) {
            return format!("{}{}", b / unit, suf);
        }
    }
    b.to_string()
}

/// Format millicores as a K8s CPU quantity: whole cores as integers (`"2"`),
/// otherwise millicores (`"500m"`).
pub fn format_k8s_cpu(c: MilliCpu) -> String {
    if c.0.is_multiple_of(1000) { (c.0 / 1000).to_string() } else { format!("{}m", c.0) }
}

/// Flyte uses K8s quantity strings.
pub fn parse_flyte_bytes(s: &str) -> Result<Bytes, UnitsError> {
    parse_k8s_bytes(s)
}
/// Flyte uses K8s quantity strings.
pub fn parse_flyte_cpu(s: &str) -> Result<MilliCpu, UnitsError> {
    parse_k8s_cpu(s)
}
/// Flyte uses K8s quantity strings.
pub fn format_flyte_bytes(b: Bytes) -> String {
    format_k8s_bytes(b.0)
}
/// Flyte uses K8s quantity strings.
pub fn format_flyte_cpu(c: MilliCpu) -> String {
    format_k8s_cpu(c)
}

/// Parse a Slurm memory value (`--mem`, `--mem-per-cpu`): a number with an
/// optional suffix K, M, G or T (powers of 1024; case-insensitive); no
/// suffix means MiB. `0` (the whole node) parses to `Bytes(0)`; callers treat
/// it specially (slurm.md: `--mem=0` is never changed).
pub fn parse_slurm_mem(s: &str) -> Result<Bytes, UnitsError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(UnitsError::Empty);
    }
    let (num, pow) = match t.as_bytes()[t.len() - 1].to_ascii_uppercase() {
        b'K' => (&t[..t.len() - 1], 10u32),
        b'M' => (&t[..t.len() - 1], 20),
        b'G' => (&t[..t.len() - 1], 30),
        b'T' => (&t[..t.len() - 1], 40),
        c if c.is_ascii_digit() || c == b'.' => (t, 20),
        _ => return Err(UnitsError::Invalid(s.into())),
    };
    let (neg, mantissa, exp10) = parse_decimal(num, s)?;
    if neg && mantissa != 0 {
        return Err(UnitsError::Negative(s.into()));
    }
    Quantity { negative: false, mantissa, exp10, pow2: pow }.to_bytes(s)
}

/// Format bytes for Slurm: the largest of T, G, M that divides exactly, else
/// KiB rounded up (`"1536M"`, `"64G"`, `"513K"`).
pub fn format_slurm_mem(b: Bytes) -> String {
    let v = b.0;
    if v == 0 {
        return "0".into();
    }
    for (suf, p) in [("T", 40u32), ("G", 30), ("M", 20)] {
        if v.is_multiple_of(1u64 << p) {
            return format!("{}{}", v >> p, suf);
        }
    }
    format!("{}K", v.div_ceil(1024))
}

/// Parse a Ray memory value (bytes, possibly fractional), rounded up.
pub fn parse_ray_memory(v: f64) -> Result<Bytes, UnitsError> {
    if !v.is_finite() {
        return Err(UnitsError::Invalid(v.to_string()));
    }
    if v < 0.0 {
        return Err(UnitsError::Negative(v.to_string()));
    }
    if v >= u64::MAX as f64 {
        return Err(UnitsError::Overflow(v.to_string()));
    }
    Ok(Bytes::from_f64(v))
}

/// Format bytes for Ray (an integer byte count).
pub fn format_ray_memory(b: Bytes) -> u64 {
    b.0
}

/// Parse Ray `num_cpus` (fractional cores) to millicores, rounded up.
pub fn parse_ray_cpus(v: f64) -> Result<MilliCpu, UnitsError> {
    if !v.is_finite() || v < 0.0 {
        return Err(UnitsError::Invalid(v.to_string()));
    }
    Ok(MilliCpu::from_cores_f64(v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn k8s_examples() {
        assert_eq!(parse_k8s_bytes("1.5Gi").unwrap(), Bytes(1_610_612_736));
        assert_eq!(parse_k8s_bytes("128974848").unwrap(), Bytes(128_974_848));
        assert_eq!(parse_k8s_bytes("129M").unwrap(), Bytes(129_000_000));
        assert_eq!(parse_k8s_bytes("1e9").unwrap(), Bytes(1_000_000_000));
        assert_eq!(parse_k8s_bytes("1E3").unwrap(), Bytes(1000));
        assert_eq!(parse_k8s_bytes("1E").unwrap(), Bytes(10u64.pow(18)));
        assert_eq!(parse_k8s_bytes("123Mi").unwrap(), Bytes(123 * 1048576));
        assert_eq!(parse_k8s_bytes("0.5").unwrap(), Bytes(1), "fractional bytes round up");
        assert_eq!(parse_k8s_bytes(" 64Gi ").unwrap(), Bytes::gib(64));
        assert_eq!(parse_k8s_cpu("500m").unwrap(), MilliCpu(500));
        assert_eq!(parse_k8s_cpu("1.5").unwrap(), MilliCpu(1500));
        assert_eq!(parse_k8s_cpu("2").unwrap(), MilliCpu(2000));
        assert_eq!(parse_k8s_cpu("100u").unwrap(), MilliCpu(1), "rounds up");
        assert_eq!(parse_k8s_cpu("0.25").unwrap(), MilliCpu(250));
        for bad in ["", "Gi", "1.2.3", "1Gb", "1K", "abc", "1e", "--1", "1 Gi"] {
            assert!(parse_k8s_quantity(bad).is_err(), "{bad:?}");
        }
        assert!(matches!(parse_k8s_bytes("-1Gi"), Err(UnitsError::Negative(_))));
        assert!(matches!(parse_k8s_bytes("100Ei"), Err(UnitsError::Overflow(_))));
    }

    #[test]
    fn k8s_format() {
        assert_eq!(format_k8s_bytes(Bytes::gib(64).0), "64Gi");
        assert_eq!(format_k8s_bytes(1_610_612_736), "1.5Gi");
        assert_eq!(format_k8s_bytes(129_000_000), "129M");
        assert_eq!(format_k8s_bytes(768 * 1048576), "768Mi");
        assert_eq!(format_k8s_bytes(1001), "1001");
        assert_eq!(format_k8s_bytes(0), "0");
        assert_eq!(format_k8s_cpu(MilliCpu(250)), "250m");
        assert_eq!(format_k8s_cpu(MilliCpu(8000)), "8");
    }

    #[test]
    fn slurm_examples() {
        assert_eq!(parse_slurm_mem("4096").unwrap(), Bytes::gib(4));
        assert_eq!(parse_slurm_mem("4G").unwrap(), Bytes::gib(4));
        assert_eq!(parse_slurm_mem("4g").unwrap(), Bytes::gib(4));
        assert_eq!(parse_slurm_mem("512K").unwrap(), Bytes(512 * 1024));
        assert_eq!(parse_slurm_mem("1T").unwrap(), Bytes(1 << 40));
        assert_eq!(parse_slurm_mem("1.5G").unwrap(), Bytes(1_610_612_736));
        assert_eq!(parse_slurm_mem("0").unwrap(), Bytes(0));
        assert!(parse_slurm_mem("4GB").is_err());
        assert!(parse_slurm_mem("").is_err());
        assert_eq!(format_slurm_mem(Bytes::gib(64)), "64G");
        assert_eq!(format_slurm_mem(Bytes::mib(1536)), "1536M");
        assert_eq!(format_slurm_mem(Bytes(1025)), "2K");
    }

    #[test]
    fn ray_examples() {
        assert_eq!(parse_ray_memory(1.5e9).unwrap(), Bytes(1_500_000_000));
        assert_eq!(parse_ray_memory(0.2).unwrap(), Bytes(1));
        assert!(parse_ray_memory(-1.0).is_err());
        assert!(parse_ray_memory(f64::NAN).is_err());
        assert_eq!(format_ray_memory(Bytes(42)), 42);
        assert_eq!(parse_ray_cpus(0.5).unwrap(), MilliCpu(500));
    }

    proptest! {
        #[test]
        fn k8s_bytes_round_trip(b in any::<u64>()) {
            let s = format_k8s_bytes(b);
            prop_assert_eq!(parse_k8s_bytes(&s).unwrap(), Bytes(b), "{}", s);
        }

        #[test]
        fn k8s_bytes_round_trip_on_grid_like_values(n in 1u64..1_000_000, p in 0u32..4) {
            let b = n << (10 * p);
            prop_assert_eq!(parse_k8s_bytes(&format_k8s_bytes(b)).unwrap(), Bytes(b));
        }

        #[test]
        fn k8s_cpu_round_trip(m in any::<u32>()) {
            let s = format_k8s_cpu(MilliCpu(m));
            prop_assert_eq!(parse_k8s_cpu(&s).unwrap(), MilliCpu(m));
        }

        #[test]
        fn k8s_binary_suffix_exact(n in 0u64..(1 << 20), i in 0usize..4) {
            let (suf, p) = [("Ki", 10), ("Mi", 20), ("Gi", 30), ("Ti", 40)][i];
            prop_assert_eq!(parse_k8s_bytes(&format!("{n}{suf}")).unwrap(), Bytes(n << p));
        }

        #[test]
        fn k8s_decimal_cores(c in 0u32..100_000) {
            let s = format!("{}.{:03}", c / 1000, c % 1000);
            prop_assert_eq!(parse_k8s_cpu(&s).unwrap(), MilliCpu(c));
        }

        #[test]
        fn slurm_round_trip_kib_multiples(k in 0u64..(1u64 << 40)) {
            let b = Bytes(k * 1024);
            prop_assert_eq!(parse_slurm_mem(&format_slurm_mem(b)).unwrap(), b);
        }

        #[test]
        fn slurm_format_never_undershoots(b in 0u64..(1u64 << 50)) {
            let back = parse_slurm_mem(&format_slurm_mem(Bytes(b))).unwrap().0;
            prop_assert!(back >= b && back < b + 1024);
        }

        #[test]
        fn slurm_default_unit_is_mib(m in 0u64..(1 << 30)) {
            prop_assert_eq!(parse_slurm_mem(&m.to_string()).unwrap(), Bytes(m << 20));
        }

        #[test]
        fn ray_round_trip(b in 0u64..(1u64 << 52)) {
            prop_assert_eq!(parse_ray_memory(format_ray_memory(Bytes(b)) as f64).unwrap(), Bytes(b));
        }
    }
}
