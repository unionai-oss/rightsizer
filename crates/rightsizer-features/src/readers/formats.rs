//! Parquet, NPY, safetensors, CSV and JSONL readers. Each reads only the
//! byte ranges named in the module table of [`super`].

use object_store::path::Path;
use parquet::basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::Type as SchemaType;
use rightsizer_core::ColumnsInfo;

use super::{Extracted, Fetch};
use crate::FeatureError;

fn malformed(format: &'static str, message: impl Into<String>) -> FeatureError {
    FeatureError::Malformed { format, message: message.into() }
}

/// Column kinds.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Numeric,
    String,
    Nested,
    Other,
}

pub(crate) fn columns_info(cols: impl IntoIterator<Item = (String, Kind)>) -> ColumnsInfo {
    let mut c = ColumnsInfo::default();
    let mut names = Vec::new();
    for (name, k) in cols {
        c.total += 1;
        match k {
            Kind::Numeric => c.numeric += 1,
            Kind::String => c.string += 1,
            Kind::Nested => c.nested += 1,
            Kind::Other => c.other += 1,
        }
        names.push(name);
    }
    c.names = Some(names);
    c
}

// ── Parquet ─────────────────────────────────────────────────────────────

/// Footer only: the last 8 bytes (length + `PAR1`), then exactly the
/// metadata. Never a data page, never the page indexes.
pub(crate) async fn parquet(f: &mut Fetch<'_>, path: &Path, size: u64) -> Result<Extracted, FeatureError> {
    if size < 12 {
        return Err(malformed("parquet", "file too small"));
    }
    let tail = f.range(path, size - 8..size).await?;
    if &tail[4..8] == b"PARE" {
        return Err(malformed("parquet", "encrypted footer"));
    }
    if &tail[4..8] != b"PAR1" {
        return Err(malformed("parquet", "missing PAR1 magic"));
    }
    let len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
    if len + 8 > size - 4 {
        return Err(malformed("parquet", "footer length exceeds file"));
    }
    if len > f.remaining() {
        return Err(FeatureError::TooLarge(len + 8));
    }
    let buf = f.range(path, size - 8 - len..size - 8).await?;
    let md = ParquetMetaDataReader::decode_metadata(&buf).map_err(|e| malformed("parquet", e.to_string()))?;
    let fm = md.file_metadata();
    let root = fm.schema_descr().root_schema();
    let cols = root.get_fields().iter().map(|t| (t.name().to_string(), parquet_kind(t)));
    Ok(Extracted { rows: u64::try_from(fm.num_rows()).ok(), columns: Some(columns_info(cols)), ..Extracted::default() })
}

fn parquet_kind(t: &SchemaType) -> Kind {
    if t.is_group() {
        return Kind::Nested;
    }
    let info = t.get_basic_info();
    if info.has_repetition() && info.repetition() == Repetition::REPEATED {
        return Kind::Nested; // legacy 1-level list
    }
    if let Some(lt) = info.logical_type_ref() {
        return match lt {
            LogicalType::String | LogicalType::Enum | LogicalType::Json | LogicalType::Bson | LogicalType::Uuid => Kind::String,
            LogicalType::Decimal { .. } | LogicalType::Integer { .. } | LogicalType::Float16 => Kind::Numeric,
            LogicalType::Map | LogicalType::List | LogicalType::Variant { .. } => Kind::Nested,
            _ => Kind::Other,
        };
    }
    match info.converted_type() {
        ConvertedType::UTF8 | ConvertedType::ENUM | ConvertedType::JSON | ConvertedType::BSON => return Kind::String,
        ConvertedType::DECIMAL
        | ConvertedType::INT_8
        | ConvertedType::INT_16
        | ConvertedType::INT_32
        | ConvertedType::INT_64
        | ConvertedType::UINT_8
        | ConvertedType::UINT_16
        | ConvertedType::UINT_32
        | ConvertedType::UINT_64 => return Kind::Numeric,
        ConvertedType::NONE => {}
        _ => return Kind::Other,
    }
    match t.get_physical_type() {
        PhysicalType::INT32 | PhysicalType::INT64 | PhysicalType::FLOAT | PhysicalType::DOUBLE => Kind::Numeric,
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => Kind::String,
        _ => Kind::Other, // BOOLEAN, INT96 (legacy timestamps)
    }
}

// ── NPY ─────────────────────────────────────────────────────────────────

/// The first 12 bytes (magic, version, header length), then the header.
pub(crate) async fn npy(f: &mut Fetch<'_>, path: &Path, size: u64) -> Result<Extracted, FeatureError> {
    if size < 10 {
        return Err(malformed("npy", "file too small"));
    }
    let pre = f.range(path, 0..size.min(12)).await?;
    if !pre.starts_with(b"\x93NUMPY") {
        return Err(malformed("npy", "missing magic"));
    }
    let (start, hlen) = match pre[6] {
        1 => (10u64, u16::from_le_bytes([pre[8], pre[9]]) as u64),
        2 | 3 if pre.len() >= 12 => (12u64, u32::from_le_bytes([pre[8], pre[9], pre[10], pre[11]]) as u64),
        v => return Err(malformed("npy", format!("unsupported version {v}"))),
    };
    if start + hlen > size {
        return Err(malformed("npy", "header exceeds file"));
    }
    // Bytes [start, 12) were already read in the prefix (v1 only).
    let have = (pre.len() as u64).saturating_sub(start);
    if hlen.saturating_sub(have) > f.remaining() {
        return Err(FeatureError::TooLarge(start + hlen));
    }
    let mut header = pre[start.min(pre.len() as u64) as usize..].to_vec();
    if hlen > have {
        header.extend_from_slice(&f.range(path, start + have..start + hlen).await?);
    }
    header.truncate(hlen as usize);
    let text = String::from_utf8_lossy(&header);
    let descr = npy_value(&text, "descr").ok_or_else(|| malformed("npy", "no descr"))?;
    let dtype = if let Some(s) = descr.strip_prefix('\'').or_else(|| descr.strip_prefix('"')) {
        numpy_dtype_name(s.trim_end_matches(['\'', '"']))
    } else {
        "structured".to_string()
    };
    let shape_txt = npy_value(&text, "shape").ok_or_else(|| malformed("npy", "no shape"))?;
    let inner = shape_txt.trim().trim_start_matches('(').trim_end_matches(')');
    let mut shape = Vec::new();
    for part in inner.split(',') {
        let p = part.trim().trim_end_matches('L');
        if p.is_empty() {
            continue;
        }
        shape.push(p.parse::<u64>().map_err(|_| malformed("npy", format!("bad shape element {p:?}")))?);
    }
    Ok(Extracted { shape: Some(shape), dtype: Some(dtype), ..Extracted::default() })
}

/// The raw value text of `'key':` in a NumPy header dict (a quoted string,
/// a parenthesized tuple, a bracketed list, or a bare word).
fn npy_value<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let pats = [format!("'{key}':"), format!("\"{key}\":")];
    let at = pats.iter().find_map(|p| text.find(p.as_str()).map(|i| i + p.len()))?;
    let rest = text[at..].trim_start();
    let first = rest.chars().next()?;
    let end = match first {
        '\'' | '"' => rest[1..].find(first).map(|i| i + 2)?,
        '(' => rest.find(')').map(|i| i + 1)?,
        '[' => {
            let mut d = 0i32;
            let mut e = None;
            for (i, c) in rest.char_indices() {
                match c {
                    '[' => d += 1,
                    ']' => {
                        d -= 1;
                        if d == 0 {
                            e = Some(i + 1);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            e?
        }
        _ => rest.find([',', '}']).unwrap_or(rest.len()),
    };
    Some(&rest[..end])
}

/// NumPy type string (`<f8`, `|u1`) → dtype name (`float64`, `uint8`).
pub fn numpy_dtype_name(descr: &str) -> String {
    let d = descr.trim_start_matches(['<', '>', '|', '=']);
    let (kind, n) = d.split_at(d.find(|c: char| c.is_ascii_digit()).unwrap_or(d.len()));
    let bytes: u32 = n.parse().unwrap_or(0);
    match (kind, bytes) {
        ("f", 2) => "float16".into(),
        ("f", 4) => "float32".into(),
        ("f", 8) => "float64".into(),
        ("f", 16) => "float128".into(),
        ("i", b @ (1 | 2 | 4 | 8)) => format!("int{}", b * 8),
        ("u", b @ (1 | 2 | 4 | 8)) => format!("uint{}", b * 8),
        ("b", 1) | ("?", _) => "bool".into(),
        ("c", 8) => "complex64".into(),
        ("c", 16) => "complex128".into(),
        ("U", _) => "str".into(),
        ("S", _) | ("a", _) => "bytes".into(),
        ("V", _) => "void".into(),
        ("O", _) => "object".into(),
        (k, _) if k.starts_with('M') => "datetime64".into(),
        (k, _) if k.starts_with('m') => "timedelta64".into(),
        _ => descr.to_string(),
    }
}

// ── safetensors ─────────────────────────────────────────────────────────

/// The 8-byte header length, then the JSON header. Never tensor data.
pub(crate) async fn safetensors(f: &mut Fetch<'_>, path: &Path, size: u64) -> Result<Extracted, FeatureError> {
    let (params, dtype, shape) = safetensors_header(f, path, size).await?;
    Ok(Extracted { params: Some(params), dtype, shape, ..Extracted::default() })
}

/// Parameter count, majority dtype and (single-tensor) shape of a
/// safetensors file, from its header.
pub(crate) async fn safetensors_header(
    f: &mut Fetch<'_>,
    path: &Path,
    size: u64,
) -> Result<(u64, Option<String>, Option<Vec<u64>>), FeatureError> {
    if size < 8 {
        return Err(malformed("safetensors", "file too small"));
    }
    let pre = f.range(path, 0..8).await?;
    let n = u64::from_le_bytes(pre[..8].try_into().map_err(|_| malformed("safetensors", "short read"))?);
    if n.saturating_add(8) > size {
        return Err(malformed("safetensors", "header exceeds file"));
    }
    if n > f.remaining() {
        return Err(FeatureError::TooLarge(n + 8));
    }
    let buf = f.range(path, 8..8 + n).await?;
    let v: serde_json::Value = serde_json::from_slice(&buf).map_err(|e| malformed("safetensors", e.to_string()))?;
    let obj = v.as_object().ok_or_else(|| malformed("safetensors", "header is not an object"))?;
    let mut total: u64 = 0;
    let mut by_dtype: std::collections::BTreeMap<String, u64> = Default::default();
    let mut shapes = Vec::new();
    for (name, t) in obj {
        if name == "__metadata__" {
            continue;
        }
        let shape: Vec<u64> = t
            .get("shape")
            .and_then(|s| s.as_array())
            .ok_or_else(|| malformed("safetensors", format!("tensor {name} has no shape")))?
            .iter()
            .map(|d| d.as_u64().ok_or_else(|| malformed("safetensors", "bad shape")))
            .collect::<Result<_, _>>()?;
        let count = shape.iter().try_fold(1u64, |a, &d| a.checked_mul(d)).unwrap_or(u64::MAX);
        total = total.saturating_add(count);
        let dt = t.get("dtype").and_then(|d| d.as_str()).map(safetensors_dtype_name).unwrap_or_else(|| "unknown".into());
        *by_dtype.entry(dt).or_default() += count;
        shapes.push(shape);
    }
    // Majority dtype by parameters; ties go to the lexicographically smaller.
    let dtype = by_dtype.iter().max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0))).map(|(k, _)| k.clone());
    let shape = if shapes.len() == 1 { shapes.pop() } else { None };
    Ok((total, dtype, shape))
}

/// safetensors dtype (`F32`, `BF16`) → name (`float32`, `bfloat16`).
pub fn safetensors_dtype_name(d: &str) -> String {
    match d {
        "F64" => "float64",
        "F32" => "float32",
        "F16" => "float16",
        "BF16" => "bfloat16",
        "F8_E4M3" => "float8_e4m3fn",
        "F8_E5M2" => "float8_e5m2",
        "I64" => "int64",
        "I32" => "int32",
        "I16" => "int16",
        "I8" => "int8",
        "U64" => "uint64",
        "U32" => "uint32",
        "U16" => "uint16",
        "U8" => "uint8",
        "BOOL" => "bool",
        other => return other.to_ascii_lowercase(),
    }
    .to_string()
}

/// Bytes per element for a dtype name (for `total_size → params`).
pub fn dtype_bytes(name: &str) -> Option<u64> {
    Some(match name {
        "float64" | "int64" | "uint64" => 8,
        "float32" | "int32" | "uint32" => 4,
        "float16" | "bfloat16" | "int16" | "uint16" => 2,
        "float8_e4m3fn" | "float8_e5m2" | "int8" | "uint8" | "bool" => 1,
        _ => return None,
    })
}

// ── CSV / JSONL ─────────────────────────────────────────────────────────

/// The first line (without its terminator), reading 4 KiB, then 64 KiB, then
/// up to `max` bytes. `None` when no line end is found within `max` and the
/// file is longer.
pub(crate) async fn first_line(f: &mut Fetch<'_>, path: &Path, size: u64, max: u64) -> Result<Option<Vec<u8>>, FeatureError> {
    let mut buf: Vec<u8> = Vec::new();
    for step in [4096u64, 65536, max] {
        let want = step.min(max).min(size);
        let have = buf.len() as u64;
        if want <= have {
            continue;
        }
        if want - have > f.remaining() {
            break;
        }
        let chunk = f.range(path, have..want).await?;
        let scan_from = buf.len();
        buf.extend_from_slice(&chunk);
        if let Some(i) = buf[scan_from..].iter().position(|&c| c == b'\n') {
            buf.truncate(scan_from + i);
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(Some(buf));
        }
        if buf.len() as u64 >= size {
            return Ok(Some(buf)); // a single line without a terminator
        }
    }
    Ok(None)
}

/// CSV: the header line only. Delimiter: tab for `.tsv`, otherwise the most
/// frequent of `,` `\t` `;` `|` outside quotes (`,` on ties).
pub(crate) async fn csv(f: &mut Fetch<'_>, path: &Path, size: u64, tsv: bool) -> Result<Extracted, FeatureError> {
    let max = f.remaining().min(65536);
    let Some(line) = first_line(f, path, size, max).await? else {
        return Err(FeatureError::TooLarge(max));
    };
    let mut text = String::from_utf8_lossy(&line).into_owned();
    if let Some(t) = text.strip_prefix('\u{feff}') {
        text = t.to_string();
    }
    if text.trim().is_empty() {
        return Ok(Extracted { columns: Some(ColumnsInfo { total: 0, names: Some(vec![]), ..Default::default() }), ..Default::default() });
    }
    let delim = if tsv { '\t' } else { pick_delimiter(&text) };
    let names = split_csv(&text, delim);
    let total = names.len() as u32;
    Ok(Extracted { columns: Some(ColumnsInfo { total, names: Some(names), ..ColumnsInfo::default() }), ..Extracted::default() })
}

fn pick_delimiter(line: &str) -> char {
    let mut best = (',', 0usize);
    for d in [',', '\t', ';', '|'] {
        let n = split_csv(line, d).len().saturating_sub(1);
        if n > best.1 {
            best = (d, n);
        }
    }
    best.0
}

fn split_csv(line: &str, delim: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut q = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if q {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    q = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' {
            q = true;
        } else if c == delim {
            out.push(cur.trim().to_string());
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    out.push(cur.trim().to_string());
    out
}

/// JSONL: the first record's keys only, with kinds from its values
/// (number → numeric, string → string, object/array → nested, bool/null →
/// other).
pub(crate) async fn jsonl(f: &mut Fetch<'_>, path: &Path, size: u64) -> Result<Extracted, FeatureError> {
    let max = f.remaining();
    let Some(line) = first_line(f, path, size, max).await? else {
        return Err(FeatureError::TooLarge(max));
    };
    let v: serde_json::Value = serde_json::from_slice(&line).map_err(|e| malformed("jsonl", e.to_string()))?;
    let obj = v.as_object().ok_or_else(|| malformed("jsonl", "first record is not an object"))?;
    let cols = obj.iter().map(|(k, v)| {
        let kind = match v {
            serde_json::Value::Number(_) => Kind::Numeric,
            serde_json::Value::String(_) => Kind::String,
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Kind::Nested,
            _ => Kind::Other,
        };
        (k.clone(), kind)
    });
    Ok(Extracted { columns: Some(columns_info(cols)), ..Extracted::default() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn npy_header_values() {
        let h = "{'descr': '<f4', 'fortran_order': False, 'shape': (3, 4), }";
        assert_eq!(npy_value(h, "descr"), Some("'<f4'"));
        assert_eq!(npy_value(h, "shape"), Some("(3, 4)"));
        assert_eq!(numpy_dtype_name("<f8"), "float64");
        assert_eq!(numpy_dtype_name("|u1"), "uint8");
        assert_eq!(numpy_dtype_name("<i4"), "int32");
        assert_eq!(numpy_dtype_name("|b1"), "bool");
        assert_eq!(numpy_dtype_name("<U12"), "str");
    }

    #[test]
    fn csv_split() {
        assert_eq!(split_csv("a,\"b,c\",d", ','), vec!["a", "b,c", "d"]);
        assert_eq!(pick_delimiter("a;b;c"), ';');
        assert_eq!(pick_delimiter("a\tb"), '\t');
        assert_eq!(pick_delimiter("single"), ',');
    }
}
