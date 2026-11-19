//! Arrow IPC schema reader (file and stream formats), with a minimal,
//! bounds-checked FlatBuffers decoder for the `Message` → `Schema` →
//! `Field` tables (Arrow `Message.fbs`, `Schema.fbs`). Reads the file magic,
//! the first message's 8-byte prefix and the schema message; never a record
//! batch body.

use object_store::path::Path;

use super::formats::{Kind, columns_info};
use super::{Extracted, Fetch};
use crate::FeatureError;

fn malformed(message: impl Into<String>) -> FeatureError {
    FeatureError::Malformed { format: "arrow_ipc", message: message.into() }
}

pub(crate) async fn read(f: &mut Fetch<'_>, path: &Path, size: u64) -> Result<Extracted, FeatureError> {
    if size < 8 {
        return Err(malformed("file too small"));
    }
    let magic = f.range(path, 0..8).await?;
    // File format: "ARROW1" + 2 padding bytes, then the stream.
    let (offset, pre) = if magic.starts_with(b"ARROW1") {
        if size < 16 {
            return Err(malformed("file too small"));
        }
        (8u64, f.range(path, 8..16).await?)
    } else {
        (0u64, magic)
    };
    let (len, start) = if pre[0..4] == [0xff; 4] {
        (i32::from_le_bytes([pre[4], pre[5], pre[6], pre[7]]), offset + 8)
    } else {
        // Pre-0.15 streams: no continuation marker.
        (i32::from_le_bytes([pre[0], pre[1], pre[2], pre[3]]), offset + 4)
    };
    if len <= 0 {
        return Err(malformed("no schema message"));
    }
    let len = len as u64;
    if start + len > size {
        return Err(malformed("schema message exceeds file"));
    }
    // A legacy stream's first 4 body bytes were already read with the prefix.
    let already = if start == offset + 4 { 4u64 } else { 0 };
    if len.saturating_sub(already) > f.remaining() {
        return Err(FeatureError::TooLarge(start + len));
    }
    let mut buf = if already > 0 { pre[4..8].to_vec() } else { Vec::new() };
    buf.extend_from_slice(&f.range(path, start + already..start + len).await?);
    buf.truncate(len as usize);
    let cols = schema_columns(&buf).ok_or_else(|| malformed("cannot decode schema message"))?;
    Ok(Extracted { columns: Some(columns_info(cols)), ..Extracted::default() })
}

/// `(name, kind)` of each top-level field of a `Message` holding a `Schema`.
pub(crate) fn schema_columns(buf: &[u8]) -> Option<Vec<(String, Kind)>> {
    let fb = Fb(buf);
    let msg = fb.root()?;
    // Message: 0 version, 1 header_type, 2 header, 3 bodyLength.
    if fb.field_u8(msg, 1)? != 1 {
        return None; // header is not a Schema
    }
    let schema = fb.field_table(msg, 2)?;
    // Schema: 0 endianness, 1 fields, 2 custom_metadata, 3 features.
    let Some(fields) = fb.field_vector(schema, 1) else { return Some(Vec::new()) };
    let mut out = Vec::new();
    for i in 0..fields.1 {
        let field = fb.vector_table(fields, i)?;
        // Field: 0 name, 1 nullable, 2 type_type, 3 type, 4 dictionary, 5 children.
        let name = fb.field_string(field, 0).unwrap_or_default();
        let ty = fb.field_u8(field, 2).unwrap_or(0);
        out.push((name, arrow_kind(ty)));
    }
    Some(out)
}

/// Kind of an Arrow `Type` union tag.
fn arrow_kind(tag: u8) -> Kind {
    match tag {
        2 | 3 | 7 => Kind::Numeric,                      // Int, FloatingPoint, Decimal
        4 | 5 | 15 | 19 | 20 | 23 | 24 => Kind::String, // Binary, Utf8, FixedSizeBinary, Large*, *View
        12 | 13 | 14 | 16 | 17 | 21 | 25 | 26 => Kind::Nested, // List, Struct, Union, FixedSizeList, Map, LargeList, ListView, LargeListView
        _ => Kind::Other,                                // Null, Bool, Date, Time, Timestamp, Interval, Duration, RunEndEncoded
    }
}

/// Bounds-checked FlatBuffers reads. Every accessor returns `None` on a
/// malformed buffer instead of panicking.
struct Fb<'a>(&'a [u8]);

impl<'a> Fb<'a> {
    fn u8_at(&self, p: usize) -> Option<u8> {
        self.0.get(p).copied()
    }
    fn u16_at(&self, p: usize) -> Option<u16> {
        Some(u16::from_le_bytes(self.0.get(p..p.checked_add(2)?)?.try_into().ok()?))
    }
    fn u32_at(&self, p: usize) -> Option<u32> {
        Some(u32::from_le_bytes(self.0.get(p..p.checked_add(4)?)?.try_into().ok()?))
    }
    fn i32_at(&self, p: usize) -> Option<i32> {
        Some(i32::from_le_bytes(self.0.get(p..p.checked_add(4)?)?.try_into().ok()?))
    }
    fn root(&self) -> Option<usize> {
        Some(self.u32_at(0)? as usize)
    }
    /// Absolute position of field `i` of the table at `t`, if present.
    fn field_pos(&self, t: usize, i: usize) -> Option<usize> {
        let vt = (t as i64).checked_sub(self.i32_at(t)? as i64)?;
        let vt = usize::try_from(vt).ok()?;
        let vt_len = self.u16_at(vt)? as usize;
        let slot = 4 + 2 * i;
        if slot + 2 > vt_len {
            return None;
        }
        let off = self.u16_at(vt + slot)? as usize;
        if off == 0 { None } else { t.checked_add(off) }
    }
    fn field_u8(&self, t: usize, i: usize) -> Option<u8> {
        match self.field_pos(t, i) {
            Some(p) => self.u8_at(p),
            None => Some(0), // default
        }
    }
    fn deref(&self, p: usize) -> Option<usize> {
        p.checked_add(self.u32_at(p)? as usize)
    }
    fn field_table(&self, t: usize, i: usize) -> Option<usize> {
        self.deref(self.field_pos(t, i)?)
    }
    /// `(position of the first element, length)`.
    fn field_vector(&self, t: usize, i: usize) -> Option<(usize, usize)> {
        let v = self.deref(self.field_pos(t, i)?)?;
        let n = self.u32_at(v)? as usize;
        if n > self.0.len() / 4 {
            return None;
        }
        Some((v + 4, n))
    }
    fn vector_table(&self, v: (usize, usize), i: usize) -> Option<usize> {
        if i >= v.1 {
            return None;
        }
        self.deref(v.0 + 4 * i)
    }
    fn field_string(&self, t: usize, i: usize) -> Option<String> {
        let s = self.deref(self.field_pos(t, i)?)?;
        let n = self.u32_at(s)? as usize;
        let bytes = self.0.get(s + 4..(s + 4).checked_add(n)?)?;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn garbage_never_panics() {
        for len in 0..64 {
            let buf: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            let _ = schema_columns(&buf);
        }
        let _ = schema_columns(&[0xff; 32]);
    }
}
