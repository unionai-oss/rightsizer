//! Hugging Face model refs (`hf://org/model[@rev]`, SPEC §9 M3).
//!
//! On the hub's HTTP store (path `{repo}/resolve/{rev}`) it reads, within
//! the input's byte budget:
//!
//! 1. `config.json` → `dtype` (`torch_dtype`, or `dtype`);
//! 2. `model.safetensors.index.json` → `bytes` = `metadata.total_size`;
//!    `params` = `metadata.total_parameters` when present, else the sum over
//!    the shards' safetensors headers (each a bounded range read), else
//!    `total_size / bytes-per-dtype`;
//! 3. without an index, `model.safetensors` → `bytes` from HEAD and
//!    `params`/`dtype` from its header.
//!
//! Weights are never downloaded.

use std::collections::BTreeSet;

use object_store::path::Path;
use rightsizer_core::InputFormat;

use super::formats::{dtype_bytes, safetensors_header};
use super::{Extracted, Fetch, Slot, lock};
use crate::FeatureError;

async fn small_json(f: &mut Fetch<'_>, path: &Path) -> Result<serde_json::Value, FeatureError> {
    let meta = f.head(path).await?;
    if meta.size > f.remaining() {
        return Err(FeatureError::TooLarge(meta.size));
    }
    let b = f.range(path, 0..meta.size).await?;
    serde_json::from_slice(&b).map_err(|e| FeatureError::Malformed { format: "hf", message: e.to_string() })
}

pub(crate) async fn read(f: &mut Fetch<'_>, base: &Path, slot: &Slot) -> Result<Extracted, FeatureError> {
    let mut x = Extracted { format: Some(InputFormat::ModelRef), ..Extracted::default() };
    if let Ok(cfg) = small_json(f, &base.clone().join("config.json")).await {
        x.dtype = cfg
            .get("torch_dtype")
            .or_else(|| cfg.get("dtype"))
            .or_else(|| cfg.get("text_config").and_then(|t| t.get("torch_dtype")))
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());
    }
    // Publish progress so a budget expiry keeps what is known.
    x.apply(&mut lock(slot));

    match small_json(f, &base.clone().join("model.safetensors.index.json")).await {
        Ok(index) => {
            let md = index.get("metadata");
            x.bytes = md.and_then(|m| m.get("total_size")).and_then(|v| v.as_u64());
            x.params = md.and_then(|m| m.get("total_parameters")).and_then(|v| v.as_u64());
            x.apply(&mut lock(slot));
            if x.params.is_none() {
                let shards: BTreeSet<String> = index
                    .get("weight_map")
                    .and_then(|w| w.as_object())
                    .map(|w| w.values().filter_map(|v| v.as_str().map(str::to_string)).collect())
                    .unwrap_or_default();
                let mut sum: u64 = 0;
                let mut complete = !shards.is_empty();
                for s in &shards {
                    let p = base.clone().join(s.as_str());
                    let size = match f.head(&p).await {
                        Ok(m) => m.size,
                        Err(_) => {
                            complete = false;
                            break;
                        }
                    };
                    match safetensors_header(f, &p, size).await {
                        Ok((n, dt, _)) => {
                            sum = sum.saturating_add(n);
                            if x.dtype.is_none() {
                                x.dtype = dt;
                            }
                        }
                        Err(_) => {
                            complete = false;
                            break;
                        }
                    }
                }
                x.params = if complete {
                    Some(sum)
                } else {
                    let per = x.dtype.as_deref().and_then(dtype_bytes);
                    x.bytes.zip(per).map(|(b, p)| b / p)
                };
            }
        }
        Err(_) => {
            let p = base.clone().join("model.safetensors");
            let meta = f.head(&p).await?;
            x.bytes = Some(meta.size);
            x.apply(&mut lock(slot));
            let (n, dt, _) = safetensors_header(f, &p, meta.size).await?;
            x.params = Some(n);
            if x.dtype.is_none() {
                x.dtype = dt;
            }
        }
    }
    Ok(x)
}
