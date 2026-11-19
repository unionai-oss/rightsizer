//! What a remote estimator may see (SPEC §11): re-exports
//! `rightsizer-features`' privacy view, which the remote client applies to
//! every context before it computes cache keys or sends anything.
//!
//! | field | remote default | override |
//! |---|---|---|
//! | code digest, tokens | sent | — |
//! | full source | dropped | `send_source = true` |
//! | input URIs | never present (salted hash only) | — |
//! | column names, string scalars, labels | dropped (counts and kinds stay) | `send = ["columns", "strings", "labels", "labels:<key>"]` |
//! | numeric scalars, sizes, rows, shapes, dtypes, parameter counts | sent | `redact = ["scalars", "sizes", "rows", "shapes", "dtypes", "params", "columns"]` |
//! | history | sent, ≤ `history_limit` | `history_limit` |

pub use rightsizer_features::{Privacy, redact_for_remote};

/// The privacy settings remote members get (kept as an alias for callers
/// that name the remote view).
pub type RemotePrivacy = Privacy;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use rightsizer_core::*;

    #[test]
    fn defaults_drop_source_names_strings_labels() {
        let mut i = InputMeta::blob("x", 10);
        i.columns =
            Some(ColumnsInfo { total: 2, numeric: 1, string: 1, names: Some(vec!["ssn".into(), "age".into()]), ..Default::default() });
        let mut s = InputMeta::blob("who", 0);
        s.scalar = Some(ScalarValue::Str("alice".into()));
        let mut n = InputMeta::blob("k", 0);
        n.scalar = Some(ScalarValue::Int(3));
        let mut r = record("r", RunStatus::Succeeded, None, None);
        r.inputs = vec![i.clone()];
        let mut ctx = ctx_with(vec![i, s, n], vec![r.clone(), r.clone(), r]);
        ctx.code = Some(CodeInfo { digest: "d".into(), tokens: vec!["t".into()], source: Some("secret".into()) });
        ctx.labels.insert("team".into(), "ml".into());
        ctx.labels.insert("owner".into(), "ann".into());
        let p = Privacy { history_limit: 2, send: vec!["labels:team".into()], ..Default::default() };
        let out = redact_for_remote(&ctx, &p);
        let code = out.code.as_ref().unwrap();
        assert!(code.source.is_none() && code.tokens == vec!["t"] && code.digest == "d");
        let cols = out.inputs[0].columns.as_ref().unwrap();
        assert!(cols.names.is_none() && cols.total == 2);
        assert!(out.inputs[1].scalar.is_none());
        assert_eq!(out.inputs[2].scalar, Some(ScalarValue::Int(3)));
        assert_eq!(out.labels.len(), 1);
        assert_eq!(out.history.len(), 2);
        assert!(out.history[0].inputs[0].columns.as_ref().unwrap().names.is_none());
        let p = Privacy { redact: vec!["sizes".into(), "scalars".into()], send_source: true, ..Default::default() };
        let out = redact_for_remote(&ctx, &p);
        assert!(out.inputs[0].bytes.is_none() && out.inputs[2].scalar.is_none());
        assert_eq!(out.code.unwrap().source.as_deref(), Some("secret"));
    }
}
