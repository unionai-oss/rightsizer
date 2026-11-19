//! Network tests: ignored by default (no network in CI). Run with
//! `cargo test -p rightsizer-features --features http,s3 -- --ignored`.

#[cfg(feature = "http")]
#[tokio::test]
#[ignore = "needs network (huggingface.co)"]
async fn hf_public_model_params() {
    use rightsizer_core::{InputFormat, InputRef};
    use rightsizer_features::{DefaultResolver, ReadBudget, read_inputs};
    let refs = vec![InputRef {
        name: "m".into(),
        uri: Some("hf://openai-community/gpt2".into()),
        bytes: None,
        format: None,
        scalar: None,
    }];
    let budget = ReadBudget { per_context: std::time::Duration::from_secs(30), ..ReadBudget::default() };
    let out = read_inputs(&refs, &DefaultResolver::default(), budget).await;
    assert_eq!(out[0].format, InputFormat::ModelRef);
    let params = out[0].params.expect("params");
    assert!((120_000_000..140_000_000).contains(&params), "gpt2 has ~124M parameters, got {params}");
    assert!(out[0].bytes.is_some());
}

#[cfg(feature = "http")]
#[tokio::test]
#[ignore = "needs network (huggingface.co)"]
async fn hf_sharded_model_params() {
    use rightsizer_core::InputRef;
    use rightsizer_features::{DefaultResolver, ReadBudget, read_inputs};
    let refs = vec![InputRef { name: "m".into(), uri: Some("hf://Qwen/Qwen2.5-3B".into()), bytes: None, format: None, scalar: None }];
    let budget = ReadBudget { per_context: std::time::Duration::from_secs(30), ..ReadBudget::default() };
    let out = read_inputs(&refs, &DefaultResolver::default(), budget).await;
    let params = out[0].params.expect("params");
    assert!((2_900_000_000..3_300_000_000).contains(&params), "Qwen2.5-3B has ~3.09B parameters, got {params}");
    assert_eq!(out[0].dtype.as_deref(), Some("bfloat16"));
    assert!(out[0].bytes.unwrap() > 5_000_000_000);
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "needs network and AWS credentials; set RIGHTSIZER_TEST_S3_URI=s3://bucket/key.parquet"]
async fn s3_parquet_footer() {
    use rightsizer_core::InputRef;
    use rightsizer_features::{DefaultResolver, ReadBudget, read_inputs};
    let Ok(uri) = std::env::var("RIGHTSIZER_TEST_S3_URI") else { return };
    let refs = vec![InputRef { name: "t".into(), uri: Some(uri), bytes: None, format: None, scalar: None }];
    let budget = ReadBudget { per_context: std::time::Duration::from_secs(30), ..ReadBudget::default() };
    let out = read_inputs(&refs, &DefaultResolver::default(), budget).await;
    assert!(out[0].bytes.is_some());
    assert!(out[0].rows.is_some());
}
