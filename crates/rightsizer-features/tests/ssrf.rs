//! SSRF guard: internal addresses are refused before any store is touched.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::*;
use rightsizer_core::InputRef;
use rightsizer_features::SsrfGuard;
use rightsizer_features::readers::{ReadOptions, read_inputs_with};
use url::Url;

fn input(uri: &str) -> InputRef {
    InputRef { name: "x".into(), uri: Some(uri.into()), bytes: None, format: None, scalar: None }
}

#[tokio::test]
async fn literal_internal_addresses_are_refused() {
    let g = SsrfGuard::default();
    for u in [
        "http://127.0.0.1/x",
        "http://127.1.2.3:8080/x",
        "http://10.0.0.5/x",
        "http://172.20.1.1/x",
        "http://192.168.0.10/x",
        "http://169.254.169.254/latest/meta-data/",
        "http://100.100.100.200/",
        "http://0.0.0.0/",
        "http://[::1]/x",
        "http://[fe80::1]/x",
        "http://[fd00:ec2::254]/latest",
        "http://[::ffff:127.0.0.1]/x",
        "http://[::ffff:a9fe:a9fe]/x",
    ] {
        let err = g.check_url(&Url::parse(u).unwrap()).await;
        assert!(err.is_err(), "{u} must be refused");
    }
    // Public literals pass without DNS.
    assert!(g.check_url(&Url::parse("https://8.8.8.8/x").unwrap()).await.is_ok());
    assert!(g.check_url(&Url::parse("ftp://8.8.8.8/x").unwrap()).await.is_err());
}

#[tokio::test]
async fn hostname_resolving_to_loopback_is_refused() {
    let g = SsrfGuard::default();
    let err = g.check_url(&Url::parse("http://localhost:9000/bucket/x.parquet").unwrap()).await;
    assert!(err.is_err(), "localhost resolves to 127.0.0.1 / ::1");
    // Allowlisted by host name, or by network.
    assert!(SsrfGuard::new(&["localhost"]).check_url(&Url::parse("http://localhost/x").unwrap()).await.is_ok());
    let by_net = SsrfGuard::new(&["127.0.0.0/8", "::1"]);
    assert!(by_net.check_url(&Url::parse("http://localhost/x").unwrap()).await.is_ok());
    assert!(by_net.check_url(&Url::parse("http://127.0.0.1/x").unwrap()).await.is_ok());
    assert!(by_net.check_url(&Url::parse("http://10.0.0.1/x").unwrap()).await.is_err());
    let metadata_only = SsrfGuard::new(&["10.0.0.0/8"]);
    assert!(metadata_only.check_url(&Url::parse("http://169.254.169.254/").unwrap()).await.is_err());
}

#[tokio::test]
async fn refused_inputs_never_reach_the_store() {
    let store = mem_store(&[("x.parquet", parquet_bytes(3, 0))]).await;
    let r = OneStore::new(store.clone() as Arc<_>);
    let o = ReadOptions { use_cache: false, ..ReadOptions::default() };
    for u in ["http://127.0.0.1:1/x.parquet", "http://localhost/x.parquet", "http://169.254.169.254/x.parquet", "https://[::1]/x.parquet"] {
        let out = read_inputs_with(&[input(u)], &r, &o).await;
        assert_eq!(out[0].bytes, None, "{u}");
        assert!(out[0].uri_hash.is_some());
    }
    assert_eq!(r.calls.load(Ordering::SeqCst), 0, "resolver never asked");
    assert_eq!(store.heads.load(Ordering::SeqCst), 0);
    assert_eq!(store.bytes_read(), 0);
}

/// With the `http` feature, the HTTP client itself enforces the guard: a
/// local server is unreachable unless allowlisted, even when the
/// pre-check is bypassed (DNS rebinding, redirects).
#[cfg(feature = "http")]
#[tokio::test]
async fn http_client_enforces_the_guard() {
    use object_store::ObjectStoreExt;
    use rightsizer_features::{DefaultResolver, StoreResolver};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else { return };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let body = b"a,b,c\n1,2,3\n";
                let resp = if req.starts_with("GET /redirect") {
                    "HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/x.csv\r\nContent-Length: 0\r\n\r\n".to_string()
                } else if req.starts_with("HEAD") {
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n\r\n", body.len())
                } else {
                    format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-{}/{}\r\n\r\n{}",
                        body.len(),
                        body.len() - 1,
                        body.len(),
                        String::from_utf8_lossy(body)
                    )
                };
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    let url = format!("http://localhost:{port}/x.csv");
    // Default guard: the client refuses localhost even if asked directly.
    let r = DefaultResolver::default();
    let (store, path) = r.resolve(&url).unwrap();
    assert!(store.head(&path).await.is_err(), "guarded client must refuse loopback");
    // Allowlisted: readable end to end.
    let g = SsrfGuard::new(&["127.0.0.0/8", "::1"]);
    let r = DefaultResolver::new(g.clone());
    let o = ReadOptions { use_cache: false, ..ReadOptions::default() }.with_guard(g);
    let out = read_inputs_with(&[input(&url)], &r, &o).await;
    assert_eq!(out[0].bytes, Some(12));
    assert_eq!(out[0].columns.as_ref().map(|c| c.total), Some(3));
    // A redirect to the metadata address is refused by the client.
    let (store, _) = r.resolve(&url).unwrap();
    let res = store.get_range(&object_store::path::Path::from("redirect"), 0..4).await;
    assert!(res.is_err());
}
