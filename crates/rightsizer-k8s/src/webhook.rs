//! The mutating admission webhook (kubernetes.md §3), feature `webhook`:
//! axum over rustls, an internal deadline shorter than the API server's,
//! fail-open (S1), pre-warmed memo.
//!
//! Skeleton: the router type only.

/// The webhook's HTTP router.
pub type Router = axum::Router;

/// TLS server configuration type.
pub type TlsConfig = rustls::ServerConfig;
