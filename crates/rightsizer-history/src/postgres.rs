//! The postgres backend (feature `postgres`, SPEC §8.1): the K8s webhook and
//! watcher deployment; per-tenant roles give `Authenticated` trust (SPEC §8.2).
//!
//! **Stub:** configuration only; the client lands next round.

/// Connection settings.
#[derive(Clone, Debug)]
pub struct PostgresConfig {
    /// `postgres://…` URL.
    pub url: String,
    /// Require TLS (rustls).
    pub tls: bool,
}

/// Keeps the client types in this feature's dependency set.
pub type Client = tokio_postgres::Client;
/// TLS connector type.
pub type Tls = tokio_postgres_rustls::MakeRustlsConnect;
