//! SSRF guard for HTTP(S) inputs (SPEC §9, §14): a URI an author controls
//! must not make the decider read from inside the network it runs in.
//!
//! Refused unless allowlisted: loopback (`127.0.0.0/8`, `::1`), private
//! (`10/8`, `172.16/12`, `192.168/16`), carrier-grade NAT (`100.64/10`),
//! link-local (`169.254/16` — which includes the cloud metadata address
//! `169.254.169.254` — and `fe80::/10`), unique-local (`fc00::/7`, which
//! includes AWS's `fd00:ec2::254`), site-local (`fec0::/10`), unspecified
//! (`0.0.0.0/8`, `::`), multicast, broadcast/reserved (`240/4`), benchmarking
//! (`198.18/15`), IETF protocol assignments (`192.0.0.0/24`), NAT64 local-use
//! (`64:ff9b:1::/48`), and IPv4-mapped / NAT64 / 6to4 IPv6 forms of any of these.
//!
//! The check runs twice: [`SsrfGuard::check_url`] before any request (on
//! literal IPs and on every address the host resolves to: one bad address
//! refuses the host), and — with the `http` feature — inside the HTTP client
//! itself, as its DNS resolver and redirect policy, so a DNS answer that
//! changes between check and connect, or a redirect to a literal private IP,
//! is refused too. Proxies are disabled for guarded clients.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use url::Url;

use crate::FeatureError;

/// An IPv4 or IPv6 network in CIDR form.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// Parse `"10.0.0.0/8"`, `"fd00::/8"` or a bare address (a host route).
    pub fn parse(s: &str) -> Option<Cidr> {
        let (a, p) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = a.trim().trim_start_matches('[').trim_end_matches(']').parse().ok()?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match p {
            Some(p) => p.trim().parse::<u8>().ok().filter(|&p| p <= max)?,
            None => max,
        };
        Some(Cidr { addr, prefix })
    }

    /// Whether `ip` is inside this network. IPv4-mapped IPv6 addresses
    /// match IPv4 networks.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        match (self.addr, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                let mask = if self.prefix == 0 { 0 } else { u32::MAX << (32 - self.prefix) };
                (u32::from(n) & mask) == (u32::from(a) & mask)
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                let mask = if self.prefix == 0 { 0 } else { u128::MAX << (128 - self.prefix) };
                (u128::from(n) & mask) == (u128::from(a) & mask)
            }
            _ => false,
        }
    }
}

fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    }
}

/// Whether `ip` is in a range the guard refuses by default.
pub fn is_forbidden_ip(ip: IpAddr) -> bool {
    match canonical(ip) {
        IpAddr::V4(v4) => forbidden_v4(v4),
        IpAddr::V6(v6) => forbidden_v6(v6),
    }
}

fn forbidden_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_unspecified()
        || o[0] == 0
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || o[0] >= 240
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10
        || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18.0.0/15
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24
}

fn forbidden_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return true;
    }
    if (s[0] & 0xfe00) == 0xfc00 || (s[0] & 0xffc0) == 0xfe80 || (s[0] & 0xffc0) == 0xfec0 {
        return true;
    }
    // NAT64 local-use (64:ff9b:1::/48, RFC 8215): translator-local by definition.
    if s[0] == 0x64 && s[1] == 0xff9b && s[2] == 1 {
        return true;
    }
    // NAT64 (64:ff9b::/96) and IPv4-compatible (::/96) embed an IPv4 address.
    if (s[0] == 0x64 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0]) || s[0..6] == [0, 0, 0, 0, 0, 0] {
        let v4 = Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8);
        return forbidden_v4(v4);
    }
    // 6to4 (2002::/16) embeds an IPv4 address in segments 1–2.
    if s[0] == 0x2002 {
        let v4 = Ipv4Addr::new((s[1] >> 8) as u8, s[1] as u8, (s[2] >> 8) as u8, s[2] as u8);
        return forbidden_v4(v4);
    }
    false
}

/// The SSRF guard: default-deny for internal addresses, with an allowlist of
/// CIDRs, addresses and host names (`"minio.internal"`, `"*.corp.example"`
/// for a suffix match). An allowlisted host name skips address checks; an
/// allowlisted network admits addresses inside it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SsrfGuard {
    nets: Vec<Cidr>,
    hosts: Vec<String>,
}

impl SsrfGuard {
    /// A guard with an allowlist (`[features] allow_hosts`). Entries that
    /// parse as CIDRs or addresses are networks; anything else is a host
    /// name (case-insensitive).
    pub fn new<S: AsRef<str>>(allow: &[S]) -> Self {
        let mut g = SsrfGuard::default();
        for a in allow {
            let a = a.as_ref().trim();
            if a.is_empty() {
                continue;
            }
            match Cidr::parse(a) {
                Some(c) => g.nets.push(c),
                None => g.hosts.push(a.to_ascii_lowercase()),
            }
        }
        g
    }

    /// Whether the host name is allowlisted.
    pub fn host_allowed(&self, host: &str) -> bool {
        let h = host.trim_end_matches('.').to_ascii_lowercase();
        self.hosts.iter().any(|a| match a.strip_prefix("*.") {
            Some(suffix) => h.ends_with(&format!(".{suffix}")),
            None => *a == h,
        })
    }

    /// Whether a connection to `ip` is allowed.
    pub fn ip_allowed(&self, ip: IpAddr) -> bool {
        !is_forbidden_ip(ip) || self.nets.iter().any(|n| n.contains(ip))
    }

    /// Synchronous part of the check: scheme, allowlisted host, literal IP.
    /// `Ok(true)` = decided allowed, `Ok(false)` = needs DNS.
    pub fn precheck(&self, url: &Url) -> Result<bool, FeatureError> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(FeatureError::NotAllowed(format!("scheme {}", url.scheme())));
        }
        let host = match url.host() {
            Some(h) => h,
            None => return Err(FeatureError::NotAllowed("no host".into())),
        };
        match host {
            url::Host::Ipv4(v4) => self.ip_verdict(IpAddr::V4(v4)).map(|_| true),
            url::Host::Ipv6(v6) => self.ip_verdict(IpAddr::V6(v6)).map(|_| true),
            url::Host::Domain(d) => Ok(self.host_allowed(d)),
        }
    }

    fn ip_verdict(&self, ip: IpAddr) -> Result<(), FeatureError> {
        if self.ip_allowed(ip) { Ok(()) } else { Err(FeatureError::NotAllowed(format!("address {ip} is internal"))) }
    }

    /// Check a URL: literal IPs directly; host names by resolving them and
    /// refusing if any address is internal (unless allowlisted).
    pub async fn check_url(&self, url: &Url) -> Result<(), FeatureError> {
        if self.precheck(url)? {
            return Ok(());
        }
        let host = url.host_str().unwrap_or_default().to_string();
        let port = url.port_or_known_default().unwrap_or(443);
        let addrs: Vec<IpAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| FeatureError::NotAllowed(format!("cannot resolve {host}: {e}")))?
            .map(|a| a.ip())
            .collect();
        self.check_addrs(&host, &addrs)
    }

    /// Refuse when `addrs` is empty or any address is internal.
    pub fn check_addrs(&self, host: &str, addrs: &[IpAddr]) -> Result<(), FeatureError> {
        if addrs.is_empty() {
            return Err(FeatureError::NotAllowed(format!("{host} resolves to nothing")));
        }
        for ip in addrs {
            if !self.ip_allowed(*ip) {
                return Err(FeatureError::NotAllowed(format!("{host} resolves to internal address {ip}")));
            }
        }
        Ok(())
    }
}

/// A `reqwest` client whose DNS resolver and redirect policy enforce the
/// guard, as an `object_store` HTTP connector (feature `http`).
#[cfg(feature = "http")]
pub(crate) mod client {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::ClientOptions;
    use object_store::client::{HttpClient, HttpConnector};

    use super::SsrfGuard;

    #[derive(Debug)]
    pub(crate) struct GuardedConnector {
        pub(crate) guard: Arc<SsrfGuard>,
        pub(crate) headers: reqwest::header::HeaderMap,
    }

    struct GuardResolver(Arc<SsrfGuard>);

    impl reqwest::dns::Resolve for GuardResolver {
        fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let guard = Arc::clone(&self.0);
            let host = name.as_str().to_string();
            Box::pin(async move {
                let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
                if !guard.host_allowed(&host) {
                    let ips: Vec<_> = addrs.iter().map(|a| a.ip()).collect();
                    guard.check_addrs(&host, &ips).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                }
                Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
            })
        }
    }

    impl HttpConnector for GuardedConnector {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            let guard = Arc::clone(&self.guard);
            let redirect = reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 5 {
                    return attempt.error("too many redirects");
                }
                match guard.precheck(attempt.url()) {
                    // Host names are checked again by the resolver on connect.
                    Ok(_) => attempt.follow(),
                    Err(e) => attempt.error(e),
                }
            });
            let client = reqwest::Client::builder()
                .dns_resolver(GuardResolver(Arc::clone(&self.guard)))
                .redirect(redirect)
                .no_proxy()
                .default_headers(self.headers.clone())
                .user_agent(concat!("rightsizer/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| object_store::Error::Generic { store: "HTTP", source: Box::new(e) })?;
            Ok(HttpClient::new(client))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr() {
        let c = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(c.contains("10.2.3.4".parse().unwrap()));
        assert!(!c.contains("11.0.0.1".parse().unwrap()));
        assert!(c.contains("::ffff:10.0.0.1".parse().unwrap()));
        assert!(Cidr::parse("fd00::/8").unwrap().contains("fd12::1".parse().unwrap()));
        assert!(Cidr::parse("1.2.3.4/33").is_none());
        assert!(Cidr::parse("nope").is_none());
    }

    #[test]
    fn forbidden_ranges() {
        for ip in [
            "127.0.0.1", "10.1.1.1", "172.16.0.1", "172.31.255.255", "192.168.1.1", "169.254.169.254", "100.64.0.1",
            "0.0.0.0", "255.255.255.255", "224.0.0.1", "::1", "::", "fe80::1", "fc00::1", "fd00:ec2::254",
            "::ffff:127.0.0.1", "::ffff:169.254.169.254", "64:ff9b::a9fe:a9fe", "2002:7f00:1::1",
        ] {
            assert!(is_forbidden_ip(ip.parse().unwrap()), "{ip} should be forbidden");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "172.32.0.1", "2606:4700:4700::1111", "100.128.0.1"] {
            assert!(!is_forbidden_ip(ip.parse().unwrap()), "{ip} should be allowed");
        }
    }

    #[test]
    fn host_allowlist() {
        let g = SsrfGuard::new(&["minio.internal", "*.corp.example", "10.0.0.0/8"]);
        assert!(g.host_allowed("MINIO.internal"));
        assert!(g.host_allowed("a.corp.example"));
        assert!(!g.host_allowed("corp.example"));
        assert!(g.ip_allowed("10.9.9.9".parse().unwrap()));
        assert!(!g.ip_allowed("192.168.0.1".parse().unwrap()));
    }
}
