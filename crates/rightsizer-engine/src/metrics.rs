//! Prometheus metrics (SPEC §13.1, §8.1). Every engine owns a [`Registry`];
//! surfaces expose it (`rsz serve`'s metrics listener, the webhook).
//!
//! Registered here: every §13.1 metric the engine produces, plus
//! `rightsizer_store_errors_total` (§8.1) and
//! `rightsizer_outcomes_dropped_total` (§8.4 buffer overflow). Not here:
//! `rightsizer_watcher_lag_seconds` (the K8s watcher's) and
//! `rightsizer_store_shards` (compaction's).

use prometheus::{Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};

/// The engine's metrics.
#[derive(Clone)]
pub struct Metrics {
    /// The registry holding all of them.
    pub registry: Registry,
    /// `rightsizer_decisions_total{mode, source, platform}`.
    pub decisions: IntCounterVec,
    /// `rightsizer_decision_latency_seconds{platform}`.
    pub latency: HistogramVec,
    /// `rightsizer_budget_expired_total{platform}`.
    pub budget_expired: IntCounterVec,
    /// `rightsizer_held_total{axis, reason}`.
    pub held: IntCounterVec,
    /// `rightsizer_clamped_total{reason}`.
    pub clamped: IntCounterVec,
    /// `rightsizer_errors_total{stage, reason}`.
    pub errors: IntCounterVec,
    /// `rightsizer_caused_failures_total{platform}`.
    pub caused_failures: IntCounterVec,
    /// `rightsizer_outcome_join_ratio{platform}`.
    pub join_ratio: GaugeVec,
    /// `rightsizer_saved_resource_hours_total{axis}` (GiB·h for memory, core·h for CPU).
    pub saved_hours: prometheus::CounterVec,
    /// `rightsizer_breaker_open{scope}`.
    pub breaker_open: GaugeVec,
    /// `rightsizer_estimator_circuit_open{estimator}` (set by remote clients).
    pub estimator_circuit_open: GaugeVec,
    /// `rightsizer_store_compaction_lag_seconds`.
    pub compaction_lag: Gauge,
    /// `rightsizer_store_errors_total{op}`.
    pub store_errors: IntCounterVec,
    /// `rightsizer_outcomes_dropped_total`: buffered writes dropped on overflow.
    pub dropped: IntCounterVec,
    /// Decisions recorded per platform (denominator of the join ratio).
    pub recorded: IntCounterVec,
    /// Outcomes joined to a decision per platform (numerator of the join ratio).
    pub joined: IntCounterVec,
}

fn counter(r: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
    r.register(Box::new(c.clone())).expect("unique metric");
    c
}

fn gauge(r: &Registry, name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
    r.register(Box::new(g.clone())).expect("unique metric");
    g
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Fresh metrics in a fresh registry.
    pub fn new() -> Self {
        let r = Registry::new();
        let latency = HistogramVec::new(
            HistogramOpts::new("rightsizer_decision_latency_seconds", "Decision latency.")
                .buckets(vec![0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
            &["platform"],
        )
        .expect("valid metric");
        r.register(Box::new(latency.clone())).expect("unique metric");
        let saved = prometheus::CounterVec::new(
            Opts::new("rightsizer_saved_resource_hours_total", "Resource-hours saved by enforced decisions."),
            &["axis"],
        )
        .expect("valid metric");
        r.register(Box::new(saved.clone())).expect("unique metric");
        let lag = Gauge::new("rightsizer_store_compaction_lag_seconds", "Seconds since the last compaction.").expect("valid metric");
        r.register(Box::new(lag.clone())).expect("unique metric");
        Metrics {
            decisions: counter(&r, "rightsizer_decisions_total", "Decisions.", &["mode", "source", "platform"]),
            latency,
            budget_expired: counter(&r, "rightsizer_budget_expired_total", "Decisions whose budget expired.", &["platform"]),
            held: counter(&r, "rightsizer_held_total", "Axes held at the prior.", &["axis", "reason"]),
            clamped: counter(&r, "rightsizer_clamped_total", "Increases clamped (S4).", &["reason"]),
            errors: counter(&r, "rightsizer_errors_total", "Errors (S1: logged and counted).", &["stage", "reason"]),
            caused_failures: counter(&r, "rightsizer_caused_failures_total", "Rightsizer-caused failures (S5).", &["platform"]),
            join_ratio: gauge(&r, "rightsizer_outcome_join_ratio", "Decisions with an outcome over decisions.", &["platform"]),
            saved_hours: saved,
            breaker_open: gauge(&r, "rightsizer_breaker_open", "Open breakers.", &["scope"]),
            estimator_circuit_open: gauge(&r, "rightsizer_estimator_circuit_open", "Remote estimator circuits open.", &["estimator"]),
            compaction_lag: lag,
            store_errors: counter(&r, "rightsizer_store_errors_total", "History store errors.", &["op"]),
            dropped: counter(&r, "rightsizer_outcomes_dropped_total", "Buffered writes dropped on overflow.", &["kind"]),
            recorded: counter(&r, "rightsizer_decisions_recorded_total", "Decisions written to the decision log.", &["platform"]),
            joined: counter(&r, "rightsizer_outcomes_joined_total", "Outcomes joined to their decision.", &["platform"]),
            registry: r,
        }
    }

    /// Prometheus text exposition.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        let _ = TextEncoder::new().encode(&self.registry.gather(), &mut buf);
        String::from_utf8(buf).unwrap_or_default()
    }

    /// Recompute the join ratio of `platform`.
    pub(crate) fn update_join_ratio(&self, platform: &str) {
        let d = self.recorded.with_label_values(&[platform]).get();
        let j = self.joined.with_label_values(&[platform]).get();
        if d > 0 {
            self.join_ratio.with_label_values(&[platform]).set((j as f64 / d as f64).min(1.0));
        }
    }

    /// Count an error.
    pub(crate) fn error(&self, stage: &str, reason: &str) {
        self.errors.with_label_values(&[stage, reason]).inc();
    }
}
