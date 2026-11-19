//! The `rsz apply --watch` ladder (kubernetes.md §5): a caller-driven ladder
//! that resubmits a renamed Job one step up when it is `OOMKilled`. This is
//! the pure step function; the watch loop, the lease (engine API) and the
//! cluster calls come with the CLI.

use rightsizer_core::{Bytes, Requests};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Ladder state for one invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LadderState {
    /// Name of the original Job (without ladder suffixes).
    pub base_name: String,
    /// Steps taken so far (0 = the first, possibly reduced, attempt).
    pub step: u32,
    /// Steps allowed (2).
    pub max_steps: u32,
    /// Memory factor per step (2.0).
    pub factor: f32,
    /// The origin prior.
    pub origin: Requests,
    /// The current attempt's requests.
    pub current: Requests,
    /// The last step is raised to at least this (max(origin, largest observed success)).
    pub final_floor: Requests,
    /// Never exceed (largest node shape ∧ quota), when known.
    pub ceiling: Option<Bytes>,
    /// Whether the ladder is finished.
    pub done: bool,
}

impl LadderState {
    /// A fresh ladder for a first attempt at `applied`.
    pub fn new(base_name: impl Into<String>, origin: Requests, applied: Requests) -> Self {
        LadderState { base_name: base_name.into(), step: 0, max_steps: 2, factor: 2.0, final_floor: origin.clone(), origin, current: applied, ceiling: None, done: false }
    }
}

/// What the watched Job did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobEvent {
    /// Completed.
    Succeeded,
    /// A container was `OOMKilled`.
    OomKilled,
    /// `DeadlineExceeded` (never escalated: a ladder escalates on OOM only).
    DeadlineExceeded,
    /// Any other failure.
    Failed {
        /// Reason.
        reason: String,
    },
}

/// What to do next.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LadderAction {
    /// Stop watching; the run's final status.
    Done {
        /// Succeeded or not.
        succeeded: bool,
        /// Why it stopped.
        reason: String,
    },
    /// Submit a renamed copy of the Job one step up.
    Resubmit {
        /// New Job name.
        name: String,
        /// Requests of the new attempt.
        requests: Requests,
        /// The step this is (1-based).
        step: u32,
    },
}

/// Ladder Job name: `{base}-rsz{step}`, truncated to fit 63 characters.
pub fn step_name(base: &str, step: u32) -> String {
    let suffix = format!("-rsz{step}");
    let max = 63 - suffix.len();
    let b: String = base.chars().take(max).collect();
    format!("{}{suffix}", b.trim_end_matches('-'))
}

/// One step of the ladder.
pub fn step(s: &LadderState, ev: &JobEvent) -> (LadderState, LadderAction) {
    let mut n = s.clone();
    if s.done {
        return (n, LadderAction::Done { succeeded: false, reason: "ladder already finished".into() });
    }
    let done = |mut n: LadderState, ok: bool, why: &str| {
        n.done = true;
        (n, LadderAction::Done { succeeded: ok, reason: why.into() })
    };
    match ev {
        JobEvent::Succeeded => done(n, true, "succeeded"),
        JobEvent::DeadlineExceeded => done(n, false, "deadline exceeded (not escalated)"),
        JobEvent::Failed { reason } => done(n, false, reason),
        JobEvent::OomKilled => {
            let cur = s.current.memory.unwrap_or(Bytes(0));
            let floor = s.final_floor.memory.max(s.origin.memory).unwrap_or(Bytes(0));
            // An OOM at or above the origin prior is not rightsizer's to fix.
            if cur >= floor && s.origin.memory.is_some() {
                return done(n, false, "OOMKilled at or above the origin prior");
            }
            if s.step >= s.max_steps {
                return done(n, false, "OOMKilled; ladder exhausted");
            }
            let next_step = s.step + 1;
            let mut mem = cur.mul_f64(s.factor as f64);
            if next_step == s.max_steps {
                mem = mem.max(floor);
            }
            if let Some(c) = s.ceiling {
                mem = mem.min(c.max(floor));
            }
            if mem <= cur {
                return done(n, false, "OOMKilled; no larger size within the ceiling");
            }
            n.step = next_step;
            n.current.memory = Some(mem);
            let name = step_name(&s.base_name, next_step);
            (n.clone(), LadderAction::Resubmit { name, requests: n.current.clone(), step: next_step })
        }
    }
}

/// A resubmittable copy of a Job: renamed, with server-set fields and the
/// controller's selector labels removed.
pub fn renamed_job(job: &Value, name: &str) -> Value {
    let mut j = job.clone();
    if let Some(m) = j.get_mut("metadata").and_then(Value::as_object_mut) {
        m.insert("name".into(), Value::String(name.into()));
        for k in ["generateName", "uid", "resourceVersion", "creationTimestamp", "managedFields", "selfLink", "ownerReferences"] {
            m.remove(k);
        }
    }
    if let Some(o) = j.as_object_mut() {
        o.remove("status");
    }
    if let Some(spec) = j.get_mut("spec").and_then(Value::as_object_mut) {
        spec.remove("selector");
        spec.remove("manualSelector");
        if let Some(labels) = spec.get_mut("template").and_then(|t| t.get_mut("metadata")).and_then(|m| m.get_mut("labels")).and_then(Value::as_object_mut) {
            for k in ["controller-uid", "batch.kubernetes.io/controller-uid", "job-name", "batch.kubernetes.io/job-name"] {
                labels.remove(k);
            }
        }
    }
    j
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> LadderState {
        LadderState::new(
            "train",
            Requests { memory: Some(Bytes::gib(16)), ..Default::default() },
            Requests { memory: Some(Bytes::gib(4)), ..Default::default() },
        )
    }

    #[test]
    fn ladder_climbs_to_the_prior() {
        let (s1, a1) = step(&st(), &JobEvent::OomKilled);
        assert_eq!(a1, LadderAction::Resubmit { name: "train-rsz1".into(), requests: s1.current.clone(), step: 1 });
        assert_eq!(s1.current.memory, Some(Bytes::gib(8)));
        // The final step reaches at least the origin prior (S3(a)).
        let (s2, a2) = step(&s1, &JobEvent::OomKilled);
        assert!(matches!(a2, LadderAction::Resubmit { step: 2, .. }));
        assert_eq!(s2.current.memory, Some(Bytes::gib(16)));
        let (s3, a3) = step(&s2, &JobEvent::OomKilled);
        assert!(matches!(a3, LadderAction::Done { succeeded: false, .. }));
        assert!(s3.done);
    }

    #[test]
    fn only_oom_escalates() {
        assert!(matches!(step(&st(), &JobEvent::DeadlineExceeded).1, LadderAction::Done { succeeded: false, .. }));
        assert!(matches!(step(&st(), &JobEvent::Succeeded).1, LadderAction::Done { succeeded: true, .. }));
        let mut at_prior = st();
        at_prior.current.memory = Some(Bytes::gib(16));
        assert!(matches!(step(&at_prior, &JobEvent::OomKilled).1, LadderAction::Done { succeeded: false, .. }));
    }

    #[test]
    fn ceiling_and_names() {
        let mut s = st();
        s.factor = 8.0;
        s.ceiling = Some(Bytes::gib(24));
        let (s1, _) = step(&s, &JobEvent::OomKilled);
        assert_eq!(s1.current.memory, Some(Bytes::gib(24)));
        // The ceiling never cuts below the floor the ladder must reach.
        s.factor = 2.0;
        s.ceiling = Some(Bytes::gib(6));
        let (s1, _) = step(&s, &JobEvent::OomKilled);
        assert_eq!(s1.current.memory, Some(Bytes::gib(8)));
        let (s2, _) = step(&s1, &JobEvent::OomKilled);
        assert_eq!(s2.current.memory, Some(Bytes::gib(16)));
        assert_eq!(step_name(&"x".repeat(80), 1).len(), 63);
        let j = serde_json::json!({"metadata": {"name": "train", "uid": "u", "resourceVersion": "1"}, "spec": {"selector": {}, "template": {"metadata": {"labels": {"job-name": "train", "app": "a"}}}}, "status": {}});
        let r = renamed_job(&j, "train-rsz1");
        assert_eq!(r, serde_json::json!({"metadata": {"name": "train-rsz1"}, "spec": {"template": {"metadata": {"labels": {"app": "a"}}}}}));
    }
}
