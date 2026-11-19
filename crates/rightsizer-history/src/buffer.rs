//! Buffered writes (SPEC §8.4): when the store is unreachable, run records
//! and decisions are kept in memory (bounded, 10k entries) and retried with
//! exponential backoff, in order. Overflow is dropped and counted. Records
//! the store rejects permanently (permission, corrupt, config) are dropped
//! and counted as `rejected`; duplicates count as written.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rightsizer_core::{Decision, RunRecord};
use tokio::sync::{Mutex, Notify};

use crate::{HistoryStore, PutOutcome, StoreError};

/// Buffer settings.
#[derive(Clone, Debug, PartialEq)]
pub struct BufferOptions {
    /// Entries kept while the store is unreachable (10k).
    pub capacity: usize,
    /// First retry delay (250 ms).
    pub initial_backoff: Duration,
    /// Longest retry delay (30 s).
    pub max_backoff: Duration,
}

impl Default for BufferOptions {
    fn default() -> Self {
        BufferOptions { capacity: 10_000, initial_backoff: Duration::from_millis(250), max_backoff: Duration::from_secs(30) }
    }
}

/// Counters (for `rightsizer_errors_total{stage="store_write"}` and friends).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BufferStats {
    /// Written to the store (first try or retry).
    pub written: u64,
    /// Written on a retry.
    pub retried: u64,
    /// Currently buffered.
    pub buffered: u64,
    /// Dropped because the buffer was full.
    pub dropped: u64,
    /// Dropped because the store rejected them permanently.
    pub rejected: u64,
    /// Failed write attempts.
    pub errors: u64,
}

/// What happened to one write.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Persisted now.
    Written(PutOutcome),
    /// Buffered for retry.
    Buffered,
    /// Dropped (buffer full, or rejected by the store).
    Dropped,
}

#[derive(Clone, Debug)]
enum Pending {
    Run(Box<RunRecord>),
    Decision(Box<Decision>),
}

/// A bounded, retrying writer in front of a store.
pub struct BufferedWriter {
    store: Arc<dyn HistoryStore>,
    opts: BufferOptions,
    queue: Mutex<VecDeque<Pending>>,
    flushing: Mutex<()>,
    notify: Notify,
    written: AtomicU64,
    retried: AtomicU64,
    dropped: AtomicU64,
    rejected: AtomicU64,
    errors: AtomicU64,
    buffered: AtomicU64,
}

fn transient(e: &StoreError) -> bool {
    matches!(e, StoreError::Unreachable(_) | StoreError::Other(_) | StoreError::Conflict(_))
}

impl BufferedWriter {
    /// A writer in front of `store`.
    pub fn new(store: Arc<dyn HistoryStore>, opts: BufferOptions) -> Arc<Self> {
        Arc::new(BufferedWriter {
            store,
            opts,
            queue: Mutex::new(VecDeque::new()),
            flushing: Mutex::new(()),
            notify: Notify::new(),
            written: AtomicU64::new(0),
            retried: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            buffered: AtomicU64::new(0),
        })
    }

    /// Counters.
    pub fn stats(&self) -> BufferStats {
        BufferStats {
            written: self.written.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            buffered: self.buffered.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    /// Entries awaiting retry.
    pub async fn pending(&self) -> usize {
        self.queue.lock().await.len()
    }

    async fn write(&self, p: &Pending) -> Result<PutOutcome, StoreError> {
        match p {
            Pending::Run(r) => self.store.put_run(r).await,
            Pending::Decision(d) => self.store.put_decision(d).await.map(|_| PutOutcome::Inserted),
        }
    }

    async fn submit(&self, p: Pending) -> WriteOutcome {
        // Keep order: when something is queued, queue behind it (and nudge the retrier).
        let queued = !self.queue.lock().await.is_empty();
        if !queued {
            match self.write(&p).await {
                Ok(o) => {
                    self.written.fetch_add(1, Ordering::Relaxed);
                    return WriteOutcome::Written(o);
                }
                Err(e) if !transient(&e) => {
                    tracing::warn!(error = %e, "history write rejected; dropped");
                    self.rejected.fetch_add(1, Ordering::Relaxed);
                    return WriteOutcome::Dropped;
                }
                Err(e) => {
                    let n = self.errors.fetch_add(1, Ordering::Relaxed) + 1;
                    if n <= 3 || n.is_multiple_of(100) {
                        tracing::warn!(error = %e, "history write failed; buffered for retry");
                    }
                }
            }
        }
        let mut q = self.queue.lock().await;
        if q.len() >= self.opts.capacity {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n.is_multiple_of(1000) {
                tracing::warn!(capacity = self.opts.capacity, dropped = n, "history write buffer full; dropping");
            }
            return WriteOutcome::Dropped;
        }
        q.push_back(p);
        self.buffered.store(q.len() as u64, Ordering::Relaxed);
        drop(q);
        self.notify.notify_one();
        WriteOutcome::Buffered
    }

    /// Write a run record, or buffer it.
    pub async fn put_run(&self, rec: RunRecord) -> WriteOutcome {
        self.submit(Pending::Run(Box::new(rec))).await
    }

    /// Write a decision, or buffer it.
    pub async fn put_decision(&self, d: Decision) -> WriteOutcome {
        self.submit(Pending::Decision(Box::new(d))).await
    }

    /// Retry buffered entries in order; stop at the first transient failure.
    /// Returns how many were written.
    pub async fn flush(&self) -> usize {
        let _one = self.flushing.lock().await;
        let mut n = 0;
        loop {
            let Some(front) = self.queue.lock().await.front().cloned() else { break };
            match self.write(&front).await {
                Ok(_) => {
                    self.written.fetch_add(1, Ordering::Relaxed);
                    self.retried.fetch_add(1, Ordering::Relaxed);
                    n += 1;
                }
                Err(e) if !transient(&e) => {
                    tracing::warn!(error = %e, "buffered history write rejected; dropped");
                    self.rejected.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            let mut q = self.queue.lock().await;
            q.pop_front();
            self.buffered.store(q.len() as u64, Ordering::Relaxed);
        }
        n
    }

    /// Spawn the retry loop: flush with exponential backoff while anything is
    /// buffered, sleep until notified otherwise. Abort the handle to stop it.
    pub fn spawn_retry(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let mut backoff = me.opts.initial_backoff;
            loop {
                if me.queue.lock().await.is_empty() {
                    me.notify.notified().await;
                    backoff = me.opts.initial_backoff;
                    continue;
                }
                tokio::time::sleep(backoff).await;
                me.flush().await;
                backoff = if me.queue.lock().await.is_empty() { me.opts.initial_backoff } else { (backoff * 2).min(me.opts.max_backoff) };
            }
        })
    }
}
