//! Per-call transport-message ordering with clear-driven invalidation.
//!
//! [`OutboundQueue`](crate::queue::OutboundQueue) stores entries locally and emits
//! nothing itself. The host consumes entries in priority/sequence order. A clear
//! removes queued text media/mark messages before entering the priority queue;
//! binary frames, shutdown sentinels and unrelated messages remain queued.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Queued transport item with deterministic priority and enqueue order.
///
/// `item` follows the existing transport envelope: typically `["text", raw_json]`,
/// `["binary", bytes]`, or null for a sentinel. This record does not validate the
/// envelope or execute the contained operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// Ordering key; lower values are consumed first (`-1` for an enqueued clear, `0` otherwise).
    pub priority: i64,
    /// FIFO tiebreaker assigned by the queue owner.
    pub sequence: u64,
    /// Opaque transport payload retained with its ordering metadata.
    pub item: Value
}

/// In-memory owner of pending outbound transport entries and their sequence counter.
///
/// Methods require exclusive mutable access; the host supplies synchronization,
/// admission limits and actual network dispatch. The vector-backed implementation
/// selects entries by priority/sequence rather than maintaining a heap.
#[derive(Default)]
pub struct OutboundQueue {
    /// Pending entries, retained in storage order until selected or cleared.
    entries: Vec<Entry>,
    /// Sequence to assign to the next enqueued message; advanced after successful enqueue.
    sequence: u64
}
impl OutboundQueue {
    /// Construct a queue from existing entries and the next sequence counter.
    ///
    /// The caller must supply a counter consistent with existing identities; this
    /// constructor neither sorts entries nor validates uniqueness or payload shapes.
    pub fn with_entries(entries: Vec<Entry>, sequence: u64) -> Self { Self {entries, sequence} }
    /// Parse and enqueue one text message, returning the number of queued entries discarded.
    ///
    /// For a clear event, remove queued text messages whose JSON event is `media`
    /// or `mark`, retain other items, and enqueue the clear at priority -1. Other
    /// messages enter at priority 0. The stored raw JSON string is not rewritten.
    ///
    /// Return a serde JSON error for malformed new JSON, or malformed retained text
    /// examined while processing a clear. Parsing finishes before queue replacement,
    /// so such an error leaves entries and the sequence counter unchanged.
    pub fn enqueue_twilio(&mut self, raw: &str) -> Result<usize, serde_json::Error> {
        let message: Value = serde_json::from_str(raw)?;
        let clear = message["event"] == "clear";
        let mut discarded = 0;
        if clear {
            let mut retained = Vec::new();
            for entry in &self.entries {
                let drop = if entry.item.get(0).and_then(Value::as_str) == Some("text") {
                    let raw = entry.item[1].as_str().unwrap_or("");
                    let event: Value = serde_json::from_str(raw)?;
                    event["event"] == "media" || event["event"] == "mark"
                } else { false };
                if drop { discarded += 1; } else { retained.push(entry.clone()); }
            }
            self.entries = retained;
        }
        self.entries.push(Entry { priority:if clear {-1} else {0}, sequence:self.sequence, item:serde_json::json!(["text",raw]) });
        self.sequence += 1;
        Ok(discarded)
    }
    /// Remove the lowest `(priority, sequence)` entry, or return None for an empty queue.
    ///
    /// Selection scans pending entries; removal does not dispatch the payload.
    pub fn pop(&mut self) -> Option<Entry> {
        let (index, _) = self.entries.iter().enumerate().min_by_key(|(_,e)| (e.priority,e.sequence))?;
        Some(self.entries.remove(index))
    }
    /// Clone all pending entries in consumption order without modifying the queue.
    pub fn ordered(&self) -> Vec<Entry> {
        let mut entries = self.entries.clone(); entries.sort_by_key(|e| (e.priority,e.sequence)); entries
    }
}
