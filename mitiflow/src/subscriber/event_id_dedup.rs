//! Event-ID based deduplication for key-filtered subscribers.
//!
//! Key-filtered subscribers see a sparse subset of a publisher's sequence
//! space, so sequence-based gap detection produces false positives. Instead,
//! we deduplicate by `EventId` (UUID v7) using a bounded LRU cache.

use std::collections::{HashMap, VecDeque};

use crate::types::EventId;

/// Bounded LRU deduplication set keyed by [`EventId`].
///
/// When the capacity is reached, the oldest entry is evicted. Since `EventId`
/// is UUID v7 (time-ordered), natural eviction of old entries is correct — a
/// re-delivered event whose ID was evicted is old enough that re-delivery is
/// safe (or was already processed by the application).
pub struct EventIdDedup {
    /// Order of insertion for LRU eviction (front = oldest).
    order: VecDeque<EventId>,
    /// Fast membership lookup.
    seen: HashMap<EventId, ()>,
    /// Maximum number of entries before eviction.
    capacity: usize,
}

impl EventIdDedup {
    /// Create a new dedup set with the given capacity.
    ///
    /// # Panics
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "EventIdDedup capacity must be > 0");
        Self {
            order: VecDeque::with_capacity(capacity),
            seen: HashMap::with_capacity(capacity),
            capacity,
        }
    }

    /// Returns `true` if this event_id has not been seen before (new event).
    /// Returns `false` if it is a duplicate.
    ///
    /// New IDs are inserted into the set; if at capacity, the oldest entry
    /// is evicted first.
    pub fn is_new(&mut self, id: EventId) -> bool {
        if self.seen.contains_key(&id) {
            return false;
        }

        // Evict oldest if at capacity.
        if self.order.len() >= self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.seen.remove(&oldest);
        }

        self.order.push_back(id);
        self.seen.insert(id, ());
        true
    }

    /// Number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_event_returns_true() {
        let mut dedup = EventIdDedup::new(10);
        let id = EventId::new();
        assert!(dedup.is_new(id));
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn duplicate_returns_false() {
        let mut dedup = EventIdDedup::new(10);
        let id = EventId::new();
        assert!(dedup.is_new(id));
        assert!(!dedup.is_new(id));
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn evicts_oldest_when_at_capacity() {
        let mut dedup = EventIdDedup::new(3);
        let id1 = EventId::new();
        let id2 = EventId::new();
        let id3 = EventId::new();
        let id4 = EventId::new();

        assert!(dedup.is_new(id1));
        assert!(dedup.is_new(id2));
        assert!(dedup.is_new(id3));
        assert_eq!(dedup.len(), 3);

        // Insert a 4th — should evict id1.
        assert!(dedup.is_new(id4));
        assert_eq!(dedup.len(), 3);

        // id1 was evicted, so it's "new" again.
        assert!(dedup.is_new(id1));
        // id3 should still be present (id2 was evicted by id1's re-insertion).
        assert!(!dedup.is_new(id3));
    }

    #[test]
    fn multiple_distinct_ids_all_new() {
        let mut dedup = EventIdDedup::new(100);
        for _ in 0..50 {
            assert!(dedup.is_new(EventId::new()));
        }
        assert_eq!(dedup.len(), 50);
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn zero_capacity_panics() {
        EventIdDedup::new(0);
    }
}
