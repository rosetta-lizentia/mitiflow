//! Rendezvous (Highest Random Weight) hashing for partition assignment.
//!
//! Uses HRW hashing for deterministic, minimal-disruption partition-to-worker
//! assignment. When workers join/leave, only partitions previously assigned to
//! them are redistributed.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

/// Compute a deterministic partition number for a key.
///
/// Uses a simple hash modulo to map arbitrary key strings to a partition index
/// in `[0, num_partitions)`.
pub fn partition_for(key: &str, num_partitions: u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % num_partitions as u64) as u32
}

/// Assign a partition to a worker using Rendezvous (HRW) hashing.
///
/// The worker with the highest combined hash of `(worker_id, partition)` wins
/// ownership of the partition. Returns `None` if workers is empty.
pub fn worker_for(partition: u32, workers: &[String]) -> Option<&str> {
    workers
        .iter()
        .max_by_key(|w| hrw_score(w, partition))
        .map(|w| w.as_str())
}

/// Compute the full partition→worker assignment map.
///
/// Returns a map from `worker_id` → `Vec<partition_index>`. Each partition is
/// assigned to exactly one worker via HRW hashing.
pub fn assignments(workers: &[String], num_partitions: u32) -> HashMap<String, Vec<u32>> {
    let mut map: HashMap<String, Vec<u32>> =
        workers.iter().map(|w| (w.clone(), Vec::new())).collect();

    for p in 0..num_partitions {
        if let Some(w) = worker_for(p, workers) {
            map.get_mut(w).unwrap().push(p);
        }
    }

    // Sort each worker's partition list for deterministic output.
    for parts in map.values_mut() {
        parts.sort_unstable();
    }
    map
}

/// Compute HRW score for a (worker, partition) pair.
fn hrw_score(worker: &str, partition: u32) -> u64 {
    let mut hasher = DefaultHasher::new();
    worker.hash(&mut hasher);
    partition.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_for_deterministic() {
        let p1 = partition_for("order/123", 64);
        let p2 = partition_for("order/123", 64);
        assert_eq!(p1, p2);
        assert!(p1 < 64);
    }

    #[test]
    fn partition_for_distributes() {
        let num = 64;
        let mut counts = vec![0u32; num as usize];
        for i in 0..10_000 {
            let key = format!("key/{i}");
            counts[partition_for(&key, num) as usize] += 1;
        }
        // Every partition should have gotten at least some keys.
        assert!(counts.iter().all(|&c| c > 0));
    }

    #[test]
    fn worker_for_empty() {
        assert_eq!(worker_for(0, &[]), None);
    }

    #[test]
    fn worker_for_single() {
        let workers = vec!["w1".to_string()];
        assert_eq!(worker_for(0, &workers), Some("w1"));
        assert_eq!(worker_for(42, &workers), Some("w1"));
    }

    #[test]
    fn worker_for_deterministic() {
        let workers = vec!["a".into(), "b".into(), "c".into()];
        let w1 = worker_for(7, &workers);
        let w2 = worker_for(7, &workers);
        assert_eq!(w1, w2);
    }

    #[test]
    fn assignments_covers_all_partitions() {
        let workers = vec!["w1".into(), "w2".into(), "w3".into()];
        let map = assignments(&workers, 64);

        let total: usize = map.values().map(|v| v.len()).sum();
        assert_eq!(total, 64);

        // Every partition appears exactly once.
        let mut all: Vec<u32> = map.values().flat_map(|v| v.iter().copied()).collect();
        all.sort_unstable();
        assert_eq!(all, (0..64).collect::<Vec<u32>>());
    }

    #[test]
    fn minimal_disruption_on_join() {
        let workers_before = vec!["w1".into(), "w2".into()];
        let workers_after = vec!["w1".into(), "w2".into(), "w3".into()];

        let before = assignments(&workers_before, 64);
        let after = assignments(&workers_after, 64);

        // Count how many partitions moved.
        let mut moved = 0;
        for p in 0..64u32 {
            let owner_before = before.iter().find(|(_, ps)| ps.contains(&p)).unwrap().0;
            let owner_after = after.iter().find(|(_, ps)| ps.contains(&p)).unwrap().0;
            if owner_before != owner_after {
                moved += 1;
            }
        }

        // HRW guarantees minimal disruption: only ~1/N partitions should move
        // when adding 1 worker. With 3 workers, roughly 64/3 ≈ 21 should move.
        // Allow some slack.
        assert!(moved <= 30, "too many partitions moved on join: {moved}");
        assert!(
            moved >= 10,
            "suspiciously few partitions moved on join: {moved}"
        );
    }

    #[test]
    fn minimal_disruption_on_leave() {
        let workers_before = vec!["w1".into(), "w2".into(), "w3".into()];
        let workers_after = vec!["w1".into(), "w3".into()];

        let before = assignments(&workers_before, 64);
        let after = assignments(&workers_after, 64);

        // Only partitions that belonged to w2 should move.
        let w2_partitions = &before["w2"];
        for p in 0..64u32 {
            let owner_before = before.iter().find(|(_, ps)| ps.contains(&p)).unwrap().0;
            let owner_after = after.iter().find(|(_, ps)| ps.contains(&p)).unwrap().0;
            if owner_before != owner_after {
                assert!(
                    w2_partitions.contains(&p),
                    "partition {p} moved but was not owned by departed w2"
                );
            }
        }
    }
}
