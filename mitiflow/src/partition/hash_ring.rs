//! Rendezvous (Highest Random Weight) hashing for partition assignment.
//!
//! Uses HRW hashing for deterministic, minimal-disruption partition-to-worker
//! assignment. When workers join/leave, only partitions previously assigned to
//! them are redistributed.
//!
//! Also provides weighted rendezvous hashing and multi-replica assignment
//! for heterogeneous clusters and replication.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Describes a node in the cluster for weighted/replica-aware assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDescriptor {
    /// Unique node identifier.
    pub id: String,
    /// Capacity weight for weighted HRW (default: 100). Higher → more partitions.
    pub capacity: u32,
    /// Labels for rack-aware placement (e.g., `{"rack": "us-east-1a"}`).
    pub labels: HashMap<String, String>,
}

impl NodeDescriptor {
    /// Create a node descriptor with default capacity and no labels.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            capacity: 100,
            labels: HashMap::new(),
        }
    }

    /// Create a node descriptor with a specific capacity.
    pub fn with_capacity(id: impl Into<String>, capacity: u32) -> Self {
        Self {
            id: id.into(),
            capacity,
            labels: HashMap::new(),
        }
    }

    /// Set labels for rack-aware placement.
    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }
}

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

/// Weighted rendezvous hash score (Schindelhauer & Schomaker, 2005).
///
/// Score = -weight / ln(hash / H_max). Higher weight produces higher scores
/// on average, shifting more partitions to that node. When all weights are
/// equal, this reduces to standard rendezvous hashing ordering.
pub fn weighted_hrw_score(node_id: &str, partition: u32, replica: u32, weight: u32) -> f64 {
    let mut hasher = DefaultHasher::new();
    (node_id, partition, replica).hash(&mut hasher);
    let hash_val = hasher.finish();

    // Normalize to (0, 1] — avoid ln(0)
    let normalized = (hash_val as f64) / (u64::MAX as f64);
    let normalized = normalized.max(f64::MIN_POSITIVE);

    // Weighted score: higher weight → higher score in expectation
    -(weight as f64) / normalized.ln()
}

/// Assign replicas for a single partition using weighted rendezvous hashing.
///
/// Returns up to `replication_factor` distinct node IDs, ordered by descending
/// weighted HRW score. The first element is the primary replica.
pub fn assign_replicas(
    partition: u32,
    replication_factor: u32,
    nodes: &[NodeDescriptor],
) -> Vec<String> {
    let mut scored: Vec<(f64, &NodeDescriptor)> = nodes
        .iter()
        .map(|n| (weighted_hrw_score(&n.id, partition, 0, n.capacity), n))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

    scored
        .into_iter()
        .take(replication_factor as usize)
        .map(|(_, n)| n.id.clone())
        .collect()
}

/// Assign replicas with best-effort rack diversity.
///
/// First pass: select nodes preferring distinct rack labels.
/// Second pass: fill remaining slots ignoring rack constraint.
/// Deterministic — all agents compute the same result from the same inputs.
pub fn assign_replicas_rack_aware(
    partition: u32,
    replication_factor: u32,
    nodes: &[NodeDescriptor],
) -> Vec<String> {
    let mut scored: Vec<(f64, &NodeDescriptor)> = nodes
        .iter()
        .map(|n| (weighted_hrw_score(&n.id, partition, 0, n.capacity), n))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

    let rf = replication_factor as usize;
    let mut selected = Vec::with_capacity(rf);
    let mut used_racks = HashSet::new();

    // First pass: prefer rack diversity
    for (_, node) in &scored {
        if selected.len() >= rf {
            break;
        }
        let rack = node.labels.get("rack").cloned().unwrap_or_default();
        if !used_racks.contains(&rack) {
            selected.push(node.id.clone());
            used_racks.insert(rack);
        }
    }

    // Second pass: fill remaining slots ignoring rack constraint
    if selected.len() < rf {
        for (_, node) in &scored {
            if selected.len() >= rf {
                break;
            }
            if !selected.contains(&node.id) {
                selected.push(node.id.clone());
            }
        }
    }

    selected
}

/// Compute the full replicated assignment table.
///
/// Returns `partition → Vec<node_id>` where each Vec has up to `replication_factor`
/// entries (first = primary). Uses weighted HRW without rack awareness.
pub fn assignments_replicated(
    nodes: &[NodeDescriptor],
    num_partitions: u32,
    replication_factor: u32,
) -> HashMap<u32, Vec<String>> {
    (0..num_partitions)
        .map(|p| (p, assign_replicas(p, replication_factor, nodes)))
        .collect()
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

    // --- Weighted HRW tests ---

    #[test]
    fn weighted_hrw_score_positive() {
        let score = weighted_hrw_score("node-a", 0, 0, 100);
        assert!(score > 0.0, "weighted HRW score should be positive");
        assert!(score.is_finite(), "weighted HRW score should be finite");
    }

    #[test]
    fn weighted_hrw_score_deterministic() {
        let s1 = weighted_hrw_score("node-a", 5, 0, 100);
        let s2 = weighted_hrw_score("node-a", 5, 0, 100);
        assert_eq!(s1, s2);
    }

    #[test]
    fn weighted_hrw_distributes_proportionally() {
        // Node with weight 200 should get ~2x the partitions of nodes with weight 100.
        let nodes = vec![
            NodeDescriptor::with_capacity("heavy", 200),
            NodeDescriptor::with_capacity("light-a", 100),
            NodeDescriptor::with_capacity("light-b", 100),
        ];

        let table = assignments_replicated(&nodes, 1000, 1);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for replicas in table.values() {
            *counts.entry(replicas[0].clone()).or_default() += 1;
        }

        let heavy = *counts.get("heavy").unwrap_or(&0);
        let light_a = *counts.get("light-a").unwrap_or(&0);
        let light_b = *counts.get("light-b").unwrap_or(&0);

        // heavy should get roughly 500 (50%), lights ~250 each (25%).
        // Allow +/-15% slack for hash variance.
        assert!(
            heavy > 350 && heavy < 650,
            "heavy node got {heavy}/1000, expected ~500"
        );
        assert!(
            light_a > 100 && light_a < 400,
            "light-a got {light_a}/1000, expected ~250"
        );
        assert!(
            light_b > 100 && light_b < 400,
            "light-b got {light_b}/1000, expected ~250"
        );
        assert_eq!(heavy + light_a + light_b, 1000);
    }

    #[test]
    fn weighted_hrw_equal_weights_consistent_ordering() {
        // With equal weights, weighted HRW should produce a valid assignment
        // for all partitions (each partition assigned to some node).
        let nodes = vec![
            NodeDescriptor::new("a"),
            NodeDescriptor::new("b"),
            NodeDescriptor::new("c"),
        ];
        let table = assignments_replicated(&nodes, 64, 1);
        assert_eq!(table.len(), 64);
        for (_, replicas) in &table {
            assert_eq!(replicas.len(), 1);
            assert!(["a", "b", "c"].contains(&replicas[0].as_str()));
        }
    }

    #[test]
    fn assign_replicas_returns_n_distinct_nodes() {
        let nodes = vec![
            NodeDescriptor::new("a"),
            NodeDescriptor::new("b"),
            NodeDescriptor::new("c"),
            NodeDescriptor::new("d"),
        ];
        for p in 0..20 {
            let replicas = assign_replicas(p, 3, &nodes);
            assert_eq!(replicas.len(), 3, "partition {p} should get 3 replicas");
            let unique: HashSet<&String> = replicas.iter().collect();
            assert_eq!(unique.len(), 3, "partition {p} replicas should be distinct");
        }
    }

    #[test]
    fn assign_replicas_rf1_consistent() {
        // RF=1 should always produce exactly one replica per partition.
        let nodes = vec![
            NodeDescriptor::new("w1"),
            NodeDescriptor::new("w2"),
            NodeDescriptor::new("w3"),
        ];
        let table = assignments_replicated(&nodes, 64, 1);
        for (_, replicas) in &table {
            assert_eq!(replicas.len(), 1);
        }
        // All partitions covered
        assert_eq!(table.len(), 64);
    }

    #[test]
    fn assign_replicas_rf_exceeds_nodes() {
        // RF > num_nodes should return all nodes (no panic, no duplicates).
        let nodes = vec![
            NodeDescriptor::new("a"),
            NodeDescriptor::new("b"),
        ];
        let replicas = assign_replicas(0, 5, &nodes);
        assert_eq!(replicas.len(), 2, "can't have more replicas than nodes");
    }

    #[test]
    fn assign_replicas_minimal_disruption() {
        let nodes_before = vec![
            NodeDescriptor::new("a"),
            NodeDescriptor::new("b"),
            NodeDescriptor::new("c"),
        ];
        let nodes_after = vec![
            NodeDescriptor::new("a"),
            NodeDescriptor::new("b"),
            NodeDescriptor::new("c"),
            NodeDescriptor::new("d"),
        ];

        let num_partitions = 64;
        let before = assignments_replicated(&nodes_before, num_partitions, 1);
        let after = assignments_replicated(&nodes_after, num_partitions, 1);

        let mut moved = 0;
        for p in 0..num_partitions {
            if before[&p][0] != after[&p][0] {
                moved += 1;
            }
        }

        // Adding 1 node to 3 should move ~1/4 = ~16 partitions. Allow slack.
        assert!(
            moved <= 25,
            "too many primary replicas moved: {moved}/64"
        );
        assert!(
            moved >= 5,
            "suspiciously few primary replicas moved: {moved}/64"
        );
    }

    #[test]
    fn rack_aware_separates_replicas() {
        let nodes = vec![
            NodeDescriptor::new("a").with_labels([("rack".into(), "rack-1".into())].into()),
            NodeDescriptor::new("b").with_labels([("rack".into(), "rack-1".into())].into()),
            NodeDescriptor::new("c").with_labels([("rack".into(), "rack-2".into())].into()),
            NodeDescriptor::new("d").with_labels([("rack".into(), "rack-2".into())].into()),
        ];

        let mut cross_rack = 0;
        let total = 100;
        for p in 0..total {
            let replicas = assign_replicas_rack_aware(p, 2, &nodes);
            assert_eq!(replicas.len(), 2);
            let rack_0 = nodes.iter().find(|n| n.id == replicas[0]).unwrap()
                .labels.get("rack").unwrap();
            let rack_1 = nodes.iter().find(|n| n.id == replicas[1]).unwrap()
                .labels.get("rack").unwrap();
            if rack_0 != rack_1 {
                cross_rack += 1;
            }
        }

        // With 2 racks and RF=2, nearly all partitions should have cross-rack placement.
        assert!(
            cross_rack >= 95,
            "only {cross_rack}/100 partitions had cross-rack replicas, expected ≥95"
        );
    }

    #[test]
    fn rack_aware_fallback_when_insufficient_racks() {
        // All nodes in same rack — should still assign RF=2 replicas.
        let nodes = vec![
            NodeDescriptor::new("a").with_labels([("rack".into(), "rack-1".into())].into()),
            NodeDescriptor::new("b").with_labels([("rack".into(), "rack-1".into())].into()),
            NodeDescriptor::new("c").with_labels([("rack".into(), "rack-1".into())].into()),
        ];

        for p in 0..20 {
            let replicas = assign_replicas_rack_aware(p, 2, &nodes);
            assert_eq!(replicas.len(), 2, "partition {p} should get 2 replicas even with 1 rack");
            assert_ne!(replicas[0], replicas[1], "replicas should be distinct nodes");
        }
    }

    #[test]
    fn assignments_replicated_full_coverage() {
        let nodes = vec![
            NodeDescriptor::new("a"),
            NodeDescriptor::new("b"),
            NodeDescriptor::new("c"),
        ];

        let table = assignments_replicated(&nodes, 12, 2);
        assert_eq!(table.len(), 12);
        for (p, replicas) in &table {
            assert_eq!(replicas.len(), 2, "partition {p} should have 2 replicas");
            assert_ne!(replicas[0], replicas[1], "partition {p} replicas must be distinct");
        }
    }
}
