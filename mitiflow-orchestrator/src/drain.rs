//! Drain operations — move all partitions off a node for maintenance.

use std::collections::HashMap;

use mitiflow::partition::hash_ring::{NodeDescriptor, assign_replicas_rack_aware};
use mitiflow_storage::OverrideEntry;

use crate::cluster_view::ClusterView;
use crate::override_manager::OverrideManager;

/// Compute override entries that move all partitions away from `node_id`.
///
/// For each partition currently assigned to `node_id`, picks the best
/// alternative via rack-aware HRW (excluding `node_id`).
pub fn compute_drain_overrides(
    node_id: &str,
    assignments: &HashMap<(u32, u32), crate::cluster_view::AssignmentInfo>,
    all_nodes: &[NodeDescriptor],
    replication_factor: u32,
) -> Vec<OverrideEntry> {
    // Find partitions owned by node_id
    let owned: Vec<(u32, u32)> = assignments
        .iter()
        .filter(|(_, info)| info.node_id == node_id)
        .map(|(&key, _)| key)
        .collect();

    if owned.is_empty() {
        return Vec::new();
    }

    // Build alternative node list excluding the draining node
    let alternatives: Vec<NodeDescriptor> = all_nodes
        .iter()
        .filter(|n| n.id != node_id)
        .cloned()
        .collect();

    if alternatives.is_empty() {
        return Vec::new();
    }

    owned
        .into_iter()
        .map(|(partition, replica)| {
            let chain = assign_replicas_rack_aware(partition, replication_factor, &alternatives);
            let target = chain
                .into_iter()
                .next()
                .unwrap_or_else(|| alternatives[0].id.clone());

            OverrideEntry {
                partition,
                replica,
                node_id: target,
                reason: format!("drain {node_id}"),
            }
        })
        .collect()
}

/// Execute a drain operation: compute overrides and publish them.
///
/// Returns the list of override entries that were published.
pub async fn drain_node(
    node_id: &str,
    cluster_view: &ClusterView,
    override_manager: &OverrideManager,
    replication_factor: u32,
) -> Result<Vec<OverrideEntry>, Box<dyn std::error::Error + Send + Sync>> {
    let assignments = cluster_view.assignments().await;
    let nodes = cluster_view.nodes().await;

    let descriptors: Vec<NodeDescriptor> = nodes
        .iter()
        .filter(|(_, info)| info.online)
        .map(|(id, info)| {
            let capacity = info.metadata.as_ref().map(|m| m.capacity).unwrap_or(100);
            let labels = info
                .metadata
                .as_ref()
                .map(|m| m.labels.clone())
                .unwrap_or_default();
            NodeDescriptor {
                id: id.clone(),
                capacity,
                labels,
            }
        })
        .collect();

    let overrides =
        compute_drain_overrides(node_id, &assignments, &descriptors, replication_factor);

    if !overrides.is_empty() {
        override_manager
            .add_entries(overrides.clone(), None)
            .await?;
    }

    Ok(overrides)
}

/// Execute an undrain operation: remove all overrides targeting a drained node.
///
/// This removes override entries whose `reason` matches `"drain {node_id}"`.
pub async fn undrain_node(
    node_id: &str,
    override_manager: &OverrideManager,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let current = override_manager.current().await;
    let remaining: Vec<OverrideEntry> = current
        .entries
        .into_iter()
        .filter(|e| e.reason != format!("drain {node_id}"))
        .collect();
    override_manager.publish_entries(remaining, None).await
}
